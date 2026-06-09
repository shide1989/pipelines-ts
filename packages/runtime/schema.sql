-- pipelines schema (V0.6)
-- Authoritative state (runs/steps/timers) + a derived append-only log + a wakeup
-- trigger. Raw SQL — no migration framework. Idempotent: safe to run via setup()
-- repeatedly, and applied once by docker-compose initdb on a fresh volume.

-- Status enums: stable, domain-fundamental lifecycle states. Guarded so re-runs
-- don't error (CREATE TYPE has no IF NOT EXISTS).
DO $$ BEGIN CREATE TYPE run_status   AS ENUM ('pending','running','suspended','completed','failed'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE step_status  AS ENUM ('running','completed','failed'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE timer_status AS ENUM ('waiting','fired'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS workflow_runs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_name TEXT NOT NULL,
  input         JSONB NOT NULL,
  output        JSONB,
  status        run_status NOT NULL DEFAULT 'pending',  -- .run() inserts 'pending'; the worker drives the rest
  error         TEXT,
  idempotency_key TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- no run-level retry_count: retries are per-step (workflow_steps.attempts)
  UNIQUE (workflow_name, idempotency_key)
);

CREATE TABLE IF NOT EXISTS workflow_steps (
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id    UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  step_id   TEXT NOT NULL,
  output    JSONB,
  error     TEXT,
  status    step_status NOT NULL,    -- written explicitly: 'running' (intent) → 'completed' | 'failed'
  attempts  INT NOT NULL DEFAULT 0,  -- execution attempts; incremented per try (retries are per-step, in one claim)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, step_id)
);

CREATE TABLE IF NOT EXISTS workflow_timers (
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id    UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  sleep_id  TEXT NOT NULL,
  wake_at   TIMESTAMPTZ NOT NULL,
  status    timer_status NOT NULL DEFAULT 'waiting',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, sleep_id)
);

-- Derived, append-only observability/audit stream. NOT the source of truth.
-- BIGINT identity gives monotonic ordering (sets up SSE Last-Event-ID resume later).
-- event_type is TEXT on purpose: log types churn, unlike the stable status enums.
CREATE TABLE IF NOT EXISTS workflow_logs (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  payload    JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_timers_poll  ON workflow_timers (wake_at) WHERE status = 'waiting';
CREATE INDEX IF NOT EXISTS idx_runs_pending ON workflow_runs (status)   WHERE status IN ('pending','suspended');
CREATE INDEX IF NOT EXISTS idx_logs_run     ON workflow_logs (run_id, id);

-- Keep updated_at honest — don't rely on the app to set it.
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_runs_touch ON workflow_runs;
CREATE TRIGGER workflow_runs_touch  BEFORE UPDATE ON workflow_runs
  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
DROP TRIGGER IF EXISTS workflow_steps_touch ON workflow_steps;
CREATE TRIGGER workflow_steps_touch BEFORE UPDATE ON workflow_steps
  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- Wakeup trigger: instant worker pickup for newly submitted runs. Payload is the
-- id only (keeps NOTIFY under the ~8KB cap); the worker fetches the row. The
-- trigger fires inside the INSERT transaction, so NOTIFY is delivered on COMMIT —
-- the row is guaranteed visible when the listener claims it.
CREATE OR REPLACE FUNCTION notify_workflow_run()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('workflow_runs', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_run_notify ON workflow_runs;
CREATE TRIGGER workflow_run_notify
  AFTER INSERT ON workflow_runs
  FOR EACH ROW EXECUTE FUNCTION notify_workflow_run();
