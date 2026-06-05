-- pipelines schema — single database, three tables, no migrations framework.

CREATE TABLE workflow_runs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_name TEXT NOT NULL,
  input         JSONB NOT NULL,
  output        JSONB,
  status        TEXT NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'suspended', 'completed', 'failed')),
  error         TEXT,
  idempotency_key TEXT,
  retry_count   INT NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (workflow_name, idempotency_key)
);

CREATE TABLE workflow_steps (
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id    UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  step_id   TEXT NOT NULL,
  output    JSONB,
  error     TEXT,
  status    TEXT NOT NULL DEFAULT 'completed'
            CHECK (status IN ('completed', 'failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (run_id, step_id)
);

CREATE TABLE workflow_timers (
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id    UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  sleep_id  TEXT NOT NULL,
  wake_at   TIMESTAMPTZ NOT NULL,
  status    TEXT NOT NULL DEFAULT 'waiting'
            CHECK (status IN ('waiting', 'fired')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (run_id, sleep_id)
);

CREATE INDEX idx_timers_poll ON workflow_timers (wake_at) WHERE status = 'waiting';
CREATE INDEX idx_runs_status ON workflow_runs (status) WHERE status IN ('running', 'suspended');
