# pipelines — Durable Workflow Engine for TypeScript

## Vision

A minimal, open-source durable workflow engine for TypeScript/Bun. Workflows are plain async functions — durable checkpointing, replay, and long-lived timers are handled transparently via **Proxy** and **AsyncLocalStorage**. No compiler, no directives, no magic strings.

This is a **portfolio project**, not a product. The goals are:

1. Build a clean, **zero-magic API** using `Proxy` + `AsyncLocalStorage` — two underused TypeScript/Node features that eliminate boilerplate without a compiler
2. Build a **platform-agnostic runtime** that executes, checkpoints, and replays these workflows — the durable-execution core is plain TypeScript; Bun is the host, chosen for its batteries-included APIs (zero dependencies), not for deep coupling
3. Use **PostgreSQL** as the single durable store (no Redis, no external queue) — leaning on its real strengths: `SKIP LOCKED`, `LISTEN`/`NOTIFY`, JSONB, partial indexes
4. Keep it small, opinionated, and well-documented — quality over feature count

The runtime is a **library**, not a server or framework. It exposes primitives and a programmatic management API; *how* you drive it (HTTP, CLI, queue consumer, cron, direct call) is an application concern. The example ships an HTTP adapter to demonstrate this, but it lives in `examples/`, not in the runtime.

### Naming model

The library is `pipelines`; the primitive is `workflow()`. This split is deliberate:

- A **workflow** is a single durable, suspendable, resumable unit of execution (it can `sleep` for days and resume). This is the atomic primitive — `workflow(name, fn)`.
- A **pipeline** is the *composition* of workflows chained together. This is a future feature; the `pipeline()` keyword is intentionally reserved for it, not used for the atomic unit.

So the library name reflects the end goal (build pipelines out of workflows), while today's surface area is just `workflow()`, `durable()`, and `sleep()`. Do not rename `workflow()` to `pipeline()` — the distinction is intentional.

### Execution model in one breath

`workflow().run(input)` does **not** execute inline. It inserts a `pending` row and fires a `pg_notify`, then returns immediately. A **worker** wakes (instantly via `LISTEN`, or via an adaptive timer poll for sleeps), claims the run with `SKIP LOCKED`, and executes it inside `AsyncLocalStorage`. State lives in authoritative tables (`workflow_runs`, `workflow_steps`); an append-only `workflow_logs` table is a derived observability/audit stream. This is a **hybrid** model — authoritative state plus an event log — not pure event sourcing.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   Developer Code                      │
│                                                       │
│  const steps = durable({                              │
│    prepareContext: async (task) => { ... },           │
│    callLLM:        async (ctx)  => { ... },           │
│  });                                                  │
│                                                       │
│  export const processTask = workflow("processTask",   │
│    async (task) => {                                  │
│      const ctx    = await steps.prepareContext(task); │
│      const result = await steps.callLLM(ctx);         │
│      await sleep("30 seconds");                       │
│      return result;                                   │
│    }                                                  │
│  );                                                   │
└──────────────┬───────────────────────────────────────┘
               │
               │  No compile step. Proxy + AsyncLocalStorage
               │  handle checkpointing at runtime.
               │
               ▼
┌──────────────────────────────────────────────────────┐
│   Runtime Engine (library)                            │
│   platform-agnostic core; Bun host (see Tech Stack)   │
│                                                       │
│  workflow(name, fn)                                   │
│    → registers fn in an in-process registry (by name) │
│    → .run(input): INSERT pending run + return runId   │
│       (does NOT execute inline)                       │
│                                                       │
│  Worker  (startWorker(); the app runs it)             │
│    wakeup source A — LISTEN 'workflow_runs'           │
│       → new pending run → claim (SKIP LOCKED)         │
│    wakeup source B — adaptive timer poll              │
│       → sleep due (can't push on time) → claim resume │
│    on startup → scan pending + due-suspended (recover │
│       missed NOTIFYs; the run rows ARE the durable    │
│       queue)                                          │
│    execute → run fn in AsyncLocalStorage:             │
│       durable() Proxy → cache/replay steps            │
│       sleep() → insert timer + suspend (zero compute) │
│       append lifecycle rows to workflow_logs          │
│                                                       │
│  Management API (programmatic, in-process)            │
│    → run(), getRun(), listRuns(), replayRun()         │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│            PostgreSQL                                 │
│  authoritative state:                                 │
│   - workflow_runs   (pending/running/suspended/…)     │
│   - workflow_steps  (cached step results)             │
│   - workflow_timers (durable sleep records)           │
│  derived stream:                                      │
│   - workflow_logs   (append-only; audit/observability)│
│  triggers:                                            │
│   - AFTER INSERT workflow_runs → pg_notify (wake)     │
└──────────────────────────────────────────────────────┘

  Drivers (application concern — NOT in the runtime):
  examples/agentic/server.ts — thin Bun.serve adapter over the
    Management API; calls startWorker(). Could be a CLI/cron instead.
```

---

## Component 1: Proxy + AsyncLocalStorage API Layer

### Purpose

Provide a clean developer API where workflow code looks like vanilla async TypeScript — no `ctx` parameter threading, no manual step ID strings, no wrapper closures. Two native features do all the heavy lifting.

### 1.1 `durable(steps)` — Proxy-based step wrapper

```typescript
function durable<T extends Record<string, (...args: any[]) => Promise<any>>>(
  steps: T
): T;
```

`durable()` takes an object of async functions and returns a **Proxy** that looks and types identically. When a method is called on the proxy:

1. The Proxy **`get` trap** captures the **method name** (e.g. `"callLLM"`) — the step ID base
2. It reads the current **WorkflowContext** from **AsyncLocalStorage** — if there's no active context, it calls the original function directly (so steps work outside workflows too, useful for testing)
3. It appends a **call counter** for a deterministic step ID: first call to `callLLM` → `"callLLM:0"`, second → `"callLLM:1"`. Counter tracked per-method-name on the context
4. It checks the in-memory cached-steps map (pre-loaded on replay) for `(run_id, step_id)`:
   - **Cache hit (replay)**: return the cached result, don't execute the function
   - **Cache miss (first execution)**: insert an intent row (`status='running'`), execute (retrying per the policy, incrementing `attempts`), run the serializability guard on the result, update the row to `completed` with the output, append a `step.completed` log row, return. On terminal failure the row goes to `failed`.

#### Serializability guard

Step results are persisted to Postgres as JSONB, round-tripping through `JSON.stringify` / `JSON.parse`. Some values don't survive that and would silently corrupt replay (a `Date` comes back a `string` after a sleep, in code that worked on first execution). The cheap guard fails fast at write time:

```typescript
// errors.ts
export function assertSerializable(value: unknown, stepId: string): void {
  try {
    JSON.stringify(value);
  } catch (err) {
    throw new FatalError(
      `Step "${stepId}" returned a non-serializable value (circular reference or BigInt): ${(err as Error).message}`
    );
  }
}
```

Called in the cache-miss branch **before** the `INSERT`. Catches the *throwing* cases (circular refs, `BigInt`). It does not catch silent drift (`Date` → string, `Map`/`Set` → `{}`); that's a documented constraint, kept out of the hot path. Throwing `FatalError` marks the run `failed` with no retries (a non-serializable return is a code bug, not a transient fault).

#### Type safety

The Proxy preserves the full generic type of the input object. `steps.callLLM(ctx)` returns the original function's `Promise<...>` with full autocomplete. No assertions needed — the return type is `T`.

#### Step ID determinism

Step IDs are `"methodName:callIndex"`, the counter sequential per method name within a run. Reordering or adding/removing steps changes IDs and breaks in-flight workflows (same constraint as Temporal — document it). Conditional branches are safe as long as the same branch is taken on replay.

#### Outside-workflow behavior

With no active `WorkflowContext`, the Proxy calls the original function directly, no checkpointing. Steps stay independently testable and reusable in non-workflow contexts. No framework coupling leaks into business logic.

### 1.2 `workflow(name, fn)` — Registration + submission

```typescript
function workflow<T, R>(
  name: string,
  fn: (input: T) => Promise<R>,
  options?: {
    retry?: Partial<RetryPolicy>;
    onFinish?: (result: LifecycleResult<R>) => void | Promise<void>;
    onError?: (info: LifecycleError) => void | Promise<void>;
  }
): {
  run: (input: T, options?: { idempotencyKey?: string }) => Promise<{ runId: string; status: "pending" }>;
};

interface LifecycleResult<R> {
  runId: string;
  workflowName: string;
  status: "completed" | "failed";
  output?: R;
  error?: string;
}

interface LifecycleError {
  runId: string;
  workflowName: string;
  error: string;
}
```

`workflow()` does two things:

1. **Registers** `fn` in an in-process registry keyed by `name`. The worker resolves implementations by name at execution time — so workflows must be imported in the worker process (they self-register on import, the same way Temporal workers register workflows).
2. Returns a handle whose **`.run()` submits** a run: it `INSERT`s a `workflow_runs` row with `status = 'pending'` and returns `{ runId, status: 'pending' }` immediately. **It does not execute the workflow inline.** The `AFTER INSERT` trigger fires `pg_notify('workflow_runs', runId)`, and a worker picks it up.

This decoupling of submission from execution is what makes the system durable: the submitter (e.g. an HTTP handler) can crash the instant after `.run()` returns and the work still happens, because the `pending` row is a durable queue entry. It also enables multiple workers and instant pickup (no poll latency for new work).

Drivers call `.run()` through the Management API (2.3); the runtime itself never opens a transport.

#### Lifecycle hooks

`onFinish` fires once on a **terminal** state (`completed` or `failed`) — not on `suspended` (a suspended run will resume and fire it later). `onError` fires only on `failed`, after retries are exhausted. Errors thrown inside these callbacks are caught and logged, never propagated — a broken metrics hook must not fail the workflow. The engine invokes them from the terminal-state transition in `engine.ts`.

### 1.3 `sleep(duration)` — Durable timers

```typescript
async function sleep(duration: string): Promise<void>;
```

No `ctx` parameter — `sleep()` reads the `WorkflowContext` from AsyncLocalStorage.

- **First execution**: insert a `workflow_timers` row with `wake_at = now() + parseDuration(duration)`, set the run `suspended`, append a `sleep.scheduled` log row, then **suspend** by throwing `SleepInterrupt` (the engine catches it — the function unwinds, no compute consumed)
- **Replay**: if the timer fired (now ≥ wake_at), skip and continue. If not, suspend again.
- Durations: `"7 days"`, `"30 seconds"`, `"1 hour"`, `"2 weeks"` — simple regex parser
- Sleep ID is `"sleep:N"` via a per-context counter, same determinism as steps

### 1.4 `WorkflowContext` (internal, not user-facing)

```typescript
interface WorkflowContext {
  runId: string;
  workflowName: string;
  db: DatabaseClient;
  stepCounters: Map<string, number>;
  sleepCounter: number;
  cachedSteps: Map<string, unknown>;   // pre-loaded on replay
}
```

Created by the engine when it claims a run, stored in AsyncLocalStorage, never exposed. `durable()` proxies and `sleep()` read it via `workflowStorage.getStore()`; if absent, they fall through to direct execution.

---

## Component 2: Runtime Engine

### Purpose

Execute workflows with durable checkpointing, replay, and timer management. This is a **library** — it exposes a programmatic API and a startable worker; it does not open a port. Drivers (HTTP, CLI) live in `examples/`.

### 2.1 Workflow executor + replay logic (`engine.ts`)

The engine executes a single claimed run:

- **Claim**: a row-locking conditional UPDATE (`FOR UPDATE` on the row, then `SET status='running' WHERE … status IN ('pending','suspended') RETURNING …`) — atomic and single-winner, so multiple workers never double-execute the same run; the loser's re-check sees `running` and no-ops (`SKIP LOCKED` is used on the timer-firing scan, not the claim itself)
- **Pre-load cache**: one query loads all completed step results for the run, so replay is in-memory, not N+1:
  ```sql
  SELECT step_id, output FROM workflow_steps WHERE run_id = $1 AND status = 'completed';
  ```
  Only `completed` rows are cache hits. A leftover `running` row means a worker died mid-step (in-doubt) — it is **not** a hit, so the Proxy re-executes it. Default policy is at-least-once (re-run); the row makes the in-doubt case *detectable* and is the hook for step-level idempotency. True exactly-once for external side effects is not claimed (impossible in general).
- **Execute**: resolve `fn` from the registry by `workflow_name`, build `WorkflowContext`, run `fn(input)` inside `workflowStorage.run(ctx, …)`. The Proxy fast-forwards through cached steps until the first uncached step or unfired timer.
- **Handle `SleepInterrupt`**: status already set `suspended` by `sleep()`; return cleanly (not terminal, no lifecycle hook)
- **Terminal transitions**: on `completed`/`failed`, update the run, append a `run.completed`/`run.failed` log row, invoke `onFinish` (and `onError` on failure). Hook errors caught and logged.

### 2.2 Worker (`worker.ts`)

`startWorker()` is the single thing the application starts to drive execution. It is **not** auto-started by the runtime.

```typescript
function startWorker(
  db: DatabaseClient,
  options?: { maxTimerSleepMs?: number; reconcileMs?: number },
): { stop: () => Promise<void> }; // stop() drains in-flight executions + releases the ownership session
```

It combines two wakeup sources feeding one execution path:

- **Source A — new runs (push):** a **dedicated `LISTEN` connection** (`max: 1`, never shared with the query pool) on channel `workflow_runs`. On `NOTIFY` with a run id, it claims and executes. This is instant — no poll latency for new work.
- **Source B — sleeps (delayed poll):** an **adaptive** loop. It computes the next wake time and sleeps until then, rather than polling on a fixed interval:
  ```sql
  SELECT min(wake_at) FROM workflow_timers WHERE status = 'waiting';
  ```
  Sleep until that time (capped at `maxTimerSleepMs`, default 60s, so a newly-inserted sooner timer is picked up promptly). On wake, mark due timers `fired` (`FOR UPDATE SKIP LOCKED`) and resume their runs. **A poll is irreducible here because nothing inserts a row when `wake_at` arrives — you cannot push on the passage of time, so there is no `NOTIFY` to hang it on.**
- **Recovery (startup + periodic reconciliation, every `reconcileMs`):** three kinds of strandable run are scanned for and re-processed:
  - `pending` — a `NOTIFY` missed while the worker was down or dropped while up (listener hiccup, notify-queue overflow). `NOTIFY` is a latency optimization; the persisted rows are the durability guarantee.
  - `suspended` with no future timer left to wait on — due, or stuck because a timer was marked `fired` but the resume never landed (poller crash between the two). `sleep()`'s replay check is time-based (`fired OR wake_at <= now()`), so re-claiming is always safe and these states self-heal.
  - `running` — orphan *candidates* (bounded batch per pass). Liveness is decided by a per-run **session-level advisory lock**: every execution is gated by `pg_try_advisory_lock(hashtextextended(run_id::text, 0))` held on the worker's single reserved "ownership" connection (`DatabaseClient.reserve()`). A live worker's lock makes the scan skip the run; a dead worker's session is gone — its locks auto-released by Postgres — so acquiring the lock on a `running` row *proves* the claimer died, and the row is reset to `pending` (logged `run.reclaimed`) and re-executed. No lease threshold, no heartbeat; design + trade-offs (re-entrancy, partitions, fencing ceiling) in `FEATURE-multi-worker-liveness.md`. In-process exclusion is a synchronous in-flight map — same-session lock attempts are re-entrant and cannot do that job.

Multiple workers can run concurrently and safely via the advisory-lock gate + single-winner claim; there is no leader election.

### 2.3 Management API (programmatic)

Plain in-process functions for triggering and inspecting runs — the surface a driver wraps. The runtime never opens a port.

```typescript
processTask.run(input, { idempotencyKey? }): Promise<{ runId, status: "pending" }>
getRun(db, runId): Promise<WorkflowRun | null>          // includes steps + logs
listRuns(db, workflowName, { limit?, offset? }): Promise<WorkflowRun[]>
replayRun(db, runId): Promise<{ runId, status }>        // force re-execution of a completed/failed run
```

The HTTP server is **not** part of the runtime. The `agentic` example ships a thin `Bun.serve` adapter (`examples/agentic/server.ts`, ~40 lines) that maps these to routes and calls `startWorker()`:

| Method | Path | Maps to |
|--------|------|---------|
| POST | `/workflows/:name/run` | `handle.run(input)` → `{ runId }` |
| GET | `/workflows/:name/runs/:runId` | `getRun(db, runId)` |
| GET | `/workflows/:name/runs` | `listRuns(db, name)` |
| POST | `/workflows/:name/runs/:runId/replay` | `replayRun(db, runId)` |

No auth in the example — note that production use needs it. Keeping this in `examples/` makes the point that transport is swappable: a CLI or queue consumer drives the same Management API with zero runtime changes.

### 2.4 Retry policy

Global defaults, overridable per-workflow:

```typescript
interface RetryPolicy {
  maxRetries: number;        // default: 3
  backoffMs: number;         // default: 1000
  backoffMultiplier: number; // default: 2 (exponential)
}
```

On step failure: retry the step up to `maxRetries` with exponential backoff, incrementing `workflow_steps.attempts` per try (retries are per-step, in-memory within a single claim — there is no run-level retry counter). If exhausted, mark the run `failed` and store the error. Distinguish **retriable** (network, timeout) from **fatal** (validation, business logic) via a `FatalError` class that skips retries:

```typescript
import { FatalError } from "pipelines";
throw new FatalError("User already exists"); // no retry
```

### 2.5 Idempotency

Optional `idempotencyKey` on `.run()`. If a run with the same key exists, return it instead of creating a new one. Enforced by a unique constraint on `(workflow_name, idempotency_key)`.

---

## Component 3: PostgreSQL Schema

Single database. Authoritative state in three tables, a derived stream in a fourth, plus a wakeup trigger. Raw SQL — no migration framework.

```sql
-- pipelines schema

-- Status enums: stable, domain-fundamental lifecycle states.
-- ADD VALUE is cheap if ever needed (e.g. 'cancelled'); we never remove these.
CREATE TYPE run_status   AS ENUM ('pending', 'running', 'suspended', 'completed', 'failed');
CREATE TYPE step_status  AS ENUM ('running', 'completed', 'failed');
CREATE TYPE timer_status AS ENUM ('waiting', 'fired');

CREATE TABLE workflow_runs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_name TEXT NOT NULL,
  input         JSONB NOT NULL,
  output        JSONB,
  status        run_status NOT NULL DEFAULT 'pending',  -- .run() inserts 'pending'; worker drives the rest
  error         TEXT,
  idempotency_key TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- no run-level retry_count: retries are per-step (see workflow_steps.attempts)

  UNIQUE (workflow_name, idempotency_key)
);

CREATE TABLE workflow_steps (
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id    UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  step_id   TEXT NOT NULL,
  output    JSONB,
  error     TEXT,
  status    step_status NOT NULL,   -- NO default: written explicitly. 'running' (intent) → 'completed' | 'failed'
  attempts  INT NOT NULL DEFAULT 0, -- execution attempts; engine increments per try (retries are per-step, in-memory within one claim)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (run_id, step_id)
);

CREATE TABLE workflow_timers (
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id    UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  sleep_id  TEXT NOT NULL,
  wake_at   TIMESTAMPTZ NOT NULL,
  status    timer_status NOT NULL DEFAULT 'waiting',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (run_id, sleep_id)
);

-- Derived, append-only observability/audit stream. NOT the source of truth
-- (that's the tables above). BIGINT identity gives monotonic ordering, which
-- also sets up SSE Last-Event-ID resume in the later streaming phase.
-- event_type is TEXT on purpose: log event types are extensible and churn
-- (tool.called, retry.attempted, …), unlike the stable status enums above.
CREATE TABLE workflow_logs (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,   -- run.started, step.running, step.completed, sleep.scheduled, run.suspended, run.resumed, run.completed, run.failed
  payload    JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_timers_poll ON workflow_timers (wake_at) WHERE status = 'waiting';
CREATE INDEX idx_runs_pending ON workflow_runs (status) WHERE status IN ('pending', 'suspended');
CREATE INDEX idx_logs_run ON workflow_logs (run_id, id);

-- Keep updated_at honest — don't rely on the app to set it.
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_runs_touch  BEFORE UPDATE ON workflow_runs
  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER workflow_steps_touch BEFORE UPDATE ON workflow_steps
  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- Wakeup trigger: instant worker pickup for newly submitted runs.
-- Payload is the id only (keeps NOTIFY under the ~8KB cap); the worker fetches the row.
-- The trigger fires inside the INSERT transaction, so NOTIFY is delivered on COMMIT,
-- which guarantees the row is visible when the listener fetches it.
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

-- NOTE: a second trigger NOTIFYing on workflow_logs INSERT feeds the SSE
-- streaming layer. Deferred to the streaming phase (see Implementation Order).
```

---

## The Hybrid Event Log (`workflow_logs`)

This is **not** pure event sourcing. The source of truth is `workflow_runs` / `workflow_steps` / `workflow_timers`; recovery re-runs the function and skips cached steps. `workflow_logs` is a **derived, append-only stream** the engine writes at lifecycle points (`run.started`, `step.completed`, `sleep.scheduled`, `run.suspended`, `run.resumed`, `run.completed`, `run.failed`).

Why hybrid rather than pure ES: reconstructing state by folding an event log would require projection logic plus snapshotting-for-performance, and it fights the replay-skip model the engine already uses. The log gives us the audit trail and (later) real-time streaming without becoming the authoritative store.

Current scope is **backend / observability**: query the log to see exactly what happened to a run, in order. Some entries map to status changes (`run.started` → running) and some are pure observability (`step.completed`); a consumer decides which it cares about.

The log's monotonic `BIGINT` id is deliberate: in the later streaming phase, an `AFTER INSERT` trigger NOTIFYs on a `workflow_logs` channel, a single dedicated `LISTEN` connection fans out in-process to many SSE subscribers, and SSE's `Last-Event-ID` plus `WHERE id > $lastSeen` gives free crash recovery (a dropped notification or client disconnect self-heals by replaying the gap from the persisted log, then resuming live). All of that is deferred — but the schema is shaped for it now so we don't migrate later.

---

## Package Structure (Monorepo)

```
pipelines/
├── packages/
│   ├── runtime/            # durable execution engine (platform-agnostic core)
│   │   ├── package.json
│   │   ├── src/
│   │   │   ├── index.ts        # Public API: workflow, durable, sleep, FatalError, startWorker, getRun, listRuns, replayRun
│   │   │   ├── proxy.ts        # durable() Proxy implementation
│   │   │   ├── workflow.ts     # workflow() registration (registry) + .run() submission
│   │   │   ├── context.ts      # WorkflowContext + AsyncLocalStorage store
│   │   │   ├── engine.ts       # claim + execute + replay + lifecycle transitions
│   │   │   ├── worker.ts       # startWorker(): LISTEN new runs + adaptive timer poll + recovery
│   │   │   ├── sleep.ts        # sleep() + SleepInterrupt + duration parser
│   │   │   ├── log.ts          # append lifecycle rows to workflow_logs (event_type + payload)
│   │   │   ├── management.ts   # getRun, listRuns, replayRun
│   │   │   ├── db.ts           # DatabaseClient interface only — the app supplies the adapter (see examples/agentic/db.ts)
│   │   │   ├── setup.ts        # applies schema.sql + triggers (idempotent CREATE OR REPLACE)
│   │   │   ├── errors.ts       # FatalError, SleepInterrupt, assertSerializable
│   │   │   └── types.ts        # Shared types
│   │   └── tests/
│   │
│   └── pipelines/          # User-facing package (re-exports from runtime)
│       ├── package.json
│       └── src/
│           └── index.ts    # Exports: workflow, durable, sleep, FatalError, startWorker, getRun, listRuns, replayRun
│
├── examples/
│   ├── agentic/            # Primary: task intake → context → batch inference → poll → validate
│   │   ├── workflow.ts     #   the processTask workflow + steps (self-registers on import)
│   │   └── server.ts       #   thin Bun.serve adapter over the Management API + startWorker() (NOT in runtime)
│   └── onboarding/         # Minimal hello-world: signup → welcome → 7-day sleep → check-in
│
├── schema.sql              # tables + indexes + wakeup trigger (copy-paste, or applied via setup())
├── docker-compose.yml      # PostgreSQL for local dev
├── biome.json
├── pnpm-workspace.yaml
└── README.md
```

Use **pnpm** for the monorepo workspace.

---

## Tech Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| API Layer | TypeScript Proxy + AsyncLocalStorage | Zero-magic DX, no compiler, fully typed |
| Runtime host | Bun + TypeScript | Batteries-included (native TS, fast startup) → zero-dependency. The durable-execution core is platform-agnostic; Bun is confined to the `db.ts` seam (`bun:sql`), behind the `DatabaseClient` interface |
| Database | PostgreSQL | Single dependency; `SKIP LOCKED`, `LISTEN`/`NOTIFY`, JSONB, partial indexes do real work here |
| DB Client | app-supplied adapter over the `DatabaseClient` seam — `postgres` (porsager) in the examples | bun:sql was dropped: it cannot receive `LISTEN`/`NOTIFY` (as of Bun 1.3.x). The adapter is ~12 lines; note the dedicated `max: 1` connection for `LISTEN` |
| HTTP (example only) | `Bun.serve` | Used in `examples/agentic/server.ts`, not in the runtime — transport is an application concern |
| Linting | Biome | Fast, opinionated |
| Monorepo | pnpm workspaces | Simple, no turborepo overhead |
| Testing | `bun:test` | Built-in, fast (dev-only, not a runtime coupling) |

---

## Implementation Order

Each phase is independently demoable.

### Phase 1: Core engine + event-driven worker

**Deliverables:**
- PostgreSQL schema + indexes + the `workflow_runs` wakeup trigger + docker-compose
- `WorkflowContext` + AsyncLocalStorage wiring (`context.ts`)
- `durable()` Proxy (`proxy.ts`) with the serializability guard
- `workflow()` registration + `.run()` submission (insert `pending` + NOTIFY) (`workflow.ts`)
- Engine: claim (`SKIP LOCKED`) + execute + replay + lifecycle transitions (`engine.ts`)
- `sleep()` + `SleepInterrupt` + duration parser (`sleep.ts`)
- `startWorker()`: LISTEN new runs + adaptive timer poll + startup recovery (`worker.ts`)
- `workflow_logs` writes at lifecycle points (`log.ts`)
- Management API: `getRun`, `listRuns`, `replayRun` (`management.ts`)
- `agentic` example working end-to-end, incl. its HTTP adapter (`examples/agentic/server.ts`) and worker startup
- Tests: replay correctness, submission/execution decoupling, NOTIFY pickup, worker recovery on restart, timer firing, idempotency, retry, Proxy behavior outside workflows

### Phase 2: Hardening

- Retry policies with exponential backoff; `FatalError`
- Concurrent-worker isolation (`SKIP LOCKED`, no double-execution)
- Crash recovery: kill submitter after `.run()` → run still executes; kill worker mid-run → restart resumes
- Reconciliation pass for dropped NOTIFYs
- Edge cases: empty workflows, only-sleep workflows, duplicate idempotency keys, non-serializable step returns

### Phase 3: Streaming (deferred — backend-first for now)

- `workflow_logs` `AFTER INSERT` NOTIFY trigger
- `subscribe(runId, listener)` + `startLogStream()` in the runtime (single dedicated `LISTEN` connection → in-process fan-out to N subscribers; never one connection per subscriber)
- SSE endpoint in the example with `Last-Event-ID` resume (replay from the persisted log, then go live)

### Phase 4: Polish

- README with architecture diagram + "how it works" (Proxy + ALS, hybrid log, event-driven worker)
- `onboarding` minimal example (durable `sleep` in isolation)
- Optional observability UI served by the example adapter

---

## Design Decisions & Constraints

Intentional scope limits:

1. **No compiler** — Proxy + AsyncLocalStorage achieve clean DX without AST transforms. Pure TypeScript, no build tooling.

2. **Hybrid, not pure event sourcing** — authoritative state tables + a derived `workflow_logs` stream. Pure ES would force projection/folding and snapshotting and fight the replay-skip model. The log is for audit/observability/streaming, never the source of truth.

3. **Event-driven for events, delayed poll for time** — new runs and (later) log events are pushed via `pg_notify` and recovered from persisted rows on restart. Sleep expiry is the one thing that must be polled, because nothing inserts a row when a timestamp passes — you can't push on time. The poll is adaptive (sleep-until-next-due, capped), not a fixed interval.

4. **Submission decoupled from execution** — `.run()` inserts a `pending` row and returns; a worker executes. Survives submitter crashes, enables multiple workers and instant pickup. The run table is the durable queue.

5. **No parallel steps** — sequential only. `Promise.all` inside a workflow is undefined behavior. (Parallelism needs a far more complex execution model.)

6. **No child workflows** — flat execution model. (Composition into pipelines is a future feature; see Naming model.)

7. **No cron/recurring** — one-shot runs only. Cron is a timer + re-trigger, addable later.

8. **JSON-only step results** — the serializability guard (1.1) catches throwing cases at write time; silent drift (`Date`, `Map`, `undefined`) is a documented constraint. Return plain JSON-safe data.

9. **No workflow versioning** — changing step order or adding/removing steps breaks in-flight workflows (same as Temporal). Documented, not solved.

10. **SleepInterrupt pattern** — `sleep()` suspends by throwing a special error the engine catches, unwinding the stack without generator-based coroutines. Tradeoff: user `finally` blocks run on suspension — document it.

11. **Single dedicated LISTEN connection** — the worker's `LISTEN` uses its own `max: 1` connection, never the query pool (a connection in LISTEN mode can't be shared). The later SSE fan-out uses one more, shared across all subscribers in-process.

---

## Prior Art & Tradeoffs

Durable execution engines split along two axes: **programming model** and **state backbone**.

| Engine | Model | State backbone | Notes |
|--------|-------|----------------|-------|
| Temporal | Imperative replay | Dedicated server + workers | Industry standard; powerful, heavy operational footprint |
| Vercel Workflow | Imperative replay | Compiler (`"use workflow"`) + hosted | The inspiration; needs a build step |
| DBOS | Imperative replay | PostgreSQL | Closest to us; uses decorators |
| Inngest / Trigger.dev | Explicit `step.run()` | Hosted, event-driven | Clean API, you don't own the runtime |
| Mastra | Declarative graph builder | PubSub event log | Built for AI agents; serializable graph enables branching/parallelism/time-travel, at the cost of a verbose builder + schema layer |
| **pipelines (this)** | **Imperative replay** | **PostgreSQL (hybrid: state + log, LISTEN/NOTIFY)** | **No compiler (Proxy + ALS), no schema layer, event-driven worker, tight scope** |

Core decision: **imperative replay over declarative graph.** The graph model buys first-class parallelism/branching/resume-from-node but stops looking like normal code and needs a runtime schema system to survive serialization. We chose imperative — like Temporal, Vercel, DBOS — because a workflow should read like a plain async function. Tradeoff accepted: replay re-runs from the top (skipping cached steps) rather than jumping to a node, and parallelism/branching aren't first-class.

Substrate decision: **PostgreSQL, used properly.** Not just a row store — `SKIP LOCKED` makes it a safe multi-worker queue, `LISTEN`/`NOTIFY` makes new-run pickup instant, and the persisted rows make `NOTIFY`'s fire-and-forget delivery safe (recover by scanning state on restart). Temporal scales further but needs a cluster; an event-log/PubSub substrate (Mastra) decouples from any single process. Postgres is the pragmatic middle: durable, multi-worker, operable by anyone who already runs a database.

---

## Example: Agentic Task Processing (end-to-end)

Primary demo scenario; works fully in Phase 1. It models a common agentic pattern: take a task, build the LLM context, kick off a long-running inference job (e.g. a provider **Batch API** — minutes to hours, typically ~50% cheaper), poll for completion with a durable sleep between polls, then validate. One example exercises **every** core feature — step caching (the expensive submit and each poll are checkpointed), durable `sleep` (zero compute between polls, survives restarts), the per-method call counter (the loop yields `checkInference:0`, `checkInference:1`, … deterministically), retry, replay, and event-driven execution.

The money-grounded hook: crash during validation, right after an expensive inference completes, and replay returns the **cached** result instead of re-submitting and re-paying.

### Steps + workflow file

```typescript
// examples/agentic/workflow.ts
import { workflow, durable, sleep, FatalError } from "pipelines";

const steps = durable({
  prepareContext: async (task: { prompt: string; docId: string }) => {
    const context = await loadDocs(task.docId);
    return { prompt: task.prompt, context };
  },

  submitInference: async (input: { prompt: string; context: string }) => {
    const { jobId } = await llm.submitBatch(input);   // long-running job handle
    return { jobId };
  },

  checkInference: async (input: { jobId: string }) => {
    return await llm.getBatchStatus(input.jobId);      // { status, output? } — may be flaky → retried
  },

  validateOutput: async (input: { output: string }) => {
    if (!input.output?.trim()) throw new FatalError("empty LLM output");  // non-retriable
    return { output: input.output };
  },
});

export const processTask = workflow(
  "processTask",
  async (task: { prompt: string; docId: string }) => {
    const ctx = await steps.prepareContext(task);
    const { jobId } = await steps.submitInference(ctx);   // cached → never re-submitted on replay

    // Durable polling loop: zero compute while sleeping, survives restarts.
    // checkInference:N + sleep:N get deterministic IDs, so on replay each completed
    // poll returns its cached status and the loop count is stable.
    let result = await steps.checkInference({ jobId });
    while (result.status !== "completed") {
      await sleep("30 seconds");
      result = await steps.checkInference({ jobId });
    }

    const validated = await steps.validateOutput({ output: result.output! });
    return { output: validated.output };
  },
);
```

### Expected demo flow

```bash
# Terminal 1: start the example adapter (imports the workflow → registers it; starts the worker)
bun run examples/agentic/server.ts

# Terminal 2: submit a run
curl -X POST http://localhost:3000/workflows/processTask/run \
  -H "Content-Type: application/json" \
  -d '{"input": {"prompt": "Summarize the attached report", "docId": "doc_42"}}'

# Returns immediately: { "runId": "abc-123", "status": "pending" }
# The INSERT fired pg_notify; the worker picks it up in milliseconds (no poll wait),
# claims it (SKIP LOCKED), runs prepareContext + submitInference + checkInference:0,
# hits the first sleep, and suspends. Status: pending → running → suspended.

# Inspect (includes steps + the workflow_logs trail)
curl http://localhost:3000/workflows/processTask/runs/abc-123

# Fast-forward the timer for the demo:
# UPDATE workflow_timers SET wake_at = now() WHERE run_id = 'abc-123' AND status = 'waiting';
# The adaptive worker wakes, resumes (cached steps skipped), polls again; once the job
# reports "completed", it validates and finishes.

# Prove durability two ways:
#  1. Kill Terminal 2 immediately after the curl returns — the run still executes
#     (submission is decoupled; the pending row is the durable queue).
#  2. Kill the worker mid-run, restart it — startup recovery + cached steps mean
#     submitInference and prior polls are NOT re-executed.
```

A minimal `examples/onboarding/` (signup → welcome → 7-day `sleep` → check-in) is included as a bare hello-world for the durable-sleep feature in isolation. The agentic flow is the primary example.

---

## Testing Strategy

### Unit (bun:test)

- **Duration parser**: `"30 seconds"` → 30000, `"7 days"` → 604800000, etc.
- **Proxy**: step ID generation (`checkInference:0/1`), type preservation, pass-through when no context active
- **Serializability guard**: circular ref / `BigInt` → `FatalError` at checkpoint, run `failed`, no retries
- **Step caching**: execute → cached → replay → skipped
- **Replay correctness**: crash after step N → replay → 0..N skipped, N+1 executed; polling loop count stable across replay
- **SleepInterrupt**: throws, engine catches, status `suspended`
- **Lifecycle hooks**: `onFinish` on completed/failed (not suspended); `onError` only on failed; throwing hook caught, run unaffected
- **FatalError**: skips retries
- **Idempotency**: same key → same run
- **ALS isolation**: concurrent runs don't leak context

### Integration

- **Decoupled submission**: `.run()` returns `pending`; worker executes; kill the submitter after return → run still completes
- **NOTIFY pickup**: submit → worker executes within ms (not after a poll interval)
- **Worker recovery**: insert `pending` runs / due timers with no worker running → start worker → all processed (recovers missed NOTIFYs)
- **Full lifecycle**: submit → steps → sleep → timer fires → resume → complete, with the `workflow_logs` trail in order
- **Crash recovery**: kill worker mid-run → restart → replay (cached steps skipped)
- **Concurrent workers**: multiple workers, `SKIP LOCKED`, no double-execution
- **Steps outside workflows**: call a `durable()` method with no `pipeline`/`workflow` context → runs normally, no checkpointing

---

## What Makes This a Strong Portfolio Piece

1. **Advanced TypeScript** — Proxy + AsyncLocalStorage doing compiler-like work, in a real system, fully typed
2. **Distributed-systems thinking** — durable execution, deterministic replay, submission/execution decoupling, crash recovery, the honest separation of push (events) vs poll (time)
3. **PostgreSQL used properly** — `SKIP LOCKED` as a multi-worker queue, `LISTEN`/`NOTIFY` for instant wakeup, JSONB, partial indexes, monotonic-id log; and the correctness details (NOTIFY-on-commit visibility, recover-from-persisted-state)
4. **API taste** — `workflow()` / `durable()` / `sleep()` is a tiny, fully-typed surface; workflow code reads like normal async TypeScript
5. **Architectural seams** — runtime is a library; transport, drivers, and the platform (`db.ts`) are isolated and swappable
6. **Deliberate scope** — the constraints section shows what was *not* built (pure ES, parallelism, a compiler) and why; judgment, not feature-count
