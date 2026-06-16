# pipelines

A minimal durable workflow engine for TypeScript/Bun. Workflows are plain async functions — durable checkpointing, replay, and long-lived timers are handled transparently via `Proxy` + `AsyncLocalStorage`. No compiler, no code generation, no magic strings.

PostgreSQL is the only dependency. No Redis, no external queue.

**Status: v0.5.0** — core runtime complete (durable steps, sleep, retry, replay, multi-worker liveness). See `SPEC.md` for the full design.

---

## How it works

`workflow().run(input)` inserts a `pending` row and returns a `runId` immediately — it does not execute inline. A worker wakes via `LISTEN`/`NOTIFY`, claims the run with `SKIP LOCKED`, and executes it inside `AsyncLocalStorage`.

Step results are persisted to `workflow_steps`. On replay (crash, restart), `durable()` reads back the cached results instead of re-running the function. `sleep()` inserts a timer row and suspends the run — zero compute consumed while waiting, survives restarts at any duration.

The run rows in Postgres _are_ the durable queue. `NOTIFY` is a latency optimization, not a reliability mechanism.

---

## Quick start

```bash
docker compose up -d          # Postgres on :5432, schema auto-applied
```

```typescript
import { durable, setup, setDefaultDb, sleep, startWorker, workflow } from "pipelines";

// 1. Wire your DB driver to the runtime's interface (see "Database adapter" below)
const client = makeClient(/* your postgres driver */);

await setup(client);       // idempotent — creates tables, triggers, indexes
setDefaultDb(client);      // workflow().run() uses this by default
startWorker(client);       // LISTEN + adaptive timer poll + startup recovery

// 2. Define steps
const steps = durable({
  createUser: async (email: string) => ({ id: crypto.randomUUID(), email }),
  sendWelcome: async (email: string) => ({ sentTo: email }),
  sendCheckIn: async (email: string) => ({ sentTo: email }),
});

// 3. Define a workflow
export const onboard = workflow("onboard", async (email: string) => {
  const user = await steps.createUser(email);
  await steps.sendWelcome(email);
  await sleep("7 days");         // suspends, zero compute, survives restarts
  await steps.sendCheckIn(email);
  return { userId: user.id };
});

// 4. Submit a run (returns immediately, executes in the worker)
const { runId } = await onboard.run("alice@example.com");
```

---

## API

### `workflow(name, fn, options?)`

Registers a workflow and returns a handle.

```typescript
const myWorkflow = workflow("myWorkflow", async (input: Input) => {
  // ...
  return result;
}, {
  retry: { maxRetries: 3, backoffMs: 500, backoffMultiplier: 2 },
  onFinish: ({ runId, status, output }) => { /* metrics, cleanup */ },
  onError:  ({ runId, error })          => { /* alerting */ },
});

const { runId } = await myWorkflow.run(input);
const { runId } = await myWorkflow.run(input, { idempotencyKey: "unique-key" });
```

`onFinish` fires once on terminal state (`completed` or `failed`). `onError` fires on `failed` after retries are exhausted. Errors thrown inside these callbacks are caught and logged — a broken hook never fails the workflow.

### `durable(steps)`

Wraps an object of async functions with transparent checkpointing. The Proxy preserves full TypeScript types — no assertions needed.

```typescript
const steps = durable({
  fetchData:  async (id: string)   => ({ ... }),
  transform:  async (data: Data)   => ({ ... }),
});

// Inside a workflow: cached after first execution, replayed on resume
const data   = await steps.fetchData(id);
const result = await steps.transform(data);

// Outside a workflow: calls the original function directly (useful for tests)
const data = await steps.fetchData(id);
```

Step IDs are `"methodName:callIndex"` (`callLLM:0`, `callLLM:1`, …). Adding, removing, or reordering steps in a running workflow breaks replay for in-flight runs — same constraint as Temporal. Conditional branches are safe as long as the same branch runs on replay.

Step results must be JSON-serializable. Throw `FatalError` from a step to fail the run immediately, skipping retries.

### `sleep(duration)`

Suspends the workflow for a duration. Zero compute while waiting. Survives process restarts.

```typescript
await sleep("30 seconds");
await sleep("5 minutes");
await sleep("7 days");
await sleep("2 weeks");
```

Accepted units: `second(s)`, `minute(s)`, `hour(s)`, `day(s)`, `week(s)`.

### `startWorker(db, options?)`

Starts the execution loop. Returns `{ stop }`.

```typescript
const worker = startWorker(client, {
  maxTimerSleepMs: 60_000,  // cap on how long the timer poller sleeps between checks
  reconcileMs:     60_000,  // how often to scan for orphaned/missed runs
});

await worker.stop(); // graceful shutdown
```

The worker runs two wakeup sources:
- **Push** — `LISTEN 'workflow_runs'` for instant pickup of new runs
- **Poll** — adaptive timer: sleeps until the next due timer, fires it, resumes the run

On startup it scans for pending, due-suspended, and orphaned runs whose worker died mid-execution (detected via PostgreSQL advisory locks — no heartbeat, no lease threshold).

**Pool sizing**: the worker reserves one dedicated connection for its advisory locks. Size your pool at `≥ 2` or the worker's own queries starve.

### Management API

```typescript
import { getRun, listRuns, replayRun } from "pipelines";

// Full run detail: run row + steps + ordered log trail
const run = await getRun(db, runId);

// Paginated list for a workflow
const runs = await listRuns(db, "onboard", { limit: 50, offset: 0 });

// Clear output + steps, reset to pending — re-executes from scratch
await replayRun(db, runId);
```

### `setup(db, sql?)`

Applies the schema idempotently. Safe to call on every startup.

```typescript
await setup(client);                    // reads packages/runtime/schema.sql
await setup(client, customSchemaSql);   // override (useful in tests)
```

### `setDefaultDb(db)`

Sets the default `DatabaseClient` used by `workflow().run()`. Call once at startup before submitting any runs.

### `FatalError`

Throw from a step to fail the run immediately, skipping retries.

```typescript
import { FatalError } from "pipelines";

const steps = durable({
  validate: async (output: string) => {
    if (!output.trim()) throw new FatalError("empty output");
    return { output };
  },
});
```

---

## Database adapter

The runtime never imports a concrete driver. You supply a thin adapter that maps your driver onto `DatabaseClient`:

```typescript
import type { DatabaseClient } from "pipelines";

// The interface you need to satisfy:
interface DatabaseClient {
  query<T>(text: string, params?: unknown[]): Promise<T[]>;
  listen(channel: string, onNotify: (payload: string) => void): Promise<{ unlisten(): Promise<void> }>;
  reserve(): Promise<{ query<T>(text: string, params?: unknown[]): Promise<T[]>; release(): Promise<void> }>;
  close(): Promise<void>;
}
```

See `examples/agentic/db.ts` for a complete adapter using `porsager/postgres`.

**Driver note**: `bun:sql` cannot receive `NOTIFY`. Use `porsager/postgres` (or `node-postgres`) as your driver. `porsager`'s `sql.unsafe(text, params)` is parameterized; `sql.unsafe(text)` (no params) uses the simple protocol, which `setup()`'s multi-statement DDL requires — handle both cases in your adapter.

---

## Retry policy

Default: 3 retries, 500ms initial backoff, 2× multiplier. Configured per workflow, applied per step.

```typescript
workflow("myWorkflow", fn, {
  retry: {
    maxRetries:        3,
    backoffMs:         500,
    backoffMultiplier: 2,
  },
});
```

`FatalError` bypasses retries entirely.

---

## Constraints

- **Step results must be JSON-serializable.** `Date` objects become strings after a sleep; `Map`/`Set` become `{}`. The runtime fails fast with a clear error on non-serializable returns rather than silent replay corruption.
- **Step order must be stable across replays.** Reordering or removing steps while runs are in-flight breaks replay for those runs. Adding steps at the end is safe.
- **`bun:sql` cannot receive `NOTIFY`.** Use `porsager/postgres` or `node-postgres`.
- **Pool size ≥ 2.** The worker reserves one connection permanently for advisory locks.

---

## Benchmark

Numbers from a local Docker Postgres 17 on a single machine. Each durable step costs 2 sequential queries (intent write, completion write); each run adds ~8 fixed queries (submit, lock, claim, cache load, terminal write, unlock, etc.).

```
submission      1,428 runs/sec        (5,000 inserts)
drain s1  ×1w     431 workflows/sec     431 steps/sec     (1-step runs,  1 worker)
drain s10 ×1w     115 workflows/sec   1,154 steps/sec     (10-step runs, 1 worker)
drain s1  ×3w     418 workflows/sec                       (3 workers, lock contention included)
latency (idle)   p50 8.9ms   p95 12.2ms   max 34.2ms      (submit → completed, sequential)
```

Pool-size sweep (1 worker, 1-step runs) — pool is the de-facto concurrency cap:

```
pool max=2      173 workflows/sec   (floor — ownership session eats 1 slot)
pool max=5      364 workflows/sec
pool max=10     452 workflows/sec
pool max=20     489 workflows/sec
pool max=30     494 workflows/sec   (saturation on local Docker)
pool max=50     452 workflows/sec   (slight regression — connection overhead)
```

Sweet spot on local Docker is around `max=20–30`. In production with a real network the curve will shift.

---

## Dev

```bash
pnpm install
docker compose up -d                        # Postgres on :5432
pnpm check                                  # biome format + lint + organize imports
pnpm typecheck                              # tsc --noEmit across the workspace
bun test packages/runtime                   # integration tests (requires Postgres above)
bun run bench --cwd packages/runtime        # throughput/latency benchmark
```
