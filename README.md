# pipelines

Durable workflow engine for TypeScript. 
Write plain async functions, the engine handles checkpointing, replay, and long-lived timers transparently. No SWC compiler, no code generation, no magic strings.

**If you already run Postgres, you have everything you need.** No Redis, no Kafka, no Temporal cluster, no extra infra.

*Keep in mind that this is still an alpha version, although it is being extensively tested through integration tests and E2E tests.*

---

## The problem

Long-running, multi-step work is fragile. A process restart mid-job means re-running everything from scratch, double-charging APIs, re-sending emails, corrupting state. 
The usual fixes -> queues, crons, status flags in your DB; work until they don't. 
At scale they become a distributed systems problem you didn't sign up for.

Durable execution is the right abstraction: each step is checkpointed, crashes are transparent, `sleep("7 days")` just works. But the existing other options are heavy: Temporal requires its own cluster and a separate build pipeline for you to maintain; Inngest/Windmill are SaaS; DIY on top of a queue is weeks of error-prone glue.

**pipelines** is the small, self-hosted version. PostgreSQL as the durable queue, `Proxy` + `AsyncLocalStorage` to intercept step execution at runtime — no compiler needed. Drop it into any TypeScript project that already has a Postgres connection.

---

## What you get

- **Crash recovery** — a process restart replays from the last completed step, not from scratch. Already-completed steps return their cached result instantly.
- **Durable sleep** — `await sleep("7 days")` suspends with zero compute and resumes automatically when the timer fires. Survives deploys, restarts, whatever.
- **Per-step retry** — configurable backoff, `FatalError` for non-retriable failures (bad input, validation failure).
- **Idempotency keys** — deduplicate submissions at the DB level.
- **Multi-worker** — run as many workers as you need. Dead worker detection via PostgreSQL advisory locks — no heartbeat to tune, no lease threshold, no separate liveness check.
- **Replay** — re-run any completed or failed workflow from scratch via the management API.
- **Observability** — every lifecycle event goes to an append-only `workflow_logs` table. Query it however you want.
- **No lock-in** — the management API is programmatic and in-process. Wire it to HTTP, a CLI, a queue consumer, a cron — your call.

---

## Status

**v0.5.0** — core runtime complete (durable steps, sleep, retry, replay, multi-worker liveness). See `SPEC.md` for the full design.

---

## How it works

`workflow().run(input)` inserts a `pending` row and returns a `runId` immediately — it does not execute inline. A worker wakes via `LISTEN`/`NOTIFY`, claims the run with `SKIP LOCKED`, and executes it inside `AsyncLocalStorage`.

Step results are persisted to `workflow_steps`. On replay (crash, restart), `checkpoint()` reads back the cached results instead of re-running the function. `sleep()` inserts a timer row and suspends the run — zero compute consumed while waiting, survives restarts at any duration.

The run rows in Postgres _are_ the durable queue. `NOTIFY` is a latency optimization, not a reliability mechanism.

---

## Installation

```bash
pnpm add pipelines postgres
# npm install pipelines postgres
# yarn add pipelines postgres
```

---

## Quick start

```bash
docker compose up -d          # Postgres on :5432 for local dev
```

```typescript
import postgres from "postgres";
import { checkpoint, setup, setDefaultDb, sleep, startWorker, workflow } from "pipelines";
import type { DatabaseClient } from "pipelines";

// 1. Create a DatabaseClient adapter for your driver (porsager/postgres shown)
//    On Supabase: use the direct connection URL (port 5432), not the transaction
//    pooler (port 6543) — LISTEN and session locks don't work over a pooler.
const sql = postgres(process.env.DATABASE_URL!, { onnotice: () => {} });

const client: DatabaseClient = {
  query: (text, params = []) =>
    params.length ? sql.unsafe(text, params as never[]) : sql.unsafe(text),
  listen: async (channel, onNotify) => {
    const { unlisten } = await sql.listen(channel, onNotify);
    return { unlisten };
  },
  reserve: async () => {
    const reserved = await sql.reserve();
    return {
      query: (text, params = []) =>
        params.length ? reserved.unsafe(text, params as never[]) : reserved.unsafe(text),
      release: () => reserved.release(),
    };
  },
  close: () => sql.end(),
};

// 2. Boot
await setup(client);       // idempotent — creates tables, triggers, indexes
setDefaultDb(client);      // workflow().run() uses this client by default
startWorker(client);       // starts the execution loop — keep this process alive

// 3. Define steps — each is checkpointed; a crash replays from the last completed one
const steps = checkpoint({
  // Fetch context once — cached on replay, never re-fetched
  fetchContext: async (docId: string) => {
    const doc = await db.documents.findById(docId);
    return { content: doc.content };
  },

  // Submit a batch inference job — cached on replay, never double-submitted
  submitInference: async (input: { prompt: string; context: string }) => {
    const { jobId } = await openai.batches.create({ messages: [{ role: "user", content: `${input.context}\n\n${input.prompt}` }] });
    return { jobId };
  },

  // Poll job status — retried automatically on transient failures
  checkInference: async ({ jobId }: { jobId: string }) => {
    return await openai.batches.retrieve(jobId); // { status, output }
  },

  validateOutput: async ({ output }: { output: string }) => {
    if (!output?.trim()) throw new FatalError("empty LLM output"); // skips retries
    return { output };
  },
});

// 4. Define the workflow
export const runAgentTask = workflow("runAgentTask", async (task: { prompt: string; docId: string }) => {
  const { content } = await steps.fetchContext(task.docId);
  const { jobId }   = await steps.submitInference({ prompt: task.prompt, context: content });

  // Durable polling loop: zero compute while sleeping, survives restarts at any duration
  let result = await steps.checkInference({ jobId });
  while (result.status !== "completed") {
    await sleep("30 seconds");
    result = await steps.checkInference({ jobId });
  }

  return await steps.validateOutput({ output: result.output });
});

// 5. Submit a run — returns immediately, executes in the worker
const { runId } = await runAgentTask.run({ prompt: "Summarise risks", docId: "doc_42" });
```

> **Serverless / edge**: the worker is a long-running loop and cannot run in a serverless function. Run it in a dedicated process (a Node.js server, a container, a VM) and submit runs from your serverless handlers via the management API.

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

### `checkpoint(steps)`

Wraps an object of async functions with transparent checkpointing. The Proxy preserves full TypeScript types — no assertions needed.

```typescript
const steps = checkpoint({
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

**Production pattern**: run the worker in a dedicated long-lived process, separate from your HTTP server. Your API handlers submit runs via `workflow().run()` and query state via the management API — they don't need a worker running alongside them, only access to the same Postgres database.

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

The management API (`getRun`, `listRuns`, `replayRun`) takes an explicit `db` argument instead — so it can be called from any process that has a database connection, not just the one where `setDefaultDb` was called.

### `FatalError`

Throw from a step to fail the run immediately, skipping retries.

```typescript
import { FatalError } from "pipelines";

const steps = checkpoint({
  validate: async (output: string) => {
    if (!output.trim()) throw new FatalError("empty output");
    return { output };
  },
});
```

---

## Database adapter

The runtime never imports a concrete driver. You supply a thin adapter that maps your driver onto `DatabaseClient` — the quick start above shows the full adapter for `porsager/postgres`.

The interface contract:

```typescript
interface DatabaseClient {
  query<T>(text: string, params?: unknown[]): Promise<T[]>;
  listen(channel: string, onNotify: (payload: string) => void): Promise<{ unlisten(): Promise<void> }>;
  reserve(): Promise<{ query<T>(text: string, params?: unknown[]): Promise<T[]>; release(): Promise<void> }>;
  close(): Promise<void>;
}
```

**Driver note**: `bun:sql` cannot receive `NOTIFY`. Use `porsager/postgres` (or `node-postgres`). `porsager`'s `sql.unsafe(text, params)` is parameterized; `sql.unsafe(text)` (no params) uses the simple protocol, which `setup()`'s multi-statement DDL requires — handle both cases in your adapter (as shown in the quick start).

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
- **No `sleep()` inside a `Promise.all` branch.** `SleepInterrupt` exits the engine and releases the advisory lock while sibling branches are still executing — a second worker can pick up the suspended run concurrently. Plain `Promise.all` over durable steps (no sleep) is safe.
- **Workflows must be imported in the worker process.** The registry is in-process; the worker resolves runs by name at runtime. Import all workflow modules before calling `startWorker` or runs will fail with "not registered".
- **Direct Postgres connection required.** The worker uses advisory locks and `LISTEN/NOTIFY`, both session-scoped. Transaction-mode poolers (PgBouncer, Supavisor) silently break both. On Supabase, use port 5432, not 6543.
- **Requires PostgreSQL 13+.** Uses `gen_random_uuid()`, `hashtextextended()`, and `GENERATED ALWAYS AS IDENTITY`.
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
