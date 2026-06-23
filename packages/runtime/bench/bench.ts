// Throughput + latency benchmark against a real Postgres (docker compose up -d).
//
//   bun run packages/runtime/bench/bench.ts
//
// Phases:
//  1. submission   — raw .run() INSERT rate, no worker
//  2. drain s1     — pre-submitted 1-step runs, 1 worker: workflows/sec
//  3. drain s10    — pre-submitted 10-step runs, 1 worker: steps/sec
//  4. latency      — sequential runs on an idle worker: submit→completed wall time
//  5. 3 workers    — same drain with three workers (own clients/sessions), lock contention included
//
// Numbers are local-docker numbers: they measure the engine's per-step round-trip
// cost, not your production network. Each durable step costs 2 sequential queries
// (intent+log CTE, completed+log CTE); each execution adds ~8 fixed (submission
// INSERT, lock, orphan-check, claim, started log, cache pre-load, terminal+log
// CTE, unlock). Concurrency is bounded by the client pool (porsager default
// max 10, minus 1 for the reserved ownership session).

import { durable } from "../src/proxy";
import { startWorker, type Worker } from "../src/worker";
import { setDefaultDb, workflow } from "../src/workflow";
import { resetSchema, testDb, truncateAll, waitFor } from "../tests/helpers";

const db = testDb();
setDefaultDb(db);

const steps = durable({
  noop: async (i: number) => i,
});

const s1 = workflow("bench.s1", async () => (await steps.noop(0)) + 1);
const s10 = workflow("bench.s10", async () => {
  let acc = 0;
  for (let i = 0; i < 10; i++) acc += await steps.noop(i);
  return acc;
});

const completedCount = async () =>
  (
    await db.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM workflow_runs WHERE status = 'completed'",
    )
  )[0]?.n ?? 0;

async function submitMany(wf: typeof s1, n: number): Promise<number> {
  const t0 = performance.now();
  await Promise.all(Array.from({ length: n }, () => wf.run(undefined as never)));
  return performance.now() - t0;
}

/** Drain `n` pre-submitted runs with `workers` workers; returns elapsed ms. */
async function drain(n: number, workerCount: number, poolMax?: number): Promise<number> {
  // Separate client per worker — own pool, own LISTEN, own ownership session,
  // like real processes would have.
  const clients = Array.from({ length: workerCount }, () => testDb({ max: poolMax }));
  const workers: Worker[] = clients.map((c) => startWorker(c, { reconcileMs: 60_000 }));
  const t0 = performance.now();
  try {
    await waitFor(async () => (await completedCount()) >= n, 300_000, 50);
    return performance.now() - t0;
  } finally {
    await Promise.all(workers.map((w) => w.stop()));
    await Promise.all(clients.map((c) => c.close()));
  }
}

const fmt = (x: number) => x.toLocaleString("en-US", { maximumFractionDigits: 0 });
const pct = (xs: number[], p: number) => {
  const sorted = [...xs].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;
};

console.log("pipelines bench — local docker Postgres\n");
await resetSchema(db);

// 1. Submission rate (pure INSERT + NOTIFY into the void)
{
  const N = 5000;
  const ms = await submitMany(s1, N);
  console.log(
    `submission      ${fmt((N / ms) * 1000)} runs/sec        (${N} inserts in ${fmt(ms)}ms)`,
  );
  await truncateAll(db);
}

// 2. Drain 1-step workflows, 1 worker
{
  const N = 3000;
  await submitMany(s1, N);
  const ms = await drain(N, 1);
  console.log(
    `drain s1 ×1w    ${fmt((N / ms) * 1000)} workflows/sec   ${fmt((N / ms) * 1000)} steps/sec (${N} runs in ${fmt(ms)}ms)`,
  );
  await truncateAll(db);
}

// 3. Drain 10-step workflows, 1 worker
{
  const N = 500;
  await submitMany(s10, N);
  const ms = await drain(N, 1);
  console.log(
    `drain s10 ×1w   ${fmt((N / ms) * 1000)} workflows/sec   ${fmt(((N * 10) / ms) * 1000)} steps/sec (${N} runs in ${fmt(ms)}ms)`,
  );
  await truncateAll(db);
}

// 4. Per-run latency on an idle worker (NOTIFY pickup + full execution),
//    sequential so there is no queueing — this is the floor, not the ceiling.
{
  const N = 200;
  const worker = startWorker(db, { reconcileMs: 60_000 });
  await new Promise((r) => setTimeout(r, 200)); // let LISTEN come up
  const lat: number[] = [];
  for (let i = 0; i < N; i++) {
    const t0 = performance.now();
    const sub = await s1.run(undefined as never);
    await waitFor(
      async () =>
        (
          await db.query<{ ok: boolean }>(
            "SELECT status = 'completed' AS ok FROM workflow_runs WHERE id = $1",
            [sub.runId],
          )
        )[0]?.ok === true,
      10_000,
      2,
    );
    lat.push(performance.now() - t0);
  }
  await worker.stop();
  console.log(
    `latency (idle)  p50 ${pct(lat, 50).toFixed(1)}ms   p95 ${pct(lat, 95).toFixed(1)}ms   max ${pct(lat, 100).toFixed(1)}ms (submit→completed, ${N} sequential runs)`,
  );
  await truncateAll(db);
}

// 5. Drain 1-step workflows, 3 workers (lock contention included)
{
  const N = 3000;
  await submitMany(s1, N);
  const ms = await drain(N, 3);
  console.log(
    `drain s1 ×3w    ${fmt((N / ms) * 1000)} workflows/sec   (${N} runs in ${fmt(ms)}ms)`,
  );
  await truncateAll(db);
}

// 6. Pool-size sweep, 1 worker — the pool is the de facto concurrency cap, so
//    this shows where this Postgres saturates (and what a near-serial worker
//    costs). max=2 is the floor: reserve() takes the ownership session OUT of
//    the pool, so max=1 starves the worker's queries entirely (deadlock).
for (const max of [2, 5, 10, 20, 30, 50]) {
  const N = 1500;
  await submitMany(s1, N);
  const ms = await drain(N, 1, max);
  console.log(
    `pool max=${String(max).padEnd(2)}     ${fmt((N / ms) * 1000)} workflows/sec   (${N} runs in ${fmt(ms)}ms)`,
  );
  await truncateAll(db);
}

await db.close();
