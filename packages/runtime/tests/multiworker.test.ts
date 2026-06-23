// Multi-PROCESS worker tests — the real distributed environment the in-process
// herd test can't simulate: separate OS processes with their own Postgres
// sessions and advisory locks, killed with SIGKILL mid-step. This is the
// end-to-end proof of the liveness thesis: worker death ends its session,
// Postgres releases its locks, and a surviving worker reclaims the run.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Subprocess } from "bun";
import { getRun } from "../src/management";
import { setDefaultDb } from "../src/workflow";
import { mwWork } from "./fixtures/workflows";
import { resetSchema, TEST_URL, testDb, waitFor } from "./helpers";

const db = testDb();
const WORKER_MAIN = new URL("./fixtures/worker-main.ts", import.meta.url).pathname;

interface WorkerProc {
  proc: Subprocess<"ignore", "pipe", "inherit">;
  pid: number;
}
const workers: WorkerProc[] = [];

/** Spawn a worker process and wait until its LISTEN connection is live. */
async function spawnWorker(): Promise<WorkerProc> {
  const proc = Bun.spawn(["bun", "run", WORKER_MAIN], {
    stdout: "pipe",
    stderr: "inherit",
    env: { ...process.env, DATABASE_URL: TEST_URL },
  });
  const reader = proc.stdout.getReader();
  const decoder = new TextDecoder();
  let out = "";
  while (!out.includes("ready")) {
    const { value, done } = await reader.read();
    if (done) throw new Error("worker process exited before becoming ready");
    out += decoder.decode(value);
  }
  reader.releaseLock();
  const w = { proc, pid: proc.pid };
  workers.push(w);
  return w;
}

const execRows = (key: string) =>
  db.query<{ pid: number }>("SELECT pid FROM mw_exec WHERE key = $1 ORDER BY created_at", [key]);

const status = async (runId: string) => (await getRun(db, runId))?.status;

beforeAll(async () => {
  await resetSchema(db);
  await db.query(
    `CREATE TABLE IF NOT EXISTS mw_exec (
       key TEXT NOT NULL, pid INT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
     )`,
  );
  setDefaultDb(db);
  // All workers must be listening BEFORE anything is submitted, otherwise the
  // first one up vacuums the whole burst and the distribution assertion lies.
  await Promise.all([spawnWorker(), spawnWorker(), spawnWorker()]);
});

afterAll(async () => {
  for (const w of workers) w.proc.kill();
  await Promise.all(workers.map((w) => w.proc.exited));
  await db.close();
});

describe("multi-process workers", () => {
  test("a burst is distributed across workers, every run executes exactly once", async () => {
    const subs = await Promise.all(
      Array.from({ length: 16 }, (_, i) => mwWork.run({ key: `burst-${i}`, ms: 150 })),
    );
    for (const s of subs) {
      await waitFor(async () => (await status(s.runId)) === "completed", 15_000);
    }

    const pids = new Set<number>();
    for (let i = 0; i < 16; i++) {
      const rows = await execRows(`burst-${i}`);
      expect(rows.length).toBe(1); // exactly once, across all processes
      pids.add(rows[0]?.pid as number);
    }
    expect(pids.size).toBeGreaterThanOrEqual(2); // genuinely spread over the fleet
    for (const pid of pids) expect(workers.map((w) => w.pid)).toContain(pid);
  });

  test("SIGKILL mid-step: a surviving worker reclaims and completes the run", async () => {
    const sub = await mwWork.run({ key: "victim", ms: 2500 });

    // The step records its pid at start — that row tells us who to murder.
    await waitFor(async () => (await execRows("victim")).length === 1, 10_000);
    const claimerPid = (await execRows("victim"))[0]?.pid as number;
    const victim = workers.find((w) => w.pid === claimerPid);
    expect(victim).toBeDefined();

    victim?.proc.kill("SIGKILL"); // no cleanup, no finally — session just dies
    await victim?.proc.exited;

    // Session death released the lock; a survivor's reconcile pass (300ms)
    // finds the orphaned 'running' row, reclaims, and re-runs the in-doubt step.
    await waitFor(async () => (await status(sub.runId)) === "completed", 15_000);

    const run = await getRun(db, sub.runId);
    expect(run?.logs?.map((l) => l.eventType)).toContain("run.reclaimed");

    const rows = await execRows("victim");
    expect(rows.length).toBe(2); // in-doubt step re-executed (at-least-once)
    const finisherPid = rows[1]?.pid as number;
    expect(finisherPid).not.toBe(claimerPid); // by a DIFFERENT worker
    expect(run?.output).toBe(finisherPid); // which is who the workflow reports
  });
});
