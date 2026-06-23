// Integration tests — real Postgres, real worker, real LISTEN/NOTIFY.
// Covers the V0.6 contract: decoupled submission, NOTIFY pickup, step caching,
// replay, durable sleep + resume, idempotency, per-step retry, FatalError,
// lifecycle hooks, worker recovery, and outside-workflow pass-through.

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { FatalError } from "../src/errors";
import { getRun, replayRun } from "../src/management";
import { durable } from "../src/proxy";
import { sleep } from "../src/sleep";
import type { LifecycleError, LifecycleResult } from "../src/types";
import { startWorker } from "../src/worker";
import { setDefaultDb, workflow } from "../src/workflow";
import { holdAdvisoryLock, resetSchema, testDb, waitFor } from "./helpers";

const db = testDb();

// --- Observable state, reset per test ---------------------------------------
let prepareCalls = 0;
let flakyAttempts = 0;
let fatalCalls = 0;
const finishes: LifecycleResult<unknown>[] = [];
const errors: LifecycleError[] = [];

// --- Test workflows (self-register on import) -------------------------------
const echoSteps = durable({
  prepare: async (x: { v: number }) => {
    prepareCalls++;
    return { doubled: x.v * 2 };
  },
});
const echoWf = workflow(
  "test.echo",
  async (x: { v: number }) => (await echoSteps.prepare(x)).doubled,
);

const sleepSteps = durable({
  before: async () => ({ at: "before" }),
  after: async () => ({ at: "after" }),
});
const sleepWf = workflow("test.sleep", async () => {
  await sleepSteps.before();
  await sleep("1 second");
  await sleepSteps.after();
  return "done";
});

const flakySteps = durable({
  flaky: async () => {
    flakyAttempts++;
    if (flakyAttempts < 3) throw new Error("transient");
    return { ok: true };
  },
});
const flakyWf = workflow("test.flaky", async () => (await flakySteps.flaky()).ok, {
  retry: { maxRetries: 5, backoffMs: 5, backoffMultiplier: 1 },
});

const fatalSteps = durable({
  boom: async () => {
    fatalCalls++;
    throw new FatalError("nope");
  },
});
const fatalWf = workflow(
  "test.fatal",
  async () => {
    await fatalSteps.boom();
  },
  { retry: { maxRetries: 3, backoffMs: 5, backoffMultiplier: 1 } },
);

let slowCalls = 0;
const slowSteps = durable({
  crunch: async () => {
    slowCalls++;
    await Bun.sleep(700); // long enough for several reconcile passes mid-execution
    return { ok: true };
  },
});
const slowWf = workflow("test.slow", async () => {
  await slowSteps.crunch();
  return "slow-done";
});

const hooksWf = workflow(
  "test.hooks",
  async (x: { fail: boolean }) => {
    if (x.fail) throw new FatalError("boom");
    return "ok";
  },
  {
    onFinish: (r) => {
      finishes.push(r);
    },
    onError: (e) => {
      errors.push(e);
    },
  },
);

// --- Worker lifecycle helper ------------------------------------------------
async function withWorker<T>(
  fn: () => Promise<T>,
  options?: Parameters<typeof startWorker>[1],
): Promise<T> {
  const w = startWorker(db, { maxTimerSleepMs: 100, reconcileMs: 1000, ...options });
  try {
    return await fn();
  } finally {
    await w.stop();
  }
}

const status = async (runId: string) => (await getRun(db, runId))?.status;

beforeAll(async () => {
  await resetSchema(db);
  setDefaultDb(db);
});
afterAll(async () => {
  await db.close();
});
beforeEach(async () => {
  prepareCalls = 0;
  flakyAttempts = 0;
  fatalCalls = 0;
  slowCalls = 0;
  finishes.length = 0;
  errors.length = 0;
});

describe("submission + execution", () => {
  test("submit returns pending (decoupled), worker executes via NOTIFY, step cached", async () => {
    const sub = await echoWf.run({ v: 21 });
    expect(sub.status).toBe("pending"); // did NOT execute inline

    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });

    const run = await getRun(db, sub.runId);
    expect(run?.output).toBe(42);
    expect(prepareCalls).toBe(1);
    expect(run?.steps?.[0]?.stepId).toBe("prepare:0");
    expect(run?.steps?.[0]?.status).toBe("completed");
    const events = run?.logs?.map((l) => l.eventType) ?? [];
    expect(events).toEqual(
      expect.arrayContaining(["run.started", "step.running", "step.completed", "run.completed"]),
    );
  });

  test("worker recovers a run submitted while nothing was listening", async () => {
    const sub = await echoWf.run({ v: 7 }); // NOTIFY fires into the void (no worker)
    await Bun.sleep(80);
    expect(await status(sub.runId)).toBe("pending");

    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });
    expect((await getRun(db, sub.runId))?.output).toBe(14);
  });
});

describe("replay + caching", () => {
  test("replay creates a new run and skips cached steps (no re-execution)", async () => {
    const sub = await echoWf.run({ v: 5 });
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });
    expect(prepareCalls).toBe(1);

    let replay!: { runId: string };
    await withWorker(async () => {
      replay = await replayRun(db, sub.runId);
      expect(replay.runId).not.toBe(sub.runId); // new run, not in-place mutation
      await waitFor(async () => (await status(replay.runId)) === "completed");
    });
    expect(prepareCalls).toBe(1); // step served from cache, fn not re-invoked

    const replayed = await getRun(db, replay.runId);
    expect(replayed?.output).toBe(10); // 5*2
    expect(replayed?.parentRunId).toBe(sub.runId);
    expect(await status(sub.runId)).toBe("completed"); // original untouched
  });

  test("replay with useCache:false re-executes all steps", async () => {
    const sub = await echoWf.run({ v: 5 });
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });
    expect(prepareCalls).toBe(1);

    let replay!: { runId: string };
    await withWorker(async () => {
      replay = await replayRun(db, sub.runId, { useCache: false });
      await waitFor(async () => (await status(replay.runId)) === "completed");
    });
    expect(prepareCalls).toBe(2); // fn re-invoked — no cache
    expect((await getRun(db, replay.runId))?.output).toBe(10);
  });

  test("replay of a failed run resumes from the failure point", async () => {
    const sub = await flakyWf.run(undefined as never);
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });
    expect(flakyAttempts).toBe(3);

    // Force the run into a failed state to simulate a real failure scenario.
    await db.query("UPDATE workflow_runs SET status = 'failed', error = 'forced' WHERE id = $1", [sub.runId]);

    let replay!: { runId: string };
    await withWorker(async () => {
      replay = await replayRun(db, sub.runId);
      await waitFor(async () => (await status(replay.runId)) === "completed");
    });
    // The completed step was cached — fn not re-invoked.
    expect(flakyAttempts).toBe(3);
    expect((await getRun(db, replay.runId))?.output).toBe(true);
    expect((await getRun(db, replay.runId))?.parentRunId).toBe(sub.runId);
  });

  test("replayRun throws on non-terminal status", async () => {
    const sub = await echoWf.run({ v: 1 });
    await expect(replayRun(db, sub.runId)).rejects.toThrow(/not replayable/);
  });

  test("replayRun throws on unknown run id", async () => {
    await expect(replayRun(db, "00000000-0000-0000-0000-000000000000")).rejects.toThrow(/not found/);
  });
});

describe("durable sleep", () => {
  test("re-executing a run that crashed before suspending does not duplicate the timer", async () => {
    const sub = await sleepWf.run(undefined as never);

    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "suspended");
    });

    // Simulate crash: timer row exists but the suspend didn't commit.
    await db.query("UPDATE workflow_runs SET status = 'running' WHERE id = $1", [sub.runId]);

    await withWorker(async () => {
      await waitFor(
        async () => {
          const s = await status(sub.runId);
          return s === "suspended" || s === "completed";
        },
        8000,
      );
    });

    const [row] = await db.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM workflow_timers WHERE run_id = $1",
      [sub.runId],
    );
    expect(row?.n).toBe(1); // ON CONFLICT DO NOTHING — no duplicate
  });

  test("suspends on sleep, then an adaptive worker resumes to completion", async () => {
    const sub = await sleepWf.run(undefined as never);

    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "suspended");
      const mid = await getRun(db, sub.runId);
      expect(mid?.logs?.map((l) => l.eventType)).toContain("sleep.scheduled");
      await waitFor(async () => (await status(sub.runId)) === "completed", 8000);
    });

    const run = await getRun(db, sub.runId);
    expect(run?.output).toBe("done");
    expect(run?.steps?.length).toBe(2);
  });
});

describe("idempotency", () => {
  test("same key returns the same run, inserts once", async () => {
    const a = await echoWf.run({ v: 1 }, { idempotencyKey: "k1" });
    const b = await echoWf.run({ v: 999 }, { idempotencyKey: "k1" });
    expect(b.runId).toBe(a.runId);
    const rows = await db.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM workflow_runs WHERE idempotency_key = 'k1'",
    );
    expect(rows[0]?.n).toBe(1);
  });

  test("concurrent same-key submits race to one run, none throws", async () => {
    const subs = await Promise.all(
      Array.from({ length: 10 }, () => echoWf.run({ v: 1 }, { idempotencyKey: "race" })),
    );
    expect(new Set(subs.map((s) => s.runId)).size).toBe(1);
    const rows = await db.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM workflow_runs WHERE idempotency_key = 'race'",
    );
    expect(rows[0]?.n).toBe(1);
  });
});

describe("crash recovery", () => {
  test("a run whose workflow is not registered fails the run with a clear error", async () => {
    const [row] = await db.query<{ id: string }>(
      `INSERT INTO workflow_runs (workflow_name, input, idempotency_key, status)
       VALUES ('ghost.workflow', 'null'::jsonb, gen_random_uuid()::text, 'pending')
       RETURNING id`,
    );
    const runId = row!.id;

    await withWorker(async () => {
      await waitFor(async () => (await status(runId)) === "failed");
    });

    const run = await getRun(db, runId);
    expect(run?.error).toMatch(/not registered/);
    expect(run?.logs?.map((l) => l.eventType)).toContain("run.failed");
  });

  test("a run stuck 'running' (dead worker) is reclaimed and completes", async () => {
    const sub = await echoWf.run({ v: 4 }); // NOTIFY into the void (no worker yet)
    // A dead worker leaves status 'running' and no advisory lock (its session died).
    await db.query("UPDATE workflow_runs SET status = 'running' WHERE id = $1", [sub.runId]);

    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });

    const run = await getRun(db, sub.runId);
    expect(run?.output).toBe(8);
    expect(run?.logs?.map((l) => l.eventType)).toContain("run.reclaimed");
  });

  test("an actively-executing run (lock held by a live session) is not stolen", async () => {
    const sub = await echoWf.run({ v: 9 });
    await db.query("UPDATE workflow_runs SET status = 'running' WHERE id = $1", [sub.runId]);
    const die = await holdAdvisoryLock(sub.runId); // simulate the live claimer

    await withWorker(
      async () => {
        await Bun.sleep(500); // several reconcile passes while the lock is held
        expect(await status(sub.runId)).toBe("running"); // scanned, but not stolen
        expect((await getRun(db, sub.runId))?.logs?.map((l) => l.eventType)).not.toContain(
          "run.reclaimed",
        );

        await die(); // session ends → lock auto-releases → next pass reclaims
        await waitFor(async () => (await status(sub.runId)) === "completed");
      },
      { reconcileMs: 150 },
    );

    const run = await getRun(db, sub.runId);
    expect(run?.output).toBe(18);
    expect(run?.logs?.map((l) => l.eventType)).toContain("run.reclaimed");
  });

  test("a worker's reclaim scan never steals its own in-flight run", async () => {
    // Advisory locks are re-entrant within a session, so without in-process
    // exclusion the scan would "discover" our own active run as an orphan.
    const sub = await slowWf.run(undefined as never);
    await withWorker(
      async () => {
        await waitFor(async () => (await status(sub.runId)) === "completed", 8000);
      },
      { reconcileMs: 100 },
    ); // scans fire repeatedly during the 700ms step

    const run = await getRun(db, sub.runId);
    expect(slowCalls).toBe(1); // executed exactly once, not re-claimed mid-flight
    expect(run?.output).toBe("slow-done");
    expect(run?.logs?.map((l) => l.eventType)).not.toContain("run.reclaimed");
  });

  test("no advisory lock is held while a run sleeps", async () => {
    const sub = await sleepWf.run(undefined as never);
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "suspended");
      await waitFor(async () => {
        const rows = await db.query<{ n: number }>(
          "SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory'",
        );
        return rows[0]?.n === 0; // suspension released the lock — zero compute, zero locks
      });
      await waitFor(async () => (await status(sub.runId)) === "completed", 8000);
    });
    expect((await getRun(db, sub.runId))?.output).toBe("done");
  });

  test("concurrent workers never double-execute (NOTIFY herd)", async () => {
    const workers = [1, 2, 3].map(() => startWorker(db, { maxTimerSleepMs: 100 }));
    try {
      const subs = await Promise.all(Array.from({ length: 8 }, (_, i) => echoWf.run({ v: i })));
      for (const s of subs) {
        await waitFor(async () => (await status(s.runId)) === "completed");
      }
      expect(prepareCalls).toBe(8); // every run executed exactly once across 3 workers
    } finally {
      await Promise.all(workers.map((w) => w.stop()));
    }
  });

  test("a suspended run whose timer fired without a resume self-heals", async () => {
    const sub = await sleepWf.run(undefined as never);
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "suspended");
    });
    // Simulate the poller crashing between marking the timer fired and resuming.
    await db.query("UPDATE workflow_timers SET status = 'fired' WHERE run_id = $1", [sub.runId]);

    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });
    expect((await getRun(db, sub.runId))?.output).toBe("done");
  });
});

describe("registration", () => {
  test("duplicate workflow name throws at registration", () => {
    expect(() => workflow("test.echo", async () => 1)).toThrow(/already registered/);
  });
});

describe("retry + failure", () => {
  test("a flaky step retries and records attempts", async () => {
    const sub = await flakyWf.run(undefined as never);
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "completed");
    });
    expect(flakyAttempts).toBe(3);
    const run = await getRun(db, sub.runId);
    expect(run?.output).toBe(true);
    expect(run?.steps?.[0]?.attempts).toBe(3);
  });

  test("FatalError fails the run with no retries", async () => {
    const sub = await fatalWf.run(undefined as never);
    await withWorker(async () => {
      await waitFor(async () => (await status(sub.runId)) === "failed");
    });
    expect(fatalCalls).toBe(1); // not retried
    const run = await getRun(db, sub.runId);
    expect(run?.error).toContain("nope");
    expect(run?.steps?.[0]?.status).toBe("failed");
  });
});

describe("lifecycle hooks", () => {
  test("onFinish fires on completed + failed; onError only on failed", async () => {
    const ok = await hooksWf.run({ fail: false });
    const bad = await hooksWf.run({ fail: true });
    await withWorker(async () => {
      await waitFor(async () => (await status(ok.runId)) === "completed");
      await waitFor(async () => (await status(bad.runId)) === "failed");
      await waitFor(async () => finishes.length >= 2 && errors.length >= 1);
    });
    expect(finishes.map((f) => f.status).sort()).toEqual(["completed", "failed"]);
    expect(errors).toHaveLength(1);
    expect(errors[0]?.error).toContain("boom");
  });
});

describe("outside a workflow", () => {
  const unboundSteps = durable({
    prepare: async (x: { v: number }) => {
      prepareCalls++;
      return { doubled: x.v * 2 };
    },
  }, { allowUnbound: true });

  test("a durable step runs directly with no checkpointing", async () => {
    const [before] = await db.query<{ n: number }>("SELECT count(*)::int AS n FROM workflow_steps");
    const r = await unboundSteps.prepare({ v: 21 });
    expect(r).toEqual({ doubled: 42 });
    expect(prepareCalls).toBe(1);
    const [after] = await db.query<{ n: number }>("SELECT count(*)::int AS n FROM workflow_steps");
    expect(after?.n).toBe(before?.n); // nothing persisted
  });
});
