// worker.ts — the single thing an application starts to drive execution.
// NOT auto-started by the runtime.
//
// Two wakeup sources feed one execution path (executeExclusive):
//   A. new runs (push)   — LISTEN 'workflow_runs'; instant pickup, no poll latency
//   B. sleeps (poll)     — adaptive: sleep until the next due timer (capped), then
//                          fire due timers (SKIP LOCKED) and resume their runs.
//                          A poll is irreducible: nothing inserts a row when a
//                          timestamp passes, so there's no NOTIFY to hang on.
// Plus startup + periodic recovery: scan pending / due-suspended / orphaned runs
// to recover any NOTIFY missed while down or dropped while up — and any run whose
// worker died mid-execution. The persisted rows are the durable queue; NOTIFY is
// only a latency optimization.
//
// Liveness (see FEATURE-multi-worker-liveness.md): every execution is gated by a
// session-level advisory lock held on ONE reserved "ownership" connection per
// worker. Worker death ends that session and Postgres releases all its locks —
// so "this run's lock is free but its row says running" means the claimer is
// dead, with no lease threshold to tune and no heartbeat to feed. The lock only
// arbitrates BETWEEN sessions (same-session attempts are re-entrant and succeed),
// so in-process exclusion is the inflight map's job, never the lock's.

import type { DatabaseClient, Subscription } from "./db";
import { claimAndExecute } from "./engine";
import { appendLog } from "./log";

/** Advisory-lock key for a run: full-width int8 hash of the run id. */
const LOCK_KEY = "hashtextextended($1::text, 0)";

export interface Worker {
  stop: () => Promise<void>;
}

export function startWorker(
  db: DatabaseClient,
  options?: { maxTimerSleepMs?: number; reconcileMs?: number },
): Worker {
  const cap = options?.maxTimerSleepMs ?? 60_000;
  const reconcileMs = options?.reconcileMs ?? 60_000;
  let running = true;
  let sub: Subscription | undefined;
  let pollTimer: ReturnType<typeof setTimeout> | undefined;
  let reconcileTimer: ReturnType<typeof setInterval> | undefined;

  // Ownership session: all of this worker's advisory locks live here, so they
  // share the worker's fate. Never used for run/step writes (those stay on the
  // pool) — it must be free to die with the process, releasing every lock.
  // NOTE: reserve() typically takes this connection OUT of the client's pool —
  // a pool of 1 starves the worker's queries entirely. Size pools ≥ 2.
  const ownership = db.reserve().catch((err: unknown) => {
    console.error("[pipelines] FATAL: failed to reserve ownership session — worker cannot start:", err);
    process.exit(1);
  });

  // In-process exclusion, keyed by run id and registered synchronously — the
  // advisory lock cannot do this job (same-session attempts succeed).
  const inflight = new Map<string, Promise<void>>();

  const executeExclusive = async (runId: string): Promise<void> => {
    const own = await ownership;
    const locked = await own.query<{ ok: boolean }>(
      `SELECT pg_try_advisory_lock(${LOCK_KEY}) AS ok`,
      [runId],
    );
    if (!locked[0]?.ok) return; // a live worker is executing this run

    try {
      // We hold the lock, so a 'running' row can only mean its claimer died
      // (a live claimer would hold the lock). Reset it so the engine can
      // re-claim; completed steps stay cached, the in-doubt step re-runs.
      const orphaned = await db.query<{ id: string }>(
        "UPDATE workflow_runs SET status = 'pending' WHERE id = $1 AND status = 'running' RETURNING id",
        [runId],
      );
      if (orphaned[0]) await appendLog(db, runId, "run.reclaimed");

      await claimAndExecute(db, runId);
    } finally {
      // On failure the lock dangles until this session ends — log and move on.
      await own
        .query(`SELECT pg_advisory_unlock(${LOCK_KEY})`, [runId])
        .catch((err) => console.error(`[pipelines] unlock failed for ${runId}:`, err));
    }
  };

  const execute = (runId: string): Promise<void> => {
    const existing = inflight.get(runId);
    if (existing) return existing;
    const p = executeExclusive(runId)
      .catch((err) => console.error(`[pipelines] execute failed for ${runId}:`, err))
      .finally(() => inflight.delete(runId));
    inflight.set(runId, p);
    return p;
  };

  // Source A: push — new pending runs.
  db.listen("workflow_runs", (runId) => void execute(runId))
    .then((s) => {
      sub = s;
    })
    .catch((err) => console.error("[pipelines] LISTEN failed:", err));

  // Source B: adaptive timer poll.
  const pollTimers = async () => {
    if (!running) return;
    let delay = cap;
    try {
      const due = await db.query<{ run_id: string }>(
        `UPDATE workflow_timers SET status = 'fired'
         WHERE id IN (
           SELECT id FROM workflow_timers
           WHERE wake_at <= now() AND status = 'waiting'
           FOR UPDATE SKIP LOCKED
         )
         RETURNING run_id`,
      );
      for (const runId of new Set(due.map((r) => r.run_id))) void execute(runId);

      const next = await db.query<{ wake_at: Date | null }>(
        "SELECT min(wake_at) AS wake_at FROM workflow_timers WHERE status = 'waiting'",
      );
      const wakeAt = next[0]?.wake_at;
      if (wakeAt) delay = Math.max(0, Math.min(cap, new Date(wakeAt).getTime() - Date.now()));
    } catch (err) {
      console.error("[pipelines] timer poll failed:", err);
    }
    if (running) pollTimer = setTimeout(pollTimers, delay);
  };

  // Recovery, three kinds of strandable run — all funnel into execute(), whose
  // lock gate makes re-processing always safe:
  //  - 'pending' → a NOTIFY missed while down or dropped while up.
  //  - 'suspended' with no future timer left to wait on → due, or stuck because
  //    a timer was marked 'fired' but the resume never landed (poller crash or
  //    race). sleep()'s time-based skip-through makes re-claiming safe.
  //  - 'running' → orphan *candidates*. The lock is the liveness test: a live
  //    worker's lock makes executeExclusive skip; a dead worker's lock is gone,
  //    so the row gets reset and re-executed. Bounded batch; the next pass
  //    (every reconcileMs) takes the rest.
  const recover = async () => {
    const rows = await db.query<{ id: string }>(
      `SELECT id FROM workflow_runs WHERE status = 'pending'
       UNION
       SELECT r.id FROM workflow_runs r
       WHERE r.status = 'suspended'
         AND NOT EXISTS (SELECT 1 FROM workflow_timers t WHERE t.run_id = r.id
                         AND t.status = 'waiting' AND t.wake_at > now())
         AND EXISTS (SELECT 1 FROM workflow_timers t WHERE t.run_id = r.id
                     AND (t.status = 'fired' OR t.wake_at <= now()))
       UNION
       SELECT id FROM (
         SELECT id FROM workflow_runs WHERE status = 'running'
         ORDER BY updated_at LIMIT 50
       ) candidates`,
    );
    for (const { id } of rows) void execute(id);
  };

  const runRecover = () =>
    recover().catch((err) => console.error("[pipelines] recovery failed:", err));

  void runRecover();
  pollTimers();
  reconcileTimer = setInterval(() => void runRecover(), reconcileMs);

  return {
    stop: async () => {
      running = false;
      if (pollTimer) clearTimeout(pollTimer);
      if (reconcileTimer) clearInterval(reconcileTimer);
      await sub?.unlisten();
      await Promise.all(inflight.values()); // drain; each execution unlocks in its finally
      // Reserve failure was already logged at startup; nothing to release then.
      await ownership.then((own) => own.release()).catch(() => {});
    },
  };
}
