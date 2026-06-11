// worker.ts — the single thing an application starts to drive execution.
// NOT auto-started by the runtime.
//
// Two wakeup sources feed one execution path (engine.claimAndExecute):
//   A. new runs (push)   — LISTEN 'workflow_runs'; instant pickup, no poll latency
//   B. sleeps (poll)     — adaptive: sleep until the next due timer (capped), then
//                          fire due timers (SKIP LOCKED) and resume their runs.
//                          A poll is irreducible: nothing inserts a row when a
//                          timestamp passes, so there's no NOTIFY to hang on.
// Plus startup + periodic recovery: scan pending / due-suspended / stale-running
// runs to recover any NOTIFY missed while down or dropped while up — and any run
// orphaned by a dead worker. The persisted rows are the durable queue; NOTIFY is
// only a latency optimization.

import type { DatabaseClient, Subscription } from "./db";
import { claimAndExecute } from "./engine";
import { appendLog } from "./log";

export interface Worker {
  stop: () => Promise<void>;
}

export function startWorker(
  db: DatabaseClient,
  options?: { maxTimerSleepMs?: number; reconcileMs?: number; staleRunningMs?: number },
): Worker {
  const cap = options?.maxTimerSleepMs ?? 60_000;
  const reconcileMs = options?.reconcileMs ?? 60_000;
  // A 'running' run whose row hasn't been touched for this long is presumed
  // orphaned by a dead worker and reclaimed. The engine heartbeats at a quarter
  // of this, so a live execution stays several beats ahead of the threshold.
  // Set it consistently across workers. (A worker alive-but-partitioned past the
  // threshold gets double-executed — at-least-once, same as the spec's model.)
  const staleRunningMs = options?.staleRunningMs ?? 60_000;
  let running = true;
  let sub: Subscription | undefined;
  let pollTimer: ReturnType<typeof setTimeout> | undefined;
  let reconcileTimer: ReturnType<typeof setInterval> | undefined;
  const inflight = new Set<Promise<void>>();

  const execute = (runId: string) => {
    const p = claimAndExecute(db, runId, staleRunningMs / 4).catch((err) =>
      console.error(`[pipelines] execute failed for ${runId}:`, err),
    );
    inflight.add(p);
    p.finally(() => inflight.delete(p));
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

  // Recovery, three kinds of strandable run:
  //  - 'running' but untouched past the stale threshold → orphaned by a dead
  //    worker (the heartbeat keeps live ones fresh) → reset to 'pending'.
  //  - 'pending' → a NOTIFY missed while down or dropped while up.
  //  - 'suspended' with no future timer left to wait on → due, or stuck because
  //    a timer was marked 'fired' but the resume never landed (poller crash or
  //    race). sleep()'s time-based skip-through makes re-claiming always safe.
  const recover = async () => {
    const reclaimed = await db.query<{ id: string }>(
      `UPDATE workflow_runs SET status = 'pending'
       WHERE status = 'running' AND updated_at < now() - $1::interval
       RETURNING id`,
      [`${staleRunningMs} milliseconds`],
    );
    for (const { id } of reclaimed) await appendLog(db, id, "run.reclaimed");

    const rows = await db.query<{ id: string }>(
      `SELECT id FROM workflow_runs WHERE status = 'pending'
       UNION
       SELECT r.id FROM workflow_runs r
       WHERE r.status = 'suspended'
         AND NOT EXISTS (SELECT 1 FROM workflow_timers t WHERE t.run_id = r.id
                         AND t.status = 'waiting' AND t.wake_at > now())
         AND EXISTS (SELECT 1 FROM workflow_timers t WHERE t.run_id = r.id
                     AND (t.status = 'fired' OR t.wake_at <= now()))`,
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
      await Promise.all(inflight); // drain in-flight executions before reporting stopped
    },
  };
}
