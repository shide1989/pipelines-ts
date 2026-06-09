// worker.ts — the single thing an application starts to drive execution.
// NOT auto-started by the runtime.
//
// Two wakeup sources feed one execution path (engine.claimAndExecute):
//   A. new runs (push)   — LISTEN 'workflow_runs'; instant pickup, no poll latency
//   B. sleeps (poll)     — adaptive: sleep until the next due timer (capped), then
//                          fire due timers (SKIP LOCKED) and resume their runs.
//                          A poll is irreducible: nothing inserts a row when a
//                          timestamp passes, so there's no NOTIFY to hang on.
// Plus startup + periodic recovery: scan pending / due-suspended runs to recover
// any NOTIFY missed while down or dropped while up. The persisted rows are the
// durable queue; NOTIFY is only a latency optimization.

import type { DatabaseClient, Subscription } from "./db";
import { claimAndExecute } from "./engine";

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

  const execute = (runId: string) =>
    claimAndExecute(db, runId).catch((err) =>
      console.error(`[pipelines] execute failed for ${runId}:`, err),
    );

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

  // Recovery: process pending runs + due-suspended runs missed while down/dropped.
  const recover = async () => {
    const rows = await db.query<{ id: string }>(
      `SELECT id FROM workflow_runs WHERE status = 'pending'
       UNION
       SELECT r.id FROM workflow_runs r
       JOIN workflow_timers t ON t.run_id = r.id
       WHERE r.status = 'suspended' AND t.status = 'waiting' AND t.wake_at <= now()`,
    );
    for (const { id } of rows) void execute(id);
  };

  recover().catch((err) => console.error("[pipelines] recovery failed:", err));
  pollTimers();
  reconcileTimer = setInterval(() => void recover().catch(() => {}), reconcileMs);

  return {
    stop: async () => {
      running = false;
      if (pollTimer) clearTimeout(pollTimer);
      if (reconcileTimer) clearInterval(reconcileTimer);
      await sub?.unlisten();
    },
  };
}
