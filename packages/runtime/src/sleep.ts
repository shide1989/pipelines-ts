// sleep(duration) — durable timers, plus the duration parser.
//
// First execution: insert a `workflow_timers` row with wake_at = now() + parsed
// duration, set the run 'suspended', log 'sleep.scheduled', then throw
// SleepInterrupt to suspend (the engine catches it — no compute consumed).
// Replay: if the timer fired (now ≥ wake_at), skip through; otherwise suspend again.

import { workflowStorage } from "./context";
import { SleepInterrupt } from "./errors";
import { appendLog } from "./log";

export async function sleep(duration: string): Promise<void> {
  const ctx = workflowStorage.getStore();
  if (!ctx) return; // No active workflow — sleep is a no-op outside a run.

  const sleepId = `sleep:${ctx.sleepCounter++}`;

  // Replay: skip through if the timer fired OR its wake time has passed. The
  // time-based check matters: the poller's 'fired' mark and the resume claim are
  // not atomic, so a resume can replay before (or without) the mark landing —
  // elapsed time alone must be enough to make progress. Mark it fired on the way
  // through so recovery (which scans timer state) sees it as consumed.
  const rows = await ctx.db.query<{ elapsed: boolean }>(
    `SELECT (status = 'fired' OR wake_at <= now()) AS elapsed
     FROM workflow_timers WHERE run_id = $1 AND sleep_id = $2`,
    [ctx.runId, sleepId],
  );
  if (rows[0]?.elapsed) {
    await ctx.db.query(
      "UPDATE workflow_timers SET status = 'fired' WHERE run_id = $1 AND sleep_id = $2",
      [ctx.runId, sleepId],
    );
    return;
  }

  // First execution (or still waiting): schedule the timer, suspend, unwind.
  // wake_at is computed from the DB clock (now() + interval) — the same clock the
  // poller compares against, and no Date param (drivers/ORMs serialize Date
  // inconsistently; the runtime never passes raw Dates).
  await ctx.db.query(
    `INSERT INTO workflow_timers (run_id, sleep_id, wake_at) VALUES ($1, $2, now() + $3::interval)
     ON CONFLICT (run_id, sleep_id) DO NOTHING`,
    [ctx.runId, sleepId, `${parseDuration(duration)} milliseconds`],
  );
  await ctx.db.query("UPDATE workflow_runs SET status = 'suspended' WHERE id = $1", [ctx.runId]);
  await appendLog(ctx.db, ctx.runId, "sleep.scheduled", { sleepId, duration });
  throw new SleepInterrupt(sleepId);
}

const UNIT_MS: Record<string, number> = {
  second: 1000,
  minute: 60_000,
  hour: 3_600_000,
  day: 86_400_000,
  week: 604_800_000,
};

/** Parse "7 days", "30 seconds", "1 hour", "2 weeks" → milliseconds. */
export function parseDuration(duration: string): number {
  const match = /^(\d+)\s+(second|minute|hour|day|week)s?$/.exec(duration.trim());
  if (!match) throw new Error(`Invalid duration: "${duration}"`);
  return Number(match[1]) * (UNIT_MS[match[2] as string] as number);
}
