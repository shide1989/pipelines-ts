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

  const rows = await ctx.db.query<{ status: string }>(
    "SELECT status FROM workflow_timers WHERE run_id = $1 AND sleep_id = $2",
    [ctx.runId, sleepId],
  );
  // Replay: timer already fired → skip through and continue past the sleep.
  if (rows[0]?.status === "fired") return;

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
