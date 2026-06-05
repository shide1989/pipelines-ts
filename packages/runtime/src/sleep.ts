// sleep(duration) — durable timers, plus the duration parser.
//
// First execution: insert a `workflow_timers` row with wake_at = now() + parsed
// duration, then throw SleepInterrupt to suspend. Replay: if the timer fired,
// skip through; otherwise suspend again. Sleep IDs are "sleep:N" from a counter
// on the WorkflowContext.

export async function sleep(duration: string): Promise<void> {
  throw new Error(`Not implemented: sleep(${duration})`);
}

/** Parse "7 days", "30 minutes", "1 hour", "2 weeks" → milliseconds. */
export function parseDuration(duration: string): number {
  throw new Error(`Not implemented: parseDuration(${duration})`);
}
