// timer-worker.ts — startable durable-timer poller (NOT auto-started).
//
// Loops: SELECT due timers (wake_at <= now(), status = 'waiting') FOR UPDATE
// SKIP LOCKED → mark 'fired' → re-invoke the workflow (replay). SKIP LOCKED makes
// it safe to run multiple instances concurrently.

import type { DatabaseClient } from "./db";

export interface TimerWorker {
  stop: () => void;
}

export function startTimerWorker(
  _db: DatabaseClient,
  options?: { intervalMs?: number },
): TimerWorker {
  throw new Error(`Not implemented: startTimerWorker(every ${options?.intervalMs ?? 5000}ms)`);
}
