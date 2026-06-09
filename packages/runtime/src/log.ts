// log.ts — append lifecycle rows to the derived `workflow_logs` stream.
//
// This is NOT the source of truth (the state tables are). It's an append-only
// audit/observability trail the engine writes at lifecycle points, and the
// foundation for the later SSE streaming phase (monotonic BIGINT id).

import type { DatabaseClient } from "./db";
import { toJsonb } from "./json";

export type LogEventType =
  | "run.started"
  | "run.resumed"
  | "run.suspended"
  | "run.completed"
  | "run.failed"
  | "step.running"
  | "step.completed"
  | "step.failed"
  | "sleep.scheduled";

export async function appendLog(
  db: DatabaseClient,
  runId: string,
  eventType: LogEventType,
  payload: Record<string, unknown> = {},
): Promise<void> {
  await db.query(
    "INSERT INTO workflow_logs (run_id, event_type, payload) VALUES ($1, $2, $3::jsonb)",
    [runId, eventType, toJsonb(payload)],
  );
}
