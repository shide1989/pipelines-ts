// management.ts — programmatic, in-process control surface.
// Drivers (HTTP, CLI, queue consumer) wrap these; the runtime opens no port.

import type { DatabaseClient } from "./db";
import { mapRun } from "./engine";
import { parseJsonb } from "./json";
import type { LogEntry, RunSubmission, StepResult, WorkflowRun } from "./types";

// biome-ignore lint/suspicious/noExplicitAny: raw snake_case rows from the driver.
type Row = any;

/** Full run detail: the run row plus its steps and ordered log trail. */
export async function getRun(db: DatabaseClient, runId: string): Promise<WorkflowRun | null> {
  const rows = await db.query<Row>("SELECT * FROM workflow_runs WHERE id = $1", [runId]);
  const row = rows[0];
  if (!row) return null;

  const run = mapRun(row);
  const [steps, logs] = await Promise.all([
    db.query<Row>("SELECT * FROM workflow_steps WHERE run_id = $1 ORDER BY created_at, step_id", [
      runId,
    ]),
    db.query<Row>("SELECT * FROM workflow_logs WHERE run_id = $1 ORDER BY id", [runId]),
  ]);
  run.steps = steps.map(mapStep);
  run.logs = logs.map(mapLog);
  return run;
}

export async function listRuns(
  db: DatabaseClient,
  workflowName: string,
  options?: { limit?: number; offset?: number },
): Promise<WorkflowRun[]> {
  const rows = await db.query<Row>(
    `SELECT * FROM workflow_runs WHERE workflow_name = $1
     ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
    [workflowName, options?.limit ?? 50, options?.offset ?? 0],
  );
  return rows.map((row) => mapRun(row));
}

/**
 * Force re-execution of a completed/failed run: reset it to 'pending' and notify
 * a worker. Completed steps stay cached, so a failed run retries from its failed
 * step (its row isn't 'completed'), not from scratch.
 */
export async function replayRun(db: DatabaseClient, runId: string): Promise<RunSubmission> {
  const reset = await db.query<{ status: WorkflowRun["status"] }>(
    "UPDATE workflow_runs SET status = 'pending', error = NULL WHERE id = $1 AND status IN ('completed', 'failed') RETURNING status",
    [runId],
  );
  if (!reset[0]) {
    const cur = await db.query<{ status: WorkflowRun["status"] }>(
      "SELECT status FROM workflow_runs WHERE id = $1",
      [runId],
    );
    if (!cur[0]) throw new Error(`Run not found: ${runId}`);
    return { runId, status: cur[0].status }; // not in a replayable state — return as-is
  }
  // UPDATE doesn't fire the AFTER-INSERT notify trigger, so wake a worker explicitly.
  await db.query("SELECT pg_notify('workflow_runs', $1)", [runId]);
  return { runId, status: "pending" };
}

function mapStep(row: Row): StepResult {
  return {
    stepId: row.step_id,
    output: parseJsonb(row.output),
    error: row.error ?? undefined,
    status: row.status,
    attempts: Number(row.attempts),
  };
}

function mapLog(row: Row): LogEntry {
  return {
    id: Number(row.id),
    runId: row.run_id,
    eventType: row.event_type,
    payload: parseJsonb(row.payload),
    createdAt: new Date(row.created_at),
  };
}
