// management.ts — programmatic, in-process control surface.
// Drivers (HTTP, CLI, queue consumer) wrap these; the runtime opens no port.

import type { DatabaseClient } from "./db";
import type { WorkflowRun } from "./types";

export async function getRun(_db: DatabaseClient, runId: string): Promise<WorkflowRun | null> {
  throw new Error(`Not implemented: getRun(${runId})`);
}

export async function listRuns(
  _db: DatabaseClient,
  workflowName: string,
  _options?: { limit?: number; offset?: number },
): Promise<WorkflowRun[]> {
  throw new Error(`Not implemented: listRuns(${workflowName})`);
}

/** Force a fresh replay of a completed/failed run. */
export async function replayRun(_db: DatabaseClient, runId: string): Promise<WorkflowRun> {
  throw new Error(`Not implemented: replayRun(${runId})`);
}
