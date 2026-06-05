// engine.ts — workflow executor + replay logic.
//
// Responsibilities:
//  - Start a run: create the record, pre-load cached steps, execute inside ALS.
//  - Replay: pre-populate the cachedSteps map (one query), fast-forward through
//    completed steps until the first uncached step or unfired timer.
//  - Handle SleepInterrupt: mark `suspended`, return cleanly (not terminal).
//  - Terminal transitions: persist completed/failed, then fire lifecycle hooks
//    (errors caught + logged, never propagated).

import type { WorkflowContext } from "./context";
import type { DatabaseClient } from "./db";
import type { WorkflowRun } from "./types";
import type { WorkflowOptions } from "./workflow";

export interface ExecuteParams<T, R> {
  db: DatabaseClient;
  runId: string;
  workflowName: string;
  fn: (input: T) => Promise<R>;
  input: T;
  options?: WorkflowOptions<R>;
}

/** Run (or replay) a workflow function to its next checkpoint or terminal state. */
export async function executeRun<T, R>(params: ExecuteParams<T, R>): Promise<WorkflowRun<R>> {
  throw new Error(`Not implemented: executeRun(${params.workflowName})`);
}

/** Pre-load all completed step results for a run in a single query (replay). */
export async function loadCachedSteps(
  _db: DatabaseClient,
  runId: string,
): Promise<WorkflowContext["cachedSteps"]> {
  throw new Error(`Not implemented: loadCachedSteps(${runId})`);
}
