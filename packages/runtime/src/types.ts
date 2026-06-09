// Shared types for the durable runtime.

export type WorkflowStatus = "pending" | "running" | "suspended" | "completed" | "failed";

export type StepStatus = "running" | "completed" | "failed";

export type TimerStatus = "waiting" | "fired";

/** A single workflow execution, mirrors the `workflow_runs` row. */
export interface WorkflowRun<R = unknown> {
  id: string;
  workflowName: string;
  input: unknown;
  output?: R;
  status: WorkflowStatus;
  error?: string;
  idempotencyKey?: string;
  createdAt: Date;
  updatedAt: Date;
  // Populated by getRun() (the detail view), omitted by listRuns().
  steps?: StepResult[];
  logs?: LogEntry[];
}

/** A step result row from `workflow_steps`. */
export interface StepResult<T = unknown> {
  stepId: string;
  output?: T;
  error?: string;
  status: StepStatus;
  attempts: number;
}

/** A derived event from the append-only `workflow_logs` stream. */
export interface LogEntry {
  id: number;
  runId: string;
  eventType: string;
  payload: unknown;
  createdAt: Date;
}

/** What `.run()` returns: a submitted (not yet executed) run. */
export interface RunSubmission {
  runId: string;
  status: WorkflowStatus;
}

/** Per-step retry behaviour. */
export interface RetryPolicy {
  maxRetries: number;
  backoffMs: number;
  backoffMultiplier: number;
}

export interface LifecycleResult<R> {
  runId: string;
  workflowName: string;
  status: "completed" | "failed";
  output?: R;
  error?: string;
}

export interface LifecycleError {
  runId: string;
  workflowName: string;
  error: string;
}
