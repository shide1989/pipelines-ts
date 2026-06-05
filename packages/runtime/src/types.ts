// Shared types for the durable runtime.

export type WorkflowStatus = "running" | "suspended" | "completed" | "failed";

export type StepStatus = "completed" | "failed";

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
  retryCount: number;
  createdAt: Date;
  updatedAt: Date;
}

/** A cached step output, mirrors the `workflow_steps` row. */
export interface StepResult<T = unknown> {
  stepId: string;
  output?: T;
  error?: string;
  status: StepStatus;
}

/** Retry behaviour applied to step execution. */
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
