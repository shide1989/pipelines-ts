// workflow(name, fn) — durable workflow registration.
//
// Wraps an async function: creates a run record, builds a WorkflowContext, runs
// the function inside the AsyncLocalStorage scope, then persists the terminal
// state and fires lifecycle hooks. Returns a handle exposing `.run()`.

import type { LifecycleError, LifecycleResult, RetryPolicy, WorkflowRun } from "./types";

export interface WorkflowOptions<R> {
  retry?: Partial<RetryPolicy>;
  /** Fires on terminal state (completed | failed), never on suspended. */
  onFinish?: (result: LifecycleResult<R>) => void | Promise<void>;
  /** Fires only on failed, after retries are exhausted. */
  onError?: (info: LifecycleError) => void | Promise<void>;
}

export interface WorkflowHandle<T, R> {
  run: (input: T, options?: { idempotencyKey?: string }) => Promise<WorkflowRun<R>>;
}

export function workflow<T, R>(
  name: string,
  _fn: (input: T) => Promise<R>,
  _options?: WorkflowOptions<R>,
): WorkflowHandle<T, R> {
  throw new Error(`Not implemented: workflow(${name})`);
}
