// WorkflowContext + the AsyncLocalStorage store that connects the pieces.

import { AsyncLocalStorage } from "node:async_hooks";
import type { DatabaseClient } from "./db";
import type { RetryPolicy } from "./types";

/** Per-run state. Built by the engine when it claims a run, never user-facing. */
export interface WorkflowContext {
  runId: string;
  workflowName: string;
  db: DatabaseClient;
  /** Resolved retry policy applied per durable step. */
  retry: RetryPolicy;
  /** Per-method call counters. */
  stepCounters: Map<string, number>;
  sleepCounter: number;
  /** Completed step outputs pre-loaded on replay, keyed by step ID. */
  cachedSteps: Map<string, unknown>;
}

/** `durable()` proxies and `sleep()` read the active context from here. */
export const workflowStorage = new AsyncLocalStorage<WorkflowContext>();
