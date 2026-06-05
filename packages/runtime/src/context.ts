// WorkflowContext + the AsyncLocalStorage store that connects the pieces.

import { AsyncLocalStorage } from "node:async_hooks";
import type { DatabaseClient } from "./db";
import type { StepResult } from "./types";

/** Per-run state. Created by `workflow()`, never exposed to user code. */
export interface WorkflowContext {
  runId: string;
  workflowName: string;
  db: DatabaseClient;
  /** Per-method call counters: "createUser" → 2 (called twice). */
  stepCounters: Map<string, number>;
  sleepCounter: number;
  /** Step results pre-loaded on replay, keyed by step ID. */
  cachedSteps: Map<string, StepResult>;
}

/** `durable()` proxies and `sleep()` read the active context from here. */
export const workflowStorage = new AsyncLocalStorage<WorkflowContext>();
