// workflow(name, fn) — registration + submission.
//
// Two jobs:
//  1. Register fn in an in-process registry keyed by name. The worker resolves
//     implementations by name at execution time, so workflows must be imported
//     in the worker process (they self-register on import, like Temporal).
//  2. Return a handle whose `.run()` SUBMITS a run: INSERT a 'pending' row and
//     return immediately. It does NOT execute inline — the INSERT trigger fires
//     pg_notify('workflow_runs', id) and a worker picks it up. The pending row is
//     the durable queue, so the submitter can crash and the work still happens.

import type { DatabaseClient } from "./db";
import { toJsonb } from "./json";
import type {
  LifecycleError,
  LifecycleResult,
  RetryPolicy,
  RunSubmission,
  WorkflowStatus,
} from "./types";

export interface WorkflowOptions<R> {
  retry?: Partial<RetryPolicy>;
  /** Fires on terminal state (completed | failed), never on suspended. */
  onFinish?: (result: LifecycleResult<R>) => void | Promise<void>;
  /** Fires only on failed, after retries are exhausted. */
  onError?: (info: LifecycleError) => void | Promise<void>;
}

export interface WorkflowHandle<T> {
  run: (input: T, options?: { idempotencyKey?: string }) => Promise<RunSubmission>;
}

/** A registered workflow, type-erased for the worker to resolve + execute by name. */
export interface RegisteredWorkflow {
  fn: (input: unknown) => Promise<unknown>;
  options?: WorkflowOptions<unknown>;
}

const registry = new Map<string, RegisteredWorkflow>();

export function getRegisteredWorkflow(name: string): RegisteredWorkflow | undefined {
  return registry.get(name);
}

export function workflow<T, R>(
  name: string,
  fn: (input: T) => Promise<R>,
  options?: WorkflowOptions<R>,
): WorkflowHandle<T> {
  // Last-import-wins would silently route runs to the wrong fn — fail at import.
  if (registry.has(name)) throw new Error(`workflow "${name}" is already registered`);
  registry.set(name, {
    fn: fn as (input: unknown) => Promise<unknown>,
    options: options as WorkflowOptions<unknown> | undefined,
  });

  return {
    run: (input, runOptions) => submitRun(getDefaultDb(), name, input, runOptions?.idempotencyKey),
  };
}

interface SubmitRow {
  id: string;
  status: WorkflowStatus;
}

/** Insert a pending run (or return the existing one for the same idempotency key). */
async function submitRun(
  db: DatabaseClient,
  name: string,
  input: unknown,
  idempotencyKey?: string,
): Promise<RunSubmission> {
  // INSERT-first (not SELECT-then-INSERT, which races concurrent same-key
  // submits into a unique violation). NULL keys never conflict, so the no-key
  // path always returns from the INSERT; DO NOTHING means the fallback SELECT
  // only runs for a key that already exists.
  const inserted = await db.query<SubmitRow>(
    `INSERT INTO workflow_runs (workflow_name, input, idempotency_key, status)
     VALUES ($1, $2::text::jsonb, $3, 'pending')
     ON CONFLICT (workflow_name, idempotency_key) DO NOTHING
     RETURNING id, status`,
    [name, toJsonb(input), idempotencyKey ?? null],
  );
  const row =
    inserted[0] ??
    (
      await db.query<SubmitRow>(
        "SELECT id, status FROM workflow_runs WHERE workflow_name = $1 AND idempotency_key = $2",
        [name, idempotencyKey],
      )
    )[0];
  if (!row) throw new Error(`Failed to submit run for workflow "${name}"`);
  return { runId: row.id, status: row.status };
}

// --- Default DB handle ------------------------------------------------------
// `.run()` needs a DatabaseClient. The runtime is a library, so the app supplies
// one via setDefaultDb (the example server does this on boot).

let defaultDb: DatabaseClient | undefined;

export function setDefaultDb(db: DatabaseClient): void {
  defaultDb = db;
}

function getDefaultDb(): DatabaseClient {
  if (!defaultDb) {
    throw new Error(
      "No database configured. Call setDefaultDb(client) before submitting workflows.",
    );
  }
  return defaultDb;
}
