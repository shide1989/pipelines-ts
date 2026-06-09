// engine.ts — claim a run, execute/replay it, drive lifecycle transitions.
//
// Retries are per-step (see proxy.ts), so the engine runs the function ONCE: it
// either completes, suspends on a sleep, or fails (a step that exhausted its
// retries, or a FatalError, propagates here). The worker calls claimAndExecute();
// the function never runs inline at submission time.

import type { WorkflowContext } from "./context";
import { workflowStorage } from "./context";
import type { DatabaseClient } from "./db";
import { SleepInterrupt } from "./errors";
import { parseJsonb, toJsonb } from "./json";
import { appendLog } from "./log";
import type { RetryPolicy, WorkflowRun } from "./types";
import { getRegisteredWorkflow, type RegisteredWorkflow } from "./workflow";

const DEFAULT_RETRY: RetryPolicy = { maxRetries: 3, backoffMs: 1000, backoffMultiplier: 2 };

interface ClaimRow {
  workflow_name: string;
  input: unknown;
  prev_status: string;
}

/**
 * Atomically claim a `pending`/`suspended` run and run it to its next checkpoint
 * or terminal state. No-op if another worker already claimed it (the WHERE guard
 * + row lock make the transition single-winner without explicit transactions).
 */
export async function claimAndExecute(db: DatabaseClient, runId: string): Promise<void> {
  const claimed = await db.query<ClaimRow>(
    `WITH cur AS (SELECT status FROM workflow_runs WHERE id = $1 FOR UPDATE)
     UPDATE workflow_runs r SET status = 'running'
     FROM cur
     WHERE r.id = $1 AND r.status IN ('pending', 'suspended')
     RETURNING r.workflow_name, r.input, cur.status AS prev_status`,
    [runId],
  );
  const row = claimed[0];
  if (!row) return; // not claimable: already running/terminal, or lost the race

  const def = getRegisteredWorkflow(row.workflow_name);
  if (!def) {
    await failRun(
      db,
      runId,
      row.workflow_name,
      `workflow "${row.workflow_name}" not registered in this worker`,
    );
    return;
  }

  await appendLog(db, runId, row.prev_status === "suspended" ? "run.resumed" : "run.started");

  const ctx: WorkflowContext = {
    runId,
    workflowName: row.workflow_name,
    db,
    retry: { ...DEFAULT_RETRY, ...def.options?.retry },
    stepCounters: new Map(),
    sleepCounter: 0,
    cachedSteps: await loadCachedSteps(db, runId),
  };

  try {
    const output = await workflowStorage.run(ctx, () => def.fn(parseJsonb(row.input)));
    await db.query(
      "UPDATE workflow_runs SET status = 'completed', output = $2::jsonb, error = NULL WHERE id = $1",
      [runId, toJsonb(output)],
    );
    await appendLog(db, runId, "run.completed");
    await fireHooks(def, runId, row.workflow_name, "completed", output, undefined);
  } catch (err) {
    if (err instanceof SleepInterrupt) {
      // sleep() already set status = 'suspended'; suspension is not terminal.
      await appendLog(db, runId, "run.suspended", { sleepId: err.sleepId });
      return;
    }
    const message = err instanceof Error ? err.message : String(err);
    await failRun(db, runId, row.workflow_name, message, def);
  }
}

async function failRun(
  db: DatabaseClient,
  runId: string,
  workflowName: string,
  error: string,
  def?: RegisteredWorkflow,
): Promise<void> {
  await db.query("UPDATE workflow_runs SET status = 'failed', error = $2 WHERE id = $1", [
    runId,
    error,
  ]);
  await appendLog(db, runId, "run.failed", { error });
  if (def) await fireHooks(def, runId, workflowName, "failed", undefined, error);
}

async function fireHooks(
  def: RegisteredWorkflow,
  runId: string,
  workflowName: string,
  status: "completed" | "failed",
  output: unknown,
  error: string | undefined,
): Promise<void> {
  await safeHook(() => def.options?.onFinish?.({ runId, workflowName, status, output, error }));
  if (status === "failed") {
    await safeHook(() =>
      def.options?.onError?.({ runId, workflowName, error: error ?? "unknown error" }),
    );
  }
}

async function safeHook(run: () => void | Promise<void> | undefined): Promise<void> {
  try {
    await run();
  } catch (err) {
    console.error("[pipelines] lifecycle hook threw (ignored):", err);
  }
}

/** Pre-load completed step outputs for a run in one query (replay fast-forward). */
export async function loadCachedSteps(
  db: DatabaseClient,
  runId: string,
): Promise<Map<string, unknown>> {
  const rows = await db.query<{ step_id: string; output: unknown }>(
    "SELECT step_id, output FROM workflow_steps WHERE run_id = $1 AND status = 'completed'",
    [runId],
  );
  const map = new Map<string, unknown>();
  for (const row of rows) map.set(row.step_id, parseJsonb(row.output));
  return map;
}

// --- Row mapping ------------------------------------------------------------

interface RunRow {
  id: string;
  workflow_name: string;
  input: unknown;
  output: unknown;
  status: WorkflowRun["status"];
  error: string | null;
  idempotency_key: string | null;
  created_at: string | Date;
  updated_at: string | Date;
}

/** Map a raw snake_case run row into the public WorkflowRun shape. */
export function mapRun<R = unknown>(row: RunRow): WorkflowRun<R> {
  return {
    id: row.id,
    workflowName: row.workflow_name,
    input: parseJsonb(row.input),
    output: parseJsonb(row.output) as R | undefined,
    status: row.status,
    error: row.error ?? undefined,
    idempotencyKey: row.idempotency_key ?? undefined,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}
