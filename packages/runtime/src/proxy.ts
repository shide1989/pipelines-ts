import { workflowStorage } from "./context";
import { assertSerializable, FatalError } from "./errors";
import { toJsonb } from "./json";

// biome-ignore lint/suspicious/noExplicitAny: structural constraint over arbitrary async methods.
type AsyncSteps = Record<string, (...args: any[]) => Promise<any>>;

const sleepMs = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * checkpoint() — Proxy-based step wrapper, that saves each step as a state so you can resume it later.
 *
 * The `get` trap captures the method name (step ID base), reads the active WorkflowContext from AsyncLocalStorage,
 * derives a deterministic step ID ("name:callIndex"), and on cache miss: writes an intent row (status='running').
 * It executes with per-step retry (incrementing `attempts`), guards serializability, persists the result as 'completed', and logs.
 *
 * Throws when called outside a workflow context unless `allowUnbound: true` is set (for scripts/utilities).
*/
export function checkpoint<T extends AsyncSteps>(steps: T, options?: { allowUnbound?: boolean }): T {
  return new Proxy(steps, {
    get(target, prop, receiver) {
      const fn = Reflect.get(target, prop, receiver);
      if (typeof prop !== "string" || typeof fn !== "function") return fn;

      return async (...args: unknown[]) => {
        const ctx = workflowStorage.getStore();
        if (!ctx) {
          if (!options?.allowUnbound)
            throw new Error(`step "${prop}" called outside workflow context`);
          return fn(...args);
        }

        const index = ctx.stepCounters.get(prop) ?? 0;
        ctx.stepCounters.set(prop, index + 1);
        const stepId = `${prop}:${index}`;

        // Cache hit (replay): return the persisted output, don't re-execute.
        if (ctx.cachedSteps.has(stepId)) return ctx.cachedSteps.get(stepId);

        // Intent row: a leftover 'running' row (worker died mid-step) is in-doubt,
        // NOT a cache hit, so it re-executes here (at-least-once). The log row is
        // folded into the same statement (data-modifying CTE): one round-trip,
        // one commit, and the state write + its log line are atomic.
        await ctx.db.query(
          `WITH step AS (
             INSERT INTO workflow_steps (run_id, step_id, status, attempts)
             VALUES ($1, $2, 'running', 0)
             ON CONFLICT (run_id, step_id) DO UPDATE SET status = 'running'
           )
           INSERT INTO workflow_logs (run_id, event_type, payload)
           VALUES ($1, 'step.running', $3::text::jsonb)`,
          [ctx.runId, stepId, toJsonb({ stepId })],
        );

        const { maxRetries, backoffMs, backoffMultiplier } = ctx.retry;
        let attempt = 0; // failures so far
        for (;;) {
          try {
            const output = await fn(...args);
            assertSerializable(output, stepId); // throws FatalError → caught below
            await ctx.db.query(
              `WITH step AS (
                 UPDATE workflow_steps SET status = 'completed', output = $3::text::jsonb, attempts = $4
                 WHERE run_id = $1 AND step_id = $2
               )
               INSERT INTO workflow_logs (run_id, event_type, payload)
               VALUES ($1, 'step.completed', $5::text::jsonb)`,
              [
                ctx.runId,
                stepId,
                toJsonb(output),
                attempt + 1,
                toJsonb({ stepId, attempts: attempt + 1 }),
              ],
            );
            ctx.cachedSteps.set(stepId, output);
            return output;
          } catch (err) {
            attempt += 1;
            await ctx.db.query(
              "UPDATE workflow_steps SET attempts = $3 WHERE run_id = $1 AND step_id = $2",
              [ctx.runId, stepId, attempt],
            );

            // FatalError or retries exhausted → mark the step failed and propagate
            // (the engine turns this into a failed run).
            if (err instanceof FatalError || attempt > maxRetries) {
              const message = err instanceof Error ? err.message : String(err);
              await ctx.db.query(
                `WITH step AS (
                   UPDATE workflow_steps SET status = 'failed', error = $3
                   WHERE run_id = $1 AND step_id = $2
                 )
                 INSERT INTO workflow_logs (run_id, event_type, payload)
                 VALUES ($1, 'step.failed', $4::text::jsonb)`,
                [
                  ctx.runId,
                  stepId,
                  message,
                  toJsonb({ stepId, attempts: attempt, error: message }),
                ],
              );
              throw err;
            }
            await sleepMs(backoffMs * backoffMultiplier ** (attempt - 1));
          }
        }
      };
    },
  });
}
