// Primary example: task intake → context → batch inference → durable poll → validate.
// Exercises every core feature: step caching, durable sleep, per-method counters,
// retry, and replay (a crash after inference replays the cached result, not a re-pay).

import { durable, FatalError, sleep, workflow } from "pipelines";
import { llm, loadDocs } from "./clients";

const steps = durable({
  prepareContext: async (task: { prompt: string; docId: string }) => {
    const context = await loadDocs(task.docId); // fetch + assemble context window
    return { prompt: task.prompt, context };
  },

  // Kick off a long-running batch inference job; returns a handle, not the result.
  submitInference: async (input: { prompt: string; context: string }) => {
    const { jobId } = await llm.submitBatch(input);
    return { jobId };
  },

  // Poll the provider for job status (cheap, may be flaky → retried by the engine).
  checkInference: async (input: { jobId: string }) => {
    return await llm.getBatchStatus(input.jobId);
  },

  validateOutput: async (input: { output: string }) => {
    if (!input.output?.trim()) throw new FatalError("empty LLM output"); // non-retriable
    return { output: input.output };
  },
});

export const processTask = workflow(
  "processTask",
  async (task: { prompt: string; docId: string }) => {
    const ctx = await steps.prepareContext(task);
    const { jobId } = await steps.submitInference(ctx); // cached → never re-submitted on replay

    // Durable polling loop: zero compute while sleeping, survives restarts.
    // "2 seconds" keeps the demo quick — real batch jobs poll over minutes/hours;
    // the durability guarantee is identical at any duration.
    let result = await steps.checkInference({ jobId });
    while (result.status !== "completed") {
      await sleep("2 seconds");
      result = await steps.checkInference({ jobId });
    }

    // Loop exits only on status === "completed", which guarantees output is set.
    // biome-ignore lint/style/noNonNullAssertion: narrowed by the loop condition above.
    const validated = await steps.validateOutput({ output: result.output! });
    return { output: validated.output };
  },
);
