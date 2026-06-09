// Stubbed external dependencies for the agentic example.
// Replace these with real clients (vector store, provider Batch API, etc.).
// This fake batch "completes" after a fixed number of polls so the durable
// poll-loop + sleep + replay path can be exercised end-to-end without a provider.

export async function loadDocs(docId: string): Promise<string> {
  return `[docs for ${docId}]`;
}

export interface BatchStatus {
  status: "running" | "completed";
  output?: string;
}

const POLLS_UNTIL_DONE = 2;
const polls = new Map<string, number>();

export const llm = {
  async submitBatch(input: { prompt: string; context: string }): Promise<{ jobId: string }> {
    void input;
    return { jobId: crypto.randomUUID() };
  },

  // Reports "completed" on the Nth poll — deterministic and independent of
  // wall-clock, so fast-forwarding the durable sleep drives it to completion.
  // (Each checkInference step is cached, so only fresh polls reach here.)
  async getBatchStatus(jobId: string): Promise<BatchStatus> {
    const n = (polls.get(jobId) ?? 0) + 1;
    polls.set(jobId, n);
    if (n >= POLLS_UNTIL_DONE) {
      return { status: "completed", output: "summary: the report says things are fine." };
    }
    return { status: "running" };
  },
};
