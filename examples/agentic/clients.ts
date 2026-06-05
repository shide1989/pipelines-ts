// Stubbed external dependencies for the agentic example.
// Replace these with real clients (vector store, provider Batch API, etc.).

export async function loadDocs(docId: string): Promise<string> {
  throw new Error(`Not implemented: loadDocs(${docId})`);
}

export interface BatchStatus {
  status: "running" | "completed";
  output?: string;
}

export const llm = {
  async submitBatch(_input: { prompt: string; context: string }): Promise<{ jobId: string }> {
    throw new Error("Not implemented: llm.submitBatch");
  },
  async getBatchStatus(jobId: string): Promise<BatchStatus> {
    throw new Error(`Not implemented: llm.getBatchStatus(${jobId})`);
  },
};
