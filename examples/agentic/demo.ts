// One-command runbook for the agentic example. Boots the HTTP server
// (server.ts — the application running the worker) as a subprocess, then drives
// a processTask run entirely over HTTP: submit → poll → done. No curl to remember.
//
//   docker compose up -d
//   bun run examples/agentic/demo.ts

import type { WorkflowRun } from "pipelines";

const BASE = "http://localhost:3000";

const server = Bun.spawn(["bun", "run", `${import.meta.dir}/server.ts`], {
  stdout: "ignore",
  stderr: "inherit",
  env: process.env,
});

async function poll<T>(fn: () => Promise<T | undefined>, timeoutMs = 15_000): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const v = await fn();
      if (v !== undefined) return v;
    } catch {
      // server not up yet — keep waiting
    }
    await Bun.sleep(150);
  }
  throw new Error("timed out");
}

const submit = (input: unknown) =>
  fetch(`${BASE}/workflows/processTask/run`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ input }),
  }).then((r) => r.json()) as Promise<{ runId: string; status: string }>;

const getRun = (runId: string) =>
  fetch(`${BASE}/workflows/processTask/runs/${runId}`).then((r) =>
    r.json(),
  ) as Promise<WorkflowRun>;

const untilStatus = (runId: string, ...statuses: string[]) =>
  poll(async () => {
    const run = await getRun(runId);
    return statuses.includes(run.status) ? run : undefined;
  });

function show(label: string, run: WorkflowRun): void {
  const steps = (run.steps ?? []).map((s) => `${s.stepId}(${s.status})`).join(", ") || "—";
  console.log(`\n${label}`);
  console.log(`  status: ${run.status}`);
  console.log(`  steps:  ${steps}`);
  console.log(`  events: ${(run.logs ?? []).map((l) => l.eventType).join(" → ")}`);
  if (run.output !== undefined) console.log(`  output: ${JSON.stringify(run.output)}`);
}

try {
  await poll(async () =>
    (await fetch(`${BASE}/workflows/processTask/runs`)).ok ? true : undefined,
  );
  console.log("server up · submitting processTask…");

  const { runId, status } = await submit({ prompt: "Summarize the report", docId: "doc_42" });
  console.log(`runId ${runId} returned immediately (status: ${status})`);

  show(
    "⏸  suspended on the durable sleep (zero compute while waiting):",
    await untilStatus(runId, "suspended"),
  );
  show(
    "✅ finished (cached steps skipped on resume — no re-pay):",
    await untilStatus(runId, "completed", "failed"),
  );
} finally {
  server.kill();
}
