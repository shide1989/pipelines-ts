// Thin Bun.serve adapter over the Management API. NOT part of the runtime —
// transport is an application concern. No auth (production would need it).
// Uses Bun's native `routes` (typed `req.params`) — no manual path parsing.

import { desc, eq } from "drizzle-orm";
import { getRun, replayRun, setDefaultDb, setup, startWorker } from "pipelines-ts";
import { createDb } from "./db";
import { workflowRuns } from "./schema";
import { processTask, type Task } from "./workflow";

const PORT = 3000;
const json = (body: unknown, status = 200) => Response.json(body, { status });

const url = process.env.DATABASE_URL ?? "postgres://pipelines:pipelines@localhost:5432/pipelines";
const { client, orm } = createDb(url);

await setup(client); // apply schema (idempotent)
setDefaultDb(client); // handle.run() needs a db
startWorker(client, { maxTimerSleepMs: 1000 }); // LISTEN new runs + adaptive timer poll + recovery

Bun.serve({
  port: PORT,
  routes: {
    // Submit a run → { runId, status: "pending" } (does not execute inline).
    // Route is literal: this handler only knows the processTask handle
    "/workflows/processTask/run": {
      POST: async (req) => {
        const { input } = (await req.json()) as { input: Task };
        return json(await processTask.run(input));
      },
    },
    // Typed list via Drizzle
    "/workflows/:name/runs": {
      GET: async (req) =>
        json(
          await orm
            .select()
            .from(workflowRuns)
            .where(eq(workflowRuns.workflowName, req.params.name))
            .orderBy(desc(workflowRuns.createdAt))
            .limit(50),
        ),
    },
    // Run detail (incl. steps + logs)
    "/workflows/:name/runs/:runId": {
      GET: async (req) => {
        const run = await getRun(client, req.params.runId);
        return run ? json(run) : json({ error: "not found" }, 404);
      },
    },
    // Force re-execution of a finished run
    "/workflows/:name/runs/:runId/replay": {
      POST: async (req) => json(await replayRun(client, req.params.runId)),
    },
  },
  error: (err) => json({ error: err.message }, 500),
});

console.log(`[agentic] listening on http://localhost:${PORT}`);
