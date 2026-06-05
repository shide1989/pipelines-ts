// Thin Bun.serve adapter over the Management API. NOT part of the runtime —
// the transport is an application concern. No auth (production would need it).
//
//   POST /workflows/:name/run             → handle.run(input) → { runId }
//   GET  /workflows/:name/runs/:runId      → getRun(db, runId)
//   GET  /workflows/:name/runs             → listRuns(db, name)
//   POST /workflows/:name/runs/:runId/replay → replayRun(db, runId)

import { createDatabaseClient, getRun, listRuns, replayRun, startTimerWorker } from "pipelines";
import { processTask } from "./workflow";

const PORT = 3000;

export function main(): void {
  const db = createDatabaseClient(process.env.DATABASE_URL ?? "");
  startTimerWorker(db, { intervalMs: 5000 });

  // Route table wiring lives here once the runtime is implemented.
  void { db, processTask, getRun, listRuns, replayRun, PORT };
  throw new Error("Not implemented: server bootstrap");
}

main();
