// A real worker process, spawned by multiworker.test.ts (one OS process each —
// own Postgres sessions, own advisory locks, killable with SIGKILL).

import { startWorker } from "../../src/worker";
import { testDb } from "../helpers";
import "./workflows"; // workflows self-register on import

const db = testDb();
startWorker(db, { maxTimerSleepMs: 100, reconcileMs: 300 });

// porsager multiplexes all LISTENs onto one dedicated connection; this probe
// resolving means that connection is live — including the worker's
// 'workflow_runs' subscription registered just above. Only then are we "ready".
await db.listen("mw_warmup", () => {});
console.log(`ready ${process.pid}`);

await new Promise(() => {}); // stay alive until the test kills us
