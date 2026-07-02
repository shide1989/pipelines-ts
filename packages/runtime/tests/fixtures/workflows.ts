// Workflows for the multi-process worker tests. Imported by worker-main.ts
// (where they execute) and by multiworker.test.ts (for the submit handles).
//
// In-memory counters can't observe execution across processes, so the step
// records who-ran-what in a side table (mw_exec, created by the test): one row
// per execution, keyed by the run's input key + the executing process pid.
// The row is written at step START, so a kill mid-step leaves it behind — that
// is how the test finds which worker to murder, and how a reclaimed re-run
// shows up as a second row from a different pid.

import postgres from "postgres";
import { checkpoint, workflow } from "../../src/index";
import { TEST_URL } from "../helpers";

// Lazy client: opens connections only when a step executes (worker processes),
// never in the test process that merely imports the handles.
const sql = postgres(TEST_URL, { max: 2, onnotice: () => {} });

const steps = checkpoint({
  work: async (input: { key: string; ms: number }) => {
    await sql`INSERT INTO mw_exec (key, pid) VALUES (${input.key}, ${process.pid})`;
    await Bun.sleep(input.ms);
    return { by: process.pid };
  },
});

/** Sleeps `ms` inside a durable step and returns the executing worker's pid. */
export const mwWork = workflow("mw.work", async (input: { key: string; ms: number }) => {
  const result = await steps.work(input);
  return result.by;
});
