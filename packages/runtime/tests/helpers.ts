// Integration-test helpers: a porsager-backed DatabaseClient + schema lifecycle.
// These tests run against a real Postgres (docker compose up).

import postgres from "postgres";
import type { DatabaseClient } from "../src/db";
import { setup } from "../src/setup";

export const TEST_URL =
  process.env.DATABASE_URL ?? "postgres://pipelines:pipelines@localhost:5432/pipelines";

export interface TestDb extends DatabaseClient {
  raw: ReturnType<typeof postgres>;
}

// porsager: `.unsafe(text, params)` is parameterized; `.unsafe(text)` with no
// params uses the simple protocol (multi-statement DDL). Same bridge for the
// pool and for a reserved session (ReservedSql extends Sql).
const unsafeQuery =
  (sql: Pick<ReturnType<typeof postgres>, "unsafe">) =>
  <T>(text: string, params: unknown[] = []) =>
    (params.length ? sql.unsafe(text, params as never[]) : sql.unsafe(text)) as unknown as Promise<
      T[]
    >;

/** A DatabaseClient adapter over porsager — the same shape as the example's wrapper. */
export function testDb(): TestDb {
  const sql = postgres(TEST_URL, { onnotice: () => {} });
  return {
    raw: sql,
    query: unsafeQuery(sql),
    listen: async (channel, onNotify) => {
      const { unlisten } = await sql.listen(channel, onNotify);
      return { unlisten };
    },
    reserve: async () => {
      const reserved = await sql.reserve();
      return {
        query: unsafeQuery(reserved),
        release: async () => reserved.release(),
      };
    },
    close: () => sql.end(),
  };
}

/**
 * Acquire the worker's advisory lock for a run on a SEPARATE session —
 * simulates another live worker actively executing it. The returned function
 * ends that session, which is exactly how a real worker "dies".
 */
export async function holdAdvisoryLock(runId: string): Promise<() => Promise<void>> {
  const sql = postgres(TEST_URL, { max: 1, onnotice: () => {} });
  const [row] = await sql`SELECT pg_try_advisory_lock(hashtextextended(${runId}::text, 0)) AS ok`;
  if (!row?.ok) {
    await sql.end();
    throw new Error(`could not acquire test lock for ${runId}`);
  }
  return () => sql.end();
}

/** Drop and re-apply the schema from scratch (clean slate for the suite). */
export async function resetSchema(db: DatabaseClient): Promise<void> {
  await db.query("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
  await setup(db);
}

/** Wipe all rows + reset identity between tests. */
export async function truncateAll(db: DatabaseClient): Promise<void> {
  await db.query(
    "TRUNCATE workflow_runs, workflow_steps, workflow_timers, workflow_logs RESTART IDENTITY CASCADE",
  );
}

/** Poll `cond` until true or timeout. */
export async function waitFor(
  cond: () => Promise<boolean>,
  timeoutMs = 5000,
  stepMs = 25,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await cond()) return;
    await Bun.sleep(stepMs);
  }
  throw new Error(`waitFor: timed out after ${timeoutMs}ms`);
}
