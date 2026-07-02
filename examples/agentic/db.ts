// The DatabaseClient adapter ("wrapper") for the agentic example.
//
// The runtime is client-agnostic; this is the single place that maps a concrete
// driver onto the runtime's contract. We use Drizzle over porsager `postgres`
// because porsager is the listen-capable driver under Drizzle (bun:sql cannot
// receive NOTIFY). `orm` is exposed for typed reads in the server.

import { drizzle, type PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { DatabaseClient } from "pipelines-ts";
import postgres from "postgres";
import * as schema from "./schema";

export interface AgenticDb {
  client: DatabaseClient;
  orm: PostgresJsDatabase<typeof schema>;
}

// porsager: `.unsafe(text, params)` is parameterized; `.unsafe(text)` with no
// params uses the simple protocol, which setup()'s multi-statement DDL needs.
// The same bridge serves the pool and a reserved session (ReservedSql extends Sql).
const unsafeQuery =
  (sql: Pick<ReturnType<typeof postgres>, "unsafe">) =>
  <T>(text: string, params: unknown[] = []) =>
    (params.length ? sql.unsafe(text, params as never[]) : sql.unsafe(text)) as unknown as Promise<
      T[]
    >;

export function createDb(url: string): AgenticDb {
  const sql = postgres(url, { onnotice: () => {} });
  const orm = drizzle(sql, { schema });

  const client: DatabaseClient = {
    query: unsafeQuery(sql),
    listen: async (channel, onNotify) => {
      const { unlisten } = await sql.listen(channel, onNotify);
      return { unlisten };
    },
    // Pin one connection for session-scoped state (the worker's advisory locks).
    reserve: async () => {
      const reserved = await sql.reserve();
      return {
        query: unsafeQuery(reserved),
        release: async () => reserved.release(),
      };
    },
    close: () => sql.end(),
  };

  return { client, orm };
}
