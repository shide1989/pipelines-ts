// DatabaseClient seam — the only place the runtime touches a concrete driver.
// Default impl uses `bun:sql`; swap this file to run on Node (`postgres`).

/** Minimal query surface the engine depends on. Keep it driver-agnostic. */
export interface DatabaseClient {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]>;
  close(): Promise<void>;
}

/** Build a `bun:sql`-backed client from a Postgres connection string. */
export function createDatabaseClient(connectionString: string): DatabaseClient {
  throw new Error(`Not implemented: createDatabaseClient(${connectionString})`);
}
