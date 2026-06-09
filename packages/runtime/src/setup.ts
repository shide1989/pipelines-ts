// setup.ts — apply the schema (tables, indexes, triggers) idempotently.
//
// schema.sql is the canonical DDL: it includes plpgsql functions, the pg_notify
// wakeup trigger, and enum types that an ORM/migration tool can't express
// cleanly — so it's raw SQL, run through the agnostic DatabaseClient. It's
// idempotent (IF NOT EXISTS / guarded CREATE TYPE / DROP+CREATE TRIGGER), so
// calling setup() repeatedly is safe.

import { readFileSync } from "node:fs";
import type { DatabaseClient } from "./db";

const SCHEMA_PATH = new URL("../schema.sql", import.meta.url);

/** Apply the schema. Pass `sql` to override the on-disk schema.sql (e.g. tests). */
export async function setup(db: DatabaseClient, sql?: string): Promise<void> {
  await db.query(sql ?? readFileSync(SCHEMA_PATH, "utf8"));
}
