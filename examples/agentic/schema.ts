// Drizzle typed schema — a typed mirror of the runtime's tables (schema.sql is
// the canonical DDL, applied via setup()). Used for typed reads in the server,
// demonstrating that the runtime drives any client through the DatabaseClient seam.

import {
  bigint,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
} from "drizzle-orm/pg-core";

export const runStatus = pgEnum("run_status", [
  "pending",
  "running",
  "suspended",
  "completed",
  "failed",
]);
export const stepStatus = pgEnum("step_status", ["running", "completed", "failed"]);
export const timerStatus = pgEnum("timer_status", ["waiting", "fired"]);

export const workflowRuns = pgTable("workflow_runs", {
  id: uuid("id").primaryKey().defaultRandom(),
  workflowName: text("workflow_name").notNull(),
  input: jsonb("input").notNull(),
  output: jsonb("output"),
  status: runStatus("status").notNull().default("pending"),
  error: text("error"),
  idempotencyKey: text("idempotency_key"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const workflowSteps = pgTable("workflow_steps", {
  id: uuid("id").primaryKey().defaultRandom(),
  runId: uuid("run_id").notNull(),
  stepId: text("step_id").notNull(),
  output: jsonb("output"),
  error: text("error"),
  status: stepStatus("status").notNull(),
  attempts: integer("attempts").notNull().default(0),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const workflowTimers = pgTable("workflow_timers", {
  id: uuid("id").primaryKey().defaultRandom(),
  runId: uuid("run_id").notNull(),
  sleepId: text("sleep_id").notNull(),
  wakeAt: timestamp("wake_at", { withTimezone: true }).notNull(),
  status: timerStatus("status").notNull().default("waiting"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const workflowLogs = pgTable("workflow_logs", {
  id: bigint("id", { mode: "number" }).primaryKey().generatedAlwaysAsIdentity(),
  runId: uuid("run_id").notNull(),
  eventType: text("event_type").notNull(),
  payload: jsonb("payload").notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
