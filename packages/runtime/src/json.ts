// JSONB (de)serialization owned by the runtime, not the driver.
//
// Drivers disagree on JSONB: some hand back a parsed object, some (porsager via
// `unsafe`) a raw string. So the runtime serializes explicitly on write (always
// paired with a `$n::jsonb` cast in the SQL) and tolerates either shape on read.

/** Serialize a value for a JSONB column param. Always cast the placeholder `::jsonb`. */
export function toJsonb(value: unknown): string {
  return JSON.stringify(value ?? null);
}

/** Read a JSONB column the driver may return as string OR object. SQL NULL → undefined. */
export function parseJsonb(value: unknown): unknown {
  if (value === null || value === undefined) return undefined;
  return typeof value === "string" ? JSON.parse(value) : value;
}
