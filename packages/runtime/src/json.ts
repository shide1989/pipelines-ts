// JSONB (de)serialization owned by the runtime, not the driver.
//
// Drivers MUST be kept out of the (de)serialization path, in both directions:
//  - Write: a bare `$n::jsonb` cast makes PG infer the param as jsonb, and
//    drivers (porsager included) auto-JSON.stringify jsonb params — double-
//    encoding our pre-stringified value. `$n::text::jsonb` pins the param to
//    text, so the string crosses the wire untouched and PG parses it once.
//  - Read: drivers disagree on jsonb columns (parsed object vs raw string —
//    ambiguous when the stored value IS a string). A `::text` cast on the
//    column makes every driver return the same raw JSON text.
// So: text in, text out; the runtime is the only (de)serializer.

/** Serialize a value for a JSONB column param. Always cast the placeholder `::text::jsonb`. */
export function toJsonb(value: unknown): string {
  return JSON.stringify(value ?? null);
}

/** Parse a JSONB column read with a `::text` cast. SQL NULL → undefined. */
export function parseJsonb(value: string | null | undefined): unknown {
  return value == null ? undefined : JSON.parse(value);
}
