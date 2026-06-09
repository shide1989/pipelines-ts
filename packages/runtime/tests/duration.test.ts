import { describe, expect, test } from "bun:test";
import { parseDuration } from "../src/sleep";

describe("parseDuration", () => {
  test("'30 seconds' → 30000ms", () => {
    expect(parseDuration("30 seconds")).toBe(30_000);
  });
  test("'1 hour' → 3600000ms", () => {
    expect(parseDuration("1 hour")).toBe(3_600_000);
  });
  test("'30 minutes' → 1800000ms", () => {
    expect(parseDuration("30 minutes")).toBe(1_800_000);
  });
  test("'7 days' → 604800000ms", () => {
    expect(parseDuration("7 days")).toBe(604_800_000);
  });
  test("'2 weeks' → 1209600000ms", () => {
    expect(parseDuration("2 weeks")).toBe(1_209_600_000);
  });
  test("singular unit works", () => {
    expect(parseDuration("1 day")).toBe(86_400_000);
  });
  test("surrounding whitespace tolerated", () => {
    expect(parseDuration("  5 minutes  ")).toBe(300_000);
  });
  test("invalid duration throws", () => {
    expect(() => parseDuration("soon")).toThrow();
    expect(() => parseDuration("5 fortnights")).toThrow();
    expect(() => parseDuration("days")).toThrow();
  });
});
