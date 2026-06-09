import { describe, expect, test } from "bun:test";
import { assertSerializable, FatalError, SleepInterrupt } from "../src/errors";

describe("FatalError", () => {
  test("is an Error with the right name", () => {
    const err = new FatalError("nope");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("FatalError");
    expect(err.message).toBe("nope");
  });
});

describe("SleepInterrupt", () => {
  test("carries the sleep ID", () => {
    const err = new SleepInterrupt("sleep:3");
    expect(err.sleepId).toBe("sleep:3");
    expect(err.name).toBe("SleepInterrupt");
  });
});

describe("assertSerializable", () => {
  test("passes plain JSON-safe values", () => {
    expect(() => assertSerializable({ a: 1, b: [2, 3], c: "x" }, "s:0")).not.toThrow();
    expect(() => assertSerializable(null, "s:0")).not.toThrow();
  });

  test("throws FatalError on circular references", () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(() => assertSerializable(circular, "s:0")).toThrow(FatalError);
  });

  test("throws FatalError on BigInt", () => {
    expect(() => assertSerializable({ big: 10n }, "s:0")).toThrow(FatalError);
  });
});
