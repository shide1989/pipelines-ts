// Error types + the serializability guard.

/** Non-retriable failure. Marks the run `failed` immediately, no retries. */
export class FatalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FatalError";
  }
}

/**
 * Internal control-flow signal thrown by `sleep()` to unwind the call stack and
 * suspend the workflow. The engine catches it; it is never surfaced to the user.
 */
export class SleepInterrupt extends Error {
  constructor(public readonly sleepId: string) {
    super(`SleepInterrupt(${sleepId})`);
    this.name = "SleepInterrupt";
  }
}

/**
 * Fail fast at checkpoint write time on values that cannot round-trip through
 * JSONB (circular references, BigInt). Silent drift (Date → string, Map/Set → {})
 * is a documented constraint, not guarded here.
 */
export function assertSerializable(value: unknown, stepId: string): void {
  try {
    JSON.stringify(value);
  } catch (err) {
    throw new FatalError(
      `Step "${stepId}" returned a non-serializable value (circular reference or BigInt): ${(err as Error).message}`,
    );
  }
}
