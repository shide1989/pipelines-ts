// durable() — Proxy-based step wrapper.
//
// Wraps an object of async functions and returns a structurally identical Proxy.
// The `get` trap captures the method name (step ID base), reads the active
// WorkflowContext from AsyncLocalStorage, derives a deterministic step ID
// ("name:callIndex"), and checks the cache: hit → return cached, miss → execute,
// guard serializability, persist, return. Falls through to direct execution when
// there is no active context (steps stay testable outside workflows).

// biome-ignore lint/suspicious/noExplicitAny: structural constraint over arbitrary async methods.
type AsyncSteps = Record<string, (...args: any[]) => Promise<any>>;

export function durable<T extends AsyncSteps>(steps: T): T {
  throw new Error(`Not implemented: durable(${Object.keys(steps).length} steps)`);
}
