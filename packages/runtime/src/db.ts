// DatabaseClient — the runtime's normalized DB contract.
//
// The runtime is database-client AGNOSTIC: it never imports a concrete driver.
// Drivers name things differently (porsager `unsafe`/`listen`/`end`,
// node-postgres `query`/`on('notification')`/`end`, bun:sql `unsafe`/…/`close`),
// so an application bridges its driver onto these three methods with a thin
// adapter — see examples/agentic/db.ts. Whether a given client can actually
// LISTEN is the client's concern (capability checks come later); the runtime
// just codes against the contract.

/** A live LISTEN subscription; call `unlisten()` to tear it down. */
export interface Subscription {
  unlisten(): Promise<void>;
}

export interface DatabaseClient {
  /** Run a `$1,$2,…`-parameterized statement and return the rows. */
  query<T = unknown>(text: string, params?: unknown[]): Promise<T[]>;
  /** Subscribe to a NOTIFY channel; `payload` is the notification string. */
  listen(channel: string, onNotify: (payload: string) => void): Promise<Subscription>;
  /** Close all underlying connections. */
  close(): Promise<void>;
}
