# Feature: Multi-Worker Liveness via Postgres Advisory Locks

**Status:** design (v0.2 — amended after review; v0.1 in the project history)
**Amends:** `SPEC.md` (Component 2 — Worker, §2.1/§2.2). This document is additive; `SPEC.md` is not modified.
**Replaces (when implemented):** the interim lease+heartbeat reclaim shipped in `worker.ts`/`engine.ts` (`staleRunningMs` + `updated_at` heartbeat). That lease made dead-worker reclaim *work*; this design makes it *precise* — no threshold to tune, no reclaim delay on clean crash.

**v0.2 changes:** in-process in-flight set (same-session re-entrancy is a correctness hole, not a nicety); `DatabaseClient` contract extension spelled out; per-worker ownership-connection variant adopted over per-run connections; lease comparison rewritten honestly; test list extended.

---

## Problem

`SPEC.md` distributes *new* work across N workers correctly: `pg_notify` broadcasts a new run to every worker, and the conditional claim (`UPDATE … WHERE status IN ('pending','suspended') RETURNING *`) ensures exactly one executes it. Timer firing is likewise safe via `FOR UPDATE SKIP LOCKED`.

The gap is **worker death**. A worker claims a run (`status='running'`), starts executing, then dies — crash, OOM, partition, `kill -9`. Without recovery the row is stuck `running` forever. The current lease+heartbeat closes this: the engine touches `updated_at` during execution and recovery resets rows stale past `staleRunningMs` to `pending`. It works, but it has a threshold to tune, a reclaim delay equal to that threshold, and a window in which a slow-but-alive worker (heartbeat starved or partitioned) is declared dead and double-executed.

## Decision

Detect liveness with **session-level Postgres advisory locks**, not the lease and not an external coordinator (ZooKeeper/etcd/custom gossip).

Why not keep the lease: it's correct under the at-least-once model, but every parameter is a guess — too short and live runs get stolen, too long and orphans sit dead for minutes. Advisory locks delegate liveness to the **connection/session lifecycle**, which Postgres already tracks: reclaim is instant on clean crash, and there is no heartbeat loop to feed.

Why not an external coordinator: the engine's entire state already lives in Postgres, so Postgres is already the irreducible availability ceiling. A separate coordination service adds a *second* failure domain without removing the first, still suffers the same split-brain (so you'd still need DB-level fencing), and abandons the single-dependency design. It is strictly more fragile here.

**Honest ledger vs the lease** (both reduce to at-least-once — see §limits): advisory locks buy instant reclaim on clean crash, zero schema, zero heartbeat code, and free `pg_locks` observability. They cost a connection-model change and a `DatabaseClient` contract extension. The deciding factor is precision, not safety: the double-execution ceiling is identical, but the lock never *guesses* about liveness.

## Mechanism

### Relevant Postgres semantics (PG docs §13.3.5)

- A **session-level** advisory lock (`pg_advisory_lock` / `pg_try_advisory_lock`) is held until explicitly unlocked **or the session ends**, and is **automatically cleaned up by the server at session end**. This auto-release on session death is the entire basis of the design.
- Session-level locks **do not honor transaction semantics** — a lock taken in a transaction that later rolls back stays held; an unlock sticks even if its transaction fails. So lock/unlock are independent of the surrounding txn.
- Locks are **re-entrant/stacked within a session**: the same session can take the same lock N times and must unlock N times. ⚠ This means `pg_try_advisory_lock` returning `true` does NOT prove no one holds the lock — *we* might, on this very session. Advisory locks are an **inter-process** mechanism only; in-process exclusion must be handled separately (see the in-flight set below).
- Advisory locks live in the shared lock pool (`max_locks_per_transaction × max_connections`), bounded to tens–hundreds of thousands. We hold at most one lock per *actively executing* run, so this is never near the limit.
- `pg_locks` exposes all held advisory locks — useful for observability (which runs are actively executing right now).

### Lock key

One lock per run, keyed by a 64-bit hash of the run id:

```sql
pg_try_advisory_lock( hashtextextended(run_id::text, 0) )   -- bigint key space (2^64)
```

`hashtextextended` (PG 11+) gives a full `int8`, so birthday-collision probability is negligible at our scale. A collision would only cause two *unrelated* runs to mutually exclude — a spurious skip that self-heals on the next scan (**liveness, not safety**; never a double-execution).

### The in-flight set (in-process exclusion — REQUIRED)

Each worker keeps an in-memory `Set<runId>` of runs it is currently executing. Every lock attempt — claim path and reclaim scan alike — checks the set first and skips members. Without it, the re-entrancy semantics above produce a real bug: a worker's periodic reclaim scan finds its *own* active runs (`status='running'`), `pg_try_advisory_lock` **succeeds** via same-session stacking, the scan concludes "the worker that claimed this is dead," and the worker double-executes its own run in-process — with no status-guard to save it, because the reclaim path deliberately has none.

The set is cheap, is needed anyway as the seam for a future concurrency cap, and restores the invariant the lock semantics actually offer: *the lock arbitrates between sessions; the set arbitrates within one.*

### Connection model

One **dedicated ownership connection per worker**, holding all of that worker's run locks (locks only — queries and writes stay on the pool):

- Cost is O(workers), not O(concurrently-executing runs). Per-run dedicated connections were considered and rejected: they couple max concurrent workflows to `max_connections`, which is hostile to an engine whose steps block on minutes-long LLM calls.
- Worker death closes the one session and releases every lock it held at once — exactly the semantics we want.
- Trade-off accepted: a run's step writes (pool) and its lock (ownership connection) can die independently. On a partition this means a write can land after the lock released — the same at-least-once in-doubt state the engine already models with intent rows; co-locating writes with the lock would shrink but not close that window, at the cost of serializing all of a worker's runs through one connection.

### `DatabaseClient` contract extension

Session pinning cannot be expressed through the current pool-shaped contract (`query`/`listen`/`close`). The contract gains one method:

```typescript
export interface DatabaseClient {
  query<T>(text: string, params?: unknown[]): Promise<T[]>;
  listen(channel: string, onNotify: (payload: string) => void): Promise<Subscription>;
  /** Reserve a dedicated session: queries run on ONE pinned connection until release(). */
  reserve(): Promise<{ query: DatabaseClient["query"]; release: () => Promise<void> }>;
  close(): Promise<void>;
}
```

Adapter implementations: porsager `sql.reserve()`; node-postgres `pool.connect()` (+ `client.release()`). The worker calls `reserve()` once at startup for its ownership connection. This is the bulk of the implementation work and the reason this feature is staged behind the contract change.

### Claiming a run (normal path — new run or timer resume)

The advisory lock — not the status column — is the mutual-exclusion gate. `status` remains the durable lifecycle record (it must survive across a multi-day `sleep` and feed the recovery scan); the lock adds liveness-aware exclusion on top.

```
on wakeup (NOTIFY run id, or a due timer's run id):
  if runId in inflightSet:                       # in-process exclusion first
      skip
  if NOT pg_try_advisory_lock(key(runId)):       # a live session is executing it
      skip
  else:
      add runId to inflightSet
      row = UPDATE workflow_runs
              SET status='running'
              WHERE id = runId AND status IN ('pending','suspended')
              RETURNING *
      if row is empty:                            # not claimable: nothing to do
          pg_advisory_unlock(key(runId)); remove from inflightSet
          skip
      else:
          execute(row)                            # see lifecycle below
```

The lock is held **only during active execution**. On `sleep` (suspend) and on terminal completion/failure the lock is released — a run sleeping for 7 days holds no lock; the lock maps precisely to "a worker is running this body right now."

```
execute(row):
  try:
    run the workflow fn in AsyncLocalStorage (Proxy caches/replays steps)
  on SleepInterrupt:                  # status already 'suspended', timer inserted
    return                           # released in finally
  on completion/failure:
    set terminal status + log + lifecycle hooks
  finally:
    pg_advisory_unlock(key(runId))    # always release; session-end cleanup is the backstop
    remove runId from inflightSet
```

### Reclaiming orphaned runs (the crux)

This is what the lock buys us. The recovery scan (on startup, and periodically) finds rows that *claim* to be executing and tests whether anyone actually is:

```sql
-- NOTE: do NOT write `SELECT pg_try_advisory_lock(...) FROM ... LIMIT n` directly:
-- per the PG docs, LIMIT is not guaranteed to apply before the lock function runs,
-- so locks may be acquired on unintended rows and dangle. Force evaluation order
-- with a subquery, then lock the bounded set.
SELECT q.id, pg_try_advisory_lock( hashtextextended(q.id::text, 0) ) AS acquired
FROM (
  SELECT id
  FROM workflow_runs
  WHERE status = 'running'
    AND id != ALL($1)              -- $1 = this worker's inflightSet (re-entrancy guard)
  ORDER BY updated_at
  LIMIT 50
) q;
```

The scan runs **on the ownership connection** (locks must live on the session that survives per-run work) with the in-flight set excluded in SQL *and* re-checked in process. For each returned row:

- `acquired = true` → no live session held the lock → the worker that set `running` is dead. Reclaim: this worker now holds the lock, add to the in-flight set, re-execute (replay skips cached steps, the in-doubt `running` step re-runs, execution continues). Release on suspend/terminal as usual.
- `acquired = false` → a live worker holds it → genuinely executing → skip, and do nothing (we never acquired, so nothing to release).

That single `pg_try_advisory_lock` distinguishes *actively-running* from *orphaned-by-a-dead-worker* with no timeout and no heartbeat. Process at most a small bounded batch per pass so locks aren't held on rows you won't immediately execute.

## Correctness, residuals, and the honest limits

- **No expiry-window steal on a slow worker.** A lease declares a slow-but-alive worker dead when its heartbeat lags; an advisory lock cannot be held and free at once. The lease's false-positive reclaim simply doesn't exist.
- **Reclaim delay on silent partition.** A worker whose connection is dead-but-undetected still "holds" the lock until PG notices (`tcp_keepalives_idle/interval/count`, `idle_session_timeout`). Bounded *liveness* delay, not a safety violation — and yes, that tuning is the moral equivalent of a lease TTL, relocated to server config. The difference is it only governs the pathological case (partition), not every reclaim.
- **The one true double-execution path (fencing).** A worker alive and mid-step (blocked on an LLM HTTP call) whose PG connection drops → lock released → another worker reclaims → both run the step. Nothing time- or connection-based prevents this; the lease has the same ceiling. Mitigations, in order of rigor: (1) at-least-once + idempotency hook — already the documented model, the in-doubt `running` step row makes it detectable; (2) fencing token (deferred) — a per-run `claim_epoch` bumped on reclaim, step writes conditioned on `WHERE claim_epoch = mine`. True exactly-once for arbitrary external side effects remains impossible; this is the honest ceiling for both designs.
- **Always unlock, but don't depend on it.** Release in `finally`; correctness rests on session-end cleanup, which is the whole point.

## What this changes vs. the current implementation

- `worker.ts`: claim and reclaim paths gain the try-lock gate + in-flight set; the `staleRunningMs` lease reclaim and the engine heartbeat are **removed** (the reconciliation scan and the suspended/pending arms stay — they recover *work*, the lock recovers *ownership*).
- `db.ts`: `DatabaseClient.reserve()` added; both adapters (example + test helper) implement it.
- **No new columns.** `workflow_runs` is unchanged. (If fencing is later adopted, add a single `claim_epoch INT`.)

## Observability

Join `pg_locks` (advisory entries) against `workflow_runs` to show which runs are *actually* executing right now (lock held) versus merely marked `running` (possibly orphaned, lock free). More truthful than the status column alone, and free.

## Testing additions

- **Dead-worker reclaim:** worker A claims + `running`, kill A's session → another worker's scan acquires → run reclaimed and completes **with cached steps skipped** (the money shot: no re-pay on reclaim).
- **Active run not stolen:** worker A executing (lock held on A's ownership session) → worker B's scan gets `acquired = false` → B skips; no double-execution.
- **Own runs not self-stolen:** worker A executing run R → A's *own* reclaim scan must skip R (in-flight set), despite `pg_try_advisory_lock` succeeding re-entrantly on A's session.
- **Lock released on suspend:** run hits `sleep` → no advisory lock held during the wait (assert via `pg_locks`).
- **NOTIFY herd:** N workers notified of one run → exactly one acquires and claims; the rest skip.
- **LIMIT-safety:** the reclaim scan uses the subquery form; assert no locks acquired beyond the bounded batch.
- **Collision is liveness-only:** force two run ids to the same key (test seam) → they serialize, neither double-executes nor is lost.

## Out of scope (unchanged from `SPEC.md`)

No external coordinator, no membership protocol, no worker-to-worker RPC. Single store. The "separate liveness mechanism" is the Postgres session every worker is already connected to.
