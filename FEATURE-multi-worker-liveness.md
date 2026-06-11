# Feature: Multi-Worker Liveness via Postgres Advisory Locks

**Status:** implemented (v0.3) — `worker.ts` (lock gate + in-flight map + orphan scan), `db.ts` (`reserve()`), both adapters, integration tests
**Amends:** `SPEC.md` (Component 2 — Worker, §2.1/§2.2 — recovery section updated to match).
**Replaced:** the interim lease+heartbeat reclaim (`staleRunningMs` + `updated_at` heartbeat). The lease made dead-worker reclaim *work*; this makes it *precise* — no threshold to tune, no reclaim delay on clean crash.

**v0.2 changes:** in-process in-flight set (same-session re-entrancy is a correctness hole, not a nicety); `DatabaseClient` contract extension spelled out; per-worker ownership-connection variant adopted over per-run connections; lease comparison rewritten honestly; test list extended.
**v0.3 changes (implementation):** the reclaim scan selects candidates with a plain bounded query and takes each lock **per-row on the ownership session** instead of the batched `SELECT pg_try_advisory_lock(...) FROM (subquery)` form — same semantics, and the LIMIT-evaluation-order footgun can't exist when no lock function appears in a SELECT list. Reclaim needs no dedicated code path: *every* execution acquires the lock first, and "lock acquired + row still `running`" is itself the proof the claimer died, so the reset-to-pending happens inline in the one execution path.

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

This is what the lock buys us — and as implemented it needs **no dedicated reclaim path**. The recovery scan just feeds `running` rows (orphan *candidates*, bounded batch, oldest `updated_at` first) into the same `execute()` everything else uses:

```sql
-- recovery scan, the orphan-candidate arm (plain query, no lock functions —
-- the batched SELECT pg_try_advisory_lock(...) FROM ... form and its
-- LIMIT-evaluation-order footgun are avoided entirely):
SELECT id FROM (
  SELECT id FROM workflow_runs WHERE status = 'running'
  ORDER BY updated_at LIMIT 50
) candidates;
```

`execute()` dedups against the in-flight map synchronously (so a worker never feeds itself its own active runs), then `executeExclusive` takes the lock per-row on the ownership session:

- **lock not acquired** → a live session holds it → genuinely executing → skip; nothing to release.
- **lock acquired and the row is still `running`** → the claimer is dead (a live claimer would hold the lock; same-session doubles are excluded by the in-flight map) → reset the row to `pending` (logged `run.reclaimed`) and fall through to the normal claim + execute. Replay skips cached steps; the in-doubt `running` step re-runs.
- **lock acquired and the row is `pending`/`suspended`/terminal** → the normal path: claim it (or no-op).

One `pg_try_advisory_lock` distinguishes *actively-running* from *orphaned-by-a-dead-worker* with no timeout and no heartbeat. Rows beyond the batch wait for the next reconcile pass.

## Correctness, residuals, and the honest limits

- **No expiry-window steal on a slow worker.** A lease declares a slow-but-alive worker dead when its heartbeat lags; an advisory lock cannot be held and free at once. The lease's false-positive reclaim simply doesn't exist.
- **Reclaim delay on silent partition.** A worker whose connection is dead-but-undetected still "holds" the lock until PG notices (`tcp_keepalives_idle/interval/count`, `idle_session_timeout`). Bounded *liveness* delay, not a safety violation — and yes, that tuning is the moral equivalent of a lease TTL, relocated to server config. The difference is it only governs the pathological case (partition), not every reclaim.
- **The one true double-execution path (fencing).** A worker alive and mid-step (blocked on an LLM HTTP call) whose PG connection drops → lock released → another worker reclaims → both run the step. Nothing time- or connection-based prevents this; the lease has the same ceiling. Mitigations, in order of rigor: (1) at-least-once + idempotency hook — already the documented model, the in-doubt `running` step row makes it detectable; (2) fencing token (deferred) — a per-run `claim_epoch` bumped on reclaim, step writes conditioned on `WHERE claim_epoch = mine`. True exactly-once for arbitrary external side effects remains impossible; this is the honest ceiling for both designs.
- **Always unlock, but don't depend on it.** Release in `finally`; correctness rests on session-end cleanup, which is the whole point.

## What changed (implemented)

- `worker.ts`: every execution path gates on the try-lock + in-flight map; the `staleRunningMs` lease reclaim and the engine heartbeat are **removed** (the reconciliation scan and the suspended/pending arms stay — they recover *work*, the lock recovers *ownership*).
- `db.ts`: `DatabaseClient.reserve()` → `ReservedSession { query, release }`; both adapters (example + test helper) implement it over porsager `sql.reserve()`.
- **No new columns.** `workflow_runs` is unchanged. (If fencing is later adopted, add a single `claim_epoch INT`.)

## Observability

Join `pg_locks` (advisory entries) against `workflow_runs` to show which runs are *actually* executing right now (lock held) versus merely marked `running` (possibly orphaned, lock free). More truthful than the status column alone, and free.

## Tests (implemented in `packages/runtime/tests/integration.test.ts`, "crash recovery")

- **Dead-worker reclaim:** a `running` row with no live lock holder → reclaimed instantly on the next scan, completes, `run.reclaimed` logged.
- **Active run not stolen:** the run's lock held by a separate live session (test helper `holdAdvisoryLock`) → scans skip it across multiple reconcile passes; ending that session (the "death") triggers reclaim on the next pass.
- **Own runs not self-stolen:** a 700ms step with `reconcileMs: 100` → repeated scans during execution; the in-flight map keeps the worker from re-entrantly stealing its own run (step executes exactly once, no `run.reclaimed`).
- **Lock released on suspend:** during a durable sleep, `pg_locks` shows zero advisory locks.
- **NOTIFY herd / concurrent workers:** 3 workers, 8 runs → every step executes exactly once.

Not implemented: the LIMIT-safety test (moot — the per-row form has no lock function in any SELECT list) and the key-collision seam (would require injectable key derivation for a negligible-probability, liveness-only event; revisit if the key fn ever becomes configurable).

## Out of scope (unchanged from `SPEC.md`)

No external coordinator, no membership protocol, no worker-to-worker RPC. Single store. The "separate liveness mechanism" is the Postgres session every worker is already connected to.
