# Failure Scenarios & Recovery Guarantees

How `pipelines` handles every failure case. Each scenario shows what state is left in the DB, what detects it, and what happens next.

---

## Notation

| Status | Meaning |
|---|---|
| `pending` | submitted, waiting for a worker to claim |
| `running` | a worker has claimed and is actively executing |
| `suspended` | a `sleep()` is in progress; worker has yielded |
| `completed` / `failed` | terminal; no further execution |

---

## 1. Happy path

**What happens:**
1. `workflow.run(input)` inserts a `pending` row, `pg_notify('workflow_runs', runId)` fires.
2. A worker receives the NOTIFY, calls `execute(runId)`.
3. Advisory lock acquired, row claimed → `running`.
4. Each step runs: intent CTE, then completion CTE (2 queries/step, atomic).
5. Terminal CTE flips to `completed`/`failed`, lock released.

**Recovery needed:** none.

---

## 2. Worker was down when a run was submitted

**Failure:** NOTIFY fires into the void — the channel has no listener. Row stays `pending`.

**Detection:** startup `recover()` + periodic `reconcileTimer` (default every 60s) scan for all `pending` rows.

**Recovery:** scanned row feeds into `execute()` → lock → claim → execute. Exactly the same path as a live NOTIFY.

---

## 3. Process crash mid-step (in-doubt step)

**Failure:** worker dies after writing the intent row (`status='running'` in `workflow_steps`) but before writing the completion row. The run row stays `running`.

**Detection:** next worker (or the same worker after restart) runs `recover()`, finds the `running` row, calls `executeExclusive()`. Advisory lock acquisition succeeds (the dead session's lock is gone). The `running` run row is reset to `pending` (`run.reclaimed` logged).

**Recovery:**
- `claimAndExecute` re-runs the workflow function.
- `loadCachedSteps` replays all *completed* steps from cache instantly.
- The in-doubt step has no cache entry → it re-executes (at-least-once for that step).
- Execution continues from that point forward.

---

## 4. Process crash between lock acquisition and claim

**Failure:** worker acquires the advisory lock, then dies before the `claimAndExecute` UPDATE fires. Row stays `pending` or `suspended`.

**Detection:** same as case 3 — the session dies, Postgres releases the lock. Next `recover()` pass picks up the row.

**Recovery:** clean re-execution from scratch (no orphaned `running` row to reset).

---

## 5. Process crash mid-sleep scheduling

**Failure variants:**

| Crash point | State left |
|---|---|
| After `INSERT INTO workflow_timers` but before `UPDATE status='suspended'` | run still `running`, timer row exists |
| After `UPDATE status='suspended'` but before the `SleepInterrupt` is thrown | run `suspended`, timer row exists |

Both are safe. On re-execution the `ON CONFLICT (run_id, sleep_id) DO NOTHING` silently skips the duplicate timer insert. The `SELECT … elapsed` check uses the DB clock — if `wake_at <= now()` the sleep skips through regardless.

---

## 6. Timer fires but resume NOTIFY is lost (or poller crashes)

**Failure:** the timer poller marks the timer `fired` and calls `execute()` — but the process crashes between those two operations, or the NOTIFY is dropped. Run stays `suspended`, timer is `fired`.

**Detection:** `recover()` finds suspended runs with no remaining *future* timer (`wake_at > now() AND status='waiting'`). A run with only a `fired` or elapsed timer qualifies.

**Recovery:** feeds into `execute()` → lock → claim. On replay, `sleep()` sees `status='fired'` (or `wake_at <= now()`) and skips through immediately. Marks the timer `fired` on the way through so future recovery scans skip it cleanly.

---

## 7. Live worker, duplicate NOTIFY for the same run

**Failure:** PG NOTIFY can deliver a payload multiple times in edge cases; a `recover()` scan and a live NOTIFY can race for the same run.

**Detection (in-process):** the `inflight` map. `execute()` checks the map before doing anything — if a `Promise` is already registered for that `runId`, the duplicate returns that same Promise.

**Detection (cross-worker):** `pg_try_advisory_lock` returns false immediately if another session holds the lock. `executeExclusive` returns early, no DB writes.

**Recovery:** no duplicate execution, no corrupted state.

---

## 8. Two workers race to claim the same run

**Failure:** two workers both see a NOTIFY (or `recover()` scan) for the same `runId` at the same time.

**Layer 1 — advisory lock:** `pg_try_advisory_lock` is a single winner. The loser gets `false` and returns immediately.

**Layer 2 — claim query:** the `FOR UPDATE` + status guard in `claimAndExecute` is a second independent gate. Even without advisory locks, only one UPDATE wins.

**Result:** exactly-once execution per run transition. No duplicate step writes.

---

## 9. Duplicate submission (idempotency key)

**Scenario:** `workflow.run(input, { idempotencyKey: "k" })` called twice with the same key.

**What happens:** the INSERT uses `ON CONFLICT (workflow_name, idempotency_key) DO NOTHING`. The second call returns the `runId` of the existing row. The run executes exactly once.

---

## 10. Step retry (transient failure)

**Scenario:** a step throws a non-fatal error.

**What happens:**
1. Attempt counter increments (single UPDATE, no CTE needed — the step is already in-flight).
2. Exponential backoff sleep (in-process only; no DB state while sleeping).
3. Step re-executes. On success, the completion CTE writes `completed` + log atomically.
4. If `maxRetries` exhausted → `step.failed` logged, error propagates to the engine → `run.failed`.

**Crash during backoff sleep:** the entire run is re-executed on recovery. The step starts at attempt=0 again (at-least-once). The attempt counter is an optimization, not a correctness guarantee.

---

## 11. FatalError

**Scenario:** a step throws `FatalError("reason")`.

**What happens:** retries are skipped entirely. The step is immediately marked `failed`, the error propagates to the engine, the run is marked `failed`. `onError` hook fires. No further execution.

---

## 12. Workflow not registered on this worker

**Scenario:** a run is claimed by a worker that doesn't have that workflow's definition loaded (e.g., a rolling deploy with mixed versions).

**What happens:** `claimAndExecute` calls `getRegisteredWorkflow()`, gets `undefined`, immediately calls `failRun()` with a descriptive error. The run is marked `failed` with `"workflow X not registered in this worker"`. It does not hang.

---

## 13. Replay after a completed run

**Scenario:** `replayRun(db, runId)` is called on a completed or failed run.

**What happens:** management API clears `output`, `error`, all `workflow_steps` rows for this run, resets status to `pending`. A NOTIFY fires. The run re-executes from scratch as if it was never run.

---

## 14. Pool size too small (max=1)

**Misconfiguration:** pool configured with `max: 1`.

**What happens:** `db.reserve()` takes the one connection out of the pool for the ownership session. All subsequent `db.query()` calls block forever waiting for a connection → worker hangs, timeouts or `CONNECTION_ENDED` errors.

**Requirement:** pool `max ≥ 2`. The ownership session is permanently reserved; at least one slot must remain for run/step queries. Sweet spot for local Docker is `max=20–30`.

---

## Summary table

| Scenario | Detected by | Recovery mechanism |
|---|---|---|
| NOTIFY missed (worker down) | `recover()` scan — `pending` rows | `execute()` → lock → claim |
| Crash mid-step | `recover()` scan — `running` rows + lock test | reset to `pending`, re-execute (in-doubt step re-runs) |
| Crash mid-sleep schedule | `recover()` scan — `running` rows + lock test | re-execute; `ON CONFLICT` skips duplicate timer |
| Timer fired, resume lost | `recover()` scan — suspended + no future timer | re-execute; time-based skip-through on replay |
| Duplicate NOTIFY (same worker) | `inflight` map | returns existing Promise, no duplicate work |
| Duplicate NOTIFY (cross-worker) | `pg_try_advisory_lock` | loser returns immediately |
| Race to claim (cross-worker) | advisory lock + `FOR UPDATE` claim | exactly-once execution |
| Duplicate submission | `ON CONFLICT … DO NOTHING` | existing `runId` returned |
| Transient step failure | per-step retry loop | exponential backoff, re-execute |
| Fatal step failure | `FatalError` check | skip retries, fail run immediately |
| Unknown workflow | `getRegisteredWorkflow()` | fail run with clear error |
| Pool max=1 | (no runtime guard — it hangs) | configure `max ≥ 2` |
