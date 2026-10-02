---
type: known-fix
id: cancellation-zombie-read-lock
date: 2026-10-02
tags: [sqlite, aiosqlite, asyncio, cancellation, garbage-collection, locking, shutdown]
scope: project
symptom: "In test teardowns, after-commit dispatches wait 5–10 s with 'database is locked' error; occurs ~1 in 3 runs when SQLite outbox is in rollback-journal mode"
root_cause: "Cancelling the outbox retry loop while a SELECT is in flight leaves a cursor alive through CancelledError traceback frames; the closed sqlite3 connection cannot release its SHARED read lock until cyclic GC collects those frames; COMMIT then waits out the 5 s busy timeout"
wrong_hypotheses: ["(a) Crash sweep contends with after-commit claims — disproven by drain counterfactual that kept the crash sweep", "(b) A transaction waits on itself — disproven by WAL counterfactual which removed waits while cancellations continued", "(c) Methodology trap in the GC probe — disproven by weak-reference probe showing GC does help"]
fix_summary: "Cooperative stop for retry loop: cancellation only lands in sleep; mid-sweep, stops check the request and release claimed rows"
affected_files: ["modulith/builtin/outbox.py", "modulith/adapters/postgres_outbox.py", "modulith/runtime.py"]
fix_status: resolved
anchor_commit: 4848a03
---

# Cancellation of SQLite SELECT leaves zombie read lock

## Symptom

In `examples/marketplace/tests/test_marketplace_shipping.py`, approximately 1 in 3 runs had test teardowns that took an extra 5.0 or 10.0 s. `PostgresPublicationStore._dispatch_after_commit` logged `after-commit dispatch failed ... database is locked` at ERROR level during those slow teardowns. [Tool-Verified]

The slow teardowns were nondeterministic: the same test suite run cleanly elsewhere, but when the outbox retry loop was cancelled during a database operation, the after-commit claim would block in COMMIT waiting for a lock that was held by a closed connection.

## Wrong Hypotheses

1. **Crash sweep contends with after-commit claims** — The `_retry_loop` runs a crash sweep with `older_than=0` that might claim rows the after-commit dispatch is trying to claim. Refuted: a counterfactual that drained the running sweep (waited for it to finish) before cancelling still kept the crash sweep and had 0 slow teardowns in 55 runs, against 23 in 55 baseline runs. If contention were the cause, the waits would not disappear when the sweep is kept.

2. **A transaction waits on itself** — Perhaps an after-commit claim held a write transaction that awaited work needing the same write lock. Refuted: the SQLAlchemy stack stopped at `_dispatch_after_commit` awaiting its own `_claim_publication`, and no other live connection reported `in_transaction=True`. When the test database was switched to WAL mode, the waits disappeared while the cancellations remained. In WAL mode, a reader never blocks a COMMIT (readers and writers coexist), so if hypothesis (b) were true, WAL would have no effect.

3. **Methodology trap in the GC probe** — A strong-reference probe that held the owning task in a connection table kept the cancelled task, its traceback frames, and the cursor alive indefinitely, making it appear that `gc.collect()` had no effect. Refuted: a weak-reference probe in a later batch showed `gc.collect()` after shutdown reduced the failure rate from 12 of 25 to 1 of 25, confirming the garbage-collection mechanism.

## Root Cause

`outbox.shutdown()` cancelled the retry-loop task by calling `task.cancel()` without regard to where the task was suspended. During test teardown, the task usually sat inside `_retry_loop` → `_guarded_sweep(timedelta(0))` → `_sweep_lease`, inside a call to `PostgresPublicationStore.claim_batch` (a SELECT) or `complete_claim` (a row read).

When a CancelledError was raised inside a SELECT:

1. SQLAlchemy's `Connection._handle_dbapi_exception` treated the CancelledError as an exit exception and invalidated the connection.
2. `AsyncAdapt_aiosqlite_connection.terminate` called aiosqlite `Connection.close`, which called `sqlite3.Connection.close()`.
3. **Critical:** The `sqlite3.Cursor` object of the half-read SELECT outlived the close, because the traceback frames of the CancelledError still held references to it (through SQLAlchemy's exception context and traceback internals).
4. A sqlite3 connection with a live, stepped statement cannot release its SHARED read lock until that cursor is garbage-collected.
5. The closed connection became a zombie holding the lock.
6. The after-commit claim, running in a separate task, had already executed its UPDATE (which succeeded because RESERVED lock mode is compatible with SHARED). When it reached `await s.commit()`, it needed an EXCLUSIVE lock.
7. In rollback-journal mode (the default), a COMMIT must wait until every SHARED read lock is gone. The after-commit claim waited out the pysqlite default busy timeout of 5 s and failed with `OperationalError('database is locked')`.

Evidence:

- All 64 slow teardowns in the baseline runs followed a retry-loop cancellation inside a SELECT. [Tool-Verified]
- A standalone repro without modulith (a closed sqlite3 connection plus a live cursor on a rollback-journal database) reproduces the exact error. [Tool-Verified]
- `gc.collect()` after shutdown removed 24 of 25 cases in a weak-reference probe (the 25th was a live cancellation handler still holding the frame). [Tool-Verified]
- WAL mode removed the waits while cancellations remained, proving the blocker was a read lock, not another writer. [Tool-Verified]
- The drain counterfactual kept the crash sweep and removed every wait, ruling out contention as the root cause. [Tool-Verified]

## Fix

**Commit d11fff6**: Cooperative shutdown — `outbox.shutdown()` now stops the retry loop cooperatively instead of cancelling:

- A loop sleeping between sweeps is cancelled at once (no statement in flight).
- A loop inside a sweep is signalled with a stop request. The store call or delivery in flight completes normally. Each sweep checks the stop request before processing the next row. The lease sweep releases its remaining claimed rows uncharged with `renew_claim(id, token, 0.0)`, the same path used for rows not yet due, then returns.
- A delivery already in progress completes and `complete_claim` lands (at-least-once delivery is preserved).
- If the sweep does not return within `_shutdown_grace_seconds` (10 s), shutdown falls back to `task.cancel()` as before, with the same risk.

Changes:
- `modulith/builtin/outbox.py`: added module-level `_stop_requested` (threading.Event) and `_sweeping` flag; modified `_retry_loop` to respect the stop request; updated `_sweep_lease`, `_sweep_unclaimed`, and `_sweep_advisory` to check the stop request before each row and release remaining claims; `shutdown` now sets the request and waits up to the grace bound before cancelling.
- `modulith/adapters/postgres_outbox.py`: `_dispatch_after_commit` behaviour unchanged; it now completes without the contention-induced waits.
- `modulith/runtime.py`: added `sqlite_wal` option to `[tool.modulith.outbox_options]` to allow users to opt SQLite databases into WAL mode.

**Commits a97aec7 and 2bcb597** (2026-10-02): Defense in depth — `[tool.modulith.outbox_options] sqlite_wal = true` opts a SQLite `outbox_url` database into WAL mode. WAL mode removes the read-blocks-COMMIT behaviour for every cause, including cancellations from application code (e.g., `asyncio.timeout` wrapped queries). It is off by default because WAL persists in the user's business database file.

Regression tests confirm:
- `tests/test_outbox.py::test_shutdown_lets_an_in_flight_store_call_finish_then_dispatches_nothing` — a store call in flight completes.
- `tests/test_outbox.py::test_shutdown_between_lease_rows_releases_the_rest_uncharged` — remaining claimed rows are released uncharged.
- `tests/test_outbox.py::test_shutdown_between_lease_rows_keeps_the_claim_of_a_row_delivered_elsewhere` — rows another task is delivering keep their claims.
- `tests/test_outbox.py::test_shutdown_cancels_a_store_call_that_outlasts_the_grace_bound` — grace-bound expiry falls back to cancellation.
- `tests/test_outbox.py::test_shutdown_cancels_a_sleeping_retry_loop_at_once` — sleeping loop is cancelled without waiting.
- `tests/test_outbox_claims.py::test_shutdown_from_another_loop_lets_the_in_flight_store_call_finish` — shutdown from another event loop waits for in-flight work.
- `tests/test_outbox_claims.py::test_a_retry_loop_started_after_shutdown_dispatches_again` — a new retry loop after shutdown resumes delivery.

## Diagnostic Journey

The investigation started with observations of nondeterministic 5 s delays in test teardowns. The symptom was consistent and reproducible at scale (23 of 55 baseline runs), but the cause was opaque.

Initial hypotheses blamed contention (the crash sweep) or re-entrancy (a transaction waiting on itself). Both fell apart under counterfactuals: the drain arm that kept the crash sweep showed 0 waits, and WAL mode showed waits disappeared while cancellations remained. The real culprit was not visible in the code — it was a state left behind by the exception handling and garbage collection.

The key insight came from a direct observation: a live cursor in a closed connection was blocking a COMMIT. This was verified by dumping every live sqlite3 object while the COMMIT waited and confirming that:

1. The only closed connection was the one the retry loop had been using.
2. The cursor belonged to it, stepped in the middle of a SELECT.
3. Every open connection reported `in_transaction=False`, so no other writer was blocking.
4. The frames that held the cursor came from the CancelledError traceback.

The standalone repro (`zombie_repro.py`) confirmed the sqlite3 mechanism: a closed connection with a live cursor on a rollback-journal database reproducibly fails COMMIT with "database is locked".

What made the investigation lengthy was that the lock outlived the task: the task had finished by the time the COMMIT waited. Only direct object inspection (walking referrer chains) and the weak-reference GC counterfactual revealed that the lock was held by garbage, not a live statement or transaction.

The drain counterfactual was the turning point for ruling out competing hypotheses. By waiting for the sweep to finish before cancelling, it proved that the sweep itself was not the contender, and that preventing cancellation inside the sweep prevented the lock. That pointed toward the cooperative-stop fix.

## Generalizable Pattern

When task cancellation happens inside an async database operation (any statement: SELECT, UPDATE, INSERT, DELETE), exception traceback frames can keep references to the statement object. With certain driver stacks (e.g., SQLAlchemy + aiosqlite + sqlite3), this can prevent the connection from releasing locks until cyclic garbage collection runs.

**Check first:**
- If tests or deployments show lock waits during shutdown or cleanup, check whether any task is being cancelled while holding a statement.
- If the wait happens only on SQLite and not on Postgres/MySQL, suspect a lock held by garbage (those servers drop locks on session termination).
- If `gc.collect()` after shutdown reduces or eliminates the waits, confirm the diagnosis: the lock is in uncollected objects.

**Preferred solutions (in order):**
1. **Prefer cooperative stops over cancellation.** Cancellation at arbitrary points forces you to manage exception cleanup across every caller. A cooperative stop (a flag the async loop checks between operations) lets the loop clean up normally.
2. **Use WAL mode for SQLite.** WAL removes read-blocks-COMMIT behaviour entirely, covering cancellations from all sources (retry loop, application code with `asyncio.timeout`, etc.). It persists to the database file, so make it explicit.
3. **Avoid cancellation inside statements.** If cancellation must happen, use `asyncio.shield` around statement execution, but be prepared to handle completed statements that no one committed.

**Testing:** Cancellation tests should use weak references and `gc.collect()` to probe for zombie locks, never strong references (which keep the lock alive indefinitely). A test that holds the cancelled task in a watchdog table or captures it in a reference will mask the real condition.

## Summary

A cancelled async SELECT left its cursor alive through traceback frames, preventing the closed sqlite3 connection from releasing its SHARED read lock. A separate task's COMMIT waited out the busy timeout. The fix is a cooperative stop that prevents cancellation from landing inside statements. WAL mode is a secondary defence covering cancellations from any source.

