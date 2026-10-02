---
type: known-fix
id: cancel-in-sqlalchemy-onconnect
date: 2026-10-02
tags: [asyncio, cancellation, sqlalchemy, aiosqlite, resource-warning, shutdown, race-condition]
scope: project
symptom: "Intermittent ResourceWarning: 'aiosqlite.core.Connection object was deleted before being closed' in tests/test_broker_trace_propagation.py [database] variants (~13 of 60 runs); attributed by garbage collection to unrelated tests"
root_cause: "Consumer shutdown cancels the polling task without draining in-flight work. When _dispatch_guarded or _run is opening a connection, SQLAlchemy's NullPool checks out the connection and runs dialect on_connect handlers (sqlite pysqlite set_regexp/floor_func). A TaskGroup cancellation lands inside the on_connect handler AFTER aiosqlite has connected but BEFORE SQLAlchemy's pool sees the fully-initialized connection. The unguarded else-branch in _ConnectionRecord.__connect has no CancelledError handler, so the dbapi_connection is left open and dropped when the task dies, finalizing as a ResourceWarning when GC collects it. aiosqlite's own _connect guard works (88 of 88 cancels there were caught), but post-connect handlers are outside it."
wrong_hypotheses: ["(a) The test named in the warning leaked it — disproven: garbage-collection attribution misleads; plugin instrumentation showed the cancel landed in on_connect handlers, not test code", "(b) consumer.stop() cancelled aiosqlite's connect directly — disproven: 88 cancels landed inside aiosqlite.Connection._connect and all were guarded by its BaseException handler; zero leaked; aiosqlite 0.22.1 guard works"]
fix_summary: "PollingConsumer.stop() now waits up to _stop_drain_grace_s for in-flight poll/dispatch before cancellation; skips wait if poll is sleeping between claims"
affected_files: ["modulith/adapters/_polling_consumer.py"]
fix_status: open
anchor_commit: 33e46f7
---

# Asyncio Cancellation in SQLAlchemy On-Connect Handlers Leaves aiosqlite Connection Unclosed

## Symptom

Intermittent `ResourceWarning: 'aiosqlite.core.Connection object was deleted before being closed'` in `tests/test_broker_trace_propagation.py` when running the `[database]` parametrized variants. The warning occurred in ~13 of 60 stress-test runs. The warning's traceback attributed the leaked connection to unrelated test names (garbage-collection attribution misleads), making the actual source invisible until instrumentation was added. [Tool-Verified]

## Wrong Hypotheses

1. **The test named in the warning leaked it** — Garbage-collection finalizes cyclic objects at unpredictable times, often many tests after the test that created them. The AttributionError stacktrace names the test running when GC ran, not the test that created the connection. Disproven by adding a pytest plugin that wraps `aiosqlite.Connection.__init__/__del__` and logs which test created each connection; the leaked connections were created in `_dispatch_guarded` or `_run` tasks inside `[database]` tests, not the named test.

2. **consumer.stop() cancelled aiosqlite's connect directly** — aiosqlite guards its connect method with `except BaseException: self.stop(); self._connection = None; raise`. Instrumentation on `aiosqlite.core.Connection._connect` showed 88 CancelledErrors landed there during the 60-run stress test; all 88 were caught by the guard, and zero of those 88 leaked. The guard is installed (aiosqlite 0.22.1, `core.py`). Disproven: if aiosqlite's cancel guard were the source, the 88 cancels would have shown 88 leaks. They showed 0. The 13 leaks came from elsewhere.

## Root Cause

`PollingConsumer.stop()` calls `cancel_and_wait` on the polling task without waiting for in-flight work to finish. The polling task (`_run`) is inside `DeliveryDispatch._dispatch_batch` (the TaskGroup), which cancels `_dispatch_guarded` children when `stop()` is called.

When the cancellation lands during the critical window:

1. `_dispatch_guarded` is inside `asyncio.run()` the delivery, which eventually opens a pooled connection via SQLAlchemy.
2. SQLAlchemy's `NullPool` checks out a connection and calls `_ConnectionRecord.__connect`.
3. `_ConnectionRecord.__connect` calls the dialect's on_connect event handlers via `_exec_w_sync_on_first_run`.
4. For SQLite pysqlite, the on_connect handler runs `create_function` calls to register `set_regexp` and `floor_func` UDFs.
5. **The else-branch of `_ConnectionRecord.__connect` that runs on_connect handlers has NO BaseException guard.** Only the `pool._invoke_creator` block (step 1–3) is guarded.
6. A CancelledError raised inside the on_connect handler (steps 4–5) escapes unguarded, leaving `self.dbapi_connection` set and the record dropped.
7. The aiosqlite Connection is now owned by nobody (SQLAlchemy's pool dropped the record on exception, but didn't close the connection).
8. The connection object is held only by its task's frame, which is immediately reaped when the task dies.
9. Later, GC collects the frame, which drops the connection, triggering its `__del__` finalizer and the ResourceWarning.

Instrumentation evidence [Tool-Verified]:
- All 13 leaking connections showed creation stack ending in `_dispatch_guarded`, `_run`, or `claim_batch`.
- All 13 had a preceding `Task.cancel()` call from `PollingConsumer.stop()` → `cancel_and_wait()` → `TaskGroup.cancel_scope.cancel()`.
- 13 CancelledErrors escaped `_ConnectionRecord.__connect` with `dbapi_connection_set=True` and `aiosqlite_open=True`.
- 88 CancelledErrors escaped `aiosqlite.Connection._connect` with `_connection_set=False`; all 88 were caught by aiosqlite's `except BaseException` guard and zero leaked.
- The difference: aiosqlite's guard at line `except BaseException: self.stop(); self._connection = None; raise` ran 88 times; SQLAlchemy's guard at `_ConnectionRecord.__connect` else-branch does not exist.

Why `[database]` hop tests only: `NullPool` opens a new connection on every checkout and re-runs on_connect, multiplying exposure compared to a pooled strategy like `QueuePool`.

## Fix

**Commit 33e46f7**: `PollingConsumer.stop()` now implements a bounded graceful drain before cancellation.

After `self._stopping = True`, the method:
1. Waits up to `_stop_drain_grace_s` (1.0 s default) for the `_run` task to finish via `asyncio.wait({task}, timeout=grace)`.
2. If the task is sleeping between polls (the `_sleeping` flag set by `_sleep_between_claims`), skips the wait and falls through immediately.
3. If the task finishes within the grace period, returns without cancellation (the polling loop saw `_stopping` and exited naturally via `_should_stop()` checks).
4. If the grace period expires, falls back to the existing `_cancel` and `cancel_and_wait` (hard cancellation as before).
5. If `stop()` itself is cancelled during the drain, the finally-block still cancels the task.

The drain grace duration (1.0 s) is bounded by the idle-poll backoff cap (`_IDLE_BACKOFF_CAP_S = 0.5` + up to 0.05 s jitter), so the worst-case drain cost is ~one idle poll cycle per stop.

Changes:
- `modulith/adapters/_polling_consumer.py`: added `_let_in_flight_work_finish()` coroutine; modified `stop()` to call it before `_cancel` / `cancel_and_wait`.

Verification [Tool-Verified]:
- 60 stress-test iterations of `tests/test_broker_trace_propagation.py -k database` (baseline 13 leaks per 60 runs): **0 leaks in all 60 runs** (new; 30-run control: 0 leaks also).
- 723 consumer/broker/outbox tests pass; no new failures.
- No change in covered behavior: if a task does not exit during the grace, the cancel still lands as before (the race is narrowed, not closed).

## Diagnostic Journey

The investigation began with the intermittent ResourceWarning appearing in test runs. The warning's stacktrace attributed the leak to random test names, making the source invisible. Initial hypotheses assumed the named test was at fault.

The breakthrough came from adding a pytest plugin that wraps `aiosqlite.Connection.__init__/__del__` and logs which test created and finalized each connection. This revealed:
1. The leaked connections were created by `_dispatch_guarded` and `_run` tasks, not the tests named in the warning.
2. Every leaked connection was created in a `[database]` hop test.
3. All leaks appeared only when `PollingConsumer.stop()` was called (the test fixture's finally-block).

Further instrumentation on `sqlalchemy.pool.base._ConnectionRecord.__connect` and `aiosqlite.core.Connection._connect` showed:
- 88 CancelledErrors landed inside aiosqlite's guard: all 88 were caught and cleaned up.
- 13 CancelledErrors escaped SQLAlchemy's on_connect handler: none were guarded, and the connection was left open.
- The 13 escapes matched the 13 leaks by count (a 1:1 correspondence).

The key insight: **aiosqlite's guard worked perfectly; SQLAlchemy's unguarded on_connect handler was the culprit.** The cancel was coming from `PollingConsumer.stop()`, which called `cancel_and_wait` without draining. The fix is to drain in-flight work before the cancel lands.

A complete fix belongs upstream: guarding the else-branch of SQLAlchemy's `_ConnectionRecord.__connect`, which SQLAlchemy 2.0.51 still leaves unguarded. That is out of scope here. This fix narrows the race to work that outlives the 1 s grace: a claim or dispatch still running when the grace ends can still be cancelled inside on-connect, but in typical workloads that is rare.

## Generalizable Pattern

Asyncio cancellation is a sharp tool. When a task is cancelled at an arbitrary point, it can leave resources in partially-initialized states. Cancellation inside async context managers (enter but not exit), inside database transactions, or inside connection pools' initialization routines are high-risk windows.

**Check first:**
- When a resource pool (connection pool, thread pool, file handle) shows intermittent ResourceWarnings during shutdown, check whether cancellation is landing inside pool initialization (on_connect, on_checkout, dialect handshake, etc.).
- If the warning is attributed by GC to an unrelated test, use a plugin that wraps the resource's `__init__/__del__` to learn which test actually created it.
- If cancellation-path logs show BaseException caught in one layer but not another, the unguarded layer is the leak source.

**Preferred solutions (in order):**
1. **Drain before cancellation.** Set a flag that the async loop checks between statements, wait a bounded time for the loop to exit naturally, then cancel only if it doesn't exit in time.
2. **Shield critical sections.** Wrap resource initialization (pool checkout, on_connect handlers) with `asyncio.shield()`, but be prepared to handle cases where the resource was acquired but nobody committed it.
3. **Upstream fix (SQLAlchemy case).** File an issue with the pool implementer (SQLAlchemy, asyncpg, etc.) to guard the full initialization sequence, not just the creator.

**Testing:** Use a plugin that wraps the resource's `__init__/__del__` and logs creation/finalization per test. Add `gc.collect()` after shutdown to catch cyclic garbage. Avoid strong-reference probes (watchdog tables, captured references) which keep the resource alive and mask the real condition.

## Summary

Consumer shutdown cancelled the polling task without draining in-flight work. A TaskGroup cancellation landed inside SQLAlchemy's NullPool on_connect handler while it was registering UDFs, leaving an aiosqlite Connection unclosed. The unguarded else-branch of `_ConnectionRecord.__connect` had no BaseException handler, unlike the creator block. The fix drains in-flight work up to a grace period before falling back to cancellation, so only a claim or dispatch that outlives that 1 s grace can still leak; a consumer sleeping between polls holds no connection and is cancelled at once. A complete fix would guard SQLAlchemy's entire `__connect` sequence upstream.
