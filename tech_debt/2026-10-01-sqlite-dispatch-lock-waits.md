---
type: tech-debt
debt_status: resolved
created: 2026-10-01
updated: 2026-10-01
category: Architecture
impact: Low - On SQLite, an after-commit dispatch can wait out the 5 s busy timeout and fail with "database is locked"; the retry sweep delivers the publication later
effort: Medium - A cooperative stop for the outbox retry loop
---

# On SQLite, after-commit dispatches can wait 5 s for the write lock

## Description
In 9 of 30 runs of `examples/marketplace/tests/test_marketplace_shipping.py`, one to three test teardowns took 5 s longer each. In the run inspected with live logging, each slow teardown was an after-commit dispatch's claim waiting out SQLite's 5 s busy timeout. `PostgresPublicationStore._dispatch_after_commit` then logged `after-commit dispatch failed ... database is locked` at ERROR. [Tool-Verified]

A dump taken 2.5 s into one such teardown showed [Tool-Verified]:
- four aiosqlite connections inside transactions;
- three `_dispatch_after_commit` tasks, claiming or delivering;
- the retry loop still in its crash-recovery sweep (`outbox._retry_loop`, the `_guarded_sweep(timedelta(0))` call) while being cancelled.

## Cause
A closed sqlite3 connection held a read lock. `outbox.shutdown()` cancelled the retry-loop task while its sweep had an aiosqlite SELECT in flight: the `claim_batch` SELECT of the crash sweep, or the `complete_claim` row read. [Tool-Verified]

SQLAlchemy reacted to the CancelledError by invalidating the connection, and aiosqlite closed the sqlite3 connection. The cursor of the half-read SELECT stayed alive, because the CancelledError's traceback frames still referenced it. A sqlite3 connection with a live, stepped statement cannot release its read lock, so it stayed a zombie until cyclic GC freed those frames. [Tool-Verified]

The example database runs in rollback-journal mode, where a COMMIT must wait until every read lock is gone. The after-commit claim had already run its UPDATE, so it waited in COMMIT for the pysqlite default busy timeout of 5 s and failed. [Tool-Verified]

Evidence:
- All 64 slow teardowns in the probed baseline runs followed a retry-loop cancellation inside a SELECT.
- Shutdown that waited for the running sweep before cancelling: 0 slow runs in 55, against 23 slow teardowns in 55 comparable baseline runs.
- WAL mode removed the waits while the cancellations remained, so the blocker was a read lock rather than a second writer.
- `gc.collect()` after shutdown removed 24 of 25 cases.
- A standalone repro (closed connection plus live cursor) reproduces "database is locked" without modulith.

The crash sweep competing with after-commit claims was ruled out: the drain arm kept the `older_than=0` sweep and the waits disappeared.

## Resolution (2026-10-01)
`outbox.shutdown()` now stops the retry loop cooperatively, so a cancellation no longer lands inside a store statement or a listener delivery:
- A loop sleeping between sweeps is cancelled at once.
- A loop inside a sweep gets a stop request. The store call or delivery in flight completes. Each sweep checks the request before each row, and the lease sweep releases its undispatched rows uncharged with `renew_claim(id, token, 0.0)`. A row another task in the process is delivering keeps its claim, as in the normal sweep.
- If the sweep has not returned within `_shutdown_grace_seconds` (10 s), shutdown falls back to cancelling, as before.

Regression tests:
- `tests/test_outbox.py`: `test_shutdown_lets_an_in_flight_store_call_finish_then_dispatches_nothing`, `test_shutdown_between_lease_rows_releases_the_rest_uncharged` and `test_shutdown_between_lease_rows_keeps_the_claim_of_a_row_delivered_elsewhere` (on the SQLite store), `test_shutdown_cancels_a_store_call_that_outlasts_the_grace_bound`, `test_shutdown_cancels_a_sleeping_retry_loop_at_once`.
- `tests/test_outbox_claims.py`: `test_shutdown_from_another_loop_lets_the_in_flight_store_call_finish`, `test_a_retry_loop_started_after_shutdown_dispatches_again`.

Other cancellations of an aiosqlite statement on a rollback-journal database can still leave the same zombie read lock, for example application code that wraps queries in `asyncio.timeout`. WAL mode on the outbox engine would cover those as well; it is not part of this change.
