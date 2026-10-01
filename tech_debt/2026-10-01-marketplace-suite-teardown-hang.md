---
type: tech-debt
debt_status: resolved
created: 2026-10-01
updated: 2026-10-01
category: Testing
impact: Medium - An intermittent hang turned the integration lane red, and the runbook killed the step after 300s without a stack trace
effort: Small - One drain loop in the Postgres outbox adapter
---

# The marketplace suite could hang in a test teardown

## Description
CI run 36810588184 (commit `005e22b`), job `pytest -m integration (Python 3.13)`: the marketplace README runbook's `pytest` step timed out after 300 s, in the teardown of `test_a_country_without_a_zone_cannot_be_booked`. A rerun of the same commit passed. Locally, the full marketplace suite hung once in 46 runs on Python 3.13, in the same teardown. [Tool-Verified]

## Root cause
`PostgresPublicationStore.wait_for_dispatch()` looped while `_inflight` was non-empty. A task leaves `_inflight` only through a done callback, which its loop runs one step after the task finishes. When `Runtime.shutdown()` reached the drain inside that step, the finished task was still in the set. [Tool-Verified]

On Python 3.12+, `asyncio.gather()` over finished tasks completes before it returns (`asyncio/tasks.py`, `gather`), and awaiting a finished future does not yield (`asyncio/futures.py`, `Future.__await__`). The drain therefore spun without ever yielding, so the callback that would have ended it never ran. Python 3.11 always defers gather's completion through `add_done_callback`, which is why only the 3.13 lane hung. [Tool-Verified]

Thread dumps of the local hang at 45 s and at 100 s match this. The main thread is busy inside `wait_for_dispatch` at two different lines, no task is pending except the fixture finalizer, and every aiosqlite worker is idle.

## Resolution (2026-10-01)
`wait_for_dispatch()` now drops a finished task itself instead of waiting for its done callback.

It also stops waiting for a task whose loop has closed, because that task can never finish. `sync._run_nested_dispatch` closes its loop without cancelling the tasks left on it. Such a task is dropped with a WARNING, and the retry sweep delivers its publication.

Regression tests are in `tests/test_postgres_outbox_adapter.py`:
- `test_wait_for_dispatch_returns_when_a_finished_task_awaits_its_done_callback` fails on Python 3.12+ without the fix.
- `test_wait_for_dispatch_stops_waiting_for_a_task_whose_loop_closed` covers the closed loop.
