---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-02
category: Architecture
impact: Low - Single-process apps have no public way to clean up database connections on shutdown
effort: Low - Expose Runtime.shutdown or engine from configuration
---

# No public way to dispose outbox_url engine from single-process app

## Description
A single-process app has no public way to dispose the `outbox_url` engine on shutdown. Only workers call `Runtime.shutdown`, which disposes the engine. Single-process lifespan handlers can call `await engine.dispose()` on their own engine, but not on the configured outbox_url engine.

## Affected Areas
- `modulith/runtime.py::Runtime.bind_configured_outbox`, whose docstring states that only process-topology workers call `shutdown()`, which disposes the store and its engine.
- `modulith/runtime.py::Runtime.shutdown`

## Proposed Solution
Either: 1) expose `Runtime.shutdown()` as a public API for single-process apps, or 2) provide a helper to get the configured outbox engine for explicit disposal.

## Context
This is only a concern for long-running single-process apps or REPL environments that create many connections. Short-lived CLI tools are unaffected. [Assertion-Only]

## Resolution (2026-10-02)
`await outbox.shutdown()` disposes the store and engine that `Runtime.bind_configured_outbox` built from `outbox_url`, once the retry loop has stopped (`modulith/builtin/outbox.py::_dispose_owned_resources`, fed by `outbox._owned_resources`). No public API was added. A store the application passed to `outbox.configure()` is never disposed. Workers reach the same disposal through `Runtime.shutdown`, which calls `outbox.shutdown()`.

When the retry loop ran on another event loop than the one awaiting `shutdown()`, such as `publish_sync()`'s daemon-thread loop, the disposal runs on that loop, because a driver such as asyncpg closes a connection only on the loop that opened it.

Residual, accepted: if that loop has closed or stopped, or does not finish the disposal within `_shutdown_grace_seconds`, the engine is left open and a warning says so. `publish_sync()`'s exit handler stops its loop without closing it, so a shutdown that runs after that handler takes this path. A pool holding connections opened on both loops still has the ones from the awaiting loop closed from the retry loop, which asyncpg rejects; SQLAlchemy logs each such failure and drops the connection.

Tests:
- `tests/test_outbox.py::test_outbox_shutdown_closes_the_pool_of_the_engine_built_from_outbox_url`
- `tests/test_outbox.py::test_outbox_shutdown_leaves_an_application_configured_engine_alone`
- `tests/test_outbox.py::test_outbox_shutdown_ignores_the_outbox_url_when_the_application_configured_its_store`
- `tests/test_outbox.py::test_outbox_shutdown_twice_disposes_the_outbox_url_engine_once`
- `tests/test_outbox.py::test_outbox_shutdown_disposes_the_engine_on_the_retry_loops_own_event_loop`
- `tests/test_outbox.py::test_outbox_shutdown_does_not_wait_for_a_retry_task_whose_event_loop_stopped`
- `tests/test_outbox.py::test_outbox_shutdown_leaves_the_engine_open_with_a_warning_when_the_retry_loop_closed`
- `tests/test_outbox.py::test_outbox_shutdown_leaves_the_engine_open_with_a_warning_when_the_retry_loop_is_busy`
