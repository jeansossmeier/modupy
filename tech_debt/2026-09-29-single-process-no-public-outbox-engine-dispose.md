---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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
