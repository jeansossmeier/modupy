---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Testing
impact: Medium - Durable outbox tests fail silently if modules are purged between tests
effort: Medium - Requires fixture to preserve SQLAlchemy Session binding
---

# modulith_app teardown purges outbox hook binding

## Description
The `modulith_app` fixture's teardown purges every module first imported during a test, third-party libraries included. That breaks SQLAlchemy in two ways:
- Re-importing SQLAlchemy after its compiled extensions were purged fails with `TypeError: 'InternalTraversal' object is not callable`. This was observed in the example test suites.
- `PostgresPublicationStore._install_session_hooks` binds to the SQLAlchemy `Session` class it first imported. After a purge and re-import, the durable dispatch hook would stay on the old class and silently never fire. This is derived by reading the code.

## Affected Areas
- `modulith/testing.py::modulith_app`, which snapshots `sys.modules` and deletes everything added during the test.
- `modulith/adapters/postgres_outbox.py::PostgresPublicationStore._install_session_hooks`

## Proposed Solution
Purge only the application package's modules, not third-party libraries.

## Context
The example test suites work around it by importing `aiosqlite`, `sqlalchemy.ext.asyncio` and `sqlalchemy.orm` at module scope, before any test runs. [Tool-Verified] for the `TypeError`; [Assertion-Only] for the silent hook.
