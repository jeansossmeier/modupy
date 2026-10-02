---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
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

## Resolution (2026-10-01)
The `modulith_app` teardown now purges only the application package's modules. It takes the package from the bootstrapped configuration or, before bootstrap, from `configure(package=...)`. Third-party, stdlib and `modulith` modules stay in `sys.modules`, so SQLAlchemy is never re-imported and `PostgresPublicationStore._install_session_hooks` stays bound to the live `Session` class. A test that neither configured nor bootstrapped a package purges nothing. Application modules are still isolated per test. [Tool-Verified]

`tests/test_testing_plugin.py::test_modulith_app_purges_only_the_applications_modules` runs two tests in a subprocess, once with a configured and once with a bootstrapped package. A library and an application module are imported in the first; in the second the library is still in `sys.modules` and the application module is gone. The example suites keep their module-scope SQLAlchemy and aiosqlite imports.

## Follow-up (2026-10-02)
Purging nothing after a test that neither configured nor bootstrapped a package broke the marketplace example: 22 of its 54 tests failed with `Table 'inventory_stock' is already defined for this MetaData instance`. A test that never bootstrapped left the application modules it imported in `sys.modules`, `marketplace.db` and its shared `MetaData` among them. Every later test's snapshot therefore held that `MetaData`, while each later purge dropped the table modules registered on it, and their re-import defined the tables a second time. [Tool-Verified]

Teardown now resolves the package for such a test as bootstrap would, from `[tool.modulith] package`, `MODULITH_PACKAGE` or `[project] name`, and purges nothing only when none of those is set. `tests/test_testing_plugin.py::test_modulith_app_purges_the_project_package_when_the_test_never_bootstrapped` covers both `pyproject.toml` forms.
