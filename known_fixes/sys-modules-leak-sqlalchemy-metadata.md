---
type: known-fix
id: sys-modules-leak-sqlalchemy-metadata
date: 2026-10-02
tags: [testing, pytest, fixtures, sys-modules, import-state, sqlalchemy, registry]
scope: project
symptom: "After commit 2f6d48d, 22 of 54 marketplace example tests failed with 'sqlalchemy.exc.InvalidRequestError: Table X is already defined for this MetaData instance'. First test in each file passed; later tests in the same file failed. Bootstrap discovery logged 'module failed to import' for every module except catalog."
root_cause: "modulith_app teardown dropped only modules under the configured or bootstrapped package. A test that did neither dropped nothing. The application modules such a test imported, including marketplace.db with its shared SQLAlchemy MetaData, survived in sys.modules. Each later test's purge dropped table modules (inventory_stock, etc.) that registered on that MetaData. Their re-import in the next test ran Table(name, metadata, ...) again on the surviving MetaData, triggering the duplicate-table error. Before 2f6d48d, teardown dropped every new module, so nothing leaked."
wrong_hypotheses: ["Example code diverged from the repo — ruled out by diff showing only a scratch diagnostic probe in conftest", "The newest merge (trace propagation across the broker) introduced the break — ruled out by timeline: 2f6d48d landed at 17:42 on 2026-10-01, the break occurred then, suite never ran against the trace merge"]
fix_summary: "_application_package() resolves the package for a test that never bootstrapped using bootstrap's logic (overrides, MODULITH_PACKAGE, [tool.modulith] package, [project] name), without caller-stack detection"
affected_files: ["modulith/testing.py", "tests/test_testing_plugin.py"]
fix_status: resolved
anchor_commit: 312764d
---

# Partial sys.modules purge leaks SQLAlchemy MetaData across tests

## Symptom

In the marketplace example, 22 of 54 tests failed with `sqlalchemy.exc.InvalidRequestError: Table 'inventory_stock' is already defined for this MetaData instance` (also `notifications_notification`, `orders_order` and other table names). [Tool-Verified]

The failure pattern was:
- First test in each file passed.
- Later tests in the same file failed.
- Bootstrap discovery logged `module 'marketplace.<name>' failed to import` for every module except `catalog`.
- The example's application code and conftest were unchanged between green run (2026-10-01 01:37) and failure (2026-10-01 17:42).

## Wrong Hypotheses

1. **The example copy diverged from the repo** — Disproven: a full diff between the live repo and the example's copy showed only a scratch diagnostic probe appended to the conftest, unrelated to the failure.

2. **The newest merge (trace propagation across the broker) introduced the break** — Disproven by timeline: commit 2f6d48d landed at 17:42 on 2026-10-01 (the exact time the suite broke), while the trace merge predates 2f6d48d in the log. The suite never ran against the trace merge before 2f6d48d landed.

## Root Cause

Commit 2f6d48d changed `modulith_app` fixture teardown to drop only modules of the application package the test had configured or bootstrapped. The change fixed a problem where third-party libraries were re-imported and broke SQLAlchemy. However:

- A test that neither configured nor bootstrapped a package dropped nothing.
- The application modules such a test imported (e.g., `marketplace.db` with its shared SQLAlchemy `MetaData`) survived in `sys.modules`.
- Every later test's teardown snapshot therefore held that `MetaData` instance.
- Each later test's purge dropped table modules it had imported (e.g., `inventory_stock` module).
- When those table modules were re-imported in the next test run, they executed `Table(name, metadata, ...)` again on the surviving `MetaData`.
- SQLAlchemy raised `InvalidRequestError: Table 'inventory_stock' is already defined for this MetaData instance`.

Before 2f6d48d, teardown dropped every new module indiscriminately, so `marketplace.db` was dropped along with everything else and re-imported fresh in each test. The new selective-purge strategy was correct for third-party libraries but incomplete for tests that never bootstrapped.

## Fix

**Commit 312764d**: `_application_package()` helper resolves the package for a test that never bootstrapped, using the same logic as `Runtime._bootstrap`:

1. Check for `configure(package=...)` override (none for tests that didn't call configure).
2. Check `MODULITH_PACKAGE` environment variable.
3. Check `[tool.modulith] package` in `pyproject.toml`.
4. Fall back to `[project] name` in `pyproject.toml`.
5. If none of those is set, return `None` (purge nothing, as before).

The key difference from bootstrap detection: `_application_package()` **skips caller-stack detection**, which is meaningless during teardown when there is no active test frame.

With this logic:
- A test that never bootstrapped but sits in a project with `[project] name` set now purges that project's modules.
- A test that sits in a project with no package name set still purges nothing (preserving the pre-2f6d48d behaviour for edge cases).
- Application modules are now isolated per test, `marketplace.db` is purged after each test, and `MetaData` is fresh in the next test.

Changes:
- `modulith/testing.py`: added `_application_package()` helper; modified `modulith_app` fixture to call it for tests that never bootstrapped.
- `tests/test_testing_plugin.py`: added `test_modulith_app_purges_the_project_package_when_the_test_never_bootstrapped`, parametrized over both `pyproject.toml` forms (`[tool.modulith] package` and `[project] name`).

Verification: [Tool-Verified]
- Marketplace example suite passed 54 of 54 on Python 3.13.
- Marketplace example passed in integration lane wheel runbook.

## Diagnostic Journey

The marketplace suite broke after 2f6d48d landed, and the symptom — duplicate-table errors — pointed to SQLAlchemy's `MetaData` registry. The natural first hypothesis was that the example code or test setup had drifted from the repo, but a diff ruled that out. The timeline ruled out the trace propagation merge.

The key insight was recognizing the split state: `marketplace.db` with its shared `MetaData` survived in `sys.modules`, while individual table modules (like `marketplace.inventory.models.py`) were being dropped and re-imported. A partial purge creates this exact scenario.

The break happened only after 2f6d48d changed the purge logic to selective-per-package, which was correct for third-party libraries but left a gap: tests that never bootstrapped got no package name to purge, so application modules leaked. The solution was to give those tests the same package-name resolution that bootstrap uses, minus the caller-stack frame detection that has no meaning during teardown.

The regression test confirms that the fix works for both ways a project can specify its package name (`[tool.modulith] package` and `[project] name`), and that purging still works correctly when a package name is set.

## Generalizable Pattern

A partial `sys.modules` purge between tests creates split state when a module holding a registry (SQLAlchemy `MetaData`, a metrics registry, a plugin registry, any shared global) survives while modules that register into it are re-imported.

**Detection heuristic:**
- Errors like "already defined", "already registered", or "duplicate" that hit every test after the first in a file.
- `sys.modules` snapshots at test start and test end that show modules were dropped but a singleton registry survived.

**Root-cause pattern:**
- A purge policy drops `A` and `B` (modules that register).
- A purge policy keeps `C` (the shared registry).
- On re-import, `A` and `B` register again into the surviving `C`.
- The second registration fails.

**Prevention:**
- A purge policy must drop a registry together with every module that registers into it.
- If some modules are in scope to drop and others are not (e.g., third-party libraries), verify that the registry lives in a module you are dropping, or accept that it survives across tests and modules must be idempotent on re-import.
- A policy that purges nothing for some tests must account for the modules those tests import staying in `sys.modules` for all later tests in the session.

## Summary

The switch to selective purging of application modules (to fix SQLAlchemy re-import breakage) left a gap: tests that never bootstrapped got no package name to purge. Their application modules leaked into `sys.modules`, including `marketplace.db` with its shared `MetaData`. Later tests dropped and re-imported table modules, triggering duplicate-table errors. The fix resolves the package name for unboostrapped tests using the same logic as bootstrap, and the regression test confirms both `pyproject.toml` forms are covered.
