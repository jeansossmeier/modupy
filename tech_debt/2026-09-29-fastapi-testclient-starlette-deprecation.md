---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Testing
impact: Low - Test imports emit Starlette deprecation warning; fails with -W error
effort: Low - Install httpx2 as dependency or suppress warning
---

# fastapi.testclient emits Starlette deprecation warning

## Description
Importing `fastapi.testclient.TestClient` emits a Starlette deprecation warning: "Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead". Any test run with `-W error` fails at import, before tests even run.

## Affected Areas
- `fastapi.testclient`, which re-exports `starlette.testclient.TestClient`; Starlette emits `StarletteDeprecationWarning` when that module is imported.
- Every root test and example test that imports `fastapi.testclient.TestClient`. The warning shows in the summary of any run that imports it, including `examples/demo_app`'s own test run.

## Proposed Solution
Either: 1) add `httpx2` as a test dependency once Starlette's test client supports it, or 2) filter `StarletteDeprecationWarning` for that import in `[tool.pytest.ini_options].filterwarnings`.

## Context
This is Starlette's deprecation, not modupy's code. Observed in test runs. [Tool-Verified]

## Resolution
Resolved 2026-10-01 by the first proposed solution. The root `test` extra and the `test` group of `examples/demo_app`, `examples/marketplace` and `examples/quickstart` now declare `httpx2` (root floor `>=2.0`, the floor Starlette 1.7's own `full` extra declares; verified by importing `fastapi.testclient` with warnings as errors against `httpx2==2.0.0`). The `demo_app` and `quickstart` READMEs install it; the `marketplace` README installs through `.[test]`. `httpx` stays in the `fastapi` extra because `modulith/proxy.py` imports it. Proved by `tests/test_ci_config.py::test_projects_using_the_starlette_test_client_declare_httpx2`, `test_test_extra_floors_httpx2_at_the_release_starlette_accepts` and `tests/test_example_readmes.py::test_readme_installs_exactly_the_declared_dependencies`; `tests/test_worker.py`, the `demo_app` and `quickstart` suites, and the `marketplace` saga tests pass under `-W error::starlette.exceptions.StarletteDeprecationWarning`.
