---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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
