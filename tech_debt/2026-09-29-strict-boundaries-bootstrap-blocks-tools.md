---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Architecture
impact: Medium - Operators cannot use verify, docs, doctor, or extract --force with strict_boundaries=true
effort: Medium - Requires decoupling boundary checks from bootstrap
---

# Bootstrap raises on boundaries before verify/docs/doctor/extract --force

## Description
Under `strict_boundaries = true`, bootstrap raises on WARNING findings before `verify`, `docs`, `doctor` or `extract --force` can apply their own contracts. This blocks operators from using these tools for inspection and remediation when boundaries are misconfigured.

## Affected Areas
- `modulith/runtime.py::Runtime._bootstrap`
- `modulith/builtin/verifier.py` (boundary checking)

## Proposed Solution
Allow `verify`, `docs`, `doctor` and `extract` to run with `strict_boundaries = true` and report their own boundary findings separately, rather than failing during bootstrap.

## Context
Tools that read configuration only (without bootstrapping) are unaffected. This is an issue only for tools that require module inspection at import time.

## Resolution (2026-10-01)
`verify`, `docs`, `doctor` and `extract` bootstrap through `_bootstrap_or_exit(inspection=True)`, which sets a private context variable (`modulith.runtime._inspection_bootstrap`) for the duration of that bootstrap. While it is set, the `strict_boundaries` block in `Runtime._bootstrap` collects no abort, so each command reaches its own verdict: `verify` and `doctor` exit 1 on errors, `extract` blocks unless `--force`, and `docs` generates its files.

Application processes are unchanged. No public signature or configuration field changed, and no environment variable was added. A violating app still raises `ConfigurationError` in both the single-process and the process topology.

Tests that prove it:
- `tests/test_cli.py`: `test_verify_under_strict_boundaries_reports_violations_and_exits_one`, `test_docs_under_strict_boundaries_still_generates_files`.
- `tests/test_cli.py`: `test_app_bootstrap_still_raises_under_strict_boundaries_after_a_tool_command` shows the tolerance does not leak into bootstrapping the app.
- `tests/test_extract.py`: `test_extract_force_under_strict_boundaries_extracts_with_notes`.
- `tests/test_doctor.py`: `test_doctor_cli_under_strict_boundaries_reports_its_own_verdict`.
