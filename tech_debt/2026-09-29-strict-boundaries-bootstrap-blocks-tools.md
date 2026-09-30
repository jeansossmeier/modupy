---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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
