---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Architecture
impact: Medium - Package-level imports can violate declared boundaries without detection
effort: Medium - Requires verifier to track package re-exports
---

# Package-level imports bypass boundary verifier

## Description
Package-level imports like `from pkg import module` or `from .. import module` bypass the verifier's boundary rules. Extract catches them, but the live boundary check does not. A module can import a sibling through a package's `__init__.py` and violate boundaries without being detected.

## Affected Areas
- `modulith/builtin/verifier.py::_ImportCollector._resolve`

## Proposed Solution
Extend `_resolve` to track module re-exports from package `__init__.py` files and validate them against boundary rules.

## Context
Direct module imports (`from sister_module import ...`) are caught. The issue is only with package-level indirection. [Assertion-Only]
