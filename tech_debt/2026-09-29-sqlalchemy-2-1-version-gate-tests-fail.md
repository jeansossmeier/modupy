---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Testing
impact: High - 50+ test failures in default lane when SQLAlchemy 2.1.1 is installed (CI-affecting)
effort: Medium - Add raising=False flag and guard deletion
---

# SQLAlchemy 2.1.1 skip-locked version-gate tests fail

## Description
Under SQLAlchemy 2.1.1 (what an unlocked install resolves today), skip-locked version-gate tests fail with ~50 FAILED/ERROR results because they use `monkeypatch.delattr` on an attribute (`_mariadb_normalized_version_info`) that SQLAlchemy 2.1 removed. The attribute does not exist, so `delattr` raises AttributeError.

The product code handles this gracefully through `getattr(..., None)` defaults in the same file, so the test gate is being overly strict.

## Affected Areas
- `tests/test_db_broker.py`: the `test_skip_locked_gate_*` tests, including `test_skip_locked_gate_reads_the_version_without_sqlalchemys_mariadb_field`.
- `modulith/adapters/db_broker.py::_supports_skip_locked`, which reads the attribute through a `getattr` default and so already tolerates its absence.

## Proposed Solution
Use `monkeypatch.delattr(..., raising=False)` to handle missing attributes gracefully, matching the production code's defensive pattern.

## Context
CI installs dependencies without a lock file, so it resolves SQLAlchemy 2.1.1 and these tests fail there. The same tests pass under SQLAlchemy versions that still define the attribute. [Tool-Verified]
