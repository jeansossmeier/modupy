---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Observability
impact: Low - Trace and metric interpretation differs between outbox and memory dispatch
effort: Low - Standardize naming and linkage
---

# listener.name differs and spans not linked across outbox/broker

## Description
Multiple observability inconsistencies:
1. `listener.name` differs between in-memory dispatch and outbox dispatch
2. Spans are not linked across the outbox or the broker
3. `Configuration.observability` is read only by `info` command, not used for tracing configuration

## Affected Areas
- `modulith/runtime.py` (listener instrumentation)
- `modulith/builtin/outbox.py` (span linking)
- `modulith/adapters/db_broker.py` (span linking)

## Proposed Solution
Standardize listener naming, add parent-child links for cross-process spans, and use `Configuration.observability` consistently in all observability paths.

## Context
Derived by reading the code, not by a run. The exact symbols are not pinned yet; the files above are where each behavior lives. [Assertion-Only]
