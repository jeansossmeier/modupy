---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Architecture
impact: Medium - Allows duplicate dispatch of dead-lettered events in concurrent sweeps
effort: Medium - Requires coordination protocol or claim semantics
---

# Concurrent delivery of dead-lettered events without claim

## Description
`outbox.retry_all_dead_lettered` dispatches without a claim. A concurrent sweep may deliver the same row too, which at-least-once delivery allows but is not ideal. Additionally, when run from the CLI it never drains the after-commit tasks, so events those listeners publish wait for another process's sweep.

## Affected Areas
- `modulith/builtin/outbox.py::retry_all_dead_lettered`

## Proposed Solution
Either: 1) acquire a claim before dispatch (blocking concurrent sweeps), or 2) document that this is at-least-once behavior and multiple sweeps must be sequenced by the operator.

## Context
Operators reach it through `modulith outbox dead-letter --retry-all`, typically while the application's own processes keep sweeping. Derived by reading the code, not by a run. [Assertion-Only]
