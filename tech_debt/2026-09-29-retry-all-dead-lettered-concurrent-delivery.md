---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-30
category: Architecture
impact: Medium - Allows duplicate dispatch of dead-lettered events in concurrent sweeps
effort: Medium - Requires coordination protocol or claim semantics
---

# Concurrent delivery of dead-lettered events without claim

## Description
`outbox.retry_all_dead_lettered` dispatches without a claim. A concurrent sweep may deliver the same row too, which at-least-once delivery allows but is not ideal. Additionally, when run from the CLI it never drains the after-commit tasks, so events those listeners publish wait for another process's sweep.

That wait lasts a whole claim lease, not one sweep interval. Under `claim_strategy = "lease"`, each after-commit task claims its row before delivering (`PostgresPublicationStore._dispatch_after_commit`). The CLI returns as soon as `asyncio.run(outbox.retry_all_dead_lettered())` does, and `asyncio.run` cancels the tasks still pending. A row whose task claimed it but had not delivered it stays claimed until `claim_lease_seconds` expires: 60 s by default. Only then can a running process's sweep take it.

## Affected Areas
- `modulith/builtin/outbox.py::retry_all_dead_lettered`
- `modulith/cli.py::outbox_dead_letter`
- `modulith/adapters/postgres_outbox.py::PostgresPublicationStore._dispatch_after_commit`

## Proposed Solution
Either: 1) acquire a claim before dispatch (blocking concurrent sweeps), or 2) document that this is at-least-once behavior and multiple sweeps must be sequenced by the operator.

Separately, the CLI should finish or release its own work before exiting: await the pending after-commit dispatch tasks, or release their claims on cancellation, before `outbox_dead_letter` returns.

## Context
Operators reach it through `modulith outbox dead-letter --retry-all`, typically while the application's own processes keep sweeping.

The concurrent-delivery part is derived by reading the code, not by a run. [Assertion-Only]

The lease wait was observed in the `examples/marketplace` README runbook, in the o-300 story. After `--retry-all`, the `ShipmentBooked` rows stayed with `claim_owner` set and `attempt_count` 0 until the 60 s lease expired. The example therefore sets `claim_lease_seconds = 4` in its `[tool.modulith.outbox_options]`, and its README describes the lag. [Tool-Verified]
