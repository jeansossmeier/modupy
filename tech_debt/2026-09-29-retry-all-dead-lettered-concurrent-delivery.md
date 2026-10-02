---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-02
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

## Resolution (2026-10-02)
`retry_all_dead_lettered` delivers each reopened row through `modulith/builtin/outbox.py::_dispatch_resubmitted`, under the fence the configured claim strategy gives a sweep. Under `lease` that is a claim with renewal and fenced completion (`PostgresPublicationStore.claim_publication`); under `advisory_lock` it is the row's advisory lock; under `none` nothing fences across processes. A row a peer already holds is left to that peer.

`modulith outbox dead-letter --retry-all` runs `modulith/cli.py::_retry_all_and_drain`, which awaits the store's `wait_for_dispatch()` before the command's event loop ends. The events its listeners publish are therefore delivered before it exits.

Tests:
- `tests/test_postgres_outbox_adapter.py::test_retry_all_holds_a_claim_on_a_resubmitted_row_while_it_delivers`
- `tests/test_postgres_outbox_adapter.py::test_retry_all_leaves_a_row_a_peer_claimed_to_that_peer`
- `tests/test_postgres_outbox_adapter.py::test_retry_all_under_advisory_lock_delivers_while_holding_the_row_lock`
- `tests/test_postgres_outbox_adapter.py::test_retry_all_under_advisory_lock_without_a_lock_connection_leaves_the_row_to_the_sweep`
- `tests/test_cli.py::test_outbox_dead_letter_retry_all_delivers_the_events_its_listeners_publish`

Residual: the advisory-lock fence is tested on SQLite with stand-in lock functions only; no test takes a real Postgres advisory lock from a peer.

`examples/marketplace` keeps `claim_lease_seconds = 4`, so a claim left by a stopped process expires quickly. Its README no longer describes a lease wait after `--retry-all`.
