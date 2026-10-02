---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-02
category: Operations
impact: Low - Operators lack some recovery and visibility commands
effort: Low - Multiple small CLI additions
---

# Operator and CLI gaps

## Description
Multiple operator-facing gaps:
1. No broker dead-letter command (only outbox has one)
2. No listing of failing outbox rows that are not yet dead-lettered
3. `/_modulith/topology` shows only the first replica in scaled deployments
4. `modulith dev` lacks `--worker-port-base` flag
5. Plugin rules cannot be disabled through configuration

## Affected Areas
- `modulith/cli.py` (CLI commands)
- `modulith/proxy.py` (topology endpoint)
- `modulith/builtin/verifier.py` (rule registration)

## Proposed Solution
Add: 1) broker dead-letter CLI command, 2) outbox filtering by status, 3) full topology in actuator endpoint, 4) `--worker-port-base` for `modulith dev`, 5) `disabled_rules` config key and enforcement.

## Progress (2026-10-01)
Items 3, 4 and 5 are done. `/_modulith/topology` gives each module a `replicas` list (`tests/test_proxy.py::test_topology_lists_all_replicas_for_scaled_module`), `modulith dev` takes `--worker-port-base` (`tests/test_cli.py::test_dev_processes_topology_accepts_worker_port_base`), and `disabled_rules` turns off plugin rules by name in `verify`, `doctor` and the strict bootstrap (`modulith/builtin/verifier.py::collect_violations`). Items 1 and 2 remain open.

## Resolution (2026-10-02)
Items 1 and 2 are done, so every item is closed.
- `modulith broker dead-letter` lists and resubmits dead letters on the database, shm and redis-streams brokers, through `list_dead_letters` and `retry_dead_letters` on `DatabaseBroker`, `ShmBroker` and `RedisStreamsBroker` (contract in `modulith/adapters/_dead_letter.py`). On redis-streams, `--retry-all` resubmits a target only when its stream has one consumer group, and exits 1 naming the targets it left (`tests/test_cli.py::test_broker_dead_letter_retry_all_reports_a_refusal_and_exits_1`).
- `modulith outbox failing` lists publications that have failed but are not dead-lettered, with each one's next retry time (`PostgresPublicationStore.find_failing`, `outbox.list_failing`, `outbox.next_retry_at`; `tests/test_cli.py::test_outbox_failing_lists_failing_publications_with_next_retry`).

## Context
These are nice-to-have features for large deployments. Single-module or small-team projects are unaffected. [Assertion-Only]
