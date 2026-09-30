---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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

## Context
These are nice-to-have features for large deployments. Single-module or small-team projects are unaffected. [Assertion-Only]
