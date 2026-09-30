---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Testing
impact: Low - Auto-discovering bootstrap may re-import real modules in isolated tests
effort: Low - Add module-scope import guard or isolate discovery
---

# modulith_module isolation swaps only sys.modules

## Description
`modulith_module` isolation swaps only `sys.modules`, so an auto-discovering bootstrap may re-import the real siblings that were supposed to be mocked. Tests that rely on mock modules must ensure bootstrap does not re-discover.

## Affected Areas
- `modulith/testing.py::_module_isolation`

## Proposed Solution
Either: 1) isolate the discovery path itself (set a flag to skip auto-discovery), or 2) document that `modulith_module` tests must pass `discover=False` or import all required modules explicitly before instantiating the fixture.

## Context
Manual imports work around this. The issue only affects tests that rely on auto-discovery and mocked modules. [Assertion-Only]
