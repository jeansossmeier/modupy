---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
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

## Resolution (2026-10-01)
The recorded mechanism did not reproduce: discovery imports a sibling through `importlib.import_module`, which returns the mock already in `sys.modules`, so the mock survived a bootstrap by itself. [Tool-Verified]

The isolation did leak the real sibling another way. The retained ancestor package keeps each imported child as an attribute, and `from fakeapp import inventory` reads that attribute, not `sys.modules`. `_module_isolation` now clears those attributes for the removed modules, points them at the mocks, and restores them on exit. A mocked sibling stays the mock however it is reached, with or without an auto-discovering bootstrap. [Tool-Verified]

`tests/test_testing_plugin.py::test_modulith_module_mock_survives_auto_discovering_bootstrap` failed before the change on the `from fakeapp import inventory` binding and passes now.
