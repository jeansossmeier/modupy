---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
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

## Resolution (2026-10-01)
`_ImportCollector.visit_ImportFrom` treats `from <pkg> import <name>` as an import of `<pkg>.<name>` when that is a module or package on disk (`_is_submodule`), for absolute and relative (`from .. import <name>`) forms alike. Every rule that fires on `import <pkg>.<name>` now fires on these forms, at the same severity, with no opt-in switch: `undeclared-dependency`, `contracts-is-sink`, `no-cyclic-dependency` (TYPE_CHECKING-guarded forms stay out of the cycle graph), and `no-internal-imports` / `use-contracts`, which read the imported names. A plain attribute, a `_`-prefixed name, a wildcard, a top-level helper such as a shared `db` module, and an application module's own submodules produce no new violation.

The fix needed no `__init__.py` re-export tracking: the submodule itself becomes the import target, which is also what `modulith extract` already assumed.

Apps that relied on the gap now fail `modulith verify`, and fail to boot under `strict_boundaries`.

Tests that prove it, all in `tests/test_verifier_boundaries.py`:
- `test_package_level_sibling_import_fires_undeclared_dependency_like_direct`, `test_package_level_sibling_import_fires_contracts_is_sink_like_direct`, `test_package_level_sibling_import_is_a_dependency_cycle_like_direct`, `test_package_level_import_of_a_private_submodule_fires_rule_1_like_direct`, `test_package_level_import_keeps_type_names_for_rule_4`, `test_type_checking_package_level_import_is_checked_but_not_a_cycle`.
- No false positives: `test_importing_a_plain_attribute_of_a_package_is_not_a_submodule_import`, `test_package_level_import_of_shared_helper_is_not_a_boundary_violation`, `test_application_module_importing_its_own_submodules_is_clean`.
