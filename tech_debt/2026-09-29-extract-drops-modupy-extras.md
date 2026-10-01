---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Architecture
impact: Medium - Extracted services lose optional dependency groups like otel, postgres
effort: Medium - Requires tracking and preserving extras through extraction
---

# Extract operation drops modupy extras

## Description
Extract's `_render_pyproject` drops the source project's modupy extras (such as `otel`) and pins `==<version>`. An extracted service that relied on optional dependencies must manually add them back to its `pyproject.toml`.

## Affected Areas
- `modulith/extract.py::_render_pyproject`

## Proposed Solution
Preserve modupy extras that the source project declares and pass them through to the extracted service's dependency.

## Context
Example: a project with `dependencies = ["modupy[postgres,otel]"]` extracts to `dependencies = ["modupy==0.10.0"]`, requiring manual edit to re-add `[postgres,otel]`.

## Resolution
Added `_extract_modupy_extras` function that uses `packaging.requirements.Requirement` to parse the source project's modupy dependency and extract any extras. The function normalizes the package name per PEP 503 and returns extras as a set. In `_render_pyproject`, source extras are now merged with configuration-derived extras and sorted before rendering.

Tests added:
- `test_render_pyproject_preserves_modupy_extras_from_source` — verifies extras from source are merged and sorted
- `test_render_pyproject_handles_source_without_modupy_extras` — verifies source without extras produces config-only output
- `test_render_pyproject_no_source_modupy_dependency` — verifies other source deps pass through unchanged
