---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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
