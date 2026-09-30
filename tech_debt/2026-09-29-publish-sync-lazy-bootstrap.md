---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
category: Architecture
impact: Low - First publish_sync lazily initializes retry loop, which may delay retries
effort: Low - Explicit bootstrap before first publish_sync
---

# publish_sync lazily bootstraps on first call

## Description
The first `publish_sync` in a script lazily bootstraps on the sync loop and starts the retry loop and crash sweep. This can delay the first retry cycle by up to the configured interval.

## Affected Areas
- `modulith/sync.py::publish_sync`

## Proposed Solution
Document this behavior and recommend calling `modulith.bootstrap()` explicitly before the first `publish_sync` if immediate retry is needed. The examples follow this pattern.

## Context
Only affects synchronous scripts that use `publish_sync` and expect fast retries. Asynchronous code uses the normal bootstrap lifecycle.
