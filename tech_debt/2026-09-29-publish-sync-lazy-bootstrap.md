---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
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

## Resolution (2026-10-01)
The `publish_sync` docstring (which generates `docs/API_REFERENCE.md`), `docs/COOKBOOK.md` (recipe 4) and `docs/ARCHITECTURE.md` (§2 and §6) now say that the first `publish_sync()` bootstraps lazily on the daemon-thread loop, inside that call's `timeout`. With a durable outbox bound from `outbox_url`, that bootstrap starts the retry loop and the crash sweep on that loop. They recommend calling `modulith.bootstrap()` before the first `publish_sync()`.

The record's retry premises did not hold, so the docs do not promise faster retries. In sync code `bootstrap()` runs where no event loop is running, so it starts no retry loop. The loop then starts at the first publish made inside a bound session, which is later than the lazy path starts it, and the lazy path's first sweep ran within the first call, not one retry interval later. [Tool-Verified]

What the explicit call changes is where startup runs. With a 2 s discovery import and `timeout=1.0`, a lazy first call raised `PublishSyncTimeout` and the event was never delivered, while `bootstrap()` absorbed the 2 s and the next `publish_sync()` returned at once. A failing import raised `ConfigurationError` from the first call, or from `bootstrap()` when it came first. [Tool-Verified]

The Proposed Solution said the examples follow this pattern. None of them calls `publish_sync`. The example apps call `bootstrap()` from a lifespan or before `asyncio.run`.
