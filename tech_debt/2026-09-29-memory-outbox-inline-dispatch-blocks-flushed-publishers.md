---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Documentation
impact: Low - Undocumented behavior that can stall publishers on a SQLite file
effort: Low - Documentation only
---

# Memory outbox runs listeners inside the publisher's open transaction

## Description
The docs never say that under the memory outbox, `publish()` runs listeners inline, while the publisher's transaction is still open. When the publisher has already flushed a write to a SQLite file, it holds the file's write lock, and a listener that writes in its own transaction waits on that lock until SQLite's busy timeout expires.

## Affected Areas
- `docs/COOKBOOK.md` and `docs/ARCHITECTURE.md`, which describe the memory outbox without this caveat.
- `examples/demo_app/shop/orders/__init__.py::place_order` shows the workaround: it publishes before adding its row and never flushes first.

## Proposed Solution
Document the inline dispatch and the SQLite consequence next to the memory outbox's description, and recommend publishing before flushing.

## Context
Any single-process app on a SQLite file with the default memory outbox is affected. Postgres and MySQL take row locks, so they are not. [Assertion-Only]

## Resolution (2026-10-01)
`docs/COOKBOOK.md` (recipe 3) and `docs/ARCHITECTURE.md` (§6, after the publish sequence) now say that under the memory outbox `publish()` runs the listeners inline, while the publisher's transaction is still open. They say that a publisher which has flushed a write to a SQLite file holds its write lock, so a listener that writes in its own transaction waits out SQLite's busy timeout and fails with `database is locked`. Both recommend publishing before flushing and point at `place_order` in the demo app.

A probe on the demo app under the memory outbox confirmed each claim. After `publish()` returned, the listener's row was committed and the publisher's was not. With the publisher's order row flushed first, `publish()` raised `database is locked` after 5.1 s and the listener's row was not committed. `PRAGMA busy_timeout` read 5000 ms on the driver's connections. [Tool-Verified]

The docs state only the SQLite case. That Postgres and MySQL are unaffected, from the Context above, was not checked.
