---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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
