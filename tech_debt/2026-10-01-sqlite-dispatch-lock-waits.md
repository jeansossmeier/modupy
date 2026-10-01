---
type: tech-debt
debt_status: open
created: 2026-10-01
updated: 2026-10-01
category: Architecture
impact: Low - On SQLite, an after-commit dispatch can wait out the 5 s busy timeout and fail with "database is locked"; the retry sweep delivers the publication later
effort: Medium - The connection holding the lock is not identified yet
---

# On SQLite, after-commit dispatches can wait 5 s for the write lock

## Description
In 9 of 30 runs of `examples/marketplace/tests/test_marketplace_shipping.py`, one to three test teardowns took 5 s longer each. In the run inspected with live logging, each slow teardown was an after-commit dispatch's claim waiting out SQLite's 5 s busy timeout. `PostgresPublicationStore._dispatch_after_commit` then logged `after-commit dispatch failed ... database is locked` at ERROR. [Tool-Verified]

A dump taken 2.5 s into one such teardown showed [Tool-Verified]:
- four aiosqlite connections inside transactions;
- three `_dispatch_after_commit` tasks, claiming or delivering;
- the retry loop still in its crash-recovery sweep (`outbox._retry_loop`, the `_guarded_sweep(timedelta(0))` call) while being cancelled.

## Cause
Not established. The pool checkout stacks recorded only greenlet frames, so the dump does not show which task owns the connection that holds the lock.

One factor is visible in the code. The retry loop starts lazily at the first transactional publish, and its first sweep takes every incomplete row (`older_than=0`). That includes rows this process just committed, which their after-commit dispatches are claiming at the same time. On SQLite, every claim needs the single database write lock.

## Next step
Record which task opens each transaction, for example by capturing `asyncio.current_task()` in a pool checkout listener. Then decide whether the crash-recovery sweep should skip rows published after the retry loop started.
