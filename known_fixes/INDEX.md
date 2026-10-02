# Known Fixes Index

| ID | Date | Tags | Status | Anchor | Symptom | Root Cause |
|---|---|---|---|---|---|---|
| `cancellation-zombie-read-lock` | 2026-10-02 | sqlite, aiosqlite, asyncio, cancellation, garbage-collection, locking, shutdown | resolved | 4848a03 | Test teardowns take 5–10 s; after-commit claims wait on "database is locked" in ~1/3 of runs on rollback-journal SQLite | Cancellation during SELECT leaves cursor alive via traceback frames; closed connection holds SHARED lock until GC; COMMIT waits busy timeout |
