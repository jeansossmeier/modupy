# Known Fixes Index

| ID | Date | Tags | Status | Anchor | Symptom | Root Cause |
|---|---|---|---|---|---|---|
| `cancellation-zombie-read-lock` | 2026-10-02 | sqlite, aiosqlite, asyncio, cancellation, garbage-collection, locking, shutdown | resolved | 4848a03 | Test teardowns take 5–10 s; after-commit claims wait on "database is locked" in ~1/3 of runs on rollback-journal SQLite | Cancellation during SELECT leaves cursor alive via traceback frames; closed connection holds SHARED lock until GC; COMMIT waits busy timeout |
| `sys-modules-leak-sqlalchemy-metadata` | 2026-10-02 | testing, pytest, fixtures, sys-modules, import-state, sqlalchemy, registry | resolved | 312764d | After 2f6d48d, 22 of 54 marketplace tests failed with "Table X is already defined for this MetaData instance" | Teardown purged nothing for tests that never bootstrapped; application modules leaked into sys.modules; later tests dropped and re-imported table modules on surviving MetaData |
