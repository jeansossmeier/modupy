---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Architecture
impact: High - Prevents shared databases with existing Alembic history from using modulith migrations
effort: High - Requires an upgrade path for existing installs
---

# Alembic version table collision with business database history

## Description
The packaged Alembic setup uses the default `alembic_version` table. In a business database that has its own Alembic migration history, the two collide, which is why the examples create module tables with `metadata.create_all` instead of through migrations.

Changing the table name needs an upgrade path for existing installs, so the decision belongs to the maintainer.

## Affected Areas
- `modulith/adapters/migrations/env.py::run_migrations_online`

## Proposed Solution
Either: 1) move to a prefixed `modulith_alembic_version` table with a migration that renames the tracking table for existing installs, or 2) document that modulith migrations assume exclusive database access.

## Context
The examples work around this by using `metadata.create_all` outside the migration chain. This is a deliberate choice for examples, not a long-term solution for applications that need schema versioning.

## Resolution (2026-10-01)
`env.py` configures Alembic with `version_table="modulith_alembic_version"` in both online and offline mode, so the chain no longer reads or writes `alembic_version`. Before upgrading, `modulith/adapters/migrations/version_table.py::move_legacy_revision` moves a packaged-chain revision found in `alembic_version` into the new table, deletes that row, and drops `alembic_version` once it is empty. It runs whenever the new table holds no chain revision, including an empty one left by an interrupted move, and never touches a revision outside the chain. On PostgreSQL the move shares the upgrade's transaction and honours `-x schema=`. The named-schema guard reads the history through `read_revision_table`, which falls back to `alembic_version` for an install not yet moved. The fix covers `modulith migrate` and the raw `alembic upgrade` alike, because both run the same `env.py`.

Tests that prove it:
- `tests/test_migrations.py`: `test_fresh_upgrade_tracks_the_revision_in_modulith_alembic_version`, `test_upgrade_moves_a_legacy_head_revision_without_rerunning_migrations`, `test_upgrade_moves_a_legacy_revision_past_an_empty_modulith_alembic_version`, `test_upgrade_moves_a_legacy_mid_chain_revision_then_continues_from_it`, `test_upgrade_leaves_a_business_alembic_revision_untouched`, `test_upgrade_moves_only_the_modulith_row_out_of_a_shared_alembic_version`, `test_read_revision_table_finds_the_revision_in_either_layout`.
- `tests/test_cli.py`: `test_migrate_moves_a_revision_tracked_in_alembic_version_to_its_own_table`.
- `tests/test_migration_postgres.py` (integration): `test_upgrade_moves_a_legacy_revision_inside_the_named_schema`, plus the named-schema guard tests on both layouts.
