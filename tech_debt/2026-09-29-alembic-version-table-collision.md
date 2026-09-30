---
type: tech-debt
debt_status: open
created: 2026-09-29
updated: 2026-09-29
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
