"""Alembic environment for the modulith outbox schema.

Targets ``modulith.adapters.postgres_outbox.Base`` (the ``event_publications``
and ``event_publications_archive`` tables). The database URL is resolved from,
in order: an ``-x url=...`` argument, the ``MODULITH_DB_URL`` environment
variable, or the ``sqlalchemy.url`` config option. This keeps the migration
runnable against Postgres in production and SQLite in tests without editing
``alembic.ini``.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from modulith.adapters.postgres_outbox import Base

config = context.config
target_metadata = Base.metadata


def _resolve_url() -> str:
    x_args = context.get_x_argument(as_dictionary=True)
    return (
        x_args.get("url")
        or os.environ.get("MODULITH_DB_URL")
        or config.get_main_option("sqlalchemy.url", "")
    )


def run_migrations_offline() -> None:
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _resolve_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
