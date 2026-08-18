"""Alembic environment for the modulith schemas.

Targets both packaged schemas: ``modulith.adapters.postgres_outbox.Base`` (the
``event_publications`` / ``event_publications_archive`` outbox tables) and the
database-broker Core tables from ``modulith.adapters.db_broker.broker_schema``
(``broker_subscription`` / ``broker_message``). Both are listed so
``--autogenerate`` sees the full schema and never proposes dropping the other
half; the hand-written revisions run regardless. The database URL is resolved
from, in order: an ``-x url=...`` argument, the ``MODULITH_DB_URL`` environment
variable, or the ``sqlalchemy.url`` config option. This keeps the migrations
runnable against Postgres in production and SQLite in tests without editing
``alembic.ini``.

The target schema (Postgres only) is resolved the same way from ``-x
schema=...`` or ``MODULITH_DB_SCHEMA``: it routes every migrated table,
including ``alembic_version``, into that schema via a
``schema_translate_map`` rather than editing table objects.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from modulith.adapters.db_broker import broker_schema
from modulith.adapters.postgres_outbox import Base

logger = logging.getLogger("modulith.adapters.migrations.env")

config = context.config
# A list of MetaData (alembic multi-metadata autogenerate) — outbox + broker.
target_metadata = [Base.metadata, broker_schema()[0]]


def _resolve_url() -> str:
    x_args = context.get_x_argument(as_dictionary=True)
    return (
        x_args.get("url")
        or os.environ.get("MODULITH_DB_URL")
        or config.get_main_option("sqlalchemy.url", "")
    )


def _resolve_schema() -> str | None:
    x_args = context.get_x_argument(as_dictionary=True)
    return x_args.get("schema") or os.environ.get("MODULITH_DB_SCHEMA") or None


def run_migrations_offline() -> None:
    if _resolve_schema():
        # Offline mode renders DDL without ever opening a connection, so
        # there is no live connection to carry a schema_translate_map — a
        # schema request here would either be silently dropped or need
        # unqualified-vs-scoped DDL to diverge from the online path.
        raise SystemExit("schema requires online mode")
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    schema = _resolve_schema()
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _resolve_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        configure_kwargs: dict[str, Any] = {}
        if schema:
            if connection.dialect.name == "postgresql":
                connection = connection.execution_options(schema_translate_map={None: schema})
                configure_kwargs["version_table_schema"] = schema
            else:
                logger.warning(
                    "schema=%r is only supported on PostgreSQL; ignoring it for the %r dialect",
                    schema,
                    connection.dialect.name,
                )
                schema = None
        context.configure(
            connection=connection, target_metadata=target_metadata, **configure_kwargs
        )
        with context.begin_transaction():
            if schema:
                from sqlalchemy.schema import CreateSchema

                connection.execute(CreateSchema(schema, if_not_exists=True))
                # schema_translate_map only affects statements SQLAlchemy Core
                # compiles from a bound Table object (op.create_table's
                # CREATE TABLE). Alembic's own ALTER/batch-mode DDL (e.g.
                # 0003's batch_alter_table) renders the table name as a plain
                # string with no schema qualifier at all, so it resolves
                # through the connection's search_path instead — set it here
                # so unqualified ALTER TABLE lands in the same schema as the
                # CREATE TABLE that preceded it.
                quoted_schema = connection.dialect.identifier_preparer.quote_schema(schema)
                connection.execute(text(f"SET search_path TO {quoted_schema}, public"))
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
