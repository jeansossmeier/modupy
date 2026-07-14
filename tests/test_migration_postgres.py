"""Run the production alembic migration against a real Postgres.

``test_migrations.py`` runs the migration on SQLite, where dialect differences
hide real-Postgres errors — e.g. an ``is_dead_lettered BOOLEAN`` column whose
``server_default`` rendered as the integer ``0`` created cleanly on SQLite but
was rejected by Postgres ("column is of type boolean but default expression is
of type integer"). This test runs the exact production command —
``alembic upgrade head`` — against a live Postgres so that class of bug can
never ship green again.

Provisioned by the shared ``postgres_url`` fixture (testcontainers Postgres or
``MODULITH_TEST_POSTGRES_URL``); skipped without Docker.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

import modulith.adapters as adapters_pkg

pytestmark = [pytest.mark.integration]

MIGRATIONS = Path(adapters_pkg.__file__).parent / "migrations"
_TABLES = ("event_publications", "event_publications_archive")
# 0002's database-broker tables. Listed here so the fixture drops them too —
# otherwise the first `upgrade head` leaves them behind and every subsequent
# one fails re-creating an already-existing table (alembic_version is dropped).
_BROKER_TABLES = ("broker_message", "broker_subscription")


def _sync_url(async_url: str) -> str:
    """Alembic's command API is synchronous — swap the asyncpg driver for the
    sync psycopg driver against the same database."""
    return async_url.replace("+asyncpg", "+psycopg")


def _cfg(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _drop(engine) -> None:
    with engine.begin() as conn:
        for tbl in (*_TABLES, *_BROKER_TABLES, "alembic_version"):
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl} CASCADE"))


@pytest.fixture
def clean_pg(postgres_url: str):
    """A sync psycopg engine on a schema with no outbox/alembic tables."""
    url = _sync_url(postgres_url)
    engine = create_engine(url)
    _drop(engine)
    try:
        yield url, engine
    finally:
        _drop(engine)
        engine.dispose()


def test_alembic_upgrade_head_on_real_postgres(clean_pg) -> None:
    """The real migration applies cleanly to Postgres and creates both tables
    plus the partial pending-rows index."""
    url, engine = clean_pg
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert _TABLES[0] in tables
    assert _TABLES[1] in tables
    indexes = {ix["name"] for ix in insp.get_indexes("event_publications")}
    assert "idx_pending" in indexes


def test_migration_columns_match_orm_on_real_postgres(clean_pg) -> None:
    """The Postgres columns the migration creates match the ORM model exactly —
    guards against ORM/migration drift on the real dialect."""
    from modulith.adapters.postgres_outbox import (
        EventPublicationArchiveRow,
        EventPublicationRow,
    )

    url, engine = clean_pg
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    for model in (EventPublicationRow, EventPublicationArchiveRow):
        migrated = {c["name"] for c in insp.get_columns(model.__tablename__)}
        orm = {c.name for c in model.__table__.columns}
        assert migrated == orm, f"{model.__tablename__}: migration {migrated} != ORM {orm}"


def test_alembic_upgrade_head_creates_broker_schema_on_real_postgres(clean_pg) -> None:
    """0002 applies cleanly to Postgres over 0001 (the real multi-revision
    chain) and creates the broker tables plus their claim/prune/target indexes."""
    url, engine = clean_pg
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert "broker_subscription" in tables
    assert "broker_message" in tables
    indexes = {ix["name"] for ix in insp.get_indexes("broker_message")}
    assert "ix_broker_message_claim" in indexes
    assert "ix_broker_message_prune" in indexes
    assert "ix_broker_message_target" in indexes


def test_broker_migration_column_metadata_matches_schema_on_real_postgres(clean_pg) -> None:
    """Broker-side twin of the outbox drift guard: the 0002 migration renders
    the same Postgres schema (type/nullable/default) as db_broker.broker_schema()
    — inspector to inspector through pg_catalog, no hand-rolled normalization."""
    from modulith.adapters.db_broker import broker_schema

    url, engine = clean_pg
    tables = ["broker_subscription", "broker_message"]

    def snapshot() -> dict[str, dict[str, tuple[str, bool, object]]]:
        insp = inspect(engine)
        return {
            table: {
                c["name"]: (str(c["type"]), c["nullable"], c["default"])
                for c in insp.get_columns(table)
            }
            for table in tables
        }

    command.upgrade(_cfg(url), "head")
    migrated = snapshot()
    _drop(engine)

    metadata, _, _ = broker_schema()
    metadata.create_all(engine)
    from_schema = snapshot()

    assert migrated == from_schema


def test_alembic_downgrade_base_on_real_postgres(clean_pg) -> None:
    """``downgrade base`` removes every table (both revisions) on real Postgres."""
    url, engine = clean_pg
    cfg = _cfg(url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")

    tables = set(inspect(engine).get_table_names())
    assert _TABLES[0] not in tables
    assert _TABLES[1] not in tables
    assert "broker_subscription" not in tables
    assert "broker_message" not in tables


def test_migration_column_metadata_matches_orm_on_real_postgres(clean_pg) -> None:
    """A6-r3-136: the name-set comparison above is blind to type/nullable/
    server_default drift — the exact bug class this file's docstring cites
    (boolean server_default rendered as integer 0). Compare full column
    metadata of the migrated schema against a schema created straight from
    the ORM metadata on the same live Postgres: inspector-to-inspector, so
    both sides render through pg_catalog and equivalent definitions compare
    equal without hand-rolled normalization."""
    from modulith.adapters.postgres_outbox import (
        Base,
        EventPublicationArchiveRow,
        EventPublicationRow,
    )

    url, engine = clean_pg
    tables = [m.__tablename__ for m in (EventPublicationRow, EventPublicationArchiveRow)]

    def snapshot() -> dict[str, dict[str, tuple[str, bool, object]]]:
        insp = inspect(engine)
        return {
            table: {
                c["name"]: (str(c["type"]), c["nullable"], c["default"])
                for c in insp.get_columns(table)
            }
            for table in tables
        }

    command.upgrade(_cfg(url), "head")
    migrated = snapshot()
    _drop(engine)

    Base.metadata.create_all(engine)
    orm = snapshot()

    assert migrated == orm
