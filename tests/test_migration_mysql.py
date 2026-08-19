"""Run the production alembic migration against a real MySQL.

``test_migrations.py`` runs the migration on SQLite and ``test_migration_
postgres.py`` on Postgres. Neither dialect has MySQL's strict ``VARCHAR``
length requirement, so a regression there (an unbounded ``String`` column
sneaking back into ``0001_initial.py`` or ``0003_outbox_claim_leases.py``)
would ship green on both while failing to even ``CREATE TABLE`` on MySQL.
This file runs the exact production command — ``alembic upgrade head`` —
against a live MySQL 8 so that class of bug can never ship green again.

Also covers the specific contract ``0003_outbox_claim_leases.py`` exists to
guarantee: a deployment that already ran ``0001``/``0002`` (an "existing
deployment") converges onto the *identical* schema that a fresh
``base -> head`` install produces, once ``0003``'s ``ALTER COLUMN`` runs.
Both paths are compared column-by-column (type/nullable/default) rather than
just by name, per the drift-guard pattern used throughout this suite.

Provisioned by the shared ``mysql_url`` fixture (testcontainers MySQL or
``MODULITH_TEST_MYSQL_URL``); skipped without Docker.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

import modulith.adapters as adapters_pkg

pytestmark = [pytest.mark.integration]

MIGRATIONS = Path(adapters_pkg.__file__).parent / "migrations"
_TABLES = ("event_publications", "event_publications_archive")
# Database-broker tables — listed here so the fixture drops them too, same
# reasoning as test_migration_postgres.py's _BROKER_TABLES.
_BROKER_TABLES = (
    "broker_retained_delivery",
    "broker_retained_message",
    "broker_message",
    "broker_subscription",
)


def _sync_url(async_url: str) -> str:
    """Alembic's command API is synchronous — swap the aiomysql driver for
    the sync pymysql driver against the same database."""
    return async_url.replace("+aiomysql", "+pymysql")


def _cfg(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _drop(engine) -> None:
    with engine.begin() as conn:
        # MySQL enforces FK/other constraints across drops within a session;
        # disabling checks for the cleanup avoids ordering the drops by hand.
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for tbl in (*_TABLES, *_BROKER_TABLES, "alembic_version"):
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))


@pytest.fixture
def clean_mysql(mysql_url: str):
    """Yield a sync pymysql engine on a disposable database with no managed tables."""
    url = _sync_url(mysql_url)
    engine = create_engine(url)
    _drop(engine)
    try:
        yield url, engine
    finally:
        _drop(engine)
        engine.dispose()


def _snapshot(engine, tables: list[str]) -> dict[str, dict[str, tuple[str, bool, object]]]:
    insp = inspect(engine)
    return {
        table: {
            c["name"]: (str(c["type"]), c["nullable"], c["default"])
            for c in insp.get_columns(table)
        }
        for table in tables
    }


def test_mysql_migrations_use_disposable_database(clean_mysql) -> None:
    url, engine = clean_mysql
    database = make_url(url).database

    assert database is not None
    assert database.startswith("modupy_test_")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT DATABASE()")).scalar_one().startswith("modupy_test_")


def test_alembic_upgrade_head_on_real_mysql(clean_mysql) -> None:
    """The real migration applies cleanly to MySQL and creates both outbox
    tables plus the pending-rows indexes — the exact command production runs."""
    url, engine = clean_mysql
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert _TABLES[0] in tables
    assert _TABLES[1] in tables
    indexes = {ix["name"] for ix in insp.get_indexes("event_publications")}
    assert "idx_pending" in indexes
    # 0005's MySQL half: MySQL cannot express the Postgres functional+partial
    # claim index, so the sweep gets a plain composite index on the pending
    # predicate instead. The Postgres-only name must not appear.
    assert "ix_event_publications_pending_scan" in indexes
    assert "ix_event_publications_claim_order" not in indexes
    archive_indexes = {ix["name"] for ix in insp.get_indexes("event_publications_archive")}
    assert "ix_event_publications_archive_completed_at" in archive_indexes


def test_alembic_upgrade_head_creates_broker_schema_on_real_mysql(clean_mysql) -> None:
    """The full chain creates queue and retained-replay tables plus indexes
    on real MySQL."""
    url, engine = clean_mysql
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert "broker_subscription" in tables
    assert "broker_message" in tables
    assert "broker_retained_message" in tables
    assert "broker_retained_delivery" in tables


def test_alembic_downgrade_base_on_real_mysql(clean_mysql) -> None:
    """``downgrade base`` removes every table (both revisions) on real MySQL."""
    url, engine = clean_mysql
    cfg = _cfg(url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")

    tables = set(inspect(engine).get_table_names())
    assert _TABLES[0] not in tables
    assert _TABLES[1] not in tables
    assert "broker_subscription" not in tables
    assert "broker_message" not in tables
    assert "broker_retained_message" not in tables
    assert "broker_retained_delivery" not in tables


def test_migration_columns_match_orm_on_real_mysql(clean_mysql) -> None:
    """The MySQL columns the migration creates match the ORM model exactly —
    guards against ORM/migration drift on this dialect specifically (the
    dialect where an unbounded ``String`` column fails outright)."""
    from modulith.adapters.postgres_outbox import (
        EventPublicationArchiveRow,
        EventPublicationRow,
    )

    url, engine = clean_mysql
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    for model in (EventPublicationRow, EventPublicationArchiveRow):
        migrated = {c["name"] for c in insp.get_columns(model.__tablename__)}
        orm = {c.name for c in model.__table__.columns}
        assert migrated == orm, f"{model.__tablename__}: migration {migrated} != ORM {orm}"


def test_upgrade_from_0002_matches_upgrade_from_base_on_real_mysql(clean_mysql) -> None:
    """0003_outbox_claim_leases.py exists to converge an "existing deployment"
    (one that already ran 0001/0002) onto the identical schema a fresh
    install gets. Stopping at 0002 and then upgrading to head must produce
    column-for-column (type/nullable/default) the same outbox tables as
    upgrading straight from base to head — the exact contract this
    migration's docstring and ``_TEXT_COLUMNS`` comment describe."""
    url, engine = clean_mysql
    cfg = _cfg(url)

    command.upgrade(cfg, "0002_broker_message")
    command.upgrade(cfg, "head")
    from_0002 = _snapshot(engine, list(_TABLES))
    _drop(engine)

    command.upgrade(cfg, "head")
    from_base = _snapshot(engine, list(_TABLES))

    assert from_0002 == from_base, (
        "upgrading from 0002 must land on the identical schema as upgrading "
        f"from base: {from_0002} != {from_base}"
    )
