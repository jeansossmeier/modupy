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

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema, DropSchema

import modulith.adapters as adapters_pkg

pytestmark = [pytest.mark.integration]

MIGRATIONS = Path(adapters_pkg.__file__).parent / "migrations"
_TABLES = ("event_publications", "event_publications_archive")
# Database-broker tables. Listed here so the fixture drops them too —
# otherwise the first `upgrade head` leaves them behind and every subsequent
# one fails re-creating an already-existing table (both version tables are dropped).
_BROKER_TABLES = (
    "broker_retained_delivery",
    "broker_retained_message",
    "broker_message",
    "broker_subscription",
)


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
        for tbl in (*_TABLES, *_BROKER_TABLES, "modulith_alembic_version", "alembic_version"):
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl} CASCADE"))


@pytest.fixture(scope="module")
def migration_postgres_url(postgres_url: str) -> Iterator[str]:
    """Run destructive migration checks only inside a disposable database."""
    admin_url = make_url(_sync_url(postgres_url))
    database_name = f"modupy_migration_{uuid4().hex}"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    database_created = False
    try:
        try:
            with admin_engine.connect() as conn:
                conn.execute(text(f'CREATE DATABASE "{database_name}"'))
            database_created = True
        except SQLAlchemyError as exc:
            pytest.skip(
                "PostgreSQL migration tests require CREATEDB privilege to protect "
                f"the supplied database; temporary database creation failed ({type(exc).__name__})"
            )

        test_url = admin_url.set(database=database_name).render_as_string(hide_password=False)
        yield test_url
    finally:
        try:
            if database_created:
                with admin_engine.connect() as conn:
                    conn.execute(
                        text(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                        ),
                        {"database_name": database_name},
                    )
                    conn.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        finally:
            admin_engine.dispose()


@pytest.fixture
def clean_pg(migration_postgres_url: str):
    """Yield a sync psycopg engine on a disposable database with no managed tables."""
    url = migration_postgres_url
    engine = create_engine(url)
    _drop(engine)
    try:
        yield url, engine
    finally:
        _drop(engine)
        engine.dispose()


@pytest.fixture
def isolated_pg_schema(clean_pg):
    """Provide a unique schema and prove cleanup preserves an unrelated one."""
    _, engine = clean_pg
    schema = f"mod_test_{uuid4().hex}"
    sentinel_schema = f"mod_sentinel_{uuid4().hex}"
    with engine.begin() as conn:
        conn.execute(CreateSchema(sentinel_schema))
    try:
        yield schema
    finally:
        try:
            with engine.begin() as conn:
                conn.execute(DropSchema(schema, if_exists=True, cascade=True))
            assert sentinel_schema in inspect(engine).get_schema_names()
        finally:
            with engine.begin() as conn:
                conn.execute(DropSchema(sentinel_schema, if_exists=True, cascade=True))


def test_alembic_upgrade_head_on_real_postgres(clean_pg) -> None:
    """The real migration applies cleanly to Postgres and creates both tables
    plus the partial pending-rows index."""
    url, engine = clean_pg
    with engine.connect() as conn:
        database_name = conn.execute(text("SELECT current_database()")).scalar_one()
    assert database_name.startswith("modupy_migration_")

    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert _TABLES[0] in tables
    assert _TABLES[1] in tables
    indexes = {ix["name"] for ix in insp.get_indexes("event_publications")}
    assert "idx_pending" in indexes
    # The sweep indexes exist only in the migrations — the ORM metadata cannot
    # express the partial expression index, so the column-metadata drift guards
    # below are blind to them dropping out of the chain.
    assert "ix_event_publications_claim_order" in indexes
    # The MySQL-only sibling must not leak onto Postgres.
    assert "ix_event_publications_pending_scan" not in indexes
    archive_indexes = {ix["name"] for ix in insp.get_indexes("event_publications_archive")}
    assert "ix_event_publications_archive_completed_at" in archive_indexes


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
    """The full chain creates queue and retained-replay tables plus indexes."""
    url, engine = clean_pg
    command.upgrade(_cfg(url), "head")

    insp = inspect(engine)
    tables = set(insp.get_table_names())
    assert "broker_subscription" in tables
    assert "broker_message" in tables
    assert "broker_retained_message" in tables
    assert "broker_retained_delivery" in tables
    indexes = {ix["name"] for ix in insp.get_indexes("broker_message")}
    assert "ix_broker_message_claim" in indexes
    assert "ix_broker_message_prune" in indexes
    assert "ix_broker_message_target" in indexes
    retained_indexes = {ix["name"] for ix in insp.get_indexes("broker_retained_message")}
    assert "ix_broker_retained_message_expiry" in retained_indexes
    assert "ix_broker_retained_message_target_expiry" in retained_indexes


def test_broker_migration_column_metadata_matches_schema_on_real_postgres(clean_pg) -> None:
    """Broker-side twin of the outbox drift guard: the migrations render
    the same Postgres schema (type/nullable/default) as db_broker.broker_schema()
    — inspector to inspector through pg_catalog, no hand-rolled normalization."""
    from modulith.adapters.db_broker import broker_schema

    url, engine = clean_pg
    tables = [
        "broker_subscription",
        "broker_message",
        "broker_retained_message",
        "broker_retained_delivery",
    ]

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
    assert "broker_retained_message" not in tables
    assert "broker_retained_delivery" not in tables


def test_downgrade_0003_preserves_long_last_error(clean_pg) -> None:
    """Rolling 0003 back must not narrow ``last_error`` and abort.

    ``builtin/outbox._record_failure`` stores up to 500 characters there. While
    0003's downgrade mirrored its upgrade with an ``ALTER ... TYPE VARCHAR(255)``,
    any deployment that had recorded a longer failure could not roll back at
    all: Postgres raises StringDataRightTruncation and the whole downgrade
    transaction aborts. The conversion is now deliberately one-way, so the
    column keeps its Text width and the stored value survives intact.
    """
    url, engine = clean_pg
    cfg = _cfg(url)
    command.upgrade(cfg, "0003_outbox_claim_leases")

    long_error = "x" * 500
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO event_publications "
                "(id, event_type, payload, listener, published_at, attempt_count, last_error) "
                "VALUES (CAST(:id AS uuid), :event_type, :payload, :listener, "
                "CAST(:published_at AS timestamptz), 1, :last_error)"
            ),
            {
                "id": str(uuid4()),
                "event_type": "orders.OrderPlaced",
                "payload": b"{}",
                "listener": "inventory.on_order_placed",
                "published_at": datetime.now(UTC).isoformat(),
                "last_error": long_error,
            },
        )

    command.downgrade(cfg, "0002_broker_message")

    columns = {c["name"] for c in inspect(engine).get_columns("event_publications")}
    assert "claim_owner" not in columns  # the revision's own columns did come off
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT last_error FROM event_publications")).scalar_one()
    assert stored == long_error


def test_alembic_upgrade_head_with_schema_env_var_scopes_all_tables(
    clean_pg, isolated_pg_schema, monkeypatch
) -> None:
    """MODULITH_DB_SCHEMA routes both the outbox/broker tables and
    ``modulith_alembic_version`` into a dedicated schema, stays idempotent across a
    second upgrade, and downgrade base leaves that schema empty."""
    url, engine = clean_pg
    schema = isolated_pg_schema
    monkeypatch.setenv("MODULITH_DB_SCHEMA", schema)
    cfg = _cfg(url)
    expected_tables = {*_TABLES, *_BROKER_TABLES, "modulith_alembic_version"}

    command.upgrade(cfg, "head")

    scoped_tables = set(inspect(engine).get_table_names(schema=schema))
    assert scoped_tables == expected_tables
    public_tables = set(inspect(engine).get_table_names(schema="public"))
    assert public_tables.isdisjoint(expected_tables)

    command.upgrade(cfg, "head")  # idempotent re-run, no already-exists error
    assert set(inspect(engine).get_table_names(schema=schema)) == scoped_tables

    command.downgrade(cfg, "base")
    # downgrade base clears the version row but — same as the unscoped
    # Postgres/SQLite downgrade tests above — never drops the version table
    # itself; only the migrated tables are gone.
    assert set(inspect(engine).get_table_names(schema=schema)) <= {"modulith_alembic_version"}


def test_upgrade_moves_a_legacy_revision_inside_the_named_schema(
    clean_pg, isolated_pg_schema, monkeypatch
) -> None:
    """An install that tracked modulith in ``<schema>.alembic_version`` has
    its revision moved into ``<schema>.modulith_alembic_version`` in the same
    transaction as the upgrade, without re-running any migration."""
    url, engine = clean_pg
    schema = isolated_pg_schema
    monkeypatch.setenv("MODULITH_DB_SCHEMA", schema)
    cfg = _cfg(url)
    command.upgrade(cfg, "head")
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE "{schema}".modulith_alembic_version'))
        conn.execute(
            text(
                f'CREATE TABLE "{schema}".alembic_version (version_num VARCHAR(32) NOT NULL, '
                "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"
            )
        )
        conn.execute(
            text(f"INSERT INTO \"{schema}\".alembic_version VALUES ('0009_outbox_ts_microseconds')")
        )

    command.upgrade(cfg, "head")

    tables = set(inspect(engine).get_table_names(schema=schema))
    assert "alembic_version" not in tables
    with engine.connect() as conn:
        assert conn.execute(
            text(f'SELECT version_num FROM "{schema}".modulith_alembic_version')
        ).scalar_one() == ("0009_outbox_ts_microseconds")
    assert "modulith_alembic_version" not in inspect(engine).get_table_names(schema="public")


def test_enabling_named_schema_refuses_to_abandon_public_migration_history(
    clean_pg, isolated_pg_schema, monkeypatch
) -> None:
    url, engine = clean_pg
    cfg = _cfg(url)
    command.upgrade(cfg, "head")
    monkeypatch.setenv("MODULITH_DB_SCHEMA", isolated_pg_schema)

    with pytest.raises(SystemExit, match="public"):
        command.upgrade(cfg, "head")

    public_tables = set(inspect(engine).get_table_names(schema="public"))
    assert {*_TABLES, *_BROKER_TABLES, "modulith_alembic_version"} <= public_tables
    assert isolated_pg_schema not in inspect(engine).get_schema_names()


def test_named_schema_guard_ignores_unrelated_public_alembic_history(
    clean_pg, isolated_pg_schema, monkeypatch
) -> None:
    url, engine = clean_pg
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)"))
        conn.execute(text("INSERT INTO alembic_version VALUES ('unrelated_revision')"))
    monkeypatch.setenv("MODULITH_DB_SCHEMA", isolated_pg_schema)

    command.upgrade(_cfg(url), "head")

    with engine.connect() as conn:
        assert conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "unrelated_revision"
        )
    assert {*_TABLES, *_BROKER_TABLES, "modulith_alembic_version"} == set(
        inspect(engine).get_table_names(schema=isolated_pg_schema)
    )


def test_named_schema_guard_rejects_modulith_public_alembic_history(
    clean_pg, isolated_pg_schema, monkeypatch
) -> None:
    """A packaged revision identifies otherwise table-only history as managed."""
    url, engine = clean_pg
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)"))
        conn.execute(text("INSERT INTO alembic_version VALUES ('0001_initial')"))
    monkeypatch.setenv("MODULITH_DB_SCHEMA", isolated_pg_schema)

    with pytest.raises(SystemExit, match="alembic_version"):
        command.upgrade(_cfg(url), "head")

    assert isolated_pg_schema not in inspect(engine).get_schema_names()


def test_named_schema_guard_rejects_public_tables_with_stale_target_history(
    clean_pg, isolated_pg_schema, monkeypatch
) -> None:
    from modulith.adapters.db_broker import broker_schema
    from modulith.adapters.postgres_outbox import Base

    url, engine = clean_pg
    cfg = _cfg(url)
    monkeypatch.setenv("MODULITH_DB_SCHEMA", isolated_pg_schema)
    command.stamp(cfg, "head")

    Base.metadata.create_all(engine)
    broker_schema()[0].create_all(engine)

    with pytest.raises(SystemExit, match="public"):
        command.upgrade(cfg, "head")

    target_tables = set(inspect(engine).get_table_names(schema=isolated_pg_schema))
    assert target_tables == {"modulith_alembic_version"}
    public_tables = set(inspect(engine).get_table_names(schema="public"))
    assert {*_TABLES, *_BROKER_TABLES} <= public_tables


def test_migration_column_metadata_matches_orm_on_real_postgres(clean_pg) -> None:
    """The name-set comparison above is blind to type/nullable/
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
