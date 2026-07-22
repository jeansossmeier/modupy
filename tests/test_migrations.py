"""Verify the alembic migration builds the outbox schema (against SQLite).

The migration is dialect-portable, so we run it on SQLite — the same
``upgrade head`` runs against Postgres in production.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config

import modulith.adapters as adapters_pkg

MIGRATIONS = Path(adapters_pkg.__file__).parent / "migrations"


def _cfg(db_path: Path) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    return cfg


def _objects(db_path: Path, kind: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = ?", (kind,))
        return {r[0] for r in rows}
    finally:
        conn.close()


def _columns(db_path: Path, table: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    finally:
        conn.close()


def test_alembic_upgrade_creates_schema(tmp_path: Path) -> None:
    db = tmp_path / "outbox.db"
    command.upgrade(_cfg(db), "head")

    tables = _objects(db, "table")
    assert "event_publications" in tables
    assert "event_publications_archive" in tables
    assert "idx_pending" in _objects(db, "index")


def test_alembic_upgrade_creates_broker_schema(tmp_path: Path) -> None:
    # 0002 creates the queue tables and 0004 adds retained replay state.
    # ``upgrade head`` exercises the full linear migration chain.
    db = tmp_path / "broker.db"
    command.upgrade(_cfg(db), "head")

    tables = _objects(db, "table")
    assert "broker_subscription" in tables
    assert "broker_message" in tables
    assert "broker_retained_message" in tables
    assert "broker_retained_delivery" in tables
    indexes = _objects(db, "index")
    assert "ix_broker_message_claim" in indexes
    assert "ix_broker_message_prune" in indexes
    assert "ix_broker_message_target" in indexes
    assert "ix_broker_retained_message_expiry" in indexes
    assert "ix_broker_retained_message_target_expiry" in indexes


def test_outbox_claim_lease_columns_exist_in_migration_and_model(tmp_path: Path) -> None:
    from modulith.adapters.postgres_outbox import EventPublicationRow

    db = tmp_path / "outbox-lease.db"
    command.upgrade(_cfg(db), "head")

    expected = {"claim_owner", "claim_token", "claim_until"}
    assert expected <= _columns(db, "event_publications")
    assert expected <= {column.name for column in EventPublicationRow.__table__.columns}


def test_migration_columns_match_orm(tmp_path: Path) -> None:
    # Guards against ORM/migration drift: a column added to the model but not
    # the migration (or vice versa) would silently diverge the production
    # Postgres schema from what the adapter INSERTs, yet stay green here.
    from modulith.adapters.postgres_outbox import (
        EventPublicationArchiveRow,
        EventPublicationRow,
    )

    db = tmp_path / "outbox.db"
    command.upgrade(_cfg(db), "head")

    conn = sqlite3.connect(db)
    try:
        for model in (EventPublicationRow, EventPublicationArchiveRow):
            migrated = {r[1] for r in conn.execute(f"PRAGMA table_info({model.__tablename__})")}
            orm = {c.name for c in model.__table__.columns}
            assert migrated == orm, f"{model.__tablename__}: migration {migrated} != ORM {orm}"
    finally:
        conn.close()


def test_alembic_downgrade_removes_schema(tmp_path: Path) -> None:
    db = tmp_path / "outbox.db"
    cfg = _cfg(db)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")

    tables = _objects(db, "table")
    assert "event_publications" not in tables
    assert "event_publications_archive" not in tables
    # The full chain unwinds every broker revision before removing the outbox.
    assert "broker_subscription" not in tables
    assert "broker_message" not in tables
    assert "broker_retained_message" not in tables
    assert "broker_retained_delivery" not in tables


def test_migration_column_metadata_matches_orm(tmp_path: Path) -> None:
    """A6-r4-178: the name-set comparison above is blind to type/nullable/
    server_default drift — the exact bug class that already shipped one
    CRITICAL (boolean server_default rendered as integer 0). Compare the full
    PRAGMA table_info metadata of the migrated schema against a schema created
    straight from the ORM metadata: inspector-to-inspector, so both sides
    render through the same dialect and equivalent definitions compare equal."""
    from sqlalchemy import create_engine

    from modulith.adapters.postgres_outbox import (
        Base,
        EventPublicationArchiveRow,
        EventPublicationRow,
    )

    migrated_db = tmp_path / "migrated.db"
    command.upgrade(_cfg(migrated_db), "head")

    orm_db = tmp_path / "orm.db"
    engine = create_engine(f"sqlite:///{orm_db}")
    try:
        Base.metadata.create_all(engine)
    finally:
        engine.dispose()

    def snapshot(db_path: Path, table: str) -> dict[str, tuple[str, int, object, int]]:
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        finally:
            conn.close()
        # Keyed by name (declaration order may legitimately differ):
        # name -> (type, notnull, dflt_value, pk)
        return {r[1]: (r[2], r[3], r[4], r[5]) for r in rows}

    for model in (EventPublicationRow, EventPublicationArchiveRow):
        table = model.__tablename__
        migrated = snapshot(migrated_db, table)
        orm = snapshot(orm_db, table)
        assert migrated == orm, f"{table}: migration {migrated} != ORM {orm}"


def test_broker_migration_column_metadata_matches_schema(tmp_path: Path) -> None:
    """Same drift guard as the outbox test, for the database-broker tables:
    the broker migrations must render byte-for-byte the same schema as
    ``db_broker.broker_schema()`` (type/nullable/default/pk), inspector to
    inspector so both sides go through the SQLite dialect identically."""
    from sqlalchemy import create_engine

    from modulith.adapters.db_broker import broker_schema

    migrated_db = tmp_path / "migrated.db"
    command.upgrade(_cfg(migrated_db), "head")

    metadata, _, _ = broker_schema()
    schema_db = tmp_path / "schema.db"
    engine = create_engine(f"sqlite:///{schema_db}")
    try:
        metadata.create_all(engine)
    finally:
        engine.dispose()

    def snapshot(db_path: Path, table: str) -> dict[str, tuple[str, int, object, int]]:
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        finally:
            conn.close()
        # name -> (type, notnull, dflt_value, pk); declaration order may differ.
        return {r[1]: (r[2], r[3], r[4], r[5]) for r in rows}

    for table in (
        "broker_subscription",
        "broker_message",
        "broker_retained_message",
        "broker_retained_delivery",
    ):
        migrated = snapshot(migrated_db, table)
        from_schema = snapshot(schema_db, table)
        assert migrated == from_schema, f"{table}: migration {migrated} != schema {from_schema}"


def test_broker_timestamp_types_compile_with_microseconds_for_mysql_and_mariadb() -> None:
    """Runtime and migration timestamps must retain sub-second precision."""
    import importlib

    from sqlalchemy import DateTime
    from sqlalchemy.dialects.mysql import dialect as mysql_dialect
    from sqlalchemy.dialects.mysql import mariadb

    from modulith.adapters.db_broker import broker_schema

    metadata, _, _ = broker_schema()
    timestamp_types = {
        f"{table.name}.{column.name}": column.type
        for table in metadata.tables.values()
        for column in table.columns
        if isinstance(column.type, DateTime)
    }
    assert set(timestamp_types) == {
        "broker_subscription.updated_at",
        "broker_message.available_at",
        "broker_message.claimed_at",
        "broker_message.created_at",
        "broker_retained_message.created_at",
        "broker_retained_message.expires_at",
        "broker_retained_delivery.delivered_at",
    }

    revision_0002 = importlib.import_module(
        "modulith.adapters.migrations.versions.0002_broker_message"
    )
    revision_0004 = importlib.import_module(
        "modulith.adapters.migrations.versions.0004_broker_retained_messages"
    )
    for dialect in (mysql_dialect(), mariadb.MariaDBDialect()):
        assert {
            column_type.compile(dialect=dialect) for column_type in timestamp_types.values()
        } == {"DATETIME(6)"}
        assert revision_0002._TS.compile(dialect=dialect) == "DATETIME(6)"
        assert revision_0004._TS.compile(dialect=dialect) == "DATETIME(6)"


def test_alembic_offline_mode_emits_full_ddl(tmp_path: Path, capsys) -> None:
    """A6-r2-89: offline/--sql mode (env.py's run_migrations_offline) must
    render the complete DDL — both tables and the pending partial index —
    without ever touching a database."""
    db = tmp_path / "offline.db"
    command.upgrade(_cfg(db), "head", sql=True)

    ddl = capsys.readouterr().out
    assert "CREATE TABLE event_publications (" in ddl
    assert "CREATE TABLE event_publications_archive (" in ddl
    assert "idx_pending" in ddl
    # The broker revisions also render offline, including replay tables.
    assert "CREATE TABLE broker_subscription (" in ddl
    assert "CREATE TABLE broker_message (" in ddl
    assert "CREATE TABLE broker_retained_message (" in ddl
    assert "CREATE TABLE broker_retained_delivery (" in ddl
    assert "ix_broker_message_claim" in ddl
    # Offline mode renders SQL only — the database file is never created.
    assert not db.exists()


def test_alembic_offline_mode_emits_mysql_ddl_without_a_connection(capsys) -> None:
    """Offline mode must render valid MySQL DDL too, without ever connecting —
    a bogus, unroutable host in the URL proves it (a real connection attempt
    would hang or raise; ``sql=True`` never opens a socket at all). This is
    the dialect where 0001/0003's Text-not-String fix actually matters: an
    unbounded ``VARCHAR`` fails to even render DDL on MySQL, so this guards
    the exact regression class the Docker-backed MySQL tests would otherwise
    be the only thing catching."""
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.set_main_option(
        "sqlalchemy.url", "mysql+pymysql://user:pass@offline-host-never-contacted/outbox"
    )

    command.upgrade(cfg, "head", sql=True)

    ddl = capsys.readouterr().out
    assert "CREATE TABLE event_publications (" in ddl
    assert "CREATE TABLE event_publications_archive (" in ddl
    # MySQL's VARCHAR requires an explicit length; the fixed columns render as
    # TEXT instead — this is the exact bug class this file's docstring cites.
    assert "event_type TEXT NOT NULL" in ddl
    assert "listener TEXT NOT NULL" in ddl
    assert "idx_pending" in ddl
    assert "CREATE TABLE broker_subscription (" in ddl
