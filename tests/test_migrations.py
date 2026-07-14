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


def test_alembic_upgrade_creates_schema(tmp_path: Path) -> None:
    db = tmp_path / "outbox.db"
    command.upgrade(_cfg(db), "head")

    tables = _objects(db, "table")
    assert "event_publications" in tables
    assert "event_publications_archive" in tables
    assert "idx_pending" in _objects(db, "index")


def test_alembic_upgrade_creates_broker_schema(tmp_path: Path) -> None:
    # 0002: the database-broker tables + their indexes. `upgrade head` runs the
    # full 0001 -> 0002 chain (the first migration to exercise more than one
    # revision — the chain the audit flagged as previously unverifiable).
    db = tmp_path / "broker.db"
    command.upgrade(_cfg(db), "head")

    tables = _objects(db, "table")
    assert "broker_subscription" in tables
    assert "broker_message" in tables
    indexes = _objects(db, "index")
    assert "ix_broker_message_claim" in indexes
    assert "ix_broker_message_prune" in indexes
    assert "ix_broker_message_target" in indexes


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
    # The full chain unwinds: 0002's broker tables go too (downgrade to base
    # runs 0002.downgrade then 0001.downgrade).
    assert "broker_subscription" not in tables
    assert "broker_message" not in tables


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
    the 0002 migration must render byte-for-byte the same schema as
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

    for table in ("broker_subscription", "broker_message"):
        migrated = snapshot(migrated_db, table)
        from_schema = snapshot(schema_db, table)
        assert migrated == from_schema, f"{table}: migration {migrated} != schema {from_schema}"


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
    # 0002 also renders offline (full chain), tables + indexes.
    assert "CREATE TABLE broker_subscription (" in ddl
    assert "CREATE TABLE broker_message (" in ddl
    assert "ix_broker_message_claim" in ddl
    # Offline mode renders SQL only — the database file is never created.
    assert not db.exists()
