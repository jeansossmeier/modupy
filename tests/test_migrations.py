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
    # Offline mode renders SQL only — the database file is never created.
    assert not db.exists()
