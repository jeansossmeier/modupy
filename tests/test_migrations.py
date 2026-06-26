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
