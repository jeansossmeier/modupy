"""Verify the alembic migration builds the outbox schema (against SQLite).

The migration is dialect-portable, so we run it on SQLite — the same
``upgrade head`` runs against Postgres in production.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config

import modulith.adapters as adapters_pkg
from modulith import ConfigurationError

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
    # 0005's archive-purge index. Its siblings ``ix_event_publications_claim_order``
    # and ``ix_event_publications_pending_scan`` are deliberately absent here:
    # the first is a functional + partial index 0005 creates only on Postgres,
    # the second a plain composite index only on MySQL/MariaDB — SQLite never
    # gets either. tests/test_migration_postgres.py asserts the Postgres half;
    # tests/test_migration_mysql.py asserts the MySQL half.
    assert "ix_event_publications_archive_completed_at" in _objects(db, "index")
    assert "ix_event_publications_claim_order" not in _objects(db, "index")
    assert "ix_event_publications_pending_scan" not in _objects(db, "index")


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


async def test_alembic_upgrade_head_after_broker_self_bootstrap(tmp_path: Path) -> None:
    """``DatabaseBroker`` self-bootstraps its tables with ``metadata.create_all``
    on a database no migration has ever touched (the documented zero-alembic
    broker path). Adopting the outbox afterwards runs ``upgrade head`` against
    that same database, so the broker revisions must stamp through objects that
    are already there instead of dying on 'table already exists' and pinning
    ``modulith_alembic_version`` at 0001 forever."""
    from modulith.adapters.db_broker import DatabaseBroker

    db = tmp_path / "race.db"
    broker = DatabaseBroker(url=f"sqlite+aiosqlite:///{db}")
    try:
        await broker.subscribe(["fakeapp.orders.WidgetCreated"], "modulith-inventory")
    finally:
        await broker.engine.dispose()

    assert "broker_subscription" in _objects(db, "table")

    command.upgrade(_cfg(db), "head")

    conn = sqlite3.connect(db)
    try:
        versions = {r[0] for r in conn.execute("SELECT version_num FROM modulith_alembic_version")}
    finally:
        conn.close()
    assert versions == {"0009_outbox_ts_microseconds"}

    tables = _objects(db, "table")
    assert "event_publications" in tables
    assert "broker_retained_delivery" in tables
    indexes = _objects(db, "index")
    assert "ix_broker_message_claim" in indexes
    assert "ix_broker_retained_message_target_expiry" in indexes


def test_outbox_claim_lease_columns_exist_in_migration_and_model(tmp_path: Path) -> None:
    from modulith.adapters.postgres_outbox import EventPublicationRow

    db = tmp_path / "outbox-lease.db"
    command.upgrade(_cfg(db), "head")

    expected = {"claim_owner", "claim_token", "claim_until"}
    assert expected <= _columns(db, "event_publications")
    assert expected <= {column.name for column in EventPublicationRow.__table__.columns}


def test_trace_context_columns_are_added_and_dropped_by_their_migration(tmp_path: Path) -> None:
    db = tmp_path / "trace-context.db"
    cfg = _cfg(db)
    command.upgrade(cfg, "head")
    assert "trace_context" in _columns(db, "event_publications")
    assert "trace_context" in _columns(db, "event_publications_archive")

    command.downgrade(cfg, "0007_outbox_dispatch_started")
    assert "trace_context" not in _columns(db, "event_publications")
    assert "trace_context" not in _columns(db, "event_publications_archive")

    command.upgrade(cfg, "head")
    assert "trace_context" in _columns(db, "event_publications")


def test_trace_context_migration_keeps_rows_written_before_it(tmp_path: Path) -> None:
    db = tmp_path / "trace-context-old-rows.db"
    cfg = _cfg(db)
    command.upgrade(cfg, "0007_outbox_dispatch_started")
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "INSERT INTO event_publications (id, event_type, payload, listener, published_at) "
            "VALUES ('00000000000000000000000000000001', 'e', x'01', 'l', '2026-01-01 00:00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    command.upgrade(cfg, "head")

    conn = sqlite3.connect(db)
    try:
        rows = conn.execute("SELECT event_type, trace_context FROM event_publications").fetchall()
    finally:
        conn.close()
    assert rows == [("e", None)]


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
    """The name-set comparison above is blind to type/nullable/server_default
    drift — the bug class that once shipped a boolean server_default rendered
    as the integer 0. Compare the full
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


def test_outbox_payload_columns_compile_to_longblob_for_mysql_and_mariadb() -> None:
    """The outbox payload column must hold a real event on every dialect.

    ``LargeBinary`` compiles to MySQL/MariaDB ``BLOB``, which caps at 65,535
    bytes. The outbox imposes no size limit of its own, so an event over that
    is accepted by ``save`` and dies at flush with error 1406 — inside the
    caller's *business* transaction, because the publication row is enlisted
    there. The ORM schema and the migrations that create the column must render
    the same type or the two drift apart on MySQL only.
    """
    import importlib

    from sqlalchemy.dialects.mysql import dialect as mysql_dialect
    from sqlalchemy.dialects.mysql import mariadb
    from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
    from sqlalchemy.schema import CreateTable

    from modulith.adapters.postgres_outbox import Base

    revision_0001 = importlib.import_module("modulith.adapters.migrations.versions.0001_initial")
    revision_0003 = importlib.import_module(
        "modulith.adapters.migrations.versions.0003_outbox_claim_leases"
    )
    payload_tables = ("event_publications", "event_publications_archive")

    for dialect in (mysql_dialect(), mariadb.MariaDBDialect()):
        for table_name in payload_tables:
            ddl = str(CreateTable(Base.metadata.tables[table_name]).compile(dialect=dialect))
            assert "payload LONGBLOB NOT NULL" in ddl, ddl
        assert revision_0001._PAYLOAD.compile(dialect=dialect) == "LONGBLOB"
        assert revision_0003._PAYLOAD.compile(dialect=dialect) == "LONGBLOB"

    # The variant is inert everywhere else — SQLite BLOB is already unbounded.
    for table_name in payload_tables:
        ddl = str(CreateTable(Base.metadata.tables[table_name]).compile(dialect=sqlite_dialect()))
        assert "payload BLOB NOT NULL" in ddl, ddl


def test_alembic_offline_mode_emits_full_ddl(tmp_path: Path, capsys) -> None:
    """Offline/--sql mode (env.py's run_migrations_offline) must render the
    complete DDL — both tables and the pending partial index — without ever
    touching a database."""
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
    assert "CREATE TABLE modulith_alembic_version (" in ddl
    assert "CREATE TABLE alembic_version (" not in ddl
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
    # A plain LargeBinary renders as BLOB here, capped at 65,535 bytes, so a
    # moderately large event fails at INSERT with error 1406 and takes the
    # business transaction with it. Both outbox tables must render LONGBLOB
    # when created, and 0003 must widen an install created before that.
    assert "payload BLOB NOT NULL" not in ddl
    assert ddl.count("MODIFY payload LONGBLOB NOT NULL") == 2
    assert "idx_pending" in ddl
    # MySQL cannot express 0005's Postgres functional+partial claim index, so
    # it gets a plain composite index on the pending predicate instead — both
    # sweep queries filter on these columns, and the leading ``completed_at``
    # narrows the scan to pending rows.
    assert "ix_event_publications_pending_scan" in ddl
    # The Postgres half must NOT leak onto MySQL.
    assert "ix_event_publications_claim_order" not in ddl
    assert "CREATE TABLE broker_subscription (" in ddl


def test_alembic_upgrade_with_schema_env_var_warns_and_ignores_on_sqlite(
    tmp_path: Path, monkeypatch: Any, caplog: Any
) -> None:
    """schema is a PostgreSQL-only knob; on SQLite the migration must still
    succeed against the default (unqualified) tables, with a warning that the
    option was ignored rather than silently accepted."""
    db = tmp_path / "outbox.db"
    monkeypatch.setenv("MODULITH_DB_SCHEMA", "mod_test")
    with caplog.at_level(logging.WARNING, logger="modulith.adapters.migrations.env"):
        command.upgrade(_cfg(db), "head")

    assert any("schema" in record.getMessage() for record in caplog.records)
    tables = _objects(db, "table")
    assert "event_publications" in tables
    assert "broker_subscription" in tables


def test_alembic_offline_mode_with_schema_env_var_exits(tmp_path: Path, monkeypatch: Any) -> None:
    """Offline (``--sql``) mode never opens a live connection, so it cannot
    apply a schema translation — a schema request there must fail loudly
    rather than silently emit unqualified DDL."""
    db = tmp_path / "offline.db"
    monkeypatch.setenv("MODULITH_DB_SCHEMA", "mod_test")
    with pytest.raises(SystemExit):
        command.upgrade(_cfg(db), "head", sql=True)
    assert not db.exists()


def test_alembic_rejects_invalid_schema_before_opening_a_connection(
    tmp_path: Path, monkeypatch: Any
) -> None:
    db = tmp_path / "invalid-schema.db"
    monkeypatch.setenv("MODULITH_DB_SCHEMA", "valid_name\n")

    with pytest.raises(ConfigurationError, match="schema"):
        command.upgrade(_cfg(db), "head", sql=True)

    assert not db.exists()


HEAD = "0009_outbox_ts_microseconds"
BUSINESS_REVISION = "business_rev_1"


def _versions(db_path: Path, table: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {r[0] for r in conn.execute(f"SELECT version_num FROM {table}")}
    finally:
        conn.close()


def _legacy_install(db_path: Path, migrated_to: str | None, *version_rows: str) -> None:
    """Build the pre-``modulith_alembic_version`` layout: the schema migrated to
    ``migrated_to`` with every tracked revision in Alembic's default table."""
    if migrated_to is not None:
        command.upgrade(_cfg(db_path), migrated_to)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DROP TABLE IF EXISTS modulith_alembic_version")
        conn.execute("DROP TABLE IF EXISTS alembic_version")
        conn.execute(
            "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL, "
            "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"
        )
        conn.executemany("INSERT INTO alembic_version VALUES (?)", [(r,) for r in version_rows])
        conn.commit()
    finally:
        conn.close()


def _upgrade_steps(caplog: Any) -> list[str]:
    """``"<from> -> <to>"`` for every revision Alembic ran."""
    prefix = "Running upgrade "
    return [
        record.getMessage().removeprefix(prefix).split(",")[0]
        for record in caplog.records
        if record.getMessage().startswith(prefix)
    ]


def test_fresh_upgrade_tracks_the_revision_in_modulith_alembic_version(tmp_path: Path) -> None:
    db = tmp_path / "fresh.db"
    command.upgrade(_cfg(db), "head")

    assert "alembic_version" not in _objects(db, "table")
    assert _versions(db, "modulith_alembic_version") == {HEAD}


def test_upgrade_moves_a_legacy_head_revision_without_rerunning_migrations(
    tmp_path: Path, caplog: Any
) -> None:
    db = tmp_path / "legacy-head.db"
    _legacy_install(db, HEAD, HEAD)

    with caplog.at_level(logging.INFO, logger="alembic.runtime.migration"):
        command.upgrade(_cfg(db), "head")

    assert _upgrade_steps(caplog) == []
    assert "alembic_version" not in _objects(db, "table")
    assert _versions(db, "modulith_alembic_version") == {HEAD}


def _add_empty_modulith_version_table(db_path: Path) -> None:
    """What an interrupted move leaves behind: the new table created, its row never written."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE modulith_alembic_version (version_num VARCHAR(32) NOT NULL, "
            "CONSTRAINT modulith_alembic_version_pkc PRIMARY KEY (version_num))"
        )
        conn.commit()
    finally:
        conn.close()


def test_upgrade_moves_a_legacy_revision_past_an_empty_modulith_alembic_version(
    tmp_path: Path, caplog: Any
) -> None:
    db = tmp_path / "interrupted.db"
    _legacy_install(db, HEAD, HEAD)
    _add_empty_modulith_version_table(db)

    with caplog.at_level(logging.INFO, logger="alembic.runtime.migration"):
        command.upgrade(_cfg(db), "head")

    assert _upgrade_steps(caplog) == []
    assert "alembic_version" not in _objects(db, "table")
    assert _versions(db, "modulith_alembic_version") == {HEAD}


def test_upgrade_moves_a_legacy_mid_chain_revision_then_continues_from_it(
    tmp_path: Path, caplog: Any
) -> None:
    db = tmp_path / "legacy-mid.db"
    _legacy_install(db, "0005_outbox_scan_indexes", "0005_outbox_scan_indexes")

    with caplog.at_level(logging.INFO, logger="alembic.runtime.migration"):
        command.upgrade(_cfg(db), "head")

    assert _upgrade_steps(caplog) == [
        "0005_outbox_scan_indexes -> 0006_broker_dispatch_started",
        "0006_broker_dispatch_started -> 0007_outbox_dispatch_started",
        "0007_outbox_dispatch_started -> 0008_outbox_trace_context",
        "0008_outbox_trace_context -> 0009_outbox_ts_microseconds",
    ]
    assert "alembic_version" not in _objects(db, "table")
    assert _versions(db, "modulith_alembic_version") == {HEAD}


def test_upgrade_leaves_a_business_alembic_revision_untouched(tmp_path: Path) -> None:
    db = tmp_path / "business.db"
    _legacy_install(db, None, BUSINESS_REVISION)

    command.upgrade(_cfg(db), "head")

    assert _versions(db, "alembic_version") == {BUSINESS_REVISION}
    assert _versions(db, "modulith_alembic_version") == {HEAD}
    assert "event_publications" in _objects(db, "table")


def test_upgrade_moves_only_the_modulith_row_out_of_a_shared_alembic_version(
    tmp_path: Path,
) -> None:
    db = tmp_path / "shared.db"
    _legacy_install(db, HEAD, HEAD, BUSINESS_REVISION)

    command.upgrade(_cfg(db), "head")

    assert _versions(db, "alembic_version") == {BUSINESS_REVISION}
    assert _versions(db, "modulith_alembic_version") == {HEAD}


def test_read_revision_table_finds_the_revision_in_either_layout(tmp_path: Path) -> None:
    from sqlalchemy import create_engine

    from modulith.adapters.migrations.version_table import read_revision_table

    def read(db_path: Path) -> tuple[str, frozenset[str]] | None:
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as connection:
                return read_revision_table(connection, None)
        finally:
            engine.dispose()

    current = tmp_path / "current.db"
    command.upgrade(_cfg(current), "head")
    legacy = tmp_path / "legacy.db"
    _legacy_install(legacy, "0003_outbox_claim_leases", "0003_outbox_claim_leases", "x")
    business = tmp_path / "business.db"
    _legacy_install(business, None, BUSINESS_REVISION)
    interrupted = tmp_path / "interrupted.db"
    _legacy_install(interrupted, "0003_outbox_claim_leases", "0003_outbox_claim_leases")
    _add_empty_modulith_version_table(interrupted)

    assert read(current) == ("modulith_alembic_version", frozenset({HEAD}))
    assert read(legacy) == ("alembic_version", frozenset({"0003_outbox_claim_leases"}))
    assert read(interrupted) == ("alembic_version", frozenset({"0003_outbox_claim_leases"}))
    assert read(business) == ("alembic_version", frozenset())
    assert read(tmp_path / "empty.db") is None
