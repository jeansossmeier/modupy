"""SQLite schema and transactional migrations for the durable SHM queue."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager

from ..config import ConfigurationError

SCHEMA_VERSION = 3

_COMPLETION_TOMBSTONE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS shm_completion_tombstone (
        publication_id TEXT NOT NULL
            REFERENCES shm_publication(id) ON DELETE CASCADE,
        consumer_group TEXT NOT NULL,
        completed_at REAL NOT NULL,
        PRIMARY KEY (publication_id, consumer_group)
    )
    """

_PUBLICATION_EXPIRY_INDEX_SCHEMA = """
    CREATE INDEX IF NOT EXISTS idx_shm_publication_expiry
    ON shm_publication (retained_until, sequence)
    """

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS shm_publication (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        target TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payload BLOB NOT NULL,
        headers TEXT,
        created_at REAL NOT NULL,
        retained_until REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shm_subscription (
        target TEXT NOT NULL,
        consumer_group TEXT NOT NULL,
        updated_at REAL,
        PRIMARY KEY (target, consumer_group)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shm_delivery (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        publication_id TEXT NOT NULL
            REFERENCES shm_publication(id) ON DELETE CASCADE,
        consumer_group TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending', 'claimed', 'done', 'dead')),
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        claimed_at REAL,
        claimed_by TEXT,
        claim_generation INTEGER NOT NULL DEFAULT 0,
        dispatch_started INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        completed_at REAL,
        created_at REAL NOT NULL,
        UNIQUE (publication_id, consumer_group)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_shm_delivery_claim
    ON shm_delivery (
        consumer_group, status, available_at, claimed_at, id
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_shm_publication_retained
    ON shm_publication (target, retained_until)
    """,
    _PUBLICATION_EXPIRY_INDEX_SCHEMA,
    _COMPLETION_TOMBSTONE_SCHEMA,
)


@contextmanager
def immediate_transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """Run one write under an explicit SQLite reserved-lock transaction."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    else:
        try:
            conn.commit()
        except BaseException as commit_error:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                commit_error.add_note(f"rollback also failed: {rollback_error}")
            raise


def open_database(
    path: str,
    synchronous: str,
    max_store_bytes: int,
) -> sqlite3.Connection:
    """Open, tune, and migrate one connection on its owning worker thread."""
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        _migrate(conn)
        _enable_wal(conn)
        conn.execute(f"PRAGMA synchronous={synchronous}")
        if synchronous.upper() == "FULL":
            # SQLite on macOS reaches F_FULLFSYNC (a flush through the drive
            # cache) only with these set; elsewhere they are no-ops.
            conn.execute("PRAGMA fullfsync=ON")
            conn.execute("PRAGMA checkpoint_fullfsync=ON")
        _configure_max_page_count(conn, max_store_bytes)
    except BaseException:
        conn.close()
        raise
    return conn


def _configure_max_page_count(conn: sqlite3.Connection, max_store_bytes: int) -> None:
    """Cap database growth while keeping an existing larger file readable."""
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    current_pages = int(conn.execute("PRAGMA page_count").fetchone()[0])
    if page_size <= 0:
        raise ConfigurationError(f"SQLite reported invalid page_size={page_size}")

    # Floor division keeps the page cap within the configured byte count. One
    # page is the smallest limit SQLite accepts; existing pages are retained.
    configured_pages = max(1, max_store_bytes // page_size)
    target_pages = max(current_pages, configured_pages)
    conn.execute(f"PRAGMA max_page_count={target_pages}")
    actual_pages = int(conn.execute("PRAGMA max_page_count").fetchone()[0])
    # SQLite raises a cap below the file size to the file size, so a sibling
    # growing the file mid-open yields a larger value; only a smaller one
    # means the cap was refused.
    if actual_pages < target_pages:
        raise ConfigurationError(
            "could not enforce SHM max_store_bytes: "
            f"SQLite set max_page_count={actual_pages}, expected {target_pages}"
        )


def _validate_version(version: int) -> None:
    if version < 0:
        raise RuntimeError(f"SHM store uses unsupported or corrupt schema version {version}")
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"SHM store uses newer schema version {version}; "
            f"this runtime supports up to {SCHEMA_VERSION}"
        )


def _migrate(conn: sqlite3.Connection) -> None:
    with immediate_transaction(conn):
        # Read the version only after acquiring the reserved lock. A concurrent
        # opener can finish migration while this connection waits for the lock.
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        _validate_version(version)
        if version != SCHEMA_VERSION:
            if version == 0:
                if _table_exists(conn, "shm_message"):
                    _migrate_v0(conn)
                elif _table_exists(conn, "shm_publication"):
                    _migrate_v1(conn)
                else:
                    _create_schema(conn)
            elif version == 1:
                _migrate_v1(conn)
            elif version == 2:
                _migrate_v2(conn)
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        # Idempotent and run on every open (not gated on a version bump) so a
        # store already at SCHEMA_VERSION from before this index existed gets
        # it backfilled without a schema-version migration path.
        conn.execute(_PUBLICATION_EXPIRY_INDEX_SCHEMA)
        _add_dispatch_started_column(conn)
        _add_subscription_updated_at_column(conn)


def _add_subscription_updated_at_column(conn: sqlite3.Connection) -> None:
    """Backfill the consumer liveness stamp onto an older subscription table.

    Nullable, so rows written by an older runtime simply read as never refreshed.
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(shm_subscription)")}
    if "updated_at" not in columns:
        conn.execute("ALTER TABLE shm_subscription ADD COLUMN updated_at REAL")


def _add_dispatch_started_column(conn: sqlite3.Connection) -> None:
    """Backfill the column onto a delivery table created before it existed.

    Additive and defaulted, so a store opened by an older runtime at the same
    schema version keeps working; that runtime simply never sets it.
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(shm_delivery)")}
    if "dispatch_started" not in columns:
        conn.execute(
            "ALTER TABLE shm_delivery ADD COLUMN dispatch_started INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_v0(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE shm_message RENAME TO _shm_message_v0")
    _create_schema(conn)
    # Omitting the AUTOINCREMENT column makes SQLite allocate a unique global
    # sequence. ORDER BY preserves legacy order while resolving duplicates.
    conn.execute(
        """
        INSERT INTO shm_publication (
            id, target, event_type, payload, headers,
            created_at, retained_until
        )
        SELECT id, target, event_type, payload, headers,
               created_at, created_at + 86400.0
        FROM _shm_message_v0
        ORDER BY sequence, created_at, rowid
        """
    )
    conn.execute(
        """
        INSERT INTO shm_delivery (
            publication_id, consumer_group, status, attempts,
            available_at, claimed_at, claimed_by, claim_generation,
            last_error, completed_at, created_at
        )
        SELECT id, consumer_group, status, attempts,
               created_at, claimed_at, claimed_by,
               CASE WHEN status='claimed' THEN 1 ELSE 0 END,
               last_error,
               CASE WHEN status IN ('done', 'dead') THEN created_at END,
               created_at
        FROM _shm_message_v0
        """
    )
    conn.execute("DROP TABLE _shm_message_v0")


def _migrate_v1(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE shm_publication RENAME TO _shm_publication_v1")
    conn.execute("ALTER TABLE shm_delivery RENAME TO _shm_delivery_v1")
    conn.execute("DROP INDEX IF EXISTS idx_shm_delivery_claim")
    conn.execute("DROP INDEX IF EXISTS idx_shm_publication_retained")
    _create_schema(conn)
    conn.execute(
        """
        INSERT INTO shm_publication (
            id, target, event_type, payload, headers,
            created_at, retained_until
        )
        SELECT id, target, event_type, payload, headers, created_at,
               COALESCE(retained_until, created_at + 86400.0)
        FROM _shm_publication_v1
        ORDER BY sequence, created_at, rowid
        """
    )
    conn.execute(
        """
        INSERT INTO shm_delivery (
            publication_id, consumer_group, status, attempts,
            available_at, claimed_at, claimed_by, claim_generation,
            last_error, completed_at, created_at
        )
        SELECT publication_id, consumer_group, status, attempts,
               available_at, claimed_at, claimed_by, claim_generation,
               last_error, completed_at, created_at
        FROM _shm_delivery_v1
        """
    )
    conn.execute("DROP TABLE _shm_delivery_v1")
    conn.execute("DROP TABLE _shm_publication_v1")


def _migrate_v2(conn: sqlite3.Connection) -> None:
    # V2 already has stable publication sequences and delivery IDs. Adding the
    # tombstone table in place avoids rewriting either AUTOINCREMENT table.
    conn.execute(_COMPLETION_TOMBSTONE_SCHEMA)


def _create_schema(conn: sqlite3.Connection) -> None:
    for statement in _SCHEMA:
        conn.execute(statement)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        is not None
    )


def _enable_wal(conn: sqlite3.Connection) -> None:
    """Enable WAL despite SQLite's transient lock error between openers."""
    deadline = time.monotonic() + 5.0
    while True:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as error:
            if "locked" not in str(error).lower() or time.monotonic() >= deadline:
                raise
            time.sleep(0.01)
