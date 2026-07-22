"""Compatibility, migration, and lifecycle tests for the SHM durable store."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

import pytest

from modulith.adapters._shm_coldstore import ShmColdStore
from modulith.adapters._shm_schema import immediate_transaction
from modulith.adapters._shm_store import SqliteQueueStore


def _create_v0_database(path: Path) -> None:
    """Create the exact schema shipped by the original cold-store adapter."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE shm_message (
            id TEXT PRIMARY KEY, target TEXT NOT NULL,
            consumer_group TEXT NOT NULL, event_type TEXT NOT NULL,
            payload BLOB NOT NULL, headers TEXT, status TEXT NOT NULL,
            attempts INTEGER NOT NULL, sequence INTEGER NOT NULL,
            created_at REAL NOT NULL, claimed_at REAL, claimed_by TEXT,
            last_error TEXT
        );
        CREATE TABLE shm_subscription (
            target TEXT NOT NULL, consumer_group TEXT NOT NULL,
            PRIMARY KEY (target, consumer_group)
        );
        INSERT INTO shm_subscription VALUES ('orders.Created', 'billing');
        INSERT INTO shm_message VALUES (
            'legacy-1', 'orders.Created', 'billing', 'orders.Created',
            x'7b7d', '{"event_type": "orders.Created"}', 'pending',
            2, 17, 100.0, NULL, NULL, 'previous failure'
        );
        INSERT INTO shm_message VALUES (
            'legacy-2', 'orders.Created', 'billing', 'orders.Created',
            x'7b7d', '{"event_type": "orders.Created"}', 'pending',
            0, 17, 101.0, NULL, NULL, NULL
        );
        """
    )
    conn.close()


def _create_v1_database(path: Path, *, version: int) -> None:
    """Create the pre-v2 split schema, optionally without its version marker."""
    conn = sqlite3.connect(path)
    conn.executescript(
        f"""
        CREATE TABLE shm_publication (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE, target TEXT NOT NULL,
            event_type TEXT NOT NULL, payload BLOB NOT NULL,
            headers TEXT, created_at REAL NOT NULL, retained_until REAL
        );
        CREATE TABLE shm_subscription (
            target TEXT NOT NULL, consumer_group TEXT NOT NULL,
            PRIMARY KEY (target, consumer_group)
        );
        CREATE TABLE shm_delivery (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publication_id TEXT NOT NULL, consumer_group TEXT NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL,
            available_at REAL NOT NULL, claimed_at REAL, claimed_by TEXT,
            claim_generation INTEGER NOT NULL, last_error TEXT,
            completed_at REAL, created_at REAL NOT NULL
        );
        INSERT INTO shm_subscription VALUES ('orders.Created', 'billing');
        INSERT INTO shm_publication VALUES (
            7, 'v1-publication', 'orders.Created', 'orders.Created',
            x'7b7d', NULL, 100.0, NULL
        );
        INSERT INTO shm_delivery VALUES (
            9, 'v1-publication', 'billing', 'pending', 1,
            100.0, NULL, NULL, 0, 'retry once', NULL, 100.0
        );
        PRAGMA user_version = {version};
        """
    )
    conn.close()


def _create_v2_database(path: Path) -> None:
    """Create the split v2 schema whose row identities must remain stable."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE shm_publication (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE, target TEXT NOT NULL,
            event_type TEXT NOT NULL, payload BLOB NOT NULL,
            headers TEXT, created_at REAL NOT NULL, retained_until REAL NOT NULL
        );
        CREATE TABLE shm_subscription (
            target TEXT NOT NULL, consumer_group TEXT NOT NULL,
            PRIMARY KEY (target, consumer_group)
        );
        CREATE TABLE shm_delivery (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publication_id TEXT NOT NULL
                REFERENCES shm_publication(id) ON DELETE CASCADE,
            consumer_group TEXT NOT NULL, status TEXT NOT NULL,
            attempts INTEGER NOT NULL, available_at REAL NOT NULL,
            claimed_at REAL, claimed_by TEXT,
            claim_generation INTEGER NOT NULL, last_error TEXT,
            completed_at REAL, created_at REAL NOT NULL,
            UNIQUE (publication_id, consumer_group)
        );
        INSERT INTO shm_subscription VALUES ('orders.Created', 'billing');
        INSERT INTO shm_publication VALUES (
            41, 'v2-publication', 'orders.Created', 'orders.Created',
            x'7b7d', NULL, 100.0, 86500.0
        );
        INSERT INTO shm_delivery VALUES (
            73, 'v2-publication', 'billing', 'pending', 1,
            100.0, NULL, NULL, 0, 'retry once', NULL, 100.0
        );
        PRAGMA user_version = 2;
        """
    )
    conn.close()


@pytest.fixture
async def store(tmp_path: Path):
    durable_store = ShmColdStore(str(tmp_path / "cold.db"))
    yield durable_store
    await durable_store.close()


async def test_legacy_spill_and_recover_remain_import_compatible(
    store: ShmColdStore,
) -> None:
    await store.spill(
        [
            {
                "id": "legacy-api",
                "target": "orders.Created",
                "consumer_group": "billing",
                "event_type": "orders.Created",
                "payload": b"{}",
                "headers": {"event_type": "orders.Created"},
                "sequence": 4,
            }
        ]
    )

    rows = await store.recover("billing", consumer_name="worker-1")

    assert len(rows) == 1
    assert rows[0]["message_id"] == "legacy-api"
    assert rows[0]["payload"] == b"{}"
    assert rows[0]["claim_token"].generation == 1


async def test_empty_legacy_spill_creates_no_work(store: ShmColdStore) -> None:
    await store.spill([])
    await store.subscribe(["orders.Created"], "billing")

    assert await store.claim("billing", consumer_name="worker-1") == []


async def test_operations_use_one_dedicated_worker_thread(
    store: ShmColdStore,
) -> None:
    caller_thread = threading.get_ident()

    await store.subscribe(["orders.Created"], "billing")
    first_thread = store._worker_thread_id
    await store.get_subscriptions()

    assert first_thread is not None
    assert first_thread != caller_thread
    assert store._worker_thread_id == first_thread
    assert store._executor._max_workers == 1


async def test_cancelled_publish_settles_before_cancellation_is_observed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()
    original_publish = SqliteQueueStore.publish

    def delayed_publish(self: SqliteQueueStore, *args):
        started.set()
        if not release.wait(2.0):
            raise TimeoutError("test did not release delayed publish")
        return original_publish(self, *args)

    monkeypatch.setattr(SqliteQueueStore, "publish", delayed_publish)
    store = ShmColdStore(str(tmp_path / "cancel-publish.db"))
    task = asyncio.create_task(
        store.publish("orders.Created", b"durable", publication_id="cancelled")
    )
    try:
        assert await asyncio.to_thread(started.wait, 1.0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        await store.subscribe(["orders.Created"], "billing")
        rows = await store.claim("billing", consumer_name="worker")
        assert [row["message_id"] for row in rows] == ["cancelled"]
    finally:
        release.set()
        await store.close()


async def test_cancelled_call_propagates_cancellation_after_worker_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()

    def failing_publish(self: SqliteQueueStore, *args):
        started.set()
        if not release.wait(2.0):
            raise TimeoutError("test did not release failing publish")
        raise RuntimeError("worker failed after cancellation")

    monkeypatch.setattr(SqliteQueueStore, "publish", failing_publish)
    store = ShmColdStore(str(tmp_path / "cancelled-failure.db"))
    task = asyncio.create_task(store.publish("orders.Created", b"payload"))
    try:
        assert await asyncio.to_thread(started.wait, 1.0)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await store.close()


async def test_cancelled_close_keeps_one_retryable_completion_path(
    store: ShmColdStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await store.get_subscriptions()
    started = threading.Event()
    release = threading.Event()
    original_close = SqliteQueueStore.close

    def delayed_close(self: SqliteQueueStore) -> None:
        started.set()
        if not release.wait(2.0):
            raise TimeoutError("test did not release delayed close")
        original_close(self)

    monkeypatch.setattr(SqliteQueueStore, "close", delayed_close)
    first_close = asyncio.create_task(store.close())
    assert await asyncio.to_thread(started.wait, 1.0)
    first_close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_close

    retry = asyncio.create_task(store.close())
    await asyncio.sleep(0)
    assert not retry.done()
    release.set()
    await retry

    with pytest.raises(RuntimeError, match="closed"):
        await store.get_subscriptions()


async def test_close_worker_failure_can_be_retried(
    store: ShmColdStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await store.get_subscriptions()
    original_close = SqliteQueueStore.close
    attempts = 0

    def fail_once(self: SqliteQueueStore) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected close failure")
        original_close(self)

    monkeypatch.setattr(SqliteQueueStore, "close", fail_once)

    with pytest.raises(RuntimeError, match="injected close failure"):
        await store.close()
    await store.close()

    assert attempts == 2


async def test_subscription_rows_are_exact_and_idempotent(
    store: ShmColdStore,
) -> None:
    await store.subscribe(["orders.Created", "orders.Created"], "billing")
    await store.subscribe(["orders.Created"], "billing")
    await store.subscribe(["orders.Created"], "analytics")

    assert await store.get_subscriptions() == {"orders.Created": ["analytics", "billing"]}


@pytest.mark.parametrize(("mode", "pragma"), [("NORMAL", 1), ("FULL", 2)])
async def test_configures_requested_sqlite_synchronous_mode(
    tmp_path: Path, mode: str, pragma: int
) -> None:
    store = ShmColdStore(str(tmp_path / f"{mode}.db"), synchronous=mode)
    try:
        assert await store._read_pragma("synchronous") == pragma
    finally:
        await store.close()


async def test_configures_verified_max_page_count_from_store_bytes(tmp_path: Path) -> None:
    max_store_bytes = 64 * 1024 + 123
    store = ShmColdStore(
        str(tmp_path / "bounded.db"),
        max_store_bytes=max_store_bytes,
    )
    try:
        page_size = await store._read_pragma("page_size")
        page_count = await store._read_pragma("page_count")

        assert await store._read_pragma("max_page_count") == max(
            page_count,
            max_store_bytes // page_size,
        )
    finally:
        await store.close()


async def test_existing_larger_database_remains_readable_but_cannot_grow(
    tmp_path: Path,
) -> None:
    path = tmp_path / "existing-larger.db"
    original = ShmColdStore(str(path))
    await original.subscribe(["orders.Created"], "billing")
    await original.publish(
        "orders.Created",
        b"existing",
        publication_id="existing-publication",
    )
    original_pages = await original._read_pragma("page_count")
    await original.close()

    bounded = ShmColdStore(str(path), max_store_bytes=1)
    try:
        assert await bounded._read_pragma("max_page_count") == original_pages
        rows = await bounded.claim("billing", consumer_name="worker")
        assert [row["payload"] for row in rows] == [b"existing"]
        assert await bounded._read_pragma("page_count") == original_pages
    finally:
        await bounded.close()


def test_rejects_unknown_synchronous_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="synchronous"):
        ShmColdStore(str(tmp_path / "bad.db"), synchronous="OFF")


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"completion_mode": "archive"}, "completion_mode"),
        ({"orphan_retention_seconds": 0}, "orphan_retention_seconds"),
        ({"retry_backoff_base_seconds": float("nan")}, "retry_backoff_base_seconds"),
        ({"retry_backoff_cap_seconds": True}, "retry_backoff_cap_seconds"),
    ],
)
def test_rejects_invalid_store_options(
    tmp_path: Path,
    options: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        ShmColdStore(str(tmp_path / "invalid.db"), **options)  # type: ignore[arg-type]


async def test_rejects_invalid_operation_arguments(store: ShmColdStore) -> None:
    with pytest.raises(ValueError, match="limit"):
        await store.claim("billing", limit=0)
    with pytest.raises(ValueError, match="reclaim_stale_seconds"):
        await store.claim("billing", reclaim_stale_seconds=-1)
    with pytest.raises(ValueError, match="completion_mode"):
        await store.ack(
            "cold:1:1",
            consumer_name="worker",
            completion_mode="archive",
        )
    with pytest.raises(ValueError, match="max_attempts"):
        await store.fail("cold:1:1", "failure", 0, consumer_name="worker")
    with pytest.raises(ValueError, match="retention_age_seconds"):
        await store.prune(-1)
    with pytest.raises(ValueError, match="limit"):
        await store.prune(0, limit=0)


async def test_migrates_v0_schema_transactionally_and_preserves_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.db"
    _create_v0_database(path)
    store = ShmColdStore(str(path))
    try:
        assert await store.get_subscriptions() == {"orders.Created": ["billing"]}
        rows = await store.claim("billing", consumer_name="worker-1")
        assert len(rows) == 2
        assert [row["message_id"] for row in rows] == [
            "legacy-1",
            "legacy-2",
        ]
        assert [row["sequence"] for row in rows] == [1, 2]
        assert rows[0]["attempts"] == 2
        assert rows[0]["last_error"] == "previous failure"
    finally:
        await store.close()

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "shm_message" not in tables
        assert {
            "shm_publication",
            "shm_delivery",
            "shm_subscription",
            "shm_completion_tombstone",
        } <= tables
    finally:
        conn.close()


@pytest.mark.parametrize("version", [pytest.param(1, id="v1"), pytest.param(0, id="partial")])
async def test_migrates_v1_and_unversioned_partial_schema(
    tmp_path: Path,
    version: int,
) -> None:
    path = tmp_path / f"v1-{version}.db"
    _create_v1_database(path, version=version)
    store = ShmColdStore(str(path))
    try:
        rows = await store.claim("billing", consumer_name="worker-1")

        assert len(rows) == 1
        assert rows[0]["message_id"] == "v1-publication"
        assert rows[0]["sequence"] == 1
        assert rows[0]["attempts"] == 1
        assert rows[0]["last_error"] == "retry once"
    finally:
        await store.close()

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        assert conn.execute(
            "SELECT retained_until FROM shm_publication WHERE id='v1-publication'"
        ).fetchone()[0] == pytest.approx(86500.0)
    finally:
        conn.close()


async def test_unknown_newer_schema_version_fails_without_modifying_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "future.db"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 99")
    conn.close()
    store = ShmColdStore(str(path))

    with pytest.raises(RuntimeError, match="newer schema version 99"):
        await store.get_subscriptions()
    await store.close()

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 99
    finally:
        conn.close()


async def test_concurrent_v2_migration_preserves_delivery_and_publication_ids(
    tmp_path: Path,
) -> None:
    path = tmp_path / "concurrent-v2.db"
    _create_v2_database(path)
    first = ShmColdStore(str(path))
    second = ShmColdStore(str(path))
    try:
        first_subscriptions, second_subscriptions = await asyncio.gather(
            first.get_subscriptions(),
            second.get_subscriptions(),
        )
        assert first_subscriptions == {"orders.Created": ["billing"]}
        assert second_subscriptions == {"orders.Created": ["billing"]}
    finally:
        await asyncio.gather(first.close(), second.close())

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        assert (
            conn.execute(
                "SELECT sequence FROM shm_publication WHERE id='v2-publication'"
            ).fetchone()[0]
            == 41
        )
        assert (
            conn.execute(
                "SELECT id FROM shm_delivery WHERE publication_id='v2-publication'"
            ).fetchone()[0]
            == 73
        )
        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='shm_completion_tombstone'
            """
        ).fetchone() == (1,)
    finally:
        conn.close()


def test_commit_failure_rolls_back_transaction_state(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "commit-failure.db", isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            parent_id INTEGER NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES parent(id)
                DEFERRABLE INITIALLY DEFERRED
        );
        """
    )
    try:
        with pytest.raises(sqlite3.IntegrityError):
            with immediate_transaction(conn):
                conn.execute("INSERT INTO child VALUES (1)")

        assert not conn.in_transaction
        assert conn.execute("SELECT * FROM child").fetchall() == []
    finally:
        conn.close()


async def test_close_is_idempotent_and_rejects_new_operations(
    store: ShmColdStore,
) -> None:
    await store.get_subscriptions()
    await store.close()
    await store.close()

    with pytest.raises(RuntimeError, match="closed"):
        await store.get_subscriptions()
