"""Integration tests for the SQLite-authoritative SHM broker."""

from __future__ import annotations

import asyncio
import gc
import logging
import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest

from modulith import ConfigurationError, configure
from modulith.adapters import _shm_claims
from modulith.adapters._shm_coldstore import ShmColdStore
from modulith.adapters._shm_ring import _HEADER_SIZE, _SLOT_SIZE, _SLOT_STRUCT
from modulith.adapters._shm_schema import open_database
from modulith.adapters._shm_store import SqliteQueueStore
from modulith.adapters.shm_broker import (
    ShmBroker,
    ShmConsumer,
    _opt_float,
    _opt_int,
    modulith_register_brokers,
    modulith_register_consumers,
)
from modulith.brokers import BrokerRegistry, ConsumerRegistry
from modulith.event_bus import InMemoryEventBus
from modulith.protocols import Broker
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


def _delivery_rows(path: Path) -> list[sqlite3.Row]:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        return list(
            connection.execute(
                "SELECT status, attempts, claimed_by, claim_generation "
                "FROM shm_delivery ORDER BY id"
            )
        )
    finally:
        connection.close()


@pytest.fixture()
async def broker(tmp_path: Path) -> AsyncIterator[ShmBroker]:
    instance = ShmBroker(
        shm_name="broker-hints",
        capacity=16,
        slot_size=512,
        db_path=str(tmp_path / "broker.db"),
    )
    try:
        yield instance
    finally:
        await instance.close()
        # Explicit test cleanup is separate from ordinary broker shutdown.
        instance._ring.unlink()


async def _publish_many(broker: ShmBroker, prefix: str, count: int) -> None:
    for index in range(count):
        await broker.publish("events", f"{prefix}-{index}".encode())


def _consumer_with_options(**options: Any) -> ShmConsumer:
    return ShmConsumer(
        broker=cast(ShmBroker, object()),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="worker-1",
        group="workers",
        targets=["events"],
        **options,
    )


def test_constructor_rejects_unknown_completion_mode(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="completion_mode"):
        ShmBroker(
            shm_name="invalid-mode-hints",
            db_path=str(tmp_path / "invalid-mode.db"),
            completion_mode="archive",
        )


async def test_constructor_keeps_ignored_slot_size_keyword_without_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Direct callers may retain the compatibility keyword, but it has no effect."""
    instance = ShmBroker(
        shm_name=str(tmp_path / "compatibility.hints"),
        slot_size=-1,
        db_path=str(tmp_path / "compatibility.db"),
    )
    try:
        assert instance._ring.path.stat().st_size == (
            _HEADER_SIZE + instance._ring.capacity * _SLOT_SIZE
        )
        assert not any("slot_size" in record.getMessage() for record in caplog.records)
    finally:
        await instance.close()
        instance._ring.unlink()


async def test_cancelled_close_retry_waits_for_cold_store_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()
    original_close = SqliteQueueStore.close

    def delayed_close(store: SqliteQueueStore) -> None:
        started.set()
        if not release.wait(2.0):
            raise TimeoutError("test did not release delayed close")
        original_close(store)

    monkeypatch.setattr(SqliteQueueStore, "close", delayed_close)
    instance = ShmBroker(
        shm_name="cancel-close-hints",
        db_path=str(tmp_path / "cancel-close.db"),
    )
    await instance._cold.get_subscriptions()
    first_close = asyncio.create_task(instance.close())
    try:
        assert await asyncio.to_thread(started.wait, 1.0)
        first_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_close
        assert not instance._closed

        retry = asyncio.create_task(instance.close())
        await asyncio.sleep(0)
        assert not retry.done()

        release.set()
        await retry
        assert instance._closed
        await instance.close()
    finally:
        release.set()
        await instance._cold.close()
        instance._ring.unlink()


@pytest.mark.parametrize(
    "options",
    [
        {"capacity": 0},
        {"capacity": 10**12},
        {"max_payload_bytes": 0},
        {"max_payload_bytes": 1024**3 + 1},
        {"max_store_bytes": True},
        {"max_store_bytes": 1024**4 + 1},
        {"synchronous": "OFF"},
        {"completion_mode": []},
        {"orphan_retention_seconds": 0},
        {"orphan_retention_seconds": -1},
        {"orphan_retention_seconds": float("inf")},
        {"orphan_retention_seconds": float("nan")},
        {"orphan_retention_seconds": "soon"},
        {"orphan_retention_seconds": True},
    ],
)
def test_constructor_validates_options_before_creating_files(
    tmp_path: Path,
    options: dict[str, Any],
) -> None:
    db_path = tmp_path / "invalid.db"
    hint_path = tmp_path / "invalid.hints"

    with pytest.raises(ConfigurationError):
        ShmBroker(
            shm_name=str(hint_path),
            db_path=str(db_path),
            **options,
        )

    assert not db_path.exists()
    assert not hint_path.exists()


@pytest.mark.parametrize(
    ("options", "option_name"),
    [
        ({"poll_interval_s": True}, "poll_interval_s"),
        ({"poll_interval_s": "fast"}, "poll_interval_s"),
        ({"poll_interval_s": 0}, "poll_interval_s"),
        ({"batch_size": True}, "batch_size"),
        ({"batch_size": 0}, "batch_size"),
        ({"batch_size": 10**12}, "batch_size"),
        ({"dispatch_concurrency": 0}, "dispatch_concurrency"),
        ({"dispatch_concurrency": 10**12}, "dispatch_concurrency"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"prune_interval_s": True}, "prune_interval_s"),
        ({"prune_interval_s": "often"}, "prune_interval_s"),
        ({"prune_interval_s": -1}, "prune_interval_s"),
        ({"retention_age_seconds": float("inf")}, "retention_age_seconds"),
    ],
)
def test_consumer_rejects_invalid_numeric_options(
    options: dict[str, Any],
    option_name: str,
) -> None:
    with pytest.raises(ConfigurationError, match=option_name):
        _consumer_with_options(**options)


async def test_claim_batch_rejects_unbounded_allocation_request(broker: ShmBroker) -> None:
    with pytest.raises(ConfigurationError, match="batch_size"):
        await broker.claim_batch(
            "workers",
            batch_size=10**12,
            consumer_name="worker",
        )


def test_helper_option_parsers_accept_numbers_and_reject_ambiguous_values() -> None:
    assert _opt_float(None) is None
    assert _opt_float("2.5") == 2.5
    assert _opt_int(None) is None
    assert _opt_int(7) == 7
    assert _opt_int("8") == 8

    for float_value in (True, "not-a-number"):
        with pytest.raises(ConfigurationError, match="number"):
            _opt_float(float_value)
    for int_value in (True, 2.5, "not-an-integer"):
        with pytest.raises(ConfigurationError, match="integer"):
            _opt_int(int_value)


async def test_constructor_preserves_protocol_paths_and_store_options(tmp_path: Path) -> None:
    db_path = tmp_path / "full.db"
    instance = ShmBroker(
        shm_name="simple-name",
        capacity=4,
        slot_size=128,
        db_path=str(db_path),
        synchronous="FULL",
        completion_mode="mark",
    )
    notifier_path = tmp_path / "simple-name"
    try:
        assert isinstance(instance, Broker)
        assert instance._ring.path == notifier_path
        assert notifier_path.stat().st_size == _HEADER_SIZE + 4 * _SLOT_SIZE
        assert await instance._cold._read_pragma("synchronous") == 2
        page_size = await instance._cold._read_pragma("page_size")
        page_count = await instance._cold._read_pragma("page_count")
        assert await instance._cold._read_pragma("max_page_count") == max(
            page_count,
            1024**3 // page_size,
        )
    finally:
        await instance.close()
        await instance.close()

    # close() releases handles but never removes a shared notifier.
    assert notifier_path.exists()
    instance._ring.unlink()

    explicit_path = tmp_path / "explicit.hints"
    explicit = ShmBroker(
        shm_name=str(explicit_path),
        capacity=4,
        db_path=str(tmp_path / "explicit.db"),
    )
    try:
        assert explicit._ring.path == explicit_path
    finally:
        await explicit.close()
        explicit._ring.unlink()


async def test_payload_limit_accepts_exact_boundary_and_rejects_overage_without_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "payload-limit.db"
    instance = ShmBroker(
        shm_name="payload-limit-hints",
        db_path=str(db_path),
        max_payload_bytes=4,
        max_store_bytes=64 * 1024,
    )
    try:
        await instance.subscribe(["events"], "workers")
        await instance.publish("events", b"1234")

        with pytest.raises(ConfigurationError, match="max_payload_bytes"):
            await instance.publish("events", b"12345")

        rows = await instance.claim_batch("workers", batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in rows] == [b"1234"]
        connection = sqlite3.connect(db_path)
        try:
            assert connection.execute("SELECT COUNT(*) FROM shm_publication").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM shm_delivery").fetchone()[0] == 1
        finally:
            connection.close()
    finally:
        await instance.close()
        instance._ring.unlink()


@pytest.mark.parametrize(
    ("env_suffix", "value"),
    [
        ("MAX_PAYLOAD_BYTES", "0"),
        ("MAX_PAYLOAD_BYTES", str(1024**3 + 1)),
        ("MAX_STORE_BYTES", "not-an-integer"),
        ("MAX_STORE_BYTES", str(1024**4 + 1)),
        ("ORPHAN_RETENTION_SECONDS", "0"),
        ("ORPHAN_RETENTION_SECONDS", "-5"),
        ("ORPHAN_RETENTION_SECONDS", "nan"),
        ("ORPHAN_RETENTION_SECONDS", "soon"),
    ],
)
def test_runtime_registration_rejects_invalid_storage_limit_environment(
    make_fake_app: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_suffix: str,
    value: str,
) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.setenv(f"MODULITH_BROKER_{env_suffix}", value)
    configure(
        package="fakeapp",
        topology="processes",
        broker="shm",
        broker_options={"state_dir": str(tmp_path)},
    )

    with pytest.raises(ConfigurationError, match=env_suffix.lower()):
        _runtime.ensure_bootstrapped()


async def test_publish_claim_round_trip_preserves_target_and_event_type(
    broker: ShmBroker,
) -> None:
    await broker.subscribe(["orders-stream"], "billing")
    await broker.publish(
        "orders-stream",
        b'{"order_id":"A-1"}',
        {"event_type": "contracts.OrderPlaced", "trace_id": "trace-1"},
    )

    rows = await broker.claim_batch("billing", batch_size=10, consumer_name="worker-1")

    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == str(row["claim_token"])
    assert row["target"] == "orders-stream"
    assert row["event_type"] == "contracts.OrderPlaced"
    assert row["payload"] == b'{"order_id":"A-1"}'
    assert row["headers"]["trace_id"] == "trace-1"


@pytest.mark.parametrize("failure_mode", ["exception", "unavailable"])
async def test_publish_remains_durable_when_notifier_fails(
    broker: ShmBroker,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    await broker.subscribe(["events"], "workers")

    def fail_notification(_sequence: int) -> bool:
        if failure_mode == "exception":
            raise OSError("mapping unavailable")
        return False

    monkeypatch.setattr(broker._ring, "notify", fail_notification)
    await broker.publish("events", b"durable")

    rows = await broker.claim_batch("workers", batch_size=1, consumer_name="worker")
    assert [row["payload"] for row in rows] == [b"durable"]


async def test_hint_wait_times_out_and_unavailable_ring_uses_safety_delay(
    broker: ShmBroker,
) -> None:
    assert await broker.wait_for_hint(after_sequence=-1, safety_timeout=0) is None

    broker._ring.close()
    assert await broker.wait_for_hint(after_sequence=-1, safety_timeout=-1) is None


async def test_serial_store_executor_locks_do_not_leak_across_event_loops(
    tmp_path: Path,
) -> None:
    """SerialStoreExecutor._locks must not grow forever. modulith.sync's
    _run_nested_dispatch creates and closes a fresh event loop per call, so a
    plain dict keyed by loop object would accumulate one dead Lock per closed
    loop for the life of a long-running process."""
    store = ShmColdStore(str(tmp_path / "locks.db"))

    def drive_once() -> None:
        def run_on_fresh_loop() -> None:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(store.get_subscriptions())
            finally:
                loop.close()

        thread = threading.Thread(target=run_on_fresh_loop)
        thread.start()
        thread.join(timeout=5.0)

    try:
        for _ in range(3):
            drive_once()
        gc.collect()
        len_after_few = len(store._locks)

        for _ in range(20):
            drive_once()
        gc.collect()
        len_after_many = len(store._locks)

        assert len_after_many == len_after_few
    finally:
        await store.close()


async def test_publish_from_two_event_loops_does_not_deadlock(tmp_path: Path) -> None:
    """SerialStoreExecutor._operation_lock must not bind to a single event
    loop. A broker reached from a second loop (mirroring sync.publish_sync's
    persistent daemon-thread loop) previously wedged one side's waiter
    forever while the other side raised a cross-loop RuntimeError."""
    broker = ShmBroker(
        shm_name=str(tmp_path / "two-loop.hints"),
        db_path=str(tmp_path / "two-loop.db"),
    )
    await broker.subscribe(["events"], "workers")

    other_loop = asyncio.new_event_loop()
    other_ready = threading.Event()

    def run_other_loop() -> None:
        asyncio.set_event_loop(other_loop)
        other_ready.set()
        other_loop.run_forever()

    other_thread = threading.Thread(target=run_other_loop, daemon=True)
    other_thread.start()
    try:
        assert other_ready.wait(2.0)

        other_future = asyncio.run_coroutine_threadsafe(
            _publish_many(broker, "other", 20), other_loop
        )
        await asyncio.wait_for(_publish_many(broker, "main", 20), timeout=10.0)
        await asyncio.wait_for(asyncio.wrap_future(other_future), timeout=10.0)

        rows = await broker.claim_batch("workers", batch_size=100, consumer_name="worker")
        assert len(rows) == 40
    finally:
        other_loop.call_soon_threadsafe(other_loop.stop)
        other_thread.join(timeout=5.0)
        other_loop.close()
        await broker.close()
        broker._ring.unlink()


async def test_idle_hint_reads_answer_without_unpacking_every_slot(
    broker: ShmBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A consumer whose cursor already sits at the newest hint re-reads the
    ring every few milliseconds for the whole length of its safety poll.
    Answering that from the per-slot unpack loop costs O(capacity) per read,
    which keeps an otherwise idle worker busy and stalls its event loop at
    large capacities."""
    unpacks = 0

    class CountingSlotStruct:
        def unpack(self, buffer: bytes) -> tuple[int, int]:
            nonlocal unpacks
            unpacks += 1
            sequence, complement = _SLOT_STRUCT.unpack(buffer)
            return int(sequence), int(complement)

    assert broker._ring.notify(7)
    monkeypatch.setattr("modulith.adapters._shm_ring._SLOT_STRUCT", CountingSlotStruct())

    assert broker._ring.read_hints(after_sequence=7) == []
    assert unpacks == 0

    assert broker._ring.read_hints(after_sequence=6) == [7]
    assert unpacks == broker._ring.capacity


async def test_hint_prescan_returns_exactly_what_the_per_slot_scan_returns(
    broker: ShmBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The header-word pre-scan is an optimisation only. It must agree with
    the per-slot loop on every cursor, including on a ring nobody has
    notified: all-zero slots are indistinguishable from a genuine hint of
    sequence 0 on the header word alone, so the complements have to settle
    that case."""
    cursors = (-1, 0, 3, 6, 7, 8)

    def read_all() -> dict[int, list[int]]:
        return {cursor: broker._ring.read_hints(after_sequence=cursor) for cursor in cursors}

    def scan_only() -> dict[int, list[int]]:
        monkeypatch.setattr(
            "modulith.adapters._shm_ring.ShmRing._nothing_newer",
            lambda self, mapping, after_sequence: False,
        )
        try:
            return read_all()
        finally:
            monkeypatch.undo()

    assert read_all() == scan_only()
    assert read_all()[-1] == []

    for sequence in (0, 3, 7):
        assert broker._ring.notify(sequence)

    assert read_all() == scan_only()
    assert read_all()[-1] == [0, 3, 7]


async def test_hint_file_kept_at_another_capacity_warns_that_notification_is_dead(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An existing hint file is never re-created and only attaches when its
    header matches the requested capacity, so raising ``shm_capacity`` on an
    existing deployment leaves this process unable to notify at all. Delivery
    still works — SQLite stays authoritative — so the degradation is invisible
    without the warning."""
    hint_path = str(tmp_path / "capacity.hints")
    original = ShmBroker(
        shm_name=hint_path,
        capacity=16,
        db_path=str(tmp_path / "capacity.db"),
    )
    try:
        assert original._ring.available

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            widened = ShmBroker(
                shm_name=hint_path,
                capacity=32,
                db_path=str(tmp_path / "capacity-widened.db"),
            )
        try:
            assert not widened._ring.available
            assert any(
                "could not be attached at capacity=32" in record.getMessage()
                for record in caplog.records
            )

            await widened.subscribe(["events"], "workers")
            await widened.publish("events", b"durable")
            rows = await widened.claim_batch("workers", batch_size=1, consumer_name="worker")
            assert [row["payload"] for row in rows] == [b"durable"]
        finally:
            await widened.close()
    finally:
        await original.close()
        original._ring.unlink()


async def test_prune_without_age_retention_is_disabled(broker: ShmBroker) -> None:
    assert await broker.prune(retention_count=1) == 0


async def test_producer_without_local_subscriptions_discovers_persisted_groups(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "shared.db")
    consumer = ShmBroker(shm_name="shared-hints", db_path=db_path)
    producer = ShmBroker(shm_name="shared-hints", db_path=db_path, create=False)
    try:
        await consumer.subscribe(["events"], "workers")
        await producer.publish("events", b"from-producer")

        rows = await consumer.claim_batch("workers", batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in rows] == [b"from-producer"]
    finally:
        await producer.close()
        await consumer.close()
        consumer._ring.unlink()


async def test_subscribe_replays_publications_created_before_group_exists(
    tmp_path: Path,
) -> None:
    instance = ShmBroker(shm_name="replay-hints", db_path=str(tmp_path / "replay.db"))
    try:
        await instance.publish("events", b"retained")
        await instance.subscribe(["events"], "late-workers")

        rows = await instance.claim_batch("late-workers", batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in rows] == [b"retained"]
    finally:
        await instance.close()
        instance._ring.unlink()


async def test_stale_generation_and_wrong_owner_mutations_are_noops(
    broker: ShmBroker, tmp_path: Path
) -> None:
    await broker.subscribe(["events"], "workers")
    await broker.publish("events", b"payload")
    stale = (await broker.claim_batch("workers", batch_size=1, consumer_name="worker-1"))[0]
    await asyncio.sleep(0.001)
    current = (
        await broker.claim_batch(
            "workers",
            batch_size=1,
            consumer_name="worker-2",
            reclaim_stale_seconds=0,
        )
    )[0]

    assert await broker.renew_claims([stale["id"]], consumer_name="worker-1") == 0
    await broker.ack(stale["id"], consumer_name="worker-1")
    await broker.fail(stale["id"], "stale failure", consumer_name="worker-1", max_attempts=2)
    await broker.dead_letter(stale["id"], "stale poison", consumer_name="worker-1")
    assert await broker.renew_claims([current["id"]], consumer_name="worker-1") == 0
    await broker.ack(current["id"], consumer_name="worker-1")
    await broker.fail(current["id"], "wrong owner", consumer_name="worker-1", max_attempts=2)
    await broker.dead_letter(current["id"], "wrong owner", consumer_name="worker-1")

    state = _delivery_rows(tmp_path / "broker.db")
    assert [dict(row) for row in state] == [
        {
            "status": "claimed",
            "attempts": 0,
            "claimed_by": "worker-2",
            "claim_generation": 2,
        }
    ]
    await broker.ack(current["id"], consumer_name="worker-2")


@pytest.mark.parametrize(("mode", "expected"), [("delete", []), ("mark", ["done"])])
async def test_completion_mode_is_honored(tmp_path: Path, mode: str, expected: list[str]) -> None:
    db_path = tmp_path / f"{mode}.db"
    instance = ShmBroker(
        shm_name=f"{mode}-hints",
        db_path=str(db_path),
        completion_mode=mode,
    )
    try:
        await instance.subscribe(["events"], "workers")
        await instance.publish("events", b"payload")
        row = (await instance.claim_batch("workers", batch_size=1, consumer_name="worker"))[0]
        await instance.ack(row["id"], consumer_name="worker")

        assert [row["status"] for row in _delivery_rows(db_path)] == expected
    finally:
        await instance.close()
        instance._ring.unlink()


async def test_prune_removes_at_most_one_bounded_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "prune.db"
    instance = ShmBroker(shm_name="prune-hints", db_path=str(db_path))
    try:
        await instance._cold.get_subscriptions()
        connection = sqlite3.connect(db_path)
        try:
            now = time.time()
            publications = [
                (f"p-{index}", "events", "events", b"{}", now, now + 86400) for index in range(1001)
            ]
            connection.executemany(
                "INSERT INTO shm_publication "
                "(id, target, event_type, payload, created_at, retained_until) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                publications,
            )
            connection.execute(
                "INSERT INTO shm_delivery "
                "(publication_id, consumer_group, status, attempts, available_at, "
                "claim_generation, completed_at, created_at) "
                "SELECT id, 'workers', 'dead', 1, 0, 1, 0, 0 FROM shm_publication"
            )
            connection.commit()
        finally:
            connection.close()

        assert await instance.prune(retention_age_seconds=1) == 1000
        assert len(_delivery_rows(db_path)) == 1
    finally:
        await instance.close()
        instance._ring.unlink()


def _retention_windows(path: Path) -> list[float]:
    connection = sqlite3.connect(path)
    try:
        return [
            float(row[0])
            for row in connection.execute(
                "SELECT retained_until - created_at FROM shm_publication ORDER BY sequence"
            )
        ]
    finally:
        connection.close()


def _publication_count(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM shm_publication").fetchone()[0])
    finally:
        connection.close()


async def _publish_and_drain(instance: ShmBroker, count: int) -> int:
    """Publish with an immediately acking consumer; return publishes before the store filled."""
    for index in range(count):
        try:
            await instance.publish("events", b"x" * 1024)
        except ConfigurationError as error:
            assert "store is full" in str(error)
            return index
        for row in await instance.claim_batch("workers", batch_size=10, consumer_name="worker"):
            await instance.ack(row["id"], consumer_name="worker")
    return count


async def test_orphan_retention_defaults_to_24_hours(tmp_path: Path) -> None:
    db_path = tmp_path / "default-retention.db"
    instance = ShmBroker(shm_name="default-retention-hints", db_path=str(db_path))
    try:
        await instance.publish("events", b"payload")

        assert _retention_windows(db_path) == [pytest.approx(86400.0)]
    finally:
        await instance.close()
        instance._ring.unlink()


async def test_short_orphan_retention_frees_store_space_for_drained_publications(
    tmp_path: Path,
) -> None:
    store_bytes = 256 * 1024
    attempts = 1000
    default_path = tmp_path / "default.db"
    default = ShmBroker(
        shm_name="default-hints",
        db_path=str(default_path),
        max_store_bytes=store_bytes,
    )
    try:
        await default.subscribe(["events"], "workers")
        # The drained store still fills: every acked publication is kept for 24 hours.
        filled_after = await _publish_and_drain(default, attempts)
        assert filled_after < attempts
        assert _delivery_rows(default_path) == []
    finally:
        await default.close()
        default._ring.unlink()

    short_path = tmp_path / "short.db"
    short = ShmBroker(
        shm_name="short-hints",
        db_path=str(short_path),
        max_store_bytes=store_bytes,
        orphan_retention_seconds=0.01,
    )
    try:
        await short.subscribe(["events"], "workers")

        assert await _publish_and_drain(short, attempts) == attempts
        assert _publication_count(short_path) < filled_after
    finally:
        await short.close()
        short._ring.unlink()


async def test_late_subscriber_replay_honors_configured_orphan_retention(tmp_path: Path) -> None:
    db_path = tmp_path / "replay-window.db"
    instance = ShmBroker(
        shm_name="replay-window-hints",
        db_path=str(db_path),
        orphan_retention_seconds=0.5,
    )
    try:
        await instance.subscribe(["events"], "early")
        await instance.publish("events", b"retained")
        for row in await instance.claim_batch("early", batch_size=10, consumer_name="worker"):
            await instance.ack(row["id"], consumer_name="worker")
        assert _delivery_rows(db_path) == []
        assert _retention_windows(db_path) == [pytest.approx(0.5)]

        # Every original group is done, yet a group joining inside the window still replays.
        await instance.subscribe(["events"], "late")
        replayed = await instance.claim_batch("late", batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in replayed] == [b"retained"]
        await instance.ack(replayed[0]["id"], consumer_name="worker")

        await asyncio.sleep(0.6)
        await instance.subscribe(["events"], "expired")
        assert await instance.claim_batch("expired", batch_size=10, consumer_name="worker") == []
        assert _publication_count(db_path) == 0
    finally:
        await instance.close()
        instance._ring.unlink()


@pytest.mark.parametrize(
    ("source", "expected"),
    [("config", 60.0), ("environment", 120.0)],
)
async def test_runtime_registration_applies_orphan_retention(
    make_fake_app: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source: str,
    expected: float,
) -> None:
    make_fake_app({"orders": ""})
    if source == "environment":
        monkeypatch.setenv("MODULITH_BROKER_ORPHAN_RETENTION_SECONDS", "120")
    configure(
        package="fakeapp",
        topology="processes",
        broker="shm",
        broker_options={
            "state_dir": str(tmp_path),
            "sqlite_path": "retention.db",
            "orphan_retention_seconds": 60,
        },
    )
    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    instance = cast(ShmBroker, _runtime.broker_registry.get("shm"))
    try:
        await instance.publish("events", b"payload")

        assert _retention_windows(tmp_path / "retention.db") == [pytest.approx(expected)]
    finally:
        await instance.close()
        instance._ring.unlink()


async def test_runtime_registration_honors_shm_options_and_environment(
    make_fake_app: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_BROKER_SHM_CAPACITY", "5")
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", "5")
    monkeypatch.setenv("MODULITH_BROKER_MAX_STORE_BYTES", str(64 * 1024))
    with caplog.at_level(logging.INFO, logger="modulith.adapters.shm"):
        configure(
            package="fakeapp",
            topology="processes",
            broker="shm",
            broker_options={
                "state_dir": str(tmp_path),
                "sqlite_path": "registered.db",
                "hint_path": "registered.hints",
                "shm_capacity": 4,
                "shm_slot_size": "128",
                "completion_mode": "mark",
                "sqlite_synchronous": "FULL",
                "max_payload_bytes": 4,
                "max_store_bytes": 128 * 1024,
            },
        )
        _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None

    instance = cast(ShmBroker, _runtime.broker_registry.get("shm"))
    try:
        assert instance._ring.capacity == 5
        assert instance._ring.path == tmp_path / "registered.hints"
        assert instance._ring.path.stat().st_size == _HEADER_SIZE + 5 * _SLOT_SIZE
        assert instance._completion_mode == "mark"
        assert await instance._cold._read_pragma("synchronous") == 2
        page_size = await instance._cold._read_pragma("page_size")
        page_count = await instance._cold._read_pragma("page_count")
        assert await instance._cold._read_pragma("max_page_count") == max(
            page_count,
            (64 * 1024) // page_size,
        )
        await instance.publish("events", b"12345")
        with pytest.raises(ConfigurationError, match="max_payload_bytes"):
            await instance.publish("events", b"123456")
        warnings = [
            record
            for record in caplog.records
            if "shm_slot_size is deprecated and ignored" in record.getMessage()
        ]
        assert len(warnings) == 1
        assert warnings[0].name == "modulith.config"
        assert all("slot_size=" not in record.getMessage() for record in caplog.records)
    finally:
        await instance.close()
        instance._ring.unlink()


def test_registration_hooks_ignore_non_shm_configuration(make_fake_app: Any) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", broker="memory")
    _runtime.ensure_bootstrapped()
    brokers = BrokerRegistry()
    consumers = ConsumerRegistry()

    modulith_register_brokers(brokers)
    modulith_register_consumers(consumers)

    assert "shm" not in brokers.schemes()
    assert "shm" not in consumers.schemes()


# ---------------------------------------------------------------------------
# _shm_claims.claim(): reclaim-time max_attempts parity with the database
# broker -- a stale-claim reclaim past the cap dead-letters instead of
# redelivering forever.
# ---------------------------------------------------------------------------


def _open_claims_db(path: Path) -> sqlite3.Connection:
    return open_database(str(path), "NORMAL", 10_000_000)


def _seed_claimed_delivery(
    conn: sqlite3.Connection,
    *,
    group: str,
    attempts: int,
    claimed_at: float,
    dispatch_started: bool,
) -> None:
    now = time.time()
    conn.execute(
        "INSERT INTO shm_publication "
        "(id, target, event_type, payload, headers, created_at, retained_until) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("pub-1", "events", "test.Event", b"payload", None, now, now + 3600),
    )
    conn.execute(
        "INSERT INTO shm_delivery "
        "(publication_id, consumer_group, status, attempts, available_at, "
        "claimed_at, claimed_by, claim_generation, dispatch_started, created_at) "
        "VALUES (?, ?, 'claimed', ?, ?, ?, 'worker-1', 1, ?, ?)",
        ("pub-1", group, attempts, now - 10, claimed_at, int(dispatch_started), now),
    )


def test_reclaim_dead_letters_once_max_attempts_is_exhausted(tmp_path: Path) -> None:
    conn = _open_claims_db(tmp_path / "claims.db")
    try:
        _seed_claimed_delivery(
            conn,
            group="workers",
            attempts=1,
            claimed_at=time.time() - 120,
            dispatch_started=True,
        )

        claimed = _shm_claims.claim(
            conn,
            "workers",
            10,
            "worker-2",
            reclaim_stale_seconds=60.0,
            max_claim_bytes=1_000_000,
            max_attempts=2,
        )

        assert claimed == [], "a row that exhausts max_attempts must not be redelivered"
        row = conn.execute("SELECT status, attempts, claimed_by FROM shm_delivery").fetchone()
        assert (row["status"], row["attempts"], row["claimed_by"]) == ("dead", 2, None)
    finally:
        conn.close()


def test_opening_a_store_from_before_dispatch_started_adds_the_column(tmp_path: Path) -> None:
    """A current-version store created before ``dispatch_started`` existed
    must gain the column on open, or every claim fails on it."""
    path = tmp_path / "claims.db"
    _open_claims_db(path).close()
    legacy = sqlite3.connect(path)
    legacy.execute("ALTER TABLE shm_delivery DROP COLUMN dispatch_started")
    legacy.commit()
    legacy.close()

    conn = _open_claims_db(path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(shm_delivery)")}
        assert "dispatch_started" in columns
    finally:
        conn.close()


def test_reclaim_of_a_never_started_delivery_keeps_its_retry_budget(tmp_path: Path) -> None:
    """A delivery claimed in a batch whose listener never started did not fail
    a delivery, so a stale reclaim must neither charge nor dead-letter it."""
    conn = _open_claims_db(tmp_path / "claims.db")
    try:
        _seed_claimed_delivery(
            conn,
            group="workers",
            attempts=1,
            claimed_at=time.time() - 120,
            dispatch_started=False,
        )

        claimed = _shm_claims.claim(
            conn,
            "workers",
            10,
            "worker-2",
            reclaim_stale_seconds=60.0,
            max_claim_bytes=1_000_000,
            max_attempts=2,
        )

        assert [row["attempts"] for row in claimed] == [1]
        row = conn.execute("SELECT status, attempts, claimed_by FROM shm_delivery").fetchone()
        assert (row["status"], row["attempts"], row["claimed_by"]) == ("claimed", 1, "worker-2")
    finally:
        conn.close()


def test_reclaim_below_cap_bumps_attempts_and_redelivers(tmp_path: Path) -> None:
    conn = _open_claims_db(tmp_path / "claims.db")
    try:
        _seed_claimed_delivery(
            conn,
            group="workers",
            attempts=0,
            claimed_at=time.time() - 120,
            dispatch_started=True,
        )

        claimed = _shm_claims.claim(
            conn,
            "workers",
            10,
            "worker-2",
            reclaim_stale_seconds=60.0,
            max_claim_bytes=1_000_000,
            max_attempts=3,
        )

        assert len(claimed) == 1
        assert claimed[0]["attempts"] == 1
        row = conn.execute("SELECT status, attempts, claimed_by FROM shm_delivery").fetchone()
        assert (row["status"], row["attempts"], row["claimed_by"]) == ("claimed", 1, "worker-2")
    finally:
        conn.close()


async def test_broker_claim_batch_accepts_max_attempts_without_error(
    broker: ShmBroker,
) -> None:
    """PollingConsumer._run now always passes max_attempts=... to
    claim_batch(); ShmBroker.claim_batch must accept the keyword (protocol
    parity) or every ShmConsumer poll iteration raises TypeError."""
    await broker.subscribe(["events"], "workers")
    await broker.publish("events", b"payload")

    rows = await broker.claim_batch(
        "workers", batch_size=10, consumer_name="worker", max_attempts=5
    )

    assert [row["payload"] for row in rows] == [b"payload"]


async def test_public_claim_batch_dead_letters_after_max_attempts_stale_reclaims(
    broker: ShmBroker,
    tmp_path: Path,
) -> None:
    """The reclaim cap must reach the public ShmBroker.claim_batch surface,
    not just the lower-level _shm_claims.claim() it is built on -- a stale
    reclaim below the cap redelivers with attempts bumped, and the reclaim
    that meets the cap dead-letters instead of redelivering forever. Each
    claimant starts dispatching before it is abandoned, as a consumer that
    crashes inside its listener does."""
    await broker.subscribe(["events"], "workers")
    await broker.publish("events", b"payload")

    first = await broker.claim_batch("workers", batch_size=1, consumer_name="worker-1")
    assert len(first) == 1
    assert first[0]["attempts"] == 0
    await broker.renew_claims([first[0]["id"]], consumer_name="worker-1", start_dispatch=True)

    below_cap = await broker.claim_batch(
        "workers",
        batch_size=1,
        consumer_name="worker-2",
        reclaim_stale_seconds=0,
        max_attempts=2,
    )
    assert len(below_cap) == 1
    assert below_cap[0]["attempts"] == 1
    await broker.renew_claims([below_cap[0]["id"]], consumer_name="worker-2", start_dispatch=True)

    at_cap = await broker.claim_batch(
        "workers",
        batch_size=1,
        consumer_name="worker-3",
        reclaim_stale_seconds=0,
        max_attempts=2,
    )
    assert at_cap == [], "a reclaim meeting max_attempts must dead-letter, not redeliver"
    assert [tuple(row) for row in _delivery_rows(tmp_path / "broker.db")] == [("dead", 2, None, 2)]

    never_again = await broker.claim_batch(
        "workers",
        batch_size=1,
        consumer_name="worker-4",
        reclaim_stale_seconds=0,
        max_attempts=2,
    )
    assert never_again == []


async def test_drop_group_lets_prune_reclaim_a_retired_groups_publications(tmp_path: Path) -> None:
    db = tmp_path / "q.db"
    target = "shop.contracts.events.OrderPlaced"
    first = ShmBroker(shm_name=str(tmp_path / "hints"), db_path=str(db))
    await first.subscribe([target], "modulith-inventory")
    await first.subscribe([target], "modulith-notifications")
    await first.close()

    broker = ShmBroker(shm_name=str(tmp_path / "hints"), db_path=str(db))
    try:
        await broker.subscribe([target], "modulith-inventory")
        for _ in range(20):
            await broker.publish(target, b"x" * 64, {"event_type": target})
        while rows := await broker.claim_batch(
            "modulith-inventory", batch_size=100, consumer_name="modulith-inventory:w"
        ):
            for row in rows:
                await broker.ack(row["id"], consumer_name="modulith-inventory:w")
        claimed = await broker.claim_batch(
            "modulith-notifications", batch_size=5, consumer_name="modulith-notifications:w"
        )
        assert len(claimed) == 5

        assert await broker.group_backlog() == {
            "modulith-inventory": 0,
            "modulith-notifications": 20,
        }
        assert await broker.drop_group("modulith-notifications") == (1, 20)
        assert await broker.group_backlog() == {"modulith-inventory": 0}

        conn = sqlite3.connect(db)
        conn.execute("UPDATE shm_publication SET retained_until = 0")
        conn.commit()
        conn.close()
        assert await broker.prune(retention_age_seconds=0.0) == 20
        conn = sqlite3.connect(db)
        remaining = conn.execute("SELECT COUNT(*) FROM shm_publication").fetchone()[0]
        conn.close()
        assert remaining == 0
    finally:
        await broker.close()


async def test_drop_group_of_an_unknown_group_removes_nothing(broker: ShmBroker) -> None:
    await broker.subscribe(["t.A"], "modulith-orders")
    await broker.publish("t.A", b"x", {"event_type": "t.A"})

    assert await broker.drop_group("modulith-gone") == (0, 0)
    assert await broker.group_backlog() == {"modulith-orders": 1}
