"""Integration tests for the SQLite-authoritative SHM broker."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest

from modulith import ConfigurationError, configure
from modulith.adapters._shm_ring import _HEADER_SIZE, _SLOT_SIZE
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
