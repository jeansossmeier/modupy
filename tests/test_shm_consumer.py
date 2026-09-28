"""Behavioral tests for the durable SHM polling consumer."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import struct
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from modulith import ConsumerSpec, configure, event
from modulith.adapters._shm_ring import _HEADER_SIZE, _SLOT_SIZE
from modulith.adapters.shm_broker import ShmBroker, ShmConsumer
from modulith.event_bus import InMemoryEventBus
from modulith.protocols import ConsumerHealth
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class ConsumerEvent:
    name: str


EVENT_TYPE = f"{ConsumerEvent.__module__}.{ConsumerEvent.__qualname__}"
TARGET = "orders-stream"
GROUP = "inventory"


@pytest.fixture()
async def broker(tmp_path: Path) -> AsyncIterator[ShmBroker]:
    instance = ShmBroker(
        shm_name="consumer-hints",
        capacity=16,
        db_path=str(tmp_path / "consumer.db"),
    )
    try:
        yield instance
    finally:
        await instance.close()
        instance._ring.unlink()


def _consumer(
    broker: ShmBroker,
    bus: InMemoryEventBus,
    serializer: JsonEventSerializer,
    **options: Any,
) -> ShmConsumer:
    poll_interval_s = options.pop("poll_interval_s", 0.01)
    return ShmConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="worker-1",
        group=GROUP,
        targets=[TARGET],
        poll_interval_s=poll_interval_s,
        **options,
    )


async def _until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.01)


def _delivery_state(broker: ShmBroker) -> list[tuple[str, int]]:
    connection = sqlite3.connect(broker._ring.path.with_name("consumer.db"))
    try:
        return [
            (str(status), int(attempts))
            for status, attempts in connection.execute(
                "SELECT status, attempts FROM shm_delivery ORDER BY id"
            )
        ]
    finally:
        connection.close()


class _RenewFlakyBroker(ShmBroker):
    fail_renewals = False

    async def renew_claims(
        self, row_ids: list[str], *, consumer_name: str, start_dispatch: bool = False
    ) -> int:
        if self.fail_renewals:
            raise RuntimeError("renew unavailable")
        return await super().renew_claims(
            row_ids, consumer_name=consumer_name, start_dispatch=start_dispatch
        )


class _CompletionFlakyBroker(ShmBroker):
    """Inject completion-write failures while retaining the real cold store."""

    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self.failures = {"ack", "fail", "dead_letter"}

    async def ack(self, row_id: str, *, consumer_name: str) -> None:
        if "ack" in self.failures:
            raise RuntimeError("ack unavailable")
        await super().ack(row_id, consumer_name=consumer_name)

    async def fail(
        self,
        row_id: str,
        error: str,
        *,
        consumer_name: str,
        max_attempts: int,
    ) -> None:
        if "fail" in self.failures:
            raise RuntimeError("fail unavailable")
        await super().fail(
            row_id,
            error,
            consumer_name=consumer_name,
            max_attempts=max_attempts,
        )

    async def dead_letter(
        self,
        row_id: str,
        error: str,
        *,
        consumer_name: str,
    ) -> None:
        if "dead_letter" in self.failures:
            raise RuntimeError("dead-letter unavailable")
        await super().dead_letter(row_id, error, consumer_name=consumer_name)


class _AckOnceBroker(ShmBroker):
    """Fail the first real ACK, then let stale-claim recovery complete it."""

    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self.ack_failed = asyncio.Event()
        self._remaining_ack_failures = 1

    async def ack(self, row_id: str, *, consumer_name: str) -> None:
        if self._remaining_ack_failures:
            self._remaining_ack_failures -= 1
            self.ack_failed.set()
            raise RuntimeError("ack temporarily unavailable")
        await super().ack(row_id, consumer_name=consumer_name)


# Windows' default timer ticks every ~15.6 ms, so a 1-2 ms heartbeat sleep can
# overshoot a 30-60 ms lease budget in one tick and the loop exits with zero
# renewals; 30 ms keeps every tick and the 10x budget above that granularity.
_RENEW_WINDOW_S = 0.03


class _RenewLoopBroker(ShmBroker):
    """Control renewal outcomes without replacing the real broker interface."""

    def __init__(
        self,
        *,
        fail_once: bool = False,
        renewed_count: int | None = None,
        block: bool = False,
        **options: Any,
    ) -> None:
        super().__init__(**options)
        self.fail_once = fail_once
        self.renewed_count = renewed_count
        self.block = block
        self.renew_calls = 0
        self.renew_started = asyncio.Event()

    async def renew_claims(
        self, row_ids: list[str], *, consumer_name: str, start_dispatch: bool = False
    ) -> int:
        self.renew_calls += 1
        self.renew_started.set()
        if self.block:
            await asyncio.Event().wait()
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("renew temporarily unavailable")
        if self.renewed_count is not None:
            return self.renewed_count
        return len(row_ids)


class _PublishAfterEmptyClaimBroker(ShmBroker):
    """Inject durable publishes into the empty-claim race window."""

    def __init__(
        self,
        *,
        publications: list[tuple[str, bytes, dict[str, str]]],
        corrupt_hint: bool = False,
        **options: Any,
    ) -> None:
        super().__init__(**options)
        self._publications = publications
        self._corrupt_hint = corrupt_hint
        self.corruption_applied = False
        self.published_at: float | None = None

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float = 60.0,
        max_attempts: int | None = None,
        targets: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        rows = await super().claim_batch(
            group,
            batch_size=batch_size,
            consumer_name=consumer_name,
            reclaim_stale_seconds=reclaim_stale_seconds,
            max_attempts=max_attempts,
            targets=targets,
        )
        if rows or not self._publications:
            return rows

        publications, self._publications = self._publications, []
        for target, payload, headers in publications:
            await self.publish(target, payload, headers)
        self.published_at = asyncio.get_running_loop().time()
        if self._corrupt_hint:
            sequence = self._ring.read_hints()[-1]
            offset = _HEADER_SIZE + (sequence % self._ring.capacity) * _SLOT_SIZE
            with self._ring.path.open("r+b") as file_handle:
                file_handle.seek(offset + 8)
                file_handle.write(struct.pack("<Q", 0))
            self.corruption_applied = True
        return rows


async def _close_test_broker(instance: ShmBroker) -> None:
    await instance.close()
    instance._ring.unlink()


def _publication(serializer: JsonEventSerializer, name: str) -> tuple[str, bytes, dict[str, str]]:
    return (
        TARGET,
        serializer.serialize(ConsumerEvent(name)),
        {"event_type": EVENT_TYPE},
    )


async def test_hint_wakes_consumer_before_safety_poll_timeout(tmp_path: Path) -> None:
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    instance = _PublishAfterEmptyClaimBroker(
        publications=[_publication(serializer, "hinted")],
        shm_name="early-hint",
        db_path=str(tmp_path / "early-hint.db"),
    )
    delivered: list[str] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item.name)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    consumer = _consumer(instance, bus, serializer, poll_interval_s=1.0)
    try:
        await consumer.start()
        await _until(lambda: delivered == ["hinted"], timeout=0.5)
        assert instance.published_at is not None
        assert asyncio.get_running_loop().time() - instance.published_at < 0.5
    finally:
        await consumer.stop()
        await _close_test_broker(instance)


@pytest.mark.parametrize("hint_state", ["missing", "corrupt"])
async def test_unusable_hint_falls_back_to_safety_poll(
    tmp_path: Path,
    hint_state: str,
) -> None:
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    instance = _PublishAfterEmptyClaimBroker(
        publications=[_publication(serializer, hint_state)],
        corrupt_hint=hint_state == "corrupt",
        shm_name=f"{hint_state}-hint",
        db_path=str(tmp_path / f"{hint_state}-hint.db"),
        create=hint_state != "missing",
    )
    delivered: list[str] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item.name)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    consumer = _consumer(instance, bus, serializer, poll_interval_s=0.05)
    try:
        await consumer.start()
        await _until(lambda: delivered == [hint_state], timeout=0.5)
        assert instance.published_at is not None
        assert asyncio.get_running_loop().time() - instance.published_at >= 0.04
        assert instance.corruption_applied is (hint_state == "corrupt")
        if hint_state == "corrupt":
            assert instance._ring.read_hints() == []
    finally:
        await consumer.stop()
        await _close_test_broker(instance)


async def test_shm_idle_backoff_grows_and_caps(
    broker: ShmBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ObservedIdleConsumer(ShmConsumer):
        def __init__(self, **options: Any) -> None:
            super().__init__(**options)
            self.delays: list[float] = []

        async def _wait_when_idle(self, safety_timeout: float) -> None:
            self.delays.append(safety_timeout)
            if len(self.delays) == 8:
                self._stopping = True

    monkeypatch.setattr(
        "modulith.adapters._polling_consumer.random.uniform",
        lambda _start, _end: 0.0,
    )
    consumer = ObservedIdleConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="worker-1",
        group=GROUP,
        targets=[TARGET],
        poll_interval_s=0.02,
    )

    await consumer._run()

    assert consumer.delays == pytest.approx([0.02, 0.04, 0.08, 0.16, 0.32, 0.5, 0.5, 0.5])


async def test_shm_idle_backoff_resets_after_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ObservedIdleConsumer(ShmConsumer):
        def __init__(self, **options: Any) -> None:
            super().__init__(**options)
            self.delays: list[float] = []

        async def _wait_when_idle(self, safety_timeout: float) -> None:
            self.delays.append(safety_timeout)
            if len(self.delays) == 2:
                self._stopping = True

    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    instance = _PublishAfterEmptyClaimBroker(
        publications=[_publication(serializer, "reset")],
        shm_name="reset-backoff-hints",
        db_path=str(tmp_path / "reset-backoff.db"),
    )
    delivered: list[str] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item.name)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    monkeypatch.setattr(
        "modulith.adapters._polling_consumer.random.uniform",
        lambda _start, _end: 0.0,
    )
    consumer = ObservedIdleConsumer(
        broker=instance,
        bus=bus,
        serializer=serializer,
        consumer_name="worker-1",
        group=GROUP,
        targets=[TARGET],
        poll_interval_s=0.02,
    )
    try:
        await instance.subscribe([TARGET], GROUP)
        await asyncio.wait_for(consumer._run(), timeout=30.0)

        assert delivered == ["reset"]
        assert consumer.delays == [0.02, 0.02]
    finally:
        await _close_test_broker(instance)


async def test_wrapped_hints_do_not_lose_durable_deliveries(tmp_path: Path) -> None:
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    names = ["one", "two", "three"]
    instance = _PublishAfterEmptyClaimBroker(
        publications=[_publication(serializer, name) for name in names],
        shm_name="wrapped-hints",
        capacity=2,
        db_path=str(tmp_path / "wrapped-hints.db"),
    )
    delivered: list[str] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item.name)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    consumer = _consumer(instance, bus, serializer, poll_interval_s=1.0)
    try:
        await consumer.start()
        await _until(lambda: sorted(delivered) == sorted(names), timeout=0.5)
        assert len(instance._ring.read_hints()) == 2
    finally:
        await consumer.stop()
        await _close_test_broker(instance)


async def test_delivers_real_event_when_target_differs_from_event_type(
    broker: ShmBroker,
) -> None:
    delivered: list[ConsumerEvent] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer)

    await consumer.start()
    try:
        await broker.publish(
            TARGET,
            serializer.serialize(ConsumerEvent("one")),
            {"event_type": EVENT_TYPE},
        )
        await _until(lambda: delivered == [ConsumerEvent("one")])
        await _until(lambda: _delivery_state(broker) == [])
    finally:
        await consumer.stop()


async def test_dispatch_batch_bounds_malformed_id_logs_and_delivers_valid_row(
    broker: ShmBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    delivered: list[ConsumerEvent] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer)
    await broker.subscribe([TARGET], GROUP)
    await broker.publish(
        TARGET,
        serializer.serialize(ConsumerEvent("valid")),
        {"event_type": EVENT_TYPE},
    )
    valid_row = (await broker.claim_batch(GROUP, batch_size=1, consumer_name="worker-1"))[0]
    malformed_rows: list[dict[str, Any]] = [{}, {"id": None}, {"id": 7}, {"id": ""}] * 8

    with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
        await consumer._dispatch_batch([*malformed_rows, valid_row])

    malformed_logs = [
        record
        for record in caplog.records
        if "claimed row has no valid string id" in record.getMessage()
    ]
    assert delivered == [ConsumerEvent("valid")]
    assert _delivery_state(broker) == []
    assert 1 <= len(malformed_logs) < len(malformed_rows)


async def test_listener_failure_retries_then_dead_letters(broker: ShmBroker) -> None:
    calls = 0

    async def fail(_item: ConsumerEvent) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("listener failed")

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, fail)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer, max_attempts=2)

    await consumer.start()
    try:
        await broker.publish(
            TARGET,
            serializer.serialize(ConsumerEvent("retry")),
            {"event_type": EVENT_TYPE},
        )
        await _until(lambda: _delivery_state(broker) == [("dead", 2)])
        assert calls == 2
    finally:
        await consumer.stop()


async def test_listener_cancelled_error_is_a_delivery_failure(broker: ShmBroker) -> None:
    async def cancel(_item: ConsumerEvent) -> None:
        raise asyncio.CancelledError

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, cancel)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer, max_attempts=1)
    await broker.subscribe([TARGET], GROUP)
    await broker.publish(
        TARGET,
        serializer.serialize(ConsumerEvent("cancel")),
        {"event_type": EVENT_TYPE},
    )
    row = (await broker.claim_batch(GROUP, batch_size=1, consumer_name="worker-1"))[0]

    await consumer._dispatch_one(row)

    assert _delivery_state(broker) == [("dead", 1)]


async def test_stopping_consumer_skips_claim_without_completing_it(broker: ShmBroker) -> None:
    consumer = _consumer(broker, InMemoryEventBus(), JsonEventSerializer())
    await broker.subscribe([TARGET], GROUP)
    await broker.publish(TARGET, b"payload", {"event_type": EVENT_TYPE})
    row = (await broker.claim_batch(GROUP, batch_size=1, consumer_name="worker-1"))[0]
    in_flight = {row["id"]}
    consumer._stopping = True

    await consumer._dispatch_guarded(row, asyncio.Semaphore(1), in_flight)

    assert in_flight == set()
    assert await broker.renew_claims([row["id"]], consumer_name="worker-1") == 1


async def test_cancelling_guarded_renewal_propagates_and_releases_in_flight(
    tmp_path: Path,
) -> None:
    instance = _RenewLoopBroker(
        block=True,
        shm_name="cancel-renew-hints",
        db_path=str(tmp_path / "cancel-renew.db"),
    )
    consumer = _consumer(
        instance,
        InMemoryEventBus(),
        JsonEventSerializer(),
        reclaim_stale_seconds=_RENEW_WINDOW_S,
    )
    in_flight = {"row-1"}
    task = asyncio.create_task(
        consumer._dispatch_guarded(
            {"id": "row-1", "target": TARGET},
            asyncio.Semaphore(1),
            in_flight,
        )
    )
    renew_loop: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(instance.renew_started.wait(), timeout=1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert in_flight == set()

        renew_loop = asyncio.create_task(consumer._renew_loop({"row-2"}))
        await _until(lambda: instance.renew_calls >= 2, timeout=1.0)
        renew_loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await renew_loop
    finally:
        for pending in (task, renew_loop):
            if pending is not None and not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
        await _close_test_broker(instance)


async def test_renew_loop_retries_exception_warns_on_partial_and_stops_when_empty(
    tmp_path: Path,
) -> None:
    instance = _RenewLoopBroker(
        fail_once=True,
        renewed_count=0,
        shm_name="retry-renew-hints",
        db_path=str(tmp_path / "retry-renew.db"),
    )
    consumer = _consumer(
        instance,
        InMemoryEventBus(),
        JsonEventSerializer(),
        reclaim_stale_seconds=_RENEW_WINDOW_S,
    )
    in_flight = {"row-1"}
    task = asyncio.create_task(consumer._renew_loop(in_flight))
    try:
        await _until(lambda: instance.renew_calls >= 2, timeout=1.0)
        in_flight.clear()
        await asyncio.wait_for(task, timeout=1.0)
        assert instance.renew_calls >= 2
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await _close_test_broker(instance)


async def test_renew_loop_stops_extending_a_claim_at_heartbeat_deadline(
    tmp_path: Path,
) -> None:
    instance = _RenewLoopBroker(
        shm_name="deadline-renew-hints",
        db_path=str(tmp_path / "deadline-renew.db"),
    )
    consumer = _consumer(
        instance,
        InMemoryEventBus(),
        JsonEventSerializer(),
        reclaim_stale_seconds=_RENEW_WINDOW_S,
    )
    in_flight = {"row-1"}
    try:
        await asyncio.wait_for(consumer._renew_loop(in_flight), timeout=1.0)
        assert in_flight == {"row-1"}
        assert instance.renew_calls > 0
    finally:
        await _close_test_broker(instance)


async def test_row_reclaimed_while_waiting_on_semaphore_is_not_dispatched(
    broker: ShmBroker,
) -> None:
    delivered: list[str] = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item.name)
        if item.name == "first":
            first_started.set()
            await release_first.wait()

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer, dispatch_concurrency=1)
    await broker.subscribe([TARGET], GROUP)
    for name in ("first", "second"):
        await broker.publish(
            TARGET,
            serializer.serialize(ConsumerEvent(name)),
            {"event_type": EVENT_TYPE},
        )
    rows = await broker.claim_batch(GROUP, batch_size=2, consumer_name="worker-1")

    dispatch = asyncio.create_task(consumer._dispatch_batch(rows))
    try:
        await asyncio.wait_for(first_started.wait(), timeout=1.0)
        reclaimed = await broker.claim_batch(
            GROUP,
            batch_size=2,
            consumer_name="worker-2",
            reclaim_stale_seconds=0,
        )
        assert len(reclaimed) == 2
        release_first.set()
        await asyncio.wait_for(dispatch, timeout=1.0)
    finally:
        release_first.set()
        if not dispatch.done():
            dispatch.cancel()
            await asyncio.gather(dispatch, return_exceptions=True)

    assert delivered == ["first"]


async def test_heartbeat_prevents_stale_reclaim_during_dispatch(broker: ShmBroker) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(_item: ConsumerEvent) -> None:
        started.set()
        await release.wait()

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, slow)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer, reclaim_stale_seconds=0.15)

    await consumer.start()
    try:
        await broker.publish(
            TARGET,
            serializer.serialize(ConsumerEvent("slow")),
            {"event_type": EVENT_TYPE},
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await asyncio.sleep(0.3)
        assert (
            await broker.claim_batch(
                GROUP,
                batch_size=1,
                consumer_name="worker-2",
                reclaim_stale_seconds=0.15,
            )
            == []
        )
    finally:
        release.set()
        await consumer.stop()


async def test_wedging_row_does_not_dead_letter_the_rows_claimed_behind_it(
    broker: ShmBroker,
) -> None:
    """A consumer restarted repeatedly while one listener wedges must spend
    attempts only on that row. The rows queued behind it in the same batch
    never reached their listener, so they must be delivered, not
    dead-lettered alongside it."""
    max_attempts = 3
    wedged = asyncio.Event()
    delivered: list[str] = []

    async def handle(item: ConsumerEvent) -> None:
        if item.name == "poison":
            wedged.set()
            await asyncio.Event().wait()
        delivered.append(item.name)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    await broker.subscribe([TARGET], GROUP)
    benign = [f"b{index}" for index in range(4)]
    for name in ["poison", *benign]:
        await broker.publish(
            TARGET, serializer.serialize(ConsumerEvent(name)), {"event_type": EVENT_TYPE}
        )

    def incarnation() -> ShmConsumer:
        return _consumer(
            broker,
            bus,
            serializer,
            batch_size=10,
            dispatch_concurrency=1,
            max_attempts=max_attempts,
            reclaim_stale_seconds=0.1,
        )

    for _ in range(max_attempts):
        wedged.clear()
        consumer = incarnation()
        await consumer.start()
        try:
            await asyncio.wait_for(wedged.wait(), timeout=5.0)
        finally:
            await consumer.stop()
        await asyncio.sleep(0.15)

    consumer = incarnation()
    await consumer.start()
    try:
        await _until(lambda: _delivery_state(broker) == [("dead", max_attempts)])
    except AssertionError:
        pass
    finally:
        await consumer.stop()

    assert sorted(delivered) == benign
    assert _delivery_state(broker) == [("dead", max_attempts)]


async def test_renew_failure_degrades_health_until_renewal_recovers(tmp_path: Path) -> None:
    instance = _RenewFlakyBroker(
        shm_name="renew-health-hints",
        db_path=str(tmp_path / "renew-health.db"),
    )
    delivered: list[ConsumerEvent] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item)

    async def block_poll_loop() -> None:
        await asyncio.Event().wait()

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(instance, bus, serializer)
    consumer._run = block_poll_loop  # type: ignore[method-assign]
    try:
        await consumer.start()
        await instance.publish(
            TARGET,
            serializer.serialize(ConsumerEvent("renew")),
            {"event_type": EVENT_TYPE},
        )
        row = (await instance.claim_batch(GROUP, batch_size=1, consumer_name="worker-1"))[0]

        instance.fail_renewals = True
        await consumer._dispatch_batch([row])
        assert consumer.health().status == "degraded"
        assert consumer.health().detail == "renew unavailable"
        assert delivered == []

        instance.fail_renewals = False
        await consumer._dispatch_batch([row])
        assert delivered == [ConsumerEvent("renew")]
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()
        await instance.close()
        instance._ring.unlink()


async def test_background_renewal_failure_degrades_health_until_recovery(
    tmp_path: Path,
) -> None:
    instance = _RenewFlakyBroker(
        shm_name="background-renew-health-hints",
        db_path=str(tmp_path / "background-renew-health.db"),
    )

    async def block_poll_loop() -> None:
        await asyncio.Event().wait()

    consumer = _consumer(
        instance,
        InMemoryEventBus(),
        JsonEventSerializer(),
        reclaim_stale_seconds=0.03,
    )
    consumer._run = block_poll_loop  # type: ignore[method-assign]
    renew_task: asyncio.Task[None] | None = None
    try:
        await consumer.start()
        await instance.publish(TARGET, b"{}", {"event_type": EVENT_TYPE})
        row = (await instance.claim_batch(GROUP, batch_size=1, consumer_name="worker-1"))[0]
        in_flight = {row["id"]}

        instance.fail_renewals = True
        renew_task = asyncio.create_task(consumer._renew_loop(in_flight))
        await _until(lambda: consumer.health().status == "degraded", timeout=1.0)
        assert consumer.health().detail == "renew unavailable"

        instance.fail_renewals = False
        await _until(lambda: consumer.health().status == "ready", timeout=1.0)
        in_flight.clear()
        await asyncio.wait_for(renew_task, timeout=1.0)
    finally:
        if renew_task is not None and not renew_task.done():
            renew_task.cancel()
            await asyncio.gather(renew_task, return_exceptions=True)
        await consumer.stop()
        await instance.close()
        instance._ring.unlink()


async def test_completion_failures_and_poison_rows_recover_health(
    tmp_path: Path,
) -> None:
    instance = _CompletionFlakyBroker(
        shm_name="completion-health-hints",
        db_path=str(tmp_path / "consumer.db"),
    )
    listener_fails = False

    async def handle(_item: ConsumerEvent) -> None:
        if listener_fails:
            raise RuntimeError("listener unavailable")

    async def block_poll_loop() -> None:
        await asyncio.Event().wait()

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(instance, bus, serializer, max_attempts=1)
    consumer._run = block_poll_loop  # type: ignore[method-assign]
    try:
        await consumer.start()
        for name in ("ack", "fail", "poison", "malformed"):
            payload = (
                b"not-json" if name == "malformed" else serializer.serialize(ConsumerEvent(name))
            )
            await instance.publish(TARGET, payload, {"event_type": EVENT_TYPE})
        rows = await instance.claim_batch(GROUP, batch_size=4, consumer_name="worker-1")
        ack_row, fail_row, poison_row, malformed_row = rows
        poison_row = {**poison_row, "event_type": None}

        await consumer._dispatch_one(ack_row)
        listener_fails = True
        with pytest.raises(RuntimeError, match="fail unavailable"):
            await consumer._dispatch_one(fail_row)
        with pytest.raises(RuntimeError, match="dead-letter unavailable"):
            await consumer._dispatch_one(poison_row)
        assert consumer.health().status == "degraded"

        instance.failures.remove("ack")
        listener_fails = False
        await consumer._dispatch_one(ack_row)
        assert consumer.health().status == "degraded"

        instance.failures.remove("fail")
        listener_fails = True
        await consumer._dispatch_one(fail_row)
        assert consumer.health().status == "degraded"

        instance.failures.remove("dead_letter")
        await consumer._dispatch_one(poison_row)
        await consumer._dispatch_one(malformed_row)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
        assert sorted(_delivery_state(instance)) == [
            ("dead", 0),
            ("dead", 0),
            ("dead", 1),
        ]
    finally:
        await consumer.stop()
        await instance.close()
        instance._ring.unlink()


async def test_completion_failure_clears_after_reclaim_window(tmp_path: Path) -> None:
    """A failed fail() write stops degrading health once its row is reclaimable.

    A write that still fails on the retry records a fresh failure.
    """
    instance = _CompletionFlakyBroker(
        shm_name="completion-expiry-hints",
        db_path=str(tmp_path / "consumer.db"),
    )
    instance.failures = {"fail"}

    async def handle(_item: ConsumerEvent) -> None:
        raise RuntimeError("listener unavailable")

    async def block_poll_loop() -> None:
        await asyncio.Event().wait()

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(instance, bus, serializer, reclaim_stale_seconds=0.3)
    consumer._run = block_poll_loop  # type: ignore[method-assign]
    try:
        await consumer.start()
        await instance.publish(
            TARGET, serializer.serialize(ConsumerEvent("x")), {"event_type": EVENT_TYPE}
        )
        (row,) = await instance.claim_batch(GROUP, batch_size=1, consumer_name="worker-1")

        with pytest.raises(RuntimeError, match="fail unavailable"):
            await consumer._dispatch_one(row)
        assert consumer.health() == ConsumerHealth(
            ready=False, status="degraded", detail="fail unavailable"
        )

        await asyncio.sleep(0.35)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")

        with pytest.raises(RuntimeError, match="fail unavailable"):
            await consumer._dispatch_one(row)
        assert consumer.health().status == "degraded"
    finally:
        await consumer.stop()
        await instance.close()
        instance._ring.unlink()


async def test_real_poll_loop_reclaims_after_one_shot_ack_failure(
    tmp_path: Path,
) -> None:
    instance = _AckOnceBroker(
        shm_name=str(tmp_path / "ack-recovery.hints"),
        db_path=str(tmp_path / "consumer.db"),
        completion_mode="mark",
    )
    delivered: list[ConsumerEvent] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(
        instance,
        bus,
        serializer,
        poll_interval_s=0.01,
        reclaim_stale_seconds=0.2,
    )
    event_value = ConsumerEvent("ack-recovery")
    try:
        await consumer.start()
        await instance.publish(
            TARGET,
            serializer.serialize(event_value),
            {"event_type": EVENT_TYPE},
        )

        await asyncio.wait_for(instance.ack_failed.wait(), timeout=1.0)
        await _until(lambda: consumer.health().status == "degraded", timeout=1.0)
        assert _delivery_state(instance) == [("claimed", 0)]
        assert delivered == [event_value]

        # The running poll loop, not a direct dispatch call, reclaims the stale row.
        # The poll loop always passes its configured max_attempts to claim_batch,
        # and that cap now reaches the SQLite claim layer (previously discarded),
        # so this reclaim is accounted as a real attempt: attempts goes to 1.
        await _until(lambda: delivered == [event_value, event_value], timeout=2.0)
        await _until(lambda: _delivery_state(instance) == [("done", 1)], timeout=1.0)
        await _until(lambda: consumer.health() == ConsumerHealth(True, "ready"), timeout=1.0)
    finally:
        await consumer.stop()
        await _close_test_broker(instance)


async def test_prune_enablement_respects_interval_and_retention(broker: ShmBroker) -> None:
    disabled = _consumer(
        broker,
        InMemoryEventBus(),
        JsonEventSerializer(),
        prune_interval_s=0,
        retention_age_seconds=60,
    )
    enabled = _consumer(
        broker,
        InMemoryEventBus(),
        JsonEventSerializer(),
        prune_interval_s=30,
        retention_age_seconds=60,
    )

    assert not disabled._prune_enabled()
    assert enabled._prune_enabled()


async def test_empty_shm_consumer_reconciles_without_background_tasks(
    broker: ShmBroker,
) -> None:
    await broker.subscribe([TARGET], GROUP)
    consumer = ShmConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="worker-without-listeners",
        group=GROUP,
        targets=[],
        prune_interval_s=30,
        retention_age_seconds=60,
    )

    await consumer.start()
    try:
        assert await broker._cold.get_subscriptions() == {}
        assert consumer._task is None
        assert consumer._prune_task is None
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


async def test_stop_before_start_and_repeated_stop_are_safe(broker: ShmBroker) -> None:
    consumer = _consumer(broker, InMemoryEventBus(), JsonEventSerializer())

    await consumer.stop()
    await consumer.start()
    await consumer.stop()
    await consumer.stop()

    assert consumer._task is None
    assert consumer._prune_task is None
    assert consumer.health() == ConsumerHealth(ready=False, status="stopped")


async def test_health_reports_unexpected_poll_loop_death(broker: ShmBroker) -> None:
    consumer = _consumer(
        broker,
        InMemoryEventBus(),
        JsonEventSerializer(),
        prune_interval_s=60,
        retention_age_seconds=3600,
    )

    async def crash() -> None:
        raise RuntimeError("shm poll loop crashed")

    consumer._run = crash  # type: ignore[method-assign]
    try:
        await consumer.start()
        await _until(lambda: consumer.health().status == "failed")

        health = consumer.health()
        assert health.ready is False
        assert health.detail == "shm poll loop crashed"
        failed_task = consumer._task
        stale_prune_task = consumer._prune_task
        assert failed_task is not None and failed_task.done()
        assert stale_prune_task is not None and not stale_prune_task.done()

        async def run_after_restart() -> None:
            await asyncio.Event().wait()

        consumer._run = run_after_restart  # type: ignore[method-assign]
        await consumer.start()
        await asyncio.sleep(0)

        assert consumer._task is not failed_task
        assert consumer._task is not None and not consumer._task.done()
        assert stale_prune_task.done()
        assert consumer._prune_task is not stale_prune_task
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


async def test_runtime_registry_builds_working_shm_consumer(
    make_fake_app: Callable[..., str],
    tmp_path: Path,
) -> None:
    make_fake_app({"orders": ""})
    configure(
        package="fakeapp",
        topology="processes",
        broker="shm",
        broker_options={
            "url": str(tmp_path / "runtime-consumer.db"),
            "shm_name": str(tmp_path / "runtime-consumer.hints"),
            "poll_interval_ms": "25",
            "batch_size": "7",
            "dispatch_concurrency": "2",
            "reclaim_stale_seconds": "3",
            "max_delivery_attempts": "4",
            "retention_age_seconds": "120",
            "prune_interval_seconds": "0",
        },
    )
    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None

    runtime_broker = cast(ShmBroker, _runtime.broker_registry.get("shm"))
    delivered: list[ConsumerEvent] = []

    async def handle(item: ConsumerEvent) -> None:
        delivered.append(item)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handle)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _runtime.consumer_registry.build(
        "shm",
        ConsumerSpec(
            scheme="shm",
            module_name="inventory",
            group=GROUP,
            consumer_name="worker-1",
            targets=(TARGET,),
            bus=bus,
            serializer=serializer,
            broker_registry=_runtime.broker_registry,
        ),
    )
    assert isinstance(consumer, ShmConsumer)
    assert consumer._broker is runtime_broker
    assert consumer._poll_interval_s == 0.025
    assert consumer._batch_size == 7
    assert consumer._dispatch_concurrency == 2
    assert consumer._reclaim_stale_seconds == 3
    assert consumer._max_attempts == 4
    assert consumer._retention_age_seconds == 120
    assert consumer._prune_interval_s == 0

    await consumer.start()
    try:
        await runtime_broker.publish(
            TARGET,
            serializer.serialize(ConsumerEvent("runtime")),
            {"event_type": EVENT_TYPE},
        )
        await _until(lambda: delivered == [ConsumerEvent("runtime")])
    finally:
        await consumer.stop()
        await runtime_broker.close()
        runtime_broker._ring.unlink()


def _stale_warnings(caplog: pytest.LogCaptureFixture, target: str) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "drop-group" in r.getMessage()
        and target in r.getMessage()
    ]


async def test_shm_consumer_warns_once_about_deliveries_for_a_target_it_no_longer_consumes(
    broker: ShmBroker, caplog: pytest.LogCaptureFixture
) -> None:
    await broker.subscribe([TARGET, "stale-stream"], GROUP)
    for _ in range(2):
        await broker.publish("stale-stream", b"{}", {"event_type": "t.Stale"})
    delivered: list[ConsumerEvent] = []

    async def handler(evt: ConsumerEvent) -> None:
        delivered.append(evt)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handler)
    serializer = JsonEventSerializer(allowed_event_types=[ConsumerEvent])
    consumer = _consumer(broker, bus, serializer)
    caplog.set_level(logging.WARNING)
    await consumer.start()
    try:
        for name in ("a", "b", "c"):
            await broker.publish(
                TARGET, serializer.serialize(ConsumerEvent(name)), {"event_type": EVENT_TYPE}
            )
        await _until(lambda: len(delivered) == 3)
    finally:
        await consumer.stop()

    warnings = _stale_warnings(caplog, "stale-stream")
    assert len(warnings) == 1
    assert "2 undelivered" in warnings[0]
    assert f"modulith broker drop-group {GROUP} --target stale-stream" in warnings[0]
    assert _stale_warnings(caplog, TARGET) == []


async def test_empty_shm_consumer_warns_about_deliveries_its_group_still_holds(
    broker: ShmBroker, caplog: pytest.LogCaptureFixture
) -> None:
    await broker.subscribe(["stale-stream"], GROUP)
    await broker.publish("stale-stream", b"{}", {"event_type": "t.Stale"})
    consumer = ShmConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="worker-1",
        group=GROUP,
        targets=[],
        poll_interval_s=0.01,
    )
    caplog.set_level(logging.WARNING)
    await consumer.start()
    await consumer.stop()

    warnings = _stale_warnings(caplog, "stale-stream")
    assert len(warnings) == 1
    assert "1 undelivered" in warnings[0]
