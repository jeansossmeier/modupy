"""Cross-process broker CONSUMER loop — the half that was entirely missing.

The producer (``runtime._maybe_route_to_broker``) serializes a cross-module
event and XADDs it to the broker; before this, NOTHING consumed those streams,
so every cross-process event in ``topology='processes'`` was silently dropped
(the CRITICAL audit finding).

These tests drive the consumer half end-to-end:
  * a full producer→consumer round-trip WITHOUT Redis — the exact payload +
    event_type header the runtime producer emits is fed straight into the
    consumer, which must deserialize and dispatch it to a local listener;
  * dispatch semantics: ack on success, dead-letter poison/undeliverable;
  * a real-Redis integration test (via the redis_url/redis_client fixtures).
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError, dataclass
from typing import Any

import pytest

import modulith.manifest as manifest_module
from modulith import Configuration, ConfigurationError, Manifest, configure, event, hookimpl
from modulith._consumer import BrokerConsumer, consumer_targets
from modulith.event_bus import InMemoryEventBus
from modulith.protocols import Consumer, ConsumerHealth, HealthAwareConsumer
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

# Module scope so the serializer resolves the fully-qualified class name on the
# round trip (consumer deserializes by the event_type header).


@event
@dataclass(frozen=True)
class CrossEvent:
    value: int


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class RecordingBroker:
    """Records publish() calls — the producer-side fake (cf. test_cross_process)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str] | None]] = []

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.published.append((target, payload, headers))

    async def close(self) -> None:  # pragma: no cover - registry contract
        pass


class FakeConsumerBroker:
    """In-memory stand-in for the broker's consumer surface (no Redis).

    Holds queued messages per stream; ``read`` hands out (and clears) up to
    ``count`` new messages per call — real XREADGROUP caps delivery at COUNT —
    and rejects ``block_ms <= 0`` loudly (real Redis BLOCK 0 blocks forever;
    BrokerConsumer clamps to >=1ms); ``ack``/``dead_letter`` are recorded;
    ``reclaim`` serves entries staged in ``pending``, honoring ``min_idle_ms``
    like real XAUTOCLAIM and — mirroring ``RedisStreamsBroker.reclaim`` —
    draining the FULL idle backlog per call (the real adapter follows the
    XAUTOCLAIM cursor until ``0-0``; ``count`` is its internal page size, not
    a result cap). A staged pending entry defaults to idle-forever (a crashed
    peer's message); set ``pending_idle_ms`` per (target, mid) to model a
    freshly-delivered in-flight message. A staged ``None`` entry models
    Redis < 7.0 XAUTOCLAIM returning nil for a pending entry deleted from the
    stream. Ids staged in ``lost`` are returned once via
    XAUTOCLAIM's third (deleted) element — the trimmed-while-pending loss
    channel.
    """

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[bytes, bytes]]]] = {}
        self.pending: dict[str, list[tuple[str, dict[bytes, bytes]] | None]] = {}
        # (target, mid) -> idle ms; absent = idle forever (crashed peer).
        self.pending_idle_ms: dict[tuple[str, str], float] = {}
        # target -> ids to report via reclaim's deleted element (once).
        self.lost: dict[str, list[str]] = {}
        self.groups: list[tuple[str, str]] = []
        self.acked: list[tuple[str, str]] = []
        self.dead: list[tuple[str, str, dict[bytes, bytes]]] = []
        self._counter = 0

    def deliver(self, target: str, data: bytes, headers: dict[str, str]) -> str:
        """Stage a message exactly as the producer would have XADD'd it."""
        self._counter += 1
        mid = f"{self._counter}-0"
        fields: dict[bytes, bytes] = {b"data": data}
        for key, value in headers.items():
            fields[f"h:{key}".encode()] = value.encode()
        self.streams.setdefault(target, []).append((mid, fields))
        return mid

    async def ensure_group(self, target: str, group: str | None = None) -> None:
        self.groups.append((target, group or ""))

    async def read(
        self,
        target: str,
        *,
        consumer: str,
        group: str | None = None,
        count: int = 10,
        block_ms: int = 1000,
    ) -> Any:
        if block_ms <= 0:
            raise NotImplementedError(
                "XREADGROUP BLOCK 0 blocks forever on real Redis, so "
                "BrokerConsumer clamps poll_block_ms to >=1; the fake "
                "rejects a non-positive block loudly instead of modeling it."
            )
        queued = self.streams.get(target, [])
        if not queued:
            await asyncio.sleep(block_ms / 1000)  # mimic XREADGROUP BLOCK so the loop yields
            return []
        # Real XREADGROUP delivers at most COUNT entries per call — the
        # remainder stays queued for the next read.
        delivered, self.streams[target] = queued[:count], queued[count:]
        return [(target, delivered)]

    async def ack(self, target: str, message_id: str, group: str | None = None) -> None:
        self.acked.append((target, message_id))

    async def reclaim(
        self,
        target: str,
        *,
        consumer: str,
        group: str | None = None,
        min_idle_ms: int,
        count: int = 100,
    ) -> Any:
        # Real XAUTOCLAIM only claims entries idle >= min_idle_time. The real
        # adapter pages at COUNT but follows the cursor until 0-0 — one
        # reclaim() call drains the FULL idle backlog, so the fake ignores
        # ``count`` as a result cap. Staged ``None`` entries (Redis < 7.0 nil
        # rows) are handed back once, like real nil claim results.
        claimed: list[tuple[str, dict[bytes, bytes]] | None] = []
        kept: list[tuple[str, dict[bytes, bytes]] | None] = []
        for entry in self.pending.get(target, []):
            if entry is None:
                claimed.append(None)
                continue
            mid, fields = entry
            idle = self.pending_idle_ms.get((target, mid), float("inf"))
            if idle >= min_idle_ms:
                claimed.append((mid, fields))
            else:
                kept.append((mid, fields))
        if kept:
            self.pending[target] = kept
        else:
            self.pending.pop(target, None)
        return (b"0-0", claimed, self.lost.pop(target, []))

    async def dead_letter(
        self, target: str, message_id: str, fields: dict[bytes, bytes], group: str | None = None
    ) -> None:
        self.dead.append((target, message_id, fields))


async def _until(predicate, *, timeout: float = 1.0, interval: float = 0.01) -> None:
    """Poll until predicate() is truthy or timeout — keeps loop tests bounded."""
    waited = 0.0
    while waited < timeout:
        if predicate():
            return
        await asyncio.sleep(interval)
        waited += interval


def _make_consumer(
    broker: Any,
    bus: InMemoryEventBus,
    *,
    targets: list[str],
    reclaim_min_idle_ms: int = 0,
) -> BrokerConsumer:
    return BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name="orders:1",
        group="modulith-orders",
        targets=targets,
        poll_block_ms=10,
        reclaim_min_idle_ms=reclaim_min_idle_ms,
    )


# ---------------------------------------------------------------------------
# Consumer health
# ---------------------------------------------------------------------------


def test_consumer_health_is_immutable() -> None:
    health = ConsumerHealth(ready=False, status="stopped")

    with pytest.raises(FrozenInstanceError):
        setattr(health, "status", "ready")  # noqa: B010


def test_health_is_optional_for_consumer_protocol() -> None:
    class LegacyConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    consumer = LegacyConsumer()

    assert isinstance(consumer, Consumer)
    assert not isinstance(consumer, HealthAwareConsumer)


@pytest.mark.asyncio
async def test_broker_consumer_health_tracks_start_ready_and_stop() -> None:
    class BlockingGroupBroker(FakeConsumerBroker):
        def __init__(self) -> None:
            super().__init__()
            self.starting = asyncio.Event()
            self.release = asyncio.Event()

        async def ensure_group(self, target: str, group: str | None = None) -> None:
            self.starting.set()
            await self.release.wait()
            await super().ensure_group(target, group)

    broker = BlockingGroupBroker()
    consumer = _make_consumer(broker, InMemoryEventBus(), targets=["t"])
    assert consumer.health() == ConsumerHealth(ready=False, status="stopped")

    start_task = asyncio.create_task(consumer.start())
    await broker.starting.wait()
    assert consumer.health() == ConsumerHealth(ready=False, status="starting")

    broker.release.set()
    await start_task
    assert consumer.health() == ConsumerHealth(ready=True, status="ready")

    await consumer.stop()
    assert consumer.health() == ConsumerHealth(ready=False, status="stopped")


@pytest.mark.asyncio
async def test_broker_consumer_health_reports_startup_failure() -> None:
    class FailingGroupBroker(FakeConsumerBroker):
        async def ensure_group(self, target: str, group: str | None = None) -> None:
            raise RuntimeError("group setup failed")

    consumer = _make_consumer(FailingGroupBroker(), InMemoryEventBus(), targets=["t"])

    with pytest.raises(RuntimeError, match="group setup failed"):
        await consumer.start()

    health = consumer.health()
    assert health.ready is False
    assert health.status == "failed"
    assert health.detail == "group setup failed"


@pytest.mark.asyncio
async def test_broker_consumer_health_degrades_and_recovers_with_broker() -> None:
    class FlakyReadBroker(FakeConsumerBroker):
        def __init__(self) -> None:
            super().__init__()
            self.read_calls = 0

        async def read(self, *args: Any, **kwargs: Any) -> Any:
            self.read_calls += 1
            if self.read_calls == 1:
                raise RuntimeError("broker unavailable")
            return await super().read(*args, **kwargs)

    broker = FlakyReadBroker()
    consumer = _make_consumer(broker, InMemoryEventBus(), targets=["t"])
    await consumer.start()
    try:
        await _until(lambda: consumer.health().status == "degraded")
        degraded = consumer.health()
        assert degraded.ready is False
        assert degraded.detail == "broker unavailable"

        await _until(lambda: broker.read_calls >= 2 and consumer.health().ready)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_broker_consumer_health_reports_unexpected_task_exit() -> None:
    consumer = _make_consumer(FakeConsumerBroker(), InMemoryEventBus(), targets=["t"])

    async def crash() -> None:
        raise RuntimeError("poll loop crashed")

    consumer._run = crash  # type: ignore[method-assign]
    await consumer.start()
    await _until(lambda: consumer.health().status == "failed")

    health = consumer.health()
    assert health.ready is False
    assert health.detail == "poll loop crashed"
    await consumer.stop()


@pytest.mark.asyncio
async def test_broker_consumer_completion_failures_recover_independently() -> None:
    class CompletionFlakyBroker(FakeConsumerBroker):
        def __init__(self) -> None:
            super().__init__()
            self.failures = {"ack", "dead_letter"}

        async def ack(
            self,
            target: str,
            message_id: str,
            group: str | None = None,
        ) -> None:
            if "ack" in self.failures:
                raise RuntimeError("ack unavailable")
            await super().ack(target, message_id, group)

        async def dead_letter(
            self,
            target: str,
            message_id: str,
            fields: dict[bytes, bytes],
            group: str | None = None,
        ) -> None:
            if "dead_letter" in self.failures:
                raise RuntimeError("dead-letter unavailable")
            await super().dead_letter(target, message_id, fields, group)

    async def handler(_event: CrossEvent) -> None:
        pass

    broker = CompletionFlakyBroker()
    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    consumer = _make_consumer(broker, bus, targets=["t"], reclaim_min_idle_ms=60_000)
    run_blocker = asyncio.Event()

    async def blocked_run() -> None:
        await run_blocker.wait()

    consumer._run = blocked_run  # type: ignore[method-assign]
    event_type = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    valid_fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=1)),
        b"h:event_type": event_type.encode(),
    }
    poison_fields = {b"data": b"{}"}

    await consumer.start()
    try:
        await consumer._dispatch_one("t", b"1-0", valid_fields)
        await consumer._dispatch_one("t", b"2-0", poison_fields)
        assert consumer.health().status == "degraded"

        # A healthy reclaim is unrelated to the failed completion writes.
        await consumer._reclaim("t")
        assert consumer.health().status == "degraded"

        broker.failures.remove("ack")
        await consumer._dispatch_one("t", b"3-0", valid_fields)
        assert consumer.health().status == "degraded"

        broker.failures.remove("dead_letter")
        await consumer._dispatch_one("t", b"4-0", poison_fields)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_broker_consumer_health_recovery_is_scoped_to_target() -> None:
    class TargetFlakyBroker(FakeConsumerBroker):
        def __init__(self) -> None:
            super().__init__()
            self.failing_targets = {"target-a"}

        async def ack(
            self,
            target: str,
            message_id: str,
            group: str | None = None,
        ) -> None:
            if target in self.failing_targets:
                raise RuntimeError(f"ack unavailable for {target}")
            await super().ack(target, message_id, group)

    async def handler(_event: CrossEvent) -> None:
        pass

    broker = TargetFlakyBroker()
    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    consumer = _make_consumer(
        broker, bus, targets=["target-a", "target-b"], reclaim_min_idle_ms=60_000
    )
    run_blocker = asyncio.Event()

    async def blocked_run() -> None:
        await run_blocker.wait()

    consumer._run = blocked_run  # type: ignore[method-assign]
    event_type = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=1)),
        b"h:event_type": event_type.encode(),
    }

    await consumer.start()
    try:
        await consumer._dispatch_one("target-a", b"1-0", fields)
        assert consumer.health().status == "degraded"

        # A successful ACK for target B must not hide target A's failure.
        await consumer._dispatch_one("target-b", b"2-0", fields)
        assert consumer.health().status == "degraded"

        broker.failing_targets.remove("target-a")
        await consumer._dispatch_one("target-a", b"3-0", fields)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# Headline: full producer → consumer round-trip, no Redis
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class _AckDownBroker(FakeConsumerBroker):
    """Every XACK fails; ``pending`` models the group's PEL when set."""

    def __init__(self) -> None:
        super().__init__()
        self.pending_ids: set[str] = set()

    async def ack(self, target: str, message_id: str, group: str | None = None) -> None:
        raise RuntimeError("simulated Redis blip during XACK")


def _cross_fields() -> dict[bytes, bytes]:
    event_type = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    return {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=1)),
        b"h:event_type": event_type.encode(),
    }


@pytest.mark.asyncio
async def test_broker_consumer_completion_failure_clears_once_message_is_not_pending() -> None:
    class PelBroker(_AckDownBroker):
        async def delivery_attempts(
            self, target: str, message_id: str, group: str | None = None
        ) -> int | None:
            return 1 if message_id in self.pending_ids else None

    async def handler(_event: CrossEvent) -> None:
        pass

    broker = PelBroker()
    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    consumer = _make_consumer(broker, bus, targets=["t"], reclaim_min_idle_ms=60_000)
    broker.pending_ids.add("1-0")
    await consumer.start()
    try:
        await consumer._dispatch_one("t", b"1-0", _cross_fields())
        await asyncio.sleep(0.1)
        # Still pending in the group: the failure stays visible across polls.
        assert consumer.health() == ConsumerHealth(
            ready=False, status="degraded", detail="simulated Redis blip during XACK"
        )

        # A peer reclaimed and ACKed it; XACK here is still failing.
        broker.pending_ids.discard("1-0")
        await _until(lambda: consumer.health().ready)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_broker_consumer_completion_failure_expires_after_reclaim_window() -> None:
    async def handler(_event: CrossEvent) -> None:
        pass

    broker = _AckDownBroker()
    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    consumer = _make_consumer(broker, bus, targets=["t"], reclaim_min_idle_ms=300)

    async def blocked_run() -> None:
        await asyncio.Event().wait()

    consumer._run = blocked_run  # type: ignore[method-assign]
    await consumer.start()
    try:
        await consumer._dispatch_one("t", b"1-0", _cross_fields())
        assert consumer.health().status == "degraded"

        await asyncio.sleep(0.35)
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")

        await consumer._dispatch_one("t", b"1-0", _cross_fields())
        assert consumer.health().status == "degraded"
    finally:
        await consumer.stop()


async def test_producer_to_consumer_roundtrip_without_redis(make_fake_app) -> None:
    """An event published in 'process A' reaches a listener in 'process B'.

    The producer (runtime) serializes + routes; we capture that exact message
    and feed it into the consumer, which deserializes via the event_type header
    and dispatches it. Previously only the producer half was asserted (#1/#2/#62).
    """
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, externalized, publish

                @externalized
                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    # --- Producer side: capture the serialized cross-process message ---
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    rec = RecordingBroker()
    _runtime.broker_registry.register("testbroker", rec)

    import fakeapp.orders as orders  # type: ignore[import-not-found]

    await orders.place("o-42")
    assert len(rec.published) == 1
    target, payload, headers = rec.published[0]
    assert headers is not None

    # --- Consumer side: a fresh worker bus with a listener for the SAME event ---
    from fakeapp.orders import OrderPlaced  # type: ignore[import-not-found]

    received: list[Any] = []

    async def on_placed(evt: Any) -> None:
        received.append(evt)

    bus = InMemoryEventBus()
    bus.register(OrderPlaced, on_placed)

    feed = FakeConsumerBroker()
    mid = feed.deliver(target, payload, dict(headers))
    consumer = _make_consumer(feed, bus, targets=[target])

    await consumer.start()
    try:
        await _until(lambda: received)
    finally:
        await consumer.stop()

    # The listener in the consumer process received the reconstructed event...
    assert received == [OrderPlaced(order_id="o-42")]
    # ...and the message was ACK'd so it won't be redelivered.
    assert (target, mid) in feed.acked
    assert feed.dead == []


# ---------------------------------------------------------------------------
# Dispatch semantics (direct, deterministic)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_acks_on_success() -> None:
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, bus, targets=["t"])

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    payload = JsonEventSerializer().serialize(CrossEvent(value=5))
    fields = {b"data": payload, b"h:event_type": fqn.encode()}

    await consumer._dispatch_one("t", b"1-0", fields)

    assert received == [5]
    assert broker.acked == [("t", "1-0")]
    assert broker.dead == []


@pytest.mark.asyncio
async def test_undeserializable_message_is_dead_lettered() -> None:
    bus = InMemoryEventBus()  # no listener needed; deserialize fails first
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, bus, targets=["t"])

    # event_type names a class that cannot be resolved → deserialize raises.
    fields = {b"data": b"{}", b"h:event_type": b"nonexistent.module.Ghost"}
    await consumer._dispatch_one("t", b"9-0", fields)

    assert broker.dead == [("t", "9-0", fields)]
    assert broker.acked == []  # poison is NOT acked on the source via ack()


@pytest.mark.asyncio
async def test_oversized_payload_is_dead_lettered_without_deserializing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broker/outbox row larger than the configured cap must never reach
    json.loads: it is poison, dead-lettered the same as an unresolvable
    event_type, and the poll loop keeps running afterward."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("json.loads must not run on an oversized payload")

    monkeypatch.setattr("modulith.serializers.json.loads", _boom)

    bus = InMemoryEventBus()
    broker = FakeConsumerBroker()
    consumer = BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(max_payload_bytes=20),
        consumer_name="orders:1",
        group="modulith-orders",
        targets=["t"],
        poll_block_ms=10,
        reclaim_min_idle_ms=0,
    )
    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {b"data": b"x" * 21, b"h:event_type": fqn.encode()}

    await consumer._dispatch_one("t", b"9-0", fields)

    assert broker.dead == [("t", "9-0", fields)]
    assert broker.acked == []

    # The poll loop's dispatch path survives the poison message and can still
    # process a subsequent well-formed one — under the same cap, so restore
    # json.loads and use a payload that fits within max_payload_bytes=10.
    monkeypatch.undo()
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus.register(CrossEvent, handler)
    ok_fields = {b"data": b'{"value":1}', b"h:event_type": fqn.encode()}
    await consumer._dispatch_one("t", b"10-0", ok_fields)

    assert received == [1]
    assert ("t", "10-0") in broker.acked


@pytest.mark.asyncio
async def test_missing_event_type_header_is_dead_lettered() -> None:
    bus = InMemoryEventBus()
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, bus, targets=["t"])

    fields = {b"data": b"{}"}  # no h:event_type
    await consumer._dispatch_one("t", b"3-0", fields)

    assert broker.dead == [("t", "3-0", fields)]


@pytest.mark.asyncio
async def test_repeated_dispatch_failure_eventually_dead_letters() -> None:
    async def boom(evt: CrossEvent) -> None:
        raise ValueError("listener down")

    bus = InMemoryEventBus()
    bus.register(CrossEvent, boom)
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, bus, targets=["t"])

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=1)),
        b"h:event_type": fqn.encode(),
    }

    # First failures are NOT acked and NOT dead-lettered (stay pending for retry).
    for _ in range(4):
        await consumer._dispatch_one("t", b"7-0", fields)
    assert broker.dead == []
    assert broker.acked == []
    # The 5th attempt exceeds the cap → dead-lettered.
    await consumer._dispatch_one("t", b"7-0", fields)
    assert broker.dead == [("t", "7-0", fields)]


@pytest.mark.asyncio
async def test_durable_delivery_attempts_survive_consumer_handoff() -> None:
    """Redis delivery metadata, not a worker-local counter, owns the retry budget."""

    class DurableAttemptsBroker(FakeConsumerBroker):
        async def delivery_attempts(
            self, target: str, message_id: str, group: str | None = None
        ) -> int | None:
            assert (target, message_id, group) == ("t", "7-0", "modulith-orders")
            return 5

    async def boom(evt: CrossEvent) -> None:
        raise ValueError("listener down")

    bus = InMemoryEventBus()
    bus.register(CrossEvent, boom)
    broker = DurableAttemptsBroker()
    # This simulates the replacement worker after the original worker has
    # already exhausted Redis's durable delivery count.
    replacement = _make_consumer(broker, bus, targets=["t"])
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=1)),
        b"h:event_type": f"{CrossEvent.__module__}.{CrossEvent.__qualname__}".encode(),
    }

    await replacement._dispatch_one("t", b"7-0", fields)

    assert broker.dead == [("t", "7-0", fields)]


@pytest.mark.asyncio
async def test_start_is_noop_without_targets() -> None:
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, InMemoryEventBus(), targets=[])
    await consumer.start()
    assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    await consumer.stop()
    # No groups created, no background task.
    assert broker.groups == []


def test_consumer_targets_are_fully_qualified_event_names() -> None:
    bus = InMemoryEventBus()

    async def handler(evt: CrossEvent) -> None: ...

    bus.register(CrossEvent, handler)
    cfg = Configuration(package="fakeapp", broker="redis-streams")
    targets = consumer_targets(bus, cfg, "orders")
    assert targets == [f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"]


@pytest.mark.parametrize(
    ("subscription_source", "declared_target"),
    [
        ("manifest", "from.manifest"),
        ("config", "from.config"),
        ("listener", "from.listener"),
    ],
)
def test_consumer_targets_use_only_configured_declaration_source(
    subscription_source: str,
    declared_target: str,
) -> None:
    class TargetedEvent:
        __modulith_broker_target__ = "redis-streams:events.custom"

    async def handler(evt: TargetedEvent) -> None: ...

    handler.__modulith_broker_targets__ = ("redis-streams:from.listener",)  # type: ignore[attr-defined]
    bus = InMemoryEventBus()
    bus.register(TargetedEvent, handler)
    manifest_module._manifests["fakeapp.orders"] = Manifest(  # type: ignore[attr-defined]
        package="fakeapp.orders",
        broker_targets=("redis-streams:from.manifest",),
    )
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
        subscription_source=subscription_source,
        subscriptions={"orders": ["redis-streams:from.config"]},
    )

    assert consumer_targets(bus, cfg, "orders") == [
        "events.custom",
        declared_target,
    ]


def test_consumer_targets_deduplicate_without_changing_order() -> None:
    class FirstEvent:
        __modulith_broker_target__ = "redis-streams:events.shared"

    class SecondEvent:
        pass

    async def first_handler(evt: FirstEvent) -> None: ...

    async def second_handler(evt: SecondEvent) -> None: ...

    bus = InMemoryEventBus()
    bus.register(FirstEvent, first_handler)
    bus.register(SecondEvent, second_handler)
    second_fqn = f"{SecondEvent.__module__}.{SecondEvent.__qualname__}"
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
        subscription_source="config",
        subscriptions={
            "orders": [
                f"redis-streams:{second_fqn}",
                "redis-streams:declared",
                "redis-streams:events.shared",
                "redis-streams:declared",
            ]
        },
    )

    assert consumer_targets(bus, cfg, "orders") == [
        "events.shared",
        second_fqn,
        "declared",
    ]


def test_consumer_targets_strip_scheme_and_destination_whitespace() -> None:
    manifest = Manifest(
        package="fakeapp.orders",
        broker_targets=(" redis-streams : events.orders ",),
    )
    manifest_module._manifests["fakeapp.orders"] = manifest  # type: ignore[attr-defined]
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
        subscription_source="manifest",
    )

    assert manifest.broker_targets == ("redis-streams:events.orders",)
    assert consumer_targets(InMemoryEventBus(), cfg, "orders") == ["events.orders"]


@pytest.mark.parametrize(
    "target",
    [
        "kafka:orders",
        "redis-streams:",
        ":orders",
        "orders",
    ],
)
def test_consumer_targets_reject_wrong_or_malformed_static_target(target: str) -> None:
    class TargetedEvent:
        pass

    TargetedEvent.__modulith_broker_target__ = target  # type: ignore[attr-defined]

    async def handler(evt: TargetedEvent) -> None: ...

    bus = InMemoryEventBus()
    bus.register(TargetedEvent, handler)
    cfg = Configuration(package="fakeapp", broker="redis-streams")

    with pytest.raises(ConfigurationError, match="broker target"):
        consumer_targets(bus, cfg, "orders")


@pytest.mark.parametrize(
    "target",
    [
        "kafka:orders",
        "redis-streams:",
        ":orders",
        "orders",
    ],
)
def test_consumer_targets_reject_wrong_or_malformed_declaration(target: str) -> None:
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
        subscription_source="config",
        subscriptions={"orders": [target]},
    )

    with pytest.raises(ConfigurationError, match="broker target"):
        consumer_targets(InMemoryEventBus(), cfg, "orders")


@pytest.mark.asyncio
async def test_reclaim_recovers_pending_on_start() -> None:
    """Crash recovery: messages a crashed peer left pending are reclaimed and
    dispatched on startup (the at-least-once recovery path)."""
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    broker = FakeConsumerBroker()

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=99)),
        b"h:event_type": fqn.encode(),
    }
    broker.pending["t"] = [("5-0", fields)]

    consumer = _make_consumer(broker, bus, targets=["t"])
    await consumer.start()
    try:
        await _until(lambda: received)
    finally:
        await consumer.stop()

    assert received == [99]
    assert ("t", "5-0") in broker.acked


@pytest.mark.asyncio
async def test_reclaim_retries_pending_while_worker_stays_alive() -> None:
    """Live workers must reclaim failed/pending messages after startup too."""
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    broker = FakeConsumerBroker()

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=123)),
        b"h:event_type": fqn.encode(),
    }

    consumer = _make_consumer(broker, bus, targets=["t"])
    await consumer.start()
    try:
        broker.pending["t"] = [("6-0", fields)]
        await _until(lambda: received)
    finally:
        await consumer.stop()

    assert received == [123]
    assert ("t", "6-0") in broker.acked


@pytest.mark.asyncio
async def test_fake_read_caps_delivery_at_count() -> None:
    """Real XREADGROUP delivers at most COUNT entries per call — the old fake
    drained the whole backlog in one read."""
    broker = FakeConsumerBroker()
    for i in range(3):
        broker.deliver("t", f'{{"n": {i}}}'.encode(), {"event_type": "X"})

    [(_t, first)] = await broker.read("t", consumer="c", count=2, block_ms=10)
    assert len(first) == 2  # capped, NOT all 3
    [(_t, second)] = await broker.read("t", consumer="c", count=2, block_ms=10)
    assert len(second) == 1  # the remainder was left queued, not dropped
    assert await broker.read("t", consumer="c", count=2, block_ms=1) == []


@pytest.mark.asyncio
async def test_reclaim_does_not_steal_fresh_in_flight_messages() -> None:
    """The consumer's reclaim must honor min_idle_ms: a
    freshly-delivered message a live peer is still processing is NOT
    redispatched; once idle past the threshold, it is."""
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    broker = FakeConsumerBroker()

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=42)),
        b"h:event_type": fqn.encode(),
    }
    consumer = BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name="orders:1",
        group="modulith-orders",
        targets=["t"],
        poll_block_ms=10,
        reclaim_min_idle_ms=60_000,  # the production default
    )

    broker.pending["t"] = [("8-0", fields)]
    broker.pending_idle_ms[("t", "8-0")] = 0  # freshly delivered to a live peer
    await consumer._reclaim("t")
    assert received == []  # NOT stolen from the in-flight peer
    assert broker.acked == []
    assert broker.pending["t"] == [("8-0", fields)]  # still pending

    broker.pending_idle_ms[("t", "8-0")] = 120_000  # now idle past threshold
    await consumer._reclaim("t")
    assert received == [42]
    assert ("t", "8-0") in broker.acked


@pytest.mark.asyncio
async def test_fake_reclaim_drains_full_idle_backlog_per_call() -> None:
    """The real adapter follows XAUTOCLAIM's cursor until ``0-0`` —
    one ``reclaim()`` call recovers the FULL idle-pending backlog (``count``
    is the adapter-internal page size, not a result cap). The fake mirrors
    that multi-page-drain contract; only entries still fresh (idle below
    ``min_idle_ms``) stay pending."""
    broker = FakeConsumerBroker()
    broker.pending["t"] = [(f"{i}-0", {b"data": b"{}"}) for i in range(1, 4)]
    broker.pending_idle_ms[("t", "3-0")] = 0  # fresh in-flight — NOT claimable

    _cursor, claimed, _deleted = await broker.reclaim("t", consumer="c", min_idle_ms=1_000, count=2)
    # The whole idle backlog comes back in one call, beyond count=2 pages.
    assert [m for m, _ in claimed] == ["1-0", "2-0"]
    # min_idle_ms is still honored: the fresh entry stays pending.
    assert broker.pending["t"] == [("3-0", {b"data": b"{}"})]

    broker.pending["u"] = [(f"{i}-0", {b"data": b"{}"}) for i in range(1, 5)]
    _cursor, claimed, _deleted = await broker.reclaim("u", consumer="c", min_idle_ms=0, count=2)
    assert len(claimed) == 4  # full drain, not a count-capped single page
    assert "u" not in broker.pending


@pytest.mark.asyncio
async def test_reclaim_surfaces_trimmed_pending_ids_as_lost(caplog) -> None:
    """XAUTOCLAIM's third element reports pending ids trimmed out of the
    stream — permanently lost messages. The consumer must
    surface the loss loudly and drop its retry bookkeeping, not dispatch or
    dead-letter them."""
    bus = InMemoryEventBus()
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, bus, targets=["t"])
    consumer._attempts[("t", "4-0")] = 3  # stale retry state for the lost id

    broker.lost["t"] = ["4-0"]
    with caplog.at_level("ERROR", logger="modulith.consumer"):
        await consumer._reclaim("t")

    assert "permanently lost" in caplog.text
    assert "4-0" in caplog.text
    assert ("t", "4-0") not in consumer._attempts  # bookkeeping cleared
    assert broker.acked == []  # a lost message is neither acked...
    assert broker.dead == []  # ...nor dead-lettered — it no longer exists


@pytest.mark.asyncio
async def test_reclaim_survives_nil_claimed_entries() -> None:
    """XAUTOCLAIM on Redis < 7.0 returns nil for pending entries
    deleted from the stream. A nil row must be skipped defensively — the
    entries after it are still reclaimed and dispatched, and the consumer
    does not crash."""
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    broker = FakeConsumerBroker()

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    fields = {
        b"data": JsonEventSerializer().serialize(CrossEvent(value=17)),
        b"h:event_type": fqn.encode(),
    }
    broker.pending["t"] = [None, ("5-0", fields)]  # nil row FIRST, then real work

    consumer = _make_consumer(broker, bus, targets=["t"])
    await consumer._reclaim("t")  # must not raise

    assert received == [17]  # the entry after the nil row was still dispatched
    assert ("t", "5-0") in broker.acked


@pytest.mark.asyncio
async def test_consumer_loop_stays_alive_across_nil_claimed_entries() -> None:
    """A nil claimed entry during the periodic reclaim must not kill
    the consumer task — later messages are still consumed."""
    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)
    broker = FakeConsumerBroker()
    broker.pending["t"] = [None]  # startup reclaim hits the nil row

    fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    payload = JsonEventSerializer().serialize(CrossEvent(value=23))

    consumer = _make_consumer(broker, bus, targets=["t"])
    await consumer.start()
    try:
        broker.deliver("t", payload, {"event_type": fqn})
        await _until(lambda: received)
        task = consumer._task
        assert task is not None and not task.done()
    finally:
        await consumer.stop()

    assert received == [23]


@pytest.mark.asyncio
async def test_every_subscribed_stream_is_polled_concurrently() -> None:
    """Idle delivery latency must not scale with the subscribed stream count.

    Each read blocks server-side for up to ``poll_block_ms``, so awaiting the
    targets one after another made an event landing just after its own stream
    was polled wait ``(N-1) * poll_block_ms`` for the cycle to come back
    around. The reads therefore overlap: every stream is in flight at once.
    """

    class ConcurrencyTrackingBroker(FakeConsumerBroker):
        def __init__(self) -> None:
            super().__init__()
            self.inflight = 0
            self.max_inflight = 0

        async def read(self, target: str, **kwargs: Any) -> Any:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            try:
                return await super().read(target, **kwargs)
            finally:
                self.inflight -= 1

    broker = ConcurrencyTrackingBroker()
    consumer = _make_consumer(broker, InMemoryEventBus(), targets=["a", "b", "c"])

    await consumer.start()
    try:
        await _until(lambda: broker.max_inflight == 3)
    finally:
        await consumer.stop()

    assert broker.max_inflight == 3


@pytest.mark.asyncio
async def test_consumer_dispatch_fires_the_per_listener_lifecycle_hooks() -> None:
    """A worker process must not be a telemetry blind spot.

    The hookspec scopes ``modulith_on_listener_dispatch`` / ``_error`` /
    ``_complete`` to every ``(event, listener)`` pair unconditionally, but
    dispatching a consumed message straight through ``bus.publish`` bypassed the
    runtime — so every listener invocation in a worker was untraced and a plugin
    using ``modulith_on_listener_error`` for alerting never saw a cross-process
    listener failure. The *publish* hooks stay out: this process did not publish
    the event.
    """

    class _Capture:
        def __init__(self) -> None:
            self.dispatched: list[str] = []
            self.completed: list[str] = []
            self.published: list[Any] = []

        @hookimpl
        def modulith_on_listener_dispatch(
            self, event: Any, listener_name: str, publication: Any
        ) -> None:
            self.dispatched.append(listener_name)

        @hookimpl
        def modulith_on_listener_complete(
            self, event: Any, listener_name: str, publication: Any, exception: Any
        ) -> None:
            self.completed.append(listener_name)

        @hookimpl
        def modulith_after_event_published(self, event: Any, publication: Any) -> None:
            self.published.append(event)

    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    capture = _Capture()
    broker = FakeConsumerBroker()
    _runtime._reset_for_testing()
    try:
        configure(package="hooktest", auto_discover=False)
        _runtime._extra_plugins.append(capture)
        _runtime.ensure_bootstrapped()

        bus = InMemoryEventBus()
        bus.register(CrossEvent, handler)
        consumer = _make_consumer(broker, bus, targets=["t"])

        fqn = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
        fields = {
            b"data": JsonEventSerializer().serialize(CrossEvent(value=5)),
            b"h:event_type": fqn.encode(),
        }
        await consumer._dispatch_one("t", b"1-0", fields)
    finally:
        _runtime._reset_for_testing()

    assert received == [5]
    assert [name.split(".")[-1] for name in capture.dispatched] == ["handler"]
    assert [name.split(".")[-1] for name in capture.completed] == ["handler"]
    assert capture.published == []
    assert broker.acked == [("t", "1-0")]


# ---------------------------------------------------------------------------
# Integration (real Redis) — see the redis_url/redis_client fixtures (conftest)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_integration_publish_then_consume_roundtrip(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """Real Redis: publish to a stream, then the consumer reads, deserializes,
    and dispatches it to a local listener (the genuine cross-process path).
    """
    from modulith.adapters.redis_broker import RedisStreamsBroker

    target = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    broker = RedisStreamsBroker(url=redis_url, stream_prefix=redis_key_prefix, consumer_group="g")

    received: list[int] = []

    async def handler(evt: CrossEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(CrossEvent, handler)

    consumer = BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name="itest:1",
        group="modulith-itest",
        targets=[target],
        poll_block_ms=50,
        reclaim_min_idle_ms=0,
    )
    try:
        await broker.ensure_group(target, "modulith-itest")
        payload = JsonEventSerializer().serialize(CrossEvent(value=7))
        await broker.publish(target, payload, {"event_type": target})

        await consumer.start()
        await _until(lambda: received, timeout=5.0)
    finally:
        await consumer.stop()
        await broker.close()

    assert received == [7]
