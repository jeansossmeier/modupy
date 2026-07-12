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
from dataclasses import dataclass
from typing import Any

import pytest

from modulith import configure, event
from modulith._consumer import BrokerConsumer, consumer_targets
from modulith.event_bus import InMemoryEventBus
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
    ``count`` new messages per call — real XREADGROUP caps delivery at COUNT
    (audit S3-r1-65) — and rejects ``block_ms <= 0`` loudly (real Redis BLOCK 0
    blocks forever; BrokerConsumer clamps to >=1ms — audit A7-r2-92);
    ``ack``/``dead_letter`` are recorded; ``reclaim`` serves entries staged in
    ``pending``, honoring ``min_idle_ms`` and ``count`` like real XAUTOCLAIM
    (audits S3-r2-120 / A7-r3-140). A staged pending entry defaults to
    idle-forever (a crashed peer's message); set ``pending_idle_ms`` per
    (target, mid) to model a freshly-delivered in-flight message. Ids staged
    in ``lost`` are returned once via XAUTOCLAIM's third (deleted) element —
    the trimmed-while-pending loss channel (audit S3-r3-160).
    """

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[bytes, bytes]]]] = {}
        self.pending: dict[str, list[tuple[str, dict[bytes, bytes]]]] = {}
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
                "XREADGROUP BLOCK 0 blocks forever on real Redis (audit "
                "A7-r2-92) — BrokerConsumer clamps poll_block_ms to >=1; the "
                "fake rejects a non-positive block loudly instead of modeling it."
            )
        queued = self.streams.get(target, [])
        if not queued:
            await asyncio.sleep(block_ms / 1000)  # mimic XREADGROUP BLOCK so the loop yields
            return []
        # Real XREADGROUP delivers at most COUNT entries per call (audit
        # S3-r1-65) — the remainder stays queued for the next read.
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
        # Real XAUTOCLAIM only claims entries idle >= min_idle_time (audit
        # S3-r2-120) and caps claims at COUNT (audit A7-r3-140).
        claimed: list[tuple[str, dict[bytes, bytes]]] = []
        kept: list[tuple[str, dict[bytes, bytes]]] = []
        for mid, fields in self.pending.get(target, []):
            idle = self.pending_idle_ms.get((target, mid), float("inf"))
            if idle >= min_idle_ms and len(claimed) < count:
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


def _make_consumer(broker: Any, bus: InMemoryEventBus, *, targets: list[str]) -> BrokerConsumer:
    return BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name="orders:1",
        group="modulith-orders",
        targets=targets,
        poll_block_ms=10,
        reclaim_min_idle_ms=0,
    )


# ---------------------------------------------------------------------------
# Headline: full producer → consumer round-trip, no Redis
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
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
async def test_start_is_noop_without_targets() -> None:
    broker = FakeConsumerBroker()
    consumer = _make_consumer(broker, InMemoryEventBus(), targets=[])
    await consumer.start()
    await consumer.stop()
    # No groups created, no background task.
    assert broker.groups == []


def test_consumer_targets_are_fully_qualified_event_names() -> None:
    bus = InMemoryEventBus()

    async def handler(evt: CrossEvent) -> None: ...

    bus.register(CrossEvent, handler)
    targets = consumer_targets(bus)
    assert targets == [f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"]


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
    """Real XREADGROUP delivers at most COUNT entries per call (audit
    S3-r1-65) — the old fake drained the whole backlog in one read."""
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
    """The consumer's reclaim must honor min_idle_ms (audit S3-r2-120): a
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
async def test_fake_reclaim_caps_at_count() -> None:
    """Real XAUTOCLAIM caps claims at COUNT (audit A7-r3-140) — the old fake
    handed back the entire staged backlog in one call."""
    broker = FakeConsumerBroker()
    broker.pending["t"] = [(f"{i}-0", {b"data": b"{}"}) for i in range(1, 4)]

    _cursor, claimed, _deleted = await broker.reclaim("t", consumer="c", min_idle_ms=0, count=2)
    assert [m for m, _ in claimed] == ["1-0", "2-0"]  # capped at count
    assert [m for m, _ in broker.pending["t"]] == ["3-0"]  # remainder stays pending


@pytest.mark.asyncio
async def test_reclaim_surfaces_trimmed_pending_ids_as_lost(caplog) -> None:
    """XAUTOCLAIM's third element reports pending ids trimmed out of the
    stream — permanently lost messages (audit S3-r3-160). The consumer must
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


# ---------------------------------------------------------------------------
# Integration (real Redis) — see the redis_url/redis_client fixtures (conftest)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_integration_publish_then_consume_roundtrip(redis_url, redis_client) -> None:
    """Real Redis: publish to a stream, then the consumer reads, deserializes,
    and dispatches it to a local listener (the genuine cross-process path).

    ``redis_client`` flushes the DB around the test for isolation.
    """
    from modulith.adapters.redis_broker import RedisStreamsBroker

    target = f"{CrossEvent.__module__}.{CrossEvent.__qualname__}"
    broker = RedisStreamsBroker(url=redis_url, stream_prefix="modulith.itest", consumer_group="g")

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
