"""End-to-end Redis Streams broker + consumer behaviour against real Redis.

The unit suite drives the broker and consumer against hand-rolled fakes
(``FakeRedis``/``StatefulFakeRedis``/``FakeConsumerBroker``). Those pin the
exact commands issued, but only a real Redis exercises the semantics the design
leans on: consumer-group ``XREADGROUP >`` delivery, ``XAUTOCLAIM`` reclaim of a
crashed consumer's pending entries, dead-letter routing of poison messages,
backlog delivery for a group created at id ``0``, exactly-once-per-group load
balancing, per-module group fan-out, and ``MAXLEN`` trimming.

Provisioned by the shared ``redis_url``/``redis_client`` fixtures (testcontainers
Redis or ``MODULITH_TEST_REDIS_URL``); skipped without Docker.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from modulith import event
from modulith._consumer import BrokerConsumer
from modulith.adapters.redis_broker import RedisStreamsBroker
from modulith.event_bus import InMemoryEventBus
from modulith.serializers import JsonEventSerializer

pytestmark = [pytest.mark.integration]


@event
@dataclass(frozen=True)
class StreamEvent:
    value: int


_TARGET = f"{StreamEvent.__module__}.{StreamEvent.__qualname__}"


def _stream(redis_key_prefix: str) -> str:
    return f"{redis_key_prefix}.{_TARGET}"


def _dlq(redis_key_prefix: str) -> str:
    return f"{_stream(redis_key_prefix)}.dead"


async def _until(predicate, *, timeout: float = 8.0, interval: float = 0.02) -> None:
    """Poll a sync predicate (reads a local list) until true or timeout."""
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if predicate():
            return
        await asyncio.sleep(interval)
    if not predicate():
        raise AssertionError("condition not met within timeout")


async def _until_async(coro_predicate, *, timeout: float = 10.0, interval: float = 0.05) -> None:
    """Poll an async predicate (queries Redis) until true or timeout."""
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await coro_predicate():
            return
        await asyncio.sleep(interval)
    if not await coro_predicate():
        raise AssertionError("condition not met within timeout")


def _broker(redis_url: str, redis_key_prefix: str, **kwargs) -> RedisStreamsBroker:
    return RedisStreamsBroker(
        url=redis_url, stream_prefix=redis_key_prefix, consumer_group="default", **kwargs
    )


def _consumer(
    broker, group: str, name: str, sink: list[int], *, reclaim_min_idle_ms: int = 0
) -> BrokerConsumer:
    bus = InMemoryEventBus()

    async def handler(evt: StreamEvent) -> None:
        sink.append(evt.value)

    bus.register(StreamEvent, handler)
    return BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name=name,
        group=group,
        targets=[_TARGET],
        poll_block_ms=50,
        reclaim_min_idle_ms=reclaim_min_idle_ms,
    )


async def _publish(broker, value: int) -> None:
    payload = JsonEventSerializer().serialize(StreamEvent(value=value))
    await broker.publish(_TARGET, payload, {"event_type": _TARGET})


# ---------------------------------------------------------------------------
# Consumer-first delivery: the running loop picks up a later publish
# ---------------------------------------------------------------------------


async def test_running_consumer_receives_event_published_after_start(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """The canonical cross-process path: the consumer loop is already running
    (group created, blocking read active) when the producer publishes."""
    broker = _broker(redis_url, redis_key_prefix)
    received: list[int] = []
    consumer = _consumer(broker, group="modulith-mod", name="c:1", sink=received)
    try:
        await consumer.start()
        await _publish(broker, 42)
        await _until(lambda: received == [42])
    finally:
        await consumer.stop()
        await broker.close()


# ---------------------------------------------------------------------------
# Backlog: a message published before the group exists is still delivered
# ---------------------------------------------------------------------------


async def test_message_published_before_group_is_delivered(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """XGROUP CREATE at id ``0`` delivers the pre-existing backlog. A regression
    to ``$`` would silently drop these on real Redis while passing the fake."""
    broker = _broker(redis_url, redis_key_prefix)
    received: list[int] = []
    consumer = _consumer(broker, group="modulith-mod", name="c:1", sink=received)
    try:
        await _publish(broker, 7)  # published BEFORE any group/consumer exists
        await consumer.start()
        await _until(lambda: received == [7])
    finally:
        await consumer.stop()
        await broker.close()


# ---------------------------------------------------------------------------
# XAUTOCLAIM: a crashed consumer's unacked message is reclaimed by a peer
# ---------------------------------------------------------------------------


async def test_unacked_message_reclaimed_by_peer_consumer(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """Consumer A reads a message and crashes before ACK; consumer B (same
    group) reclaims the pending entry via XAUTOCLAIM and delivers it."""
    broker = _broker(redis_url, redis_key_prefix)
    group = "modulith-mod"
    await broker.ensure_group(_TARGET, group)
    await _publish(broker, 5)

    # Consumer A reads it under the group, then "crashes" without acking.
    read = await broker.read(_TARGET, consumer="A", group=group, count=10, block_ms=500)
    assert read and read[0][1], "A should have read the message"

    received: list[int] = []
    consumer_b = _consumer(broker, group=group, name="B", sink=received)
    try:
        await consumer_b.start()  # reclaims A's idle pending entry (min_idle=0)
        await _until(lambda: received == [5])
    finally:
        await consumer_b.stop()
        await broker.close()


# ---------------------------------------------------------------------------
# Dead-letter routing on real Redis
# ---------------------------------------------------------------------------


async def test_poison_message_missing_header_is_dead_lettered(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """A message lacking the ``event_type`` header is routed to ``<stream>.dead``
    and acked on the source stream — it cannot block the consumer forever."""
    broker = _broker(redis_url, redis_key_prefix)
    dlq = _dlq(redis_key_prefix)
    received: list[int] = []
    consumer = _consumer(broker, group="modulith-mod", name="c:1", sink=received)

    async def dlq_ready() -> bool:
        return await redis_client.xlen(dlq) == 1

    try:
        await broker.ensure_group(_TARGET, "modulith-mod")
        await broker.publish(_TARGET, b"not-a-real-event")  # no event_type header
        await consumer.start()
        await _until_async(dlq_ready)
        assert received == []  # never dispatched to a listener
        assert await redis_client.xlen(dlq) == 1
    finally:
        await consumer.stop()
        await broker.close()


async def test_dead_letter_retry_after_interruption_is_idempotent(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """Replaying a completed transfer cannot append a second original message."""
    broker = _broker(redis_url, redis_key_prefix)
    dlq = _dlq(redis_key_prefix)
    group = "modulith-mod"
    try:
        await broker.ensure_group(_TARGET, group)
        await broker.publish(_TARGET, b"poison")
        [(_stream, [(message_id, fields)])] = await broker.read(
            _TARGET, consumer="c:1", group=group, block_ms=50
        )
        message_id = message_id.decode() if isinstance(message_id, bytes) else message_id

        # This represents a client retry after Redis completed the first Lua
        # transfer but the client lost its response before observing success.
        await broker.dead_letter(_TARGET, message_id, fields, group)
        await broker.dead_letter(_TARGET, message_id, fields, group)

        assert await redis_client.xlen(dlq) == 1
        [(_dead_id, dead_fields)] = await redis_client.xrange(dlq)
        assert dead_fields[b"h:source_message_id"] == message_id.encode()
        assert dead_fields[b"h:source_group"] == group.encode()
    finally:
        await broker.close()


async def test_repeated_dispatch_failures_dead_letter_on_real_redis(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """A listener that always raises exhausts the delivery budget; the message is
    routed to the DLQ rather than redelivered forever."""
    broker = _broker(redis_url, redis_key_prefix)
    dlq = _dlq(redis_key_prefix)
    bus = InMemoryEventBus()

    async def boom(evt: StreamEvent) -> None:
        raise RuntimeError("listener always fails")

    bus.register(StreamEvent, boom)
    consumer = BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name="c:1",
        group="modulith-mod",
        targets=[_TARGET],
        poll_block_ms=50,
        reclaim_min_idle_ms=0,
    )

    async def dlq_ready() -> bool:
        return await redis_client.xlen(dlq) == 1

    try:
        await broker.ensure_group(_TARGET, "modulith-mod")
        await _publish(broker, 1)
        await consumer.start()
        await _until_async(dlq_ready)
        assert await redis_client.xlen(dlq) == 1
    finally:
        await consumer.stop()
        await broker.close()


# ---------------------------------------------------------------------------
# Consumer-group semantics: load balancing and per-module fan-out
# ---------------------------------------------------------------------------


async def test_same_group_delivers_each_message_exactly_once(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """Two consumers in ONE group share the stream — each message is delivered to
    exactly one of them (no duplication).

    A production reclaim idle (60s) is used so healthy peers don't XAUTOCLAIM each
    other's in-flight messages — reclaim is for crash recovery, not steady state.
    """
    broker_a = _broker(redis_url, redis_key_prefix)
    broker_b = _broker(redis_url, redis_key_prefix)
    got_a: list[int] = []
    got_b: list[int] = []
    c_a = _consumer(
        broker_a, group="modulith-shared", name="a", sink=got_a, reclaim_min_idle_ms=60_000
    )
    c_b = _consumer(
        broker_b, group="modulith-shared", name="b", sink=got_b, reclaim_min_idle_ms=60_000
    )
    try:
        await c_a.start()
        await c_b.start()
        for v in range(10):
            await _publish(broker_a, v)
        await _until(lambda: len(got_a) + len(got_b) == 10)
        assert sorted(got_a + got_b) == list(range(10))  # all delivered, none twice
        assert set(got_a).isdisjoint(got_b)
    finally:
        await c_a.stop()
        await c_b.stop()
        await broker_a.close()
        await broker_b.close()


async def test_distinct_groups_each_receive_every_message(
    redis_url, redis_client, redis_key_prefix
) -> None:
    """Two consumers in DIFFERENT groups (the per-consuming-module pattern) each
    receive every published event — the fan-out the design depends on."""
    broker_inv = _broker(redis_url, redis_key_prefix)
    broker_notif = _broker(redis_url, redis_key_prefix)
    got_inv: list[int] = []
    got_notif: list[int] = []
    c_inv = _consumer(broker_inv, group="modulith-inventory", name="i:1", sink=got_inv)
    c_notif = _consumer(broker_notif, group="modulith-notifications", name="n:1", sink=got_notif)
    try:
        await c_inv.start()
        await c_notif.start()
        await _publish(broker_inv, 99)
        await _until(lambda: got_inv == [99] and got_notif == [99])
    finally:
        await c_inv.stop()
        await c_notif.stop()
        await broker_inv.close()
        await broker_notif.close()


# ---------------------------------------------------------------------------
# MAXLEN trimming keeps the stream bounded on real Redis
# ---------------------------------------------------------------------------


async def test_stream_is_trimmed_to_maxlen(redis_url, redis_client, redis_key_prefix) -> None:
    """A bounded stream does not grow without limit: publishing far more than
    ``max_stream_len`` messages leaves the stream trimmed well below the total."""
    broker = _broker(redis_url, redis_key_prefix, max_stream_len=10)
    try:
        total = 500
        for v in range(total):
            await _publish(broker, v)
        length = await redis_client.xlen(_stream(redis_key_prefix))
        assert 0 < length < total  # MAXLEN ~ enforced (approximate trimming)
    finally:
        await broker.close()
