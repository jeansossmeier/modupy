"""The full cross-process event path: runtime publish → real Redis → consumer.

``test_cross_process.py`` proves the runtime's *routing decision* with a
``FakeBroker``; ``test_consumer.py`` proves the consumer loop with a fake. This
file closes the seam between them on real Redis: a ``publish()`` through the live
runtime in ``topology="processes"`` is serialized, XADDed to a real stream by the
auto-registered ``RedisStreamsBroker``, and a real ``BrokerConsumer`` reads,
deserializes, and dispatches it to a listener — the genuine producer→broker→
consumer→listener path a two-process deployment relies on.

Provisioned by the shared ``redis_url``/``redis_client`` fixtures; skipped
without Docker.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from modulith import configure, event, externalized, publish
from modulith._consumer import BrokerConsumer
from modulith.adapters.redis_broker import RedisStreamsBroker
from modulith.builtin import outbox
from modulith.event_bus import InMemoryEventBus
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

pytestmark = [pytest.mark.integration]

_PREFIX = "modulith.cpe2e"


@event
@dataclass(frozen=True)
class RemoteOnlyEvent:
    """Has no local listener — a pure cross-module event."""

    value: int


@event
@externalized
@dataclass(frozen=True)
class FannedOutEvent:
    """Bare @externalized: dispatched locally AND sent to the broker."""

    value: int


@pytest.fixture(autouse=True)
def _reset():
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    outbox._reset_for_testing()


async def _until(predicate, *, timeout: float = 8.0, interval: float = 0.02) -> None:
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if predicate():
            return
        await asyncio.sleep(interval)
    if not predicate():
        raise AssertionError("condition not met within timeout")


def _start_producer_runtime(redis_url: str) -> None:
    """Bring up a process-topology runtime whose broker is a real Redis."""
    configure(
        package="cpe2e",
        topology="processes",
        broker="redis-streams",
        broker_options={"url": redis_url, "stream_prefix": _PREFIX},
        auto_discover=False,
    )
    _runtime.ensure_bootstrapped()
    # The redis-streams adapter is wired by the built-in registration hook.
    assert _runtime.broker_registry is not None
    assert "redis-streams" in _runtime.broker_registry.schemes()


def _make_consumer(redis_url: str, target: str, sink: list[int]) -> BrokerConsumer:
    broker = RedisStreamsBroker(url=redis_url, stream_prefix=_PREFIX, consumer_group="modulith-x")
    bus = InMemoryEventBus()

    async def handler(evt) -> None:
        sink.append(evt.value)

    # Register against the concrete event class resolved from the target.
    module, _, qualname = target.rpartition(".")
    import importlib

    cls = getattr(importlib.import_module(module), qualname)
    bus.register(cls, handler)
    return BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(),
        consumer_name="x:1",
        group="modulith-x",
        targets=[target],
        poll_block_ms=50,
        reclaim_min_idle_ms=0,
    )


async def test_runtime_publish_routes_through_real_redis_to_consumer(
    redis_url, redis_client
) -> None:
    """An event with no local listener, published through the live runtime in
    process topology, is delivered to a real consumer over real Redis."""
    target = f"{RemoteOnlyEvent.__module__}.{RemoteOnlyEvent.__qualname__}"
    received: list[int] = []
    consumer = _make_consumer(redis_url, target, received)

    _start_producer_runtime(redis_url)
    try:
        await consumer.start()  # group ready before we publish
        await publish(RemoteOnlyEvent(value=3))  # routed to the broker by the runtime
        await _until(lambda: received == [3])
    finally:
        await consumer.stop()
        await consumer._broker.close()
        await _runtime.shutdown()


async def test_externalized_event_fans_out_locally_and_over_redis(redis_url, redis_client) -> None:
    """A bare @externalized event with a local listener is dispatched in-process
    AND routed to the broker — both the local listener and a remote consumer
    receive it."""
    target = f"{FannedOutEvent.__module__}.{FannedOutEvent.__qualname__}"
    remote_received: list[int] = []
    consumer = _make_consumer(redis_url, target, remote_received)

    _start_producer_runtime(redis_url)

    local_received: list[int] = []

    async def local_handler(evt: FannedOutEvent) -> None:
        local_received.append(evt.value)

    assert _runtime.event_bus is not None
    _runtime.event_bus.register(FannedOutEvent, local_handler)

    try:
        await consumer.start()
        await publish(FannedOutEvent(value=8))
        await _until(lambda: local_received == [8] and remote_received == [8])
    finally:
        await consumer.stop()
        await consumer._broker.close()
        await _runtime.shutdown()
