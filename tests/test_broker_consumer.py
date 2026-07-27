"""BrokerConsumer resilience and Redis broker documentation contracts.

Each test names the failure mode it reproduces. All tests are deterministic:
no wall-clock sleeps as synchronization — loops are driven by injected fakes and
bounded polling helpers (the one timed window, in the backoff test, asserts an
upper bound that timing jitter can only make easier to satisfy).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from modulith import event
from modulith._consumer import BrokerConsumer
from modulith.event_bus import InMemoryEventBus
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class ConsumerEvent:
    value: int


_FQN = f"{ConsumerEvent.__module__}.{ConsumerEvent.__qualname__}"


def _fields_for(value: int) -> dict[bytes, bytes]:
    return {
        b"data": JsonEventSerializer().serialize(ConsumerEvent(value=value)),
        b"h:event_type": _FQN.encode(),
    }


class InjectableBroker:
    """Consumer-surface fake with per-method failure injection.

    ``read`` hands out (and clears) queued messages; ``reclaim`` serves the
    ``pending`` dict and returns the full XAUTOCLAIM 3-tuple including the
    ``deleted`` ids element; ``fail_*`` lists are exceptions raised (popped
    front-first) on the next call of the matching method.
    """

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[bytes, bytes]]]] = {}
        self.pending: dict[str, list[tuple[str, dict[bytes, bytes]]]] = {}
        self.deleted: dict[str, list[bytes]] = {}
        self.groups: list[tuple[str, str]] = []
        self.acked: list[tuple[str, str]] = []
        self.dead: list[tuple[str, str]] = []
        self.read_calls: list[dict[str, Any]] = []
        self.reclaim_calls: int = 0
        self.fail_read: list[Exception] = []
        self.fail_reclaim: list[Exception] = []
        self.fail_ack: list[Exception] = []
        self.fail_dead_letter: list[Exception] = []
        # When True, read/reclaim raise NOGROUP (Redis lost stream/group state)
        # until ensure_group() is called again — mimics a fresh-restarted Redis.
        self.nogroup = False

    def deliver(self, target: str, value: int) -> str:
        mid = f"{len(self.streams.get(target, [])) + 1}-0"
        self.streams.setdefault(target, []).append((mid, _fields_for(value)))
        return mid

    async def ensure_group(self, target: str, group: str | None = None) -> None:
        self.groups.append((target, group or ""))
        self.nogroup = False

    async def read(
        self,
        target: str,
        *,
        consumer: str,
        group: str | None = None,
        count: int = 10,
        block_ms: int = 1000,
    ) -> Any:
        self.read_calls.append({"target": target, "block_ms": block_ms})
        if self.nogroup:
            raise RuntimeError(f"NOGROUP No such key '{target}' or consumer group 'g'")
        if self.fail_read:
            raise self.fail_read.pop(0)
        queued = self.streams.get(target, [])
        if not queued:
            await asyncio.sleep(block_ms / 1000)
            return []
        self.streams[target] = []
        return [(target, queued)]

    async def ack(self, target: str, message_id: str, group: str | None = None) -> None:
        if self.fail_ack:
            raise self.fail_ack.pop(0)
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
        self.reclaim_calls += 1
        if self.nogroup:
            raise RuntimeError(f"NOGROUP No such key '{target}' or consumer group 'g'")
        if self.fail_reclaim:
            raise self.fail_reclaim.pop(0)
        return (b"0-0", self.pending.pop(target, []), self.deleted.pop(target, []))

    async def dead_letter(
        self, target: str, message_id: str, fields: dict[bytes, bytes], group: str | None = None
    ) -> None:
        if self.fail_dead_letter:
            raise self.fail_dead_letter.pop(0)
        self.dead.append((target, message_id))


async def _until(predicate: Any, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    """Poll until ``predicate()`` is truthy; raise TimeoutError past ``timeout``.

    Raising (rather than returning silently) makes the poll itself the failure
    point instead of deferring to a later assert; the deadline is loop-clock
    based so delayed wakeups still terminate the wait.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise TimeoutError(f"condition not met within {timeout}s: {predicate}")
        await asyncio.sleep(interval)


def _make_consumer(
    broker: Any,
    bus: InMemoryEventBus | None = None,
    *,
    targets: list[str],
    poll_block_ms: int = 10,
) -> BrokerConsumer:
    return BrokerConsumer(
        broker=broker,
        bus=bus or InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="g05:1",
        group="modulith-g05",
        targets=targets,
        poll_block_ms=poll_block_ms,
        reclaim_min_idle_ms=0,
    )


# ---------------------------------------------------------------------------
# XAUTOCLAIM's deleted-ids element must be surfaced, not discarded
# ---------------------------------------------------------------------------


async def test_reclaim_surfaces_deleted_pending_messages(caplog) -> None:
    """A still-pending message trimmed out of the stream (MAXLEN)
    is reported by XAUTOCLAIM's 3rd tuple element ('deleted'). The consumer
    must log the permanent loss at ERROR with target + ids — previously
    result[2] was silently discarded and the data loss was invisible."""
    broker = InjectableBroker()
    broker.deleted["t"] = [b"1783798103847-0"]
    consumer = _make_consumer(broker, targets=["t"])

    with caplog.at_level(logging.ERROR, logger="modulith.consumer"):
        await consumer._reclaim("t")

    loss_records = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "1783798103847-0" in r.getMessage()
    ]
    assert loss_records, "trimmed-while-pending message loss must be logged at ERROR"
    assert any("t" in r.getMessage() for r in loss_records)


# ---------------------------------------------------------------------------
# Attempt counters must be keyed by (target, id), not id alone
# ---------------------------------------------------------------------------


async def test_attempt_counters_do_not_leak_across_targets() -> None:
    """Redis stream ids are stream-local, so two different targets
    can carry the identical message id. A message failing on target t1 must
    not inherit/contaminate the retry counter of an id-colliding message on
    target t2 — previously the shared mid-only key dead-lettered t2's message
    on its FIRST failure."""

    async def boom(evt: ConsumerEvent) -> None:
        raise ValueError("listener down")

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, boom)
    broker = InjectableBroker()
    consumer = _make_consumer(broker, bus, targets=["t1", "t2"])

    fields = _fields_for(1)
    # 4 failures on t1 under id 1-0 — one short of the dead-letter cap.
    for _ in range(4):
        await consumer._dispatch_one("t1", b"1-0", fields)
    assert broker.dead == []

    # First failure of a DIFFERENT stream's message with the SAME id: must
    # NOT be dead-lettered (its own attempt count is 1, not 5).
    await consumer._dispatch_one("t2", b"1-0", _fields_for(2))
    assert broker.dead == [], "t2's first failure must not inherit t1's attempt count"

    # And t1's own 5th failure still dead-letters t1 (cap preserved per target).
    await consumer._dispatch_one("t1", b"1-0", fields)
    assert broker.dead == [("t1", "1-0")]


# ---------------------------------------------------------------------------
# A broker-side ack()/dead_letter() blip must not kill the loop
# ---------------------------------------------------------------------------


async def test_ack_failure_does_not_kill_consumer_loop() -> None:
    """An exception from broker.ack() propagated out of the task and
    permanently, silently killed the whole consumer loop. It must instead be
    logged and degrade to 'message stays pending, retried later'."""
    received: list[int] = []

    async def handler(evt: ConsumerEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handler)
    broker = InjectableBroker()
    broker.fail_ack.append(ConnectionError("redis briefly down at ack time"))
    consumer = _make_consumer(broker, bus, targets=["t"])

    broker.deliver("t", 1)
    await consumer.start()
    try:
        await _until(lambda: received == [1])
        assert consumer._task is not None and not consumer._task.done(), (
            "consumer task must survive an ack() failure"
        )
        # A brand-new healthy message on the same stream is still consumed.
        broker.deliver("t", 2)
        await _until(lambda: received == [1, 2])
    finally:
        await consumer.stop()

    assert received == [1, 2]


async def test_dead_letter_failure_does_not_kill_consumer_loop() -> None:
    """An exception from broker.dead_letter() (poison-message path)
    killed the loop the same way. It must be logged and the loop must keep
    consuming subsequent messages."""
    received: list[int] = []

    async def handler(evt: ConsumerEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handler)
    broker = InjectableBroker()
    broker.fail_dead_letter.append(ConnectionError("redis briefly down at DLQ-write time"))
    consumer = _make_consumer(broker, bus, targets=["t"])

    # Poison: missing h:event_type header → dead_letter() is invoked and raises.
    broker.streams.setdefault("t", []).append(("1-0", {b"data": b"{}"}))
    await consumer.start()
    try:
        await _until(lambda: broker.read_calls)
        broker.deliver("t", 2)
        await _until(lambda: received == [2])
        assert consumer._task is not None and not consumer._task.done(), (
            "consumer task must survive a dead_letter() failure"
        )
    finally:
        await consumer.stop()

    assert received == [2]


async def test_stop_does_not_reraise_non_cancelled_task_death() -> None:
    """Companion to the ack/dead_letter cases: stop() only swallowed
    CancelledError, so calling stop() on a consumer whose task had already died
    with a real exception re-raised that exception at shutdown time. stop() must
    never raise."""
    broker = InjectableBroker()
    consumer = _make_consumer(broker, targets=["t"])

    async def _dies() -> None:
        raise ConnectionError("task already dead")

    consumer._task = asyncio.create_task(_dies())
    await asyncio.sleep(0)  # let the task run to its exception
    await consumer.stop()  # must not raise
    assert consumer._task is None


# ---------------------------------------------------------------------------
# NOGROUP must trigger group re-creation, not a permanent stall
# ---------------------------------------------------------------------------


async def test_nogroup_triggers_group_recreation_and_recovery() -> None:
    """ensure_group() was invoked exactly once at start(); when Redis
    lost stream/group state and came back fresh (e.g. crash without a snapshot),
    every subsequent read()/reclaim() failed forever with NOGROUP and the
    consumer silently never delivered another message. On NOGROUP the consumer
    must re-issue ensure_group and resume consuming."""
    received: list[int] = []

    async def handler(evt: ConsumerEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handler)
    broker = InjectableBroker()
    consumer = _make_consumer(broker, bus, targets=["t"])

    await consumer.start()
    try:
        # Redis loses all state and recovers empty; a producer publishes again.
        broker.nogroup = True
        broker.deliver("t", 42)
        await _until(lambda: received == [42])
    finally:
        await consumer.stop()

    assert received == [42], "consumer must recover from NOGROUP and keep delivering"
    # start() created the group once; NOGROUP recovery must re-create it.
    assert len(broker.groups) >= 2


# ---------------------------------------------------------------------------
# Broker outage must back off, not busy-spin (~281 failures/sec)
# ---------------------------------------------------------------------------


async def test_broker_outage_backs_off_instead_of_busy_spinning() -> None:
    """With the broker down, read()/reclaim() failures retried with
    zero backoff (~281 failures/sec measured against a killed Redis). Worse,
    when the broker raised synchronously the loop never yielded to the event
    loop at all, starving every other coroutine. With capped exponential
    backoff, a 0.4s outage window must see only a handful of broker calls —
    and the sibling coroutine (this test) must keep getting scheduled."""
    broker = InjectableBroker()
    broker.fail_read = [ConnectionError("connection refused")] * 1000
    broker.fail_reclaim = [ConnectionError("connection refused")] * 1000
    consumer = _make_consumer(broker, targets=["t"])

    await consumer.start()
    try:
        # This sleep only completes if the consumer loop yields (no starvation).
        await asyncio.sleep(0.4)
        attempts = len(broker.read_calls) + broker.reclaim_calls
        # Unbounded spin produced thousands of calls in a comparable window;
        # exponential backoff (base 0.05s) allows ~5-6. Generous 2x headroom —
        # scheduling jitter can only lengthen sleeps, i.e. REDUCE the count.
        assert attempts <= 12, f"busy-retry spin: {attempts} broker calls in 0.4s"
        assert attempts >= 2, "loop must keep retrying during the outage"
        assert consumer._task is not None and not consumer._task.done(), (
            "consumer task must survive a sustained broker outage"
        )
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# BLOCK 0 must never reach a real broker (blocks forever)
# ---------------------------------------------------------------------------


async def test_nonpositive_poll_block_ms_is_clamped() -> None:
    """Real Redis treats XREADGROUP BLOCK 0 as 'block forever
    awaiting new entries' (the test fakes modeled the opposite: an immediate
    empty return). A non-positive poll_block_ms must be clamped before it
    reaches the broker so a real worker can never hang indefinitely."""
    received: list[int] = []

    async def handler(evt: ConsumerEvent) -> None:
        received.append(evt.value)

    bus = InMemoryEventBus()
    bus.register(ConsumerEvent, handler)
    broker = InjectableBroker()
    consumer = _make_consumer(broker, bus, targets=["t"], poll_block_ms=0)

    broker.deliver("t", 1)
    await consumer.start()
    try:
        await _until(lambda: received == [1])
    finally:
        await consumer.stop()

    assert broker.read_calls, "consumer must have polled the broker"
    assert all(call["block_ms"] >= 1 for call in broker.read_calls), (
        "BLOCK 0 must never reach the broker"
    )
