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
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from modulith import event, externalized
from modulith._consumer import BrokerConsumer, consumer_targets
from modulith.adapters._polling_consumer import PollingConsumer
from modulith.adapters.shm_broker import ShmBroker, ShmConsumer
from modulith.brokers import BrokerRegistry
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


# ---------------------------------------------------------------------------
# PollingConsumer: idle backoff must never narrow below the configured poll
# interval, and its configured max_attempts must reach claim_batch()
# ---------------------------------------------------------------------------


class FakePollingBroker:
    """PollingBroker-conforming fake recording every claim_batch() call."""

    def __init__(self) -> None:
        self.claim_batch_calls: list[dict[str, Any]] = []

    async def subscribe(self, targets: list[str], group: str) -> None:
        return None

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float,
        max_attempts: int | None = None,
    ) -> list[dict[str, Any]]:
        self.claim_batch_calls.append(
            {
                "group": group,
                "batch_size": batch_size,
                "consumer_name": consumer_name,
                "reclaim_stale_seconds": reclaim_stale_seconds,
                "max_attempts": max_attempts,
            }
        )
        return []

    async def renew_claims(
        self, row_ids: list[str], *, consumer_name: str, start_dispatch: bool = False
    ) -> int:
        return 0

    async def ack(self, row_id: str, *, consumer_name: str) -> None:
        return None

    async def fail(self, row_id: str, error: str, *, consumer_name: str, max_attempts: int) -> None:
        return None

    async def dead_letter(self, row_id: str, error: str, *, consumer_name: str) -> None:
        return None

    async def prune(
        self, *, retention_age_seconds: float | None, retention_count: int | None
    ) -> int:
        return 0


def _make_polling_consumer(
    broker: FakePollingBroker,
    *,
    poll_interval_s: float,
    max_attempts: int,
    idle_wait: Any,
) -> PollingConsumer:
    return PollingConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="worker-1",
        group="workers",
        targets=["t"],
        poll_interval_s=poll_interval_s,
        batch_size=10,
        dispatch_concurrency=1,
        max_attempts=max_attempts,
        reclaim_stale_seconds=30.0,
        prune_interval_s=None,
        retention_age_seconds=None,
        retention_count=None,
        logger=logging.getLogger("test.polling_consumer"),
        scheme="fake",
        idle_backoff=True,
        idle_wait=idle_wait,
    )


async def test_idle_backoff_never_narrows_below_configured_poll_interval() -> None:
    """The clamp applied ``_IDLE_BACKOFF_CAP_S=0.5`` to the BASE interval too,
    so a configured poll_interval_ms >= 500 was silently narrowed to 0.5s on
    the very first idle poll instead of only capping backoff GROWTH above the
    configured cadence."""
    delays: list[float] = []

    async def fake_idle_wait(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    broker = FakePollingBroker()
    consumer = _make_polling_consumer(
        broker, poll_interval_s=5.0, max_attempts=3, idle_wait=fake_idle_wait
    )

    await consumer.start()
    try:
        await _until(lambda: len(delays) >= 1)
    finally:
        await consumer.stop()

    assert delays[0] >= 5.0, (
        f"first idle wait must honor the configured 5s poll interval, got {delays[0]}"
    )


async def test_polling_consumer_passes_max_attempts_to_claim_batch() -> None:
    """PollingConsumer already tracks ``max_attempts`` for dispatch-failure
    dead-lettering (_delivery_dispatch.py's fail() call), but nothing wired
    it into claim_batch() -- so a broker's reclaim-time cap (db_broker's
    claim_batch ``max_attempts`` kwarg) never saw the consumer's configured
    value and stayed permanently inert."""
    broker = FakePollingBroker()

    async def fake_idle_wait(delay: float) -> None:
        await asyncio.sleep(0)

    consumer = _make_polling_consumer(
        broker, poll_interval_s=0.01, max_attempts=7, idle_wait=fake_idle_wait
    )

    await consumer.start()
    try:
        await _until(lambda: broker.claim_batch_calls)
    finally:
        await consumer.stop()

    assert broker.claim_batch_calls[0]["max_attempts"] == 7


# ---------------------------------------------------------------------------
# stop() must return even when the poll task cannot be cancelled promptly
# ---------------------------------------------------------------------------


class _WedgedClaimBroker(FakePollingBroker):
    """``claim_batch`` never returns and swallows the first ``absorb`` cancels —
    the shape SQLAlchemy produces when a cancelled connection's graceful close
    is shielded and the driver never finishes it."""

    def __init__(self, *, absorb: int) -> None:
        super().__init__()
        self.absorb = absorb
        self.cancels = 0
        self.entered = asyncio.Event()

    async def claim_batch(self, group: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.entered.set()
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                self.cancels += 1
                if self.cancels > self.absorb:
                    raise


async def test_stop_re_cancels_a_poll_task_that_absorbed_the_first_cancel() -> None:
    broker = _WedgedClaimBroker(absorb=1)
    consumer = _make_polling_consumer(broker, poll_interval_s=0.01, max_attempts=3, idle_wait=None)
    consumer._stop_timeout_s = 0.2
    await consumer.start()
    await asyncio.wait_for(broker.entered.wait(), timeout=2.0)

    await asyncio.wait_for(consumer.stop(), timeout=5.0)

    assert broker.cancels == 2
    assert consumer.health().status == "stopped"


async def test_stop_abandons_a_poll_task_that_never_stops(caplog) -> None:
    broker = _WedgedClaimBroker(absorb=10**9)
    consumer = _make_polling_consumer(broker, poll_interval_s=0.01, max_attempts=3, idle_wait=None)
    consumer._stop_timeout_s = 0.1
    await consumer.start()
    await asyncio.wait_for(broker.entered.wait(), timeout=2.0)
    task = consumer._task
    assert task is not None

    try:
        with caplog.at_level(logging.ERROR, logger="test.polling_consumer"):
            await asyncio.wait_for(consumer.stop(), timeout=5.0)

        assert not task.done()
        assert consumer._task is None
        assert consumer.health().status == "stopped"
        assert any("abandon" in record.getMessage() for record in caplog.records)
    finally:
        # Release the wedged task even on failure: a task that swallows every
        # cancel would otherwise wedge the loop's own shutdown and hang pytest.
        broker.absorb = 0
        task.cancel()
        await asyncio.wait({task}, timeout=2.0)
    assert task.done()


async def test_broker_consumer_stop_abandons_a_task_that_never_stops(caplog) -> None:
    """Same guarantee for the Redis consumer's own stop(): it must return."""
    consumer = _make_consumer(InjectableBroker(), targets=["t"])
    consumer._stop_timeout_s = 0.1
    entered = asyncio.Event()
    absorb = {"cancels": True}

    async def _wedged() -> None:
        entered.set()
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                if not absorb["cancels"]:
                    raise

    task = asyncio.create_task(_wedged())
    consumer._task = task
    await asyncio.wait_for(entered.wait(), timeout=2.0)

    try:
        with caplog.at_level(logging.ERROR, logger="modulith.consumer"):
            await asyncio.wait_for(consumer.stop(), timeout=5.0)

        assert not task.done()
        assert consumer._task is None
        assert any("abandon" in record.getMessage() for record in caplog.records)
    finally:
        absorb["cancels"] = False
        task.cancel()
        await asyncio.wait({task}, timeout=2.0)
    assert task.done()


@event
@externalized(target="shm: orders.placed")
@dataclass(frozen=True)
class PaddedTargetEvent:
    order_id: str


async def test_whitespace_padded_target_reaches_the_shm_consumer(tmp_path: Path) -> None:
    """Producer and consumer resolve a padded target to the same destination.

    The producer publishes the raw padded string, as a hook-resolved target or
    an outbox row persisted with that target would hand it to the registry.
    """
    broker = ShmBroker(shm_name="padded-target-hints", db_path=str(tmp_path / "padded.db"))
    registry = BrokerRegistry()
    registry.register("shm", broker)
    delivered: list[PaddedTargetEvent] = []

    async def on_order(item: PaddedTargetEvent) -> None:
        delivered.append(item)

    bus = InMemoryEventBus()
    bus.register(PaddedTargetEvent, on_order)
    cfg = SimpleNamespace(
        broker="shm", subscription_source="listener", package=None, subscriptions={}
    )
    serializer = JsonEventSerializer(allowed_event_types=[PaddedTargetEvent])
    consumer = ShmConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=consumer_targets(bus, cfg, "inventory"),
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        await registry.publish(
            "shm: orders.placed",
            serializer.serialize(PaddedTargetEvent("o-1")),
            {"event_type": f"{__name__}.PaddedTargetEvent"},
        )
        deadline = asyncio.get_running_loop().time() + 5.0
        while not delivered and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        assert delivered == [PaddedTargetEvent("o-1")]
    finally:
        await consumer.stop()
        await broker.close()
        broker._ring.unlink()
