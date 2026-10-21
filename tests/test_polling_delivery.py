"""Delivery scheduling of the polling consumer behind the SHM and database brokers.

Each test runs against both brokers: they share ``PollingConsumer``, and each
implements the claim-time charge for a delivery its consumer died or wedged in.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from modulith import event
from modulith.adapters._consumer_protocol import RECLAIMED_MID_DISPATCH
from modulith.adapters._polling_consumer import PollingConsumer
from modulith.adapters.db_broker import DatabaseBroker, DatabaseConsumer
from modulith.adapters.shm_broker import ShmBroker, ShmConsumer
from modulith.event_bus import InMemoryEventBus
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class Ticket:
    name: str


EVENT_TYPE = f"{Ticket.__module__}.{Ticket.__qualname__}"
TARGET = "tickets"
GROUP = "modulith-agent"
SERIALIZER = JsonEventSerializer(allowed_event_types=[Ticket])


class _Crashable:
    """Drops every write once ``crashed`` is set, as a killed process makes none."""

    crashed = False

    async def renew_claims(
        self, row_ids: list[str], *, consumer_name: str, start_dispatch: bool = False
    ) -> int:
        if self.crashed:
            return 0
        return await super().renew_claims(  # type: ignore[misc, no-any-return]
            row_ids, consumer_name=consumer_name, start_dispatch=start_dispatch
        )

    async def ack(self, row_id: str, *, consumer_name: str) -> None:
        if not self.crashed:
            await super().ack(row_id, consumer_name=consumer_name)  # type: ignore[misc]

    async def fail(self, row_id: str, error: str, *, consumer_name: str, max_attempts: int) -> bool:
        if self.crashed:
            return False
        return await super().fail(  # type: ignore[misc, no-any-return]
            row_id, error, consumer_name=consumer_name, max_attempts=max_attempts
        )

    async def release_claims(self, row_ids: list[str], *, consumer_name: str) -> int:
        if self.crashed:
            return 0
        return await super().release_claims(  # type: ignore[misc, no-any-return]
            row_ids, consumer_name=consumer_name
        )

    async def release_interrupted_claims(self, row_ids: list[str], *, consumer_name: str) -> int:
        if self.crashed:
            return 0
        return await super().release_interrupted_claims(  # type: ignore[misc, no-any-return]
            row_ids, consumer_name=consumer_name
        )


class _CrashableShm(_Crashable, ShmBroker):
    pass


class _CrashableDatabase(_Crashable, DatabaseBroker):
    pass


@pytest.fixture(params=["shm", "database"])
async def broker(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Any]:
    if request.param == "shm":
        shm = _CrashableShm(shm_name="delivery-hints", db_path=str(tmp_path / "shm.db"))
        try:
            yield shm
        finally:
            await shm.close()
            shm._ring.unlink()
        return
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'broker.db'}", poolclass=NullPool
    )
    database = _CrashableDatabase(engine=engine)
    try:
        yield database
    finally:
        await engine.dispose()


def _consumer(broker: Any, bus: InMemoryEventBus, name: str, **options: Any) -> PollingConsumer:
    factory = ShmConsumer if isinstance(broker, ShmBroker) else DatabaseConsumer
    consumer: PollingConsumer = factory(
        broker=broker,
        bus=bus,
        serializer=SERIALIZER,
        consumer_name=name,
        group=GROUP,
        targets=[TARGET],
        poll_interval_s=0.01,
        **options,
    )
    return consumer


async def _publish(broker: Any, *names: str) -> None:
    for name in names:
        await broker.publish(TARGET, SERIALIZER.serialize(Ticket(name)), {"event_type": EVENT_TYPE})


async def _until(predicate: Callable[[], bool], *, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.01)


async def test_a_poison_row_that_kills_its_process_dead_letters_alone(
    broker: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Healthy rows running beside a row that kills the process are charged by
    the first reclaim with it. Each charged row then runs alone, so the crashes
    that follow charge only the killer: it is dead-lettered, with an ERROR,
    and every healthy row is delivered."""
    max_attempts = 3
    healthy = ["h1", "h2", "h3", "h4"]
    delivered: list[str] = []
    crashed = asyncio.Event()

    async def triage(ticket: Ticket) -> None:
        if ticket.name == "poison":
            # Let the rows claimed with it start, so the crash catches them running.
            await asyncio.sleep(0.15)
            broker.crashed = True
            crashed.set()
            await asyncio.Event().wait()
        await asyncio.sleep(0.3)
        delivered.append(ticket.name)

    bus = InMemoryEventBus()
    bus.register(Ticket, triage)
    await broker.subscribe([TARGET], GROUP)
    await _publish(broker, "h1", "h2", "poison", "h3", "h4")
    caplog.set_level(logging.WARNING, logger="modulith.adapters")

    async def dead_letters() -> list[Any]:
        return list(await broker.list_dead_letters())

    for incarnation in range(1, 2 * max_attempts + 2):
        crashed.clear()
        broker.crashed = False
        consumer = _consumer(
            broker,
            bus,
            f"agent-{incarnation}",
            max_attempts=max_attempts,
            reclaim_stale_seconds=0.3,
        )
        consumer._stop_drain_grace_s = 0.0
        await consumer.start()
        try:
            deadline = asyncio.get_running_loop().time() + 10.0
            while not crashed.is_set() and not await dead_letters():
                assert asyncio.get_running_loop().time() < deadline, "no crash and no dead letter"
                await asyncio.sleep(0.02)
            if not crashed.is_set():
                # The healthy rows still queued are delivered next; a schedule
                # that dead-lettered them leaves the assertions below to say so.
                with contextlib.suppress(AssertionError):
                    await _until(lambda: sorted(delivered) == healthy, timeout=5.0)
        finally:
            await consumer.stop()
        if not crashed.is_set():
            break

    dead = await dead_letters()
    assert sorted(delivered) == healthy
    assert [(row.event_type, row.attempts) for row in dead] == [(EVENT_TYPE, max_attempts)]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "dead-lettered" in errors[0].getMessage()
    assert EVENT_TYPE in errors[0].getMessage()


async def test_the_renew_deadline_counts_from_each_rows_own_start(broker: Any) -> None:
    """A row that waited for a dispatch slot keeps its claim and leaves health
    ready while its listener runs within the deadline, though the batch it was
    claimed in started longer ago than that."""
    reclaim_stale_seconds = 0.2  # renew deadline: 2 s
    started: list[str] = []

    async def triage(ticket: Ticket) -> None:
        started.append(ticket.name)
        await asyncio.sleep(1.4)

    bus = InMemoryEventBus()
    bus.register(Ticket, triage)
    await broker.subscribe([TARGET], GROUP)
    await _publish(broker, "a", "b", "c")
    consumer = _consumer(
        broker,
        bus,
        "agent-1",
        dispatch_concurrency=2,
        reclaim_stale_seconds=reclaim_stale_seconds,
    )
    await consumer.start()
    try:
        await _until(lambda: len(started) == 2)
        await asyncio.sleep(2.4)
        assert len(started) == 3, "the third row should be running by now"
        assert consumer.health().status == "ready"
        peer = await broker.claim_batch(
            GROUP,
            batch_size=10,
            consumer_name="agent-2",
            reclaim_stale_seconds=reclaim_stale_seconds,
        )
        assert peer == []
    finally:
        await consumer.stop()


async def test_a_hung_listener_does_not_stop_the_consumer_claiming_new_rows(broker: Any) -> None:
    """Rows published while one listener hangs are claimed and delivered as
    dispatch slots free up, and no more listeners than dispatch_concurrency
    ever run at once."""
    release = asyncio.Event()
    delivered: list[str] = []
    running = 0
    most_running = 0

    async def triage(ticket: Ticket) -> None:
        nonlocal running, most_running
        running += 1
        most_running = max(most_running, running)
        try:
            if ticket.name == "hang":
                await release.wait()
            else:
                await asyncio.sleep(0.05)
            delivered.append(ticket.name)
        finally:
            running -= 1

    bus = InMemoryEventBus()
    bus.register(Ticket, triage)
    await broker.subscribe([TARGET], GROUP)
    await _publish(broker, "hang")
    consumer = _consumer(broker, bus, "agent-1", dispatch_concurrency=2, reclaim_stale_seconds=5.0)
    await consumer.start()
    try:
        await _until(lambda: running == 1)
        later = [f"t{index}" for index in range(6)]
        await _publish(broker, *later)
        await _until(lambda: sorted(delivered) == later, timeout=5.0)
        assert most_running == 2
    finally:
        release.set()
        await consumer.stop()


async def test_a_charged_reclaim_waits_out_the_failure_backoff(tmp_path: Path) -> None:
    """A reclaim that charges a delivery its consumer died in puts it back to
    pending for the failure backoff instead of handing it out at once; once
    the backoff ends it is claimed, marked as reclaimed mid-dispatch."""
    shm = ShmBroker(
        shm_name="backoff-hints",
        db_path=str(tmp_path / "backoff.db"),
        retry_backoff_base_seconds=0.4,
        retry_backoff_cap_seconds=0.4,
    )
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'b.db'}", poolclass=NullPool)
    database = DatabaseBroker(
        engine=engine, retry_backoff_base_seconds=0.4, retry_backoff_cap_seconds=0.4
    )
    try:
        for instance in (shm, database):
            await instance.subscribe([TARGET], GROUP)
            await _publish(instance, "a")
            (row,) = await instance.claim_batch(GROUP, batch_size=10, consumer_name="c1")
            await instance.renew_claims([row["id"]], consumer_name="c1", start_dispatch=True)

            reclaimed = await instance.claim_batch(
                GROUP, batch_size=10, consumer_name="c2", reclaim_stale_seconds=0.0
            )
            assert reclaimed == [], type(instance).__name__
            await asyncio.sleep(0.5)
            (later,) = await instance.claim_batch(GROUP, batch_size=10, consumer_name="c2")
            assert (later["attempts"], later["last_error"]) == (1, RECLAIMED_MID_DISPATCH)
    finally:
        await shm.close()
        shm._ring.unlink()
        await engine.dispose()


async def test_a_failed_delivery_waits_out_the_configured_backoff(tmp_path: Path) -> None:
    shm = ShmBroker(
        shm_name="fail-backoff-hints",
        db_path=str(tmp_path / "fail-backoff.db"),
        retry_backoff_base_seconds=0.4,
        retry_backoff_cap_seconds=0.4,
    )
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'f.db'}", poolclass=NullPool)
    database = DatabaseBroker(
        engine=engine, retry_backoff_base_seconds=0.4, retry_backoff_cap_seconds=0.4
    )
    try:
        for instance in (shm, database):
            await instance.subscribe([TARGET], GROUP)
            await _publish(instance, "a")
            (row,) = await instance.claim_batch(GROUP, batch_size=10, consumer_name="c1")
            await instance.fail(row["id"], "boom", consumer_name="c1", max_attempts=5)

            await asyncio.sleep(0.2)
            assert await instance.claim_batch(GROUP, batch_size=10, consumer_name="c1") == []
            await asyncio.sleep(0.3)
            (later,) = await instance.claim_batch(GROUP, batch_size=10, consumer_name="c1")
            assert (later["attempts"], later["last_error"]) == (1, "boom")
    finally:
        await shm.close()
        shm._ring.unlink()
        await engine.dispose()
