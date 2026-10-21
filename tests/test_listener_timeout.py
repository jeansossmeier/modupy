"""``@listener(timeout=...)`` bounds one listener invocation on every dispatch
path: the in-memory publish, the durable outbox dispatch and the cross-process
consumer's ``dispatch_local``. An expired budget is a listener failure, so each
path's existing failure handling (raise, failed attempt, redelivery) applies.
"""

import asyncio
import functools
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from modulith import EventPublication, ListenerTimeoutError, configure, event, listener
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class Slow:
    n: int


@pytest.fixture(autouse=True)
def _reset() -> Any:
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    yield
    asyncio.run(outbox.shutdown())
    _runtime._reset_for_testing()
    outbox._reset_for_testing()


class _Store:
    """In-memory PublicationStore: save() is an upsert by id."""

    def __init__(self) -> None:
        self.rows: dict[UUID, EventPublication] = {}
        self.completed: list[UUID] = []

    async def save(self, publication: EventPublication) -> None:
        self.rows[publication.id] = publication

    async def mark_complete(self, publication_id: UUID) -> None:
        self.completed.append(publication_id)

    async def find_incomplete(self, older_than: Any) -> list[EventPublication]:
        return []

    async def archive(self, publication_id: UUID) -> None:
        pass

    async def delete(self, publication_id: UUID) -> None:
        pass


async def test_async_listener_past_its_timeout_is_cancelled_and_fails_the_publish() -> None:
    configure(package="timeouttest", auto_discover=False)
    cancelled = asyncio.Event()

    @listener(timeout=0.05)
    async def hang(evt: Slow) -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    started = time.monotonic()
    with pytest.raises(ListenerTimeoutError, match=r"hang did not finish within 0\.05s"):
        await _runtime.publish(Slow(n=1))
    assert time.monotonic() - started < 1
    assert cancelled.is_set()


async def test_listener_that_swallows_its_cancellation_completes_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    configure(package="timeouttest", auto_discover=False)
    seen: list[int] = []

    @listener(timeout=0.05)
    async def stubborn(evt: Slow) -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        seen.append(evt.n)

    with caplog.at_level(logging.WARNING, logger="modulith"):
        await _runtime.publish(Slow(n=3))
    assert seen == [3]
    assert "stubborn returned after its 0.05s timeout" in caplog.text


async def test_task_group_cancelled_by_the_budget_still_fails_as_a_timeout() -> None:
    configure(package="timeouttest", auto_discover=False)

    @listener(timeout=0.05)
    async def fan_out(evt: Slow) -> None:
        async def child() -> None:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise RuntimeError("cleanup failed while cancelled") from None

        async with asyncio.TaskGroup() as group:
            group.create_task(child())

    with pytest.raises(ListenerTimeoutError, match="fan_out did not finish within"):
        await _runtime.publish(Slow(n=1))


async def test_listener_inside_its_timeout_runs_to_completion() -> None:
    configure(package="timeouttest", auto_discover=False)
    seen: list[int] = []

    @listener(timeout=5)
    async def quick(evt: Slow) -> None:
        await asyncio.sleep(0)
        seen.append(evt.n)

    await _runtime.publish(Slow(n=7))
    assert seen == [7]


async def test_timeout_error_raised_by_the_listener_itself_is_not_relabelled() -> None:
    configure(package="timeouttest", auto_discover=False)

    @listener(timeout=5)
    async def own_timeout(evt: Slow) -> None:
        raise TimeoutError("upstream API timed out")

    with pytest.raises(TimeoutError, match="upstream API timed out") as info:
        await _runtime.publish(Slow(n=1))
    assert not isinstance(info.value, ListenerTimeoutError)


def test_sync_listener_rejects_a_timeout() -> None:
    """Failing a sync listener's delivery while its thread runs on would let the
    retry run alongside it, so a timeout is refused at registration."""
    configure(package="timeouttest", auto_discover=False)

    def block(evt: Slow) -> None:
        pass

    with pytest.raises(TypeError, match="block is a plain def, which cannot take a timeout"):
        listener(timeout=5)(block)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    assert _runtime.event_bus.listeners_for(Slow) == []


async def _notify(channel: str, evt: Slow) -> None:
    pass


async def _audit(channel: str, evt: Slow) -> None:
    pass


def test_timed_partial_keeps_the_outbox_id_of_the_function_it_wraps() -> None:
    configure(package="timeouttest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    bare = functools.partial(_notify, "smtp")

    listener(timeout=5)(functools.partial(_notify, "smtp"))
    listener(timeout=5)(functools.partial(_audit, "smtp"))

    assert _runtime.event_bus is not None
    ids = [outbox._listener_id(h) for h in _runtime.event_bus.listeners_for(Slow)]
    assert ids == [outbox._listener_id(bare), f"{__name__}._audit"]


async def test_outbox_records_a_timed_out_listener_as_a_failed_attempt() -> None:
    store = _Store()
    outbox.configure(store, JsonEventSerializer(allowed_event_types=[Slow]), start_loop=False)
    configure(package="timeouttest", auto_discover=False)
    _runtime.ensure_bootstrapped()

    @listener(timeout=0.05)
    async def hang(evt: Slow) -> None:
        await asyncio.sleep(3600)

    assert _runtime.event_bus is not None
    (registered,) = _runtime.event_bus.listeners_for(Slow)
    # The bounded wrapper keeps the handler's outbox identity, so rows persisted
    # before a timeout was added still resolve to it.
    assert outbox._listener_id(registered) == outbox._listener_id(hang)
    pub = EventPublication(
        id=uuid4(),
        payload=JsonEventSerializer().serialize(Slow(n=1)),
        event_type=f"{Slow.__module__}.{Slow.__qualname__}",
        listener=outbox._listener_id(registered),
        published_at=datetime.now(UTC),
    )
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert store.completed == []
    assert pub.attempt_count == 1
    assert pub.last_error is not None and "did not finish within 0.05s" in pub.last_error


async def test_cross_process_dispatch_raises_for_a_timed_out_listener() -> None:
    configure(package="timeouttest", auto_discover=False)
    _runtime.ensure_bootstrapped()

    @listener(timeout=0.05)
    async def hang(evt: Slow) -> None:
        await asyncio.sleep(3600)

    assert _runtime.event_bus is not None
    with pytest.raises(ListenerTimeoutError):
        await _runtime.dispatch_local(Slow(n=1), _runtime.event_bus)


def test_timeout_keeps_the_manifest_origin_marker() -> None:
    configure(package="timeouttest", auto_discover=False)
    _runtime.ensure_bootstrapped()

    @listener(timeout=1)
    async def marked(evt: Slow) -> None:
        pass

    assert _runtime.event_bus is not None
    (registered,) = _runtime.event_bus.listeners_for(Slow)
    assert getattr(registered, "__modulith_sync_wrapped__", None) is marked


@pytest.mark.parametrize("bad", ["5", True, [1]])
def test_listener_rejects_a_non_numeric_timeout(bad: object) -> None:
    with pytest.raises(TypeError, match="timeout"):
        listener(timeout=bad)  # type: ignore[call-overload]


@pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), 10**400])
def test_listener_rejects_a_non_positive_or_non_finite_timeout(bad: float) -> None:
    with pytest.raises(ValueError, match="timeout"):
        listener(timeout=bad)
