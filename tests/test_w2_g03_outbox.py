"""Regression tests for the W2 G03 outbox audit findings.

Each test cites the audit finding id it reproduces in its docstring. These
exercise the storage-agnostic plugin logic against in-memory stub stores —
no database. Async/timing behavior is synchronized with events and barriers,
never with sleeps.
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from modulith import EventPublication, event, hookimpl
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

# ---------------------------------------------------------------------------
# Module-scope event (so the serializer can resolve the FQCN on round trips).
# ---------------------------------------------------------------------------


@event
@dataclass(frozen=True)
class G03Event:
    value: int


received: list[int] = []


async def record(event: G03Event) -> None:
    received.append(event.value)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeSession:
    """Stand-in for a SQLAlchemy AsyncSession — only ``.info`` is used."""

    def __init__(self) -> None:
        self.info: dict[str, object] = {}


class StubStore:
    """Minimal in-memory PublicationStore; save() is an upsert-by-id."""

    def __init__(self) -> None:
        self.rows: dict[UUID, EventPublication] = {}
        self.find_incomplete_calls: list[timedelta] = []
        self.completed: list[UUID] = []
        self.swept = asyncio.Event()

    async def save(self, publication: EventPublication) -> None:
        self.rows[publication.id] = publication

    async def mark_complete(self, publication_id: UUID) -> None:
        self.completed.append(publication_id)
        row = self.rows.get(publication_id)
        if row is not None:
            row.completed_at = datetime.now(UTC)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        self.find_incomplete_calls.append(older_than)
        self.swept.set()
        now = datetime.now(UTC)
        return [
            p
            for p in self.rows.values()
            if p.completed_at is None
            and p.published_at is not None
            and (now - p.published_at) >= older_than
        ]

    async def archive(self, publication_id: UUID) -> None:
        self.rows.pop(publication_id, None)

    async def delete(self, publication_id: UUID) -> None:
        self.rows.pop(publication_id, None)


@pytest.fixture(autouse=True)
def _reset() -> Any:
    received.clear()
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    yield
    asyncio.run(outbox.shutdown())
    _runtime._reset_for_testing()
    outbox._reset_for_testing()


def _bootstrap_with_listener(handler: Any) -> None:
    _runtime.configure(package="g03test", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(G03Event, handler)


def _make_pub(handler: Any, *, value: int = 1, **overrides: Any) -> EventPublication:
    serializer = JsonEventSerializer()
    defaults: dict[str, Any] = dict(
        id=uuid4(),
        payload=serializer.serialize(G03Event(value=value)),
        event_type=f"{G03Event.__module__}.{G03Event.__qualname__}",
        listener=outbox._listener_id(handler),
        published_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return EventPublication(**defaults)


# ---------------------------------------------------------------------------
# A5-r4-175 — completion failure must record a failed attempt
# ---------------------------------------------------------------------------


class FailingCompletionStore(StubStore):
    async def mark_complete(self, publication_id: UUID) -> None:
        raise ConnectionError("db down at completion time")


async def test_completion_failure_records_failed_attempt() -> None:
    """A5-r4-175: a failure in _complete() must route through _record_failure
    so attempt_count/last_error/backoff/dead-lettering engage, instead of the
    exception escaping with the record forever looking never-attempted."""
    store = FailingCompletionStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=21)
    await store.save(pub)

    # Must not propagate the store's ConnectionError out of dispatch.
    await outbox._dispatch_publication(pub)

    assert received == [21]  # the listener DID run (at-least-once, not lost)
    assert pub.attempt_count == 1  # the completion failure aged the record
    assert pub.last_error is not None and "db down" in pub.last_error
    assert pub.last_attempt_at is not None
    assert not outbox._backoff_elapsed(pub)  # backoff now gates immediate retry


# ---------------------------------------------------------------------------
# A5-r2-84 / A5-r4-176 — observe-only hookimpls must never gate outbox dispatch
# (fixed at the plugin-manager level by the G09 observe shield; these lock the
# contract on the outbox dispatch path specifically)
# ---------------------------------------------------------------------------


class _RaisingObserverPlugin:
    @hookimpl
    def modulith_on_listener_dispatch(self, event: Any, listener_name: str) -> None:
        raise RuntimeError("buggy tracer: dispatch span failed")

    @hookimpl
    def modulith_on_listener_complete(self, event: Any, listener_name: str) -> None:
        raise RuntimeError("buggy tracer: complete span failed")

    @hookimpl
    def modulith_on_listener_error(self, event: Any, listener_name: str) -> None:
        raise RuntimeError("buggy tracer: error span failed")


async def test_raising_dispatch_hookimpl_does_not_gate_listener() -> None:
    """A5-r2-84: a raising modulith_on_listener_dispatch hookimpl must not
    block listener invocation — the hookspec promises 'it observes, it
    doesn't gate'."""
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)
    assert _runtime.plugin_manager is not None
    _runtime.plugin_manager.register(_RaisingObserverPlugin())

    pub = _make_pub(record, value=41)
    await store.save(pub)

    await outbox._dispatch_publication(pub)  # must not propagate

    assert received == [41]  # the listener ran regardless
    assert pub.completed_at is not None  # and the publication completed


async def test_raising_error_and_complete_hookimpls_do_not_mask_failure() -> None:
    """A5-r4-176: raising modulith_on_listener_error/_complete hookimpls must
    be swallowed ('must not re-raise') — not escape dispatch and kill the
    retry loop while masking the listener's own failure handling."""

    async def boom(event: G03Event) -> None:
        raise ValueError("listener failed")

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(boom)
    assert _runtime.plugin_manager is not None
    _runtime.plugin_manager.register(_RaisingObserverPlugin())

    pub = _make_pub(boom, value=42)
    await store.save(pub)

    await outbox._dispatch_publication(pub)  # must not propagate

    assert pub.attempt_count == 1  # the failure was still recorded
    assert pub.last_error is not None and "listener failed" in pub.last_error
    assert pub.completed_at is None  # stays incomplete for the retry loop


# ---------------------------------------------------------------------------
# S1-r2-100 — retry-loop task must not inherit the creating publish's session
# ---------------------------------------------------------------------------


async def test_retry_loop_dispatch_does_not_inherit_bound_session() -> None:
    """S1-r2-100: the retry task copies the ambient contextvars at creation
    time; when it is lazily started during a transactional publish it froze
    the request's bound session forever, so cascading publishes from
    retry-dispatched listeners enlisted in a stale, never-committed session."""
    store = StubStore()
    sessions_seen: list[object] = []
    delivered = asyncio.Event()

    async def spy(event: G03Event) -> None:
        sessions_seen.append(outbox._current_session.get())
        delivered.set()

    _bootstrap_with_listener(spy)

    session = FakeSession()
    token = outbox._current_session.set(session)
    try:
        # The retry task is created while a request session is bound —
        # exactly the lazy-start situation of the first transactional publish.
        outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
        pub = _make_pub(spy, value=31)
        await store.save(pub)  # no suspension point — sweep hasn't run yet
    finally:
        outbox._current_session.reset(token)

    await asyncio.wait_for(delivered.wait(), timeout=2)  # crash sweep dispatches
    assert sessions_seen == [None]


# ---------------------------------------------------------------------------
# A5-r1-14 — dead_letter_after_attempts must be >= 1
# ---------------------------------------------------------------------------


def test_configure_rejects_nonpositive_dead_letter_threshold() -> None:
    """A5-r1-14: a threshold <= 0 makes every record instantly dead-lettered,
    silently blackholing crash-recovered publications forever."""
    store = StubStore()
    for bad in (0, -3):
        with pytest.raises(ValueError, match="dead_letter_after_attempts"):
            outbox.configure(
                store, JsonEventSerializer(), dead_letter_after_attempts=bad, start_loop=False
            )


# ---------------------------------------------------------------------------
# A5-r1-15 — a failing sweep must not kill the retry loop
# ---------------------------------------------------------------------------


class FlakyStore(StubStore):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0
        self.survived = asyncio.Event()

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        self.calls += 1
        if self.calls == 2:
            raise ConnectionError("simulated transient store error")
        if self.calls >= 3:
            self.survived.set()
        return []


async def test_sweep_failure_does_not_kill_retry_loop() -> None:
    """A5-r1-15: a transient store error during one sweep must only skip that
    sweep — previously it permanently killed the retry-loop task."""
    store = FlakyStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=0.01)

    await asyncio.wait_for(store.survived.wait(), timeout=2)

    task = outbox._retry_task
    assert task is not None and not task.done()


# ---------------------------------------------------------------------------
# A5-r2-83 / S1-r1-47 — _reset_for_testing must cancel the retry task
# ---------------------------------------------------------------------------


async def test_reset_for_testing_cancels_retry_task() -> None:
    """A5-r2-83 / S1-r1-47: _reset_for_testing() dropped the task reference
    without cancelling, leaking ghost retry loops that keep polling whatever
    store is currently bound."""
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    task = outbox._retry_task
    assert task is not None

    outbox._reset_for_testing()

    done, _pending = await asyncio.wait({task}, timeout=1)
    assert task in done and task.cancelled()


# ---------------------------------------------------------------------------
# A5-r2-85 — naive datetimes from a nonconforming store must not crash backoff
# ---------------------------------------------------------------------------


def test_backoff_elapsed_tolerates_naive_datetimes() -> None:
    """A5-r2-85: a custom store may round-trip naive datetimes (SQLite loses
    tz); _backoff_elapsed must interpret them as UTC instead of crashing the
    retry loop with a naive/aware subtraction TypeError."""
    naive_now_utc = datetime.now(UTC).replace(tzinfo=None)
    pub = EventPublication(
        id=uuid4(),
        payload=b"{}",
        event_type="x.Y",
        listener="x.on_y",
        published_at=naive_now_utc,
        attempt_count=1,
        last_attempt_at=naive_now_utc,
    )
    assert outbox._backoff_elapsed(pub) is False  # just attempted → still backing off

    pub.last_attempt_at = naive_now_utc - timedelta(hours=1)
    assert outbox._backoff_elapsed(pub) is True


# ---------------------------------------------------------------------------
# A5-r3-134 — shutdown() window must not allow a second retry loop to spawn
# ---------------------------------------------------------------------------


async def test_shutdown_window_does_not_spawn_second_retry_loop() -> None:
    """A5-r3-134: shutdown() nulled _retry_task before cancellation completed,
    so a concurrent transactional publish spun up a second retry loop that
    survived shutdown entirely."""
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    task1 = outbox._retry_task
    assert task1 is not None

    shutdown_task = asyncio.ensure_future(outbox.shutdown())
    await asyncio.sleep(0)  # shutdown() requested cancellation; not yet completed
    assert not shutdown_task.done()

    # A transactional publish landing in the window must not spawn a new loop.
    outbox._ensure_retry_loop()
    spawned = outbox._retry_task

    await shutdown_task

    assert spawned is task1  # the pending-cancellation task still owns the slot
    assert outbox._retry_task is None  # shutdown() cleared it after completion
    assert task1.cancelled()


# ---------------------------------------------------------------------------
# A5-r1-16 — re-configure must not silently reuse the stale retry loop
# ---------------------------------------------------------------------------


async def test_reconfigure_replaces_retry_loop_and_resweeps_new_store() -> None:
    """A5-r1-16 (adjudicated): configure() with a live retry task must cancel
    and restart it so the new store gets its own crash sweep."""
    store_a = StubStore()
    store_b = StubStore()
    outbox.configure(store_a, JsonEventSerializer(), retry_interval_seconds=60)
    task_a = outbox._retry_task
    assert task_a is not None

    outbox.configure(store_b, JsonEventSerializer(), retry_interval_seconds=60)
    task_b = outbox._retry_task

    assert task_b is not None and task_b is not task_a
    done, _pending = await asyncio.wait({task_a}, timeout=1)
    assert task_a in done and task_a.cancelled()

    await asyncio.wait_for(store_b.swept.wait(), timeout=2)
    assert timedelta(0) in store_b.find_incomplete_calls  # fresh crash sweep


# ---------------------------------------------------------------------------
# S1-r2-102 — retry-loop startup must be single-shot across OS threads/loops
# ---------------------------------------------------------------------------


def test_ensure_retry_loop_single_task_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S1-r2-102: two event loops on two OS threads (main loop + sync.py's
    daemon-thread loop) racing _ensure_retry_loop() must produce exactly one
    retry-loop task — the module slot is process-global.

    Seam: _ensure_retry_loop calls _retry_loop() to build the coroutine INSIDE
    its check-then-create window, so a barrier there forces both threads into
    the window whenever nothing serializes them."""
    barrier = threading.Barrier(2)
    done_barrier = threading.Barrier(2)  # no cleanup until both threads ensured
    creations: list[int] = []

    def fake_retry_loop() -> Any:
        creations.append(threading.get_ident())
        with suppress(threading.BrokenBarrierError):
            barrier.wait(timeout=0.5)

        async def stub() -> None:
            await asyncio.sleep(3600)

        return stub()

    monkeypatch.setattr(outbox, "_retry_loop", fake_retry_loop)

    def thread_body() -> None:
        loop = asyncio.new_event_loop()
        try:

            async def driver() -> None:
                outbox._ensure_retry_loop()

            loop.run_until_complete(driver())

            # Cancelling this thread's stub task before the OTHER thread ran
            # its check would make a second creation legitimate (the slot's
            # task really is done) — hold both threads here until both checked.
            done_barrier.wait(timeout=5)

            async def cleanup() -> None:
                tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            loop.run_until_complete(cleanup())
        finally:
            loop.close()

    threads = [threading.Thread(target=thread_body) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    # The surviving reference points at a task on an already-closed foreign
    # loop; clear it so the fixture teardown doesn't try to await it.
    outbox._retry_task = None
    assert len(creations) == 1


# ---------------------------------------------------------------------------
# S1-r2-103 — the in-flight guard must be atomic across OS threads/loops
# ---------------------------------------------------------------------------


def test_inflight_guard_is_atomic_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S1-r2-103: the documented same-process non-reentrancy guarantee must
    hold across two event loops on two OS threads — the plain-set check+add
    let both dispatchers pass the check before either added, delivering the
    same publication to its listener concurrently."""
    check_barrier = threading.Barrier(2)
    overlap_barrier = threading.Barrier(2)
    counts_lock = threading.Lock()
    active = 0
    max_active = 0

    class BarrieredSet(set):  # type: ignore[type-arg]
        """Set whose membership check computes its answer, then parks on a
        barrier before returning it — freezing both threads' check results
        inside the check→add window when nothing serializes them."""

        def __contains__(self, item: object) -> bool:
            result = super().__contains__(item)
            with suppress(threading.BrokenBarrierError):
                check_barrier.wait(timeout=0.5)
            return result

    async def slow_listener(event: G03Event) -> None:
        nonlocal active, max_active
        with counts_lock:
            active += 1
            max_active = max(max_active, active)
        # If BOTH dispatch attempts entered the listener, they meet here and
        # the overlap is recorded; a lone entrant just times out and proceeds.
        with suppress(threading.BrokenBarrierError):
            overlap_barrier.wait(timeout=0.5)
        with counts_lock:
            active -= 1

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(slow_listener)
    monkeypatch.setattr(outbox, "_inflight_ids", BarrieredSet())

    pub_id = uuid4()

    def thread_body() -> None:
        pub = _make_pub(slow_listener, id=pub_id)
        asyncio.run(outbox._dispatch_publication(pub))

    threads = [threading.Thread(target=thread_body) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert max_active == 1  # never delivered concurrently within one process
