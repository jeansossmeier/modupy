"""Regression tests for outbox claim / concurrency behaviour.

Each test's docstring states the contract it pins. These
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
from modulith.config import ConfigurationError
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
    _runtime.configure(package="outbox_claims_test", auto_discover=False)
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
# Completion failure must record a failed attempt
# ---------------------------------------------------------------------------


class FailingCompletionStore(StubStore):
    async def mark_complete(self, publication_id: UUID) -> None:
        raise ConnectionError("db down at completion time")


async def test_completion_failure_records_failed_attempt() -> None:
    """A failure in _complete() must route through _record_failure
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


async def test_completion_failure_does_not_set_completed_at() -> None:
    """``completed_at`` must be set only AFTER the store's completion
    write actually succeeds. Setting it beforehand (then failing the store
    call) leaves the in-memory record looking completed while the resave in
    _record_failure persists an inconsistent row: attempt_count incremented
    on a record that also claims to be completed."""
    store = FailingCompletionStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=22)
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert pub.completed_at is None


# ---------------------------------------------------------------------------
# Retry-task shutdown must be safe across event loops
# ---------------------------------------------------------------------------


async def test_shutdown_stops_retry_task_running_on_a_foreign_loop() -> None:
    """shutdown() must work when the retry task lives on a DIFFERENT event
    loop than the one shutdown() is awaited from (e.g. sync.py's persistent
    daemon-thread loop runs the retry task while the app's main loop awaits
    outbox.shutdown() during teardown). Directly ``await``-ing a task bound
    to another loop raises ("Task got Future attached to a different
    loop") — shutdown() must request cancellation thread-safely and poll for
    completion instead of awaiting the foreign task object."""
    store = StubStore()
    foreign_loop = asyncio.new_event_loop()
    thread = threading.Thread(target=foreign_loop.run_forever, daemon=True)
    thread.start()
    try:

        async def _configure_on_foreign_loop() -> None:
            outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)

        asyncio.run_coroutine_threadsafe(_configure_on_foreign_loop(), foreign_loop).result(
            timeout=5
        )

        task = outbox._retry_task
        assert task is not None
        assert task.get_loop() is foreign_loop

        await asyncio.wait_for(outbox.shutdown(), timeout=5)

        assert outbox._retry_task is None
        assert task.done()
    finally:
        foreign_loop.call_soon_threadsafe(foreign_loop.stop)
        thread.join(timeout=2)
        foreign_loop.close()


class HeldFindStore(StubStore):
    """``find_incomplete`` waits for ``gate`` and records how it ended."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.gate: asyncio.Event | None = None
        self.outcome: str | None = None

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        self.gate = asyncio.Event()
        self.entered.set()
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.outcome = "cancelled"
            raise
        self.outcome = "completed"
        return []


async def test_shutdown_from_another_loop_lets_the_in_flight_store_call_finish() -> None:
    store = HeldFindStore()
    foreign_loop = asyncio.new_event_loop()
    thread = threading.Thread(target=foreign_loop.run_forever, daemon=True)
    thread.start()
    try:

        async def _configure_on_foreign_loop() -> None:
            outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)

        asyncio.run_coroutine_threadsafe(_configure_on_foreign_loop(), foreign_loop).result(
            timeout=5
        )
        task = outbox._retry_task
        assert task is not None
        assert await asyncio.to_thread(store.entered.wait, 5)

        stopping = asyncio.create_task(outbox.shutdown())
        await asyncio.sleep(0.05)
        assert not stopping.done()
        assert store.gate is not None
        foreign_loop.call_soon_threadsafe(store.gate.set)
        await asyncio.wait_for(stopping, timeout=5)

        assert store.outcome == "completed"
        assert task.done() and not task.cancelled()
        assert outbox._retry_task is None
    finally:
        foreign_loop.call_soon_threadsafe(foreign_loop.stop)
        thread.join(timeout=2)
        foreign_loop.close()


async def test_a_retry_loop_started_by_configure_after_shutdown_dispatches_again() -> None:
    store = StubStore()
    _bootstrap_with_listener(record)
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    await outbox.shutdown()
    pub = _make_pub(record, value=51)
    await store.save(pub)

    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    task = outbox._retry_task
    assert task is not None
    while not store.find_incomplete_calls:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.01)

    assert received == [51]
    assert not task.done()


# ---------------------------------------------------------------------------
# Observe-only hookimpls must never gate outbox dispatch
# (fixed at the plugin-manager level by the observe-shield; these lock the
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
    """A raising modulith_on_listener_dispatch hookimpl must not
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
    """Raising modulith_on_listener_error/_complete hookimpls must
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
# Retry-loop task must not inherit the creating publish's session
# ---------------------------------------------------------------------------


async def test_retry_loop_dispatch_does_not_inherit_bound_session() -> None:
    """The retry task copies the ambient contextvars at creation
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
# dead_letter_after_attempts must be >= 1
# ---------------------------------------------------------------------------


def test_configure_rejects_nonpositive_dead_letter_threshold() -> None:
    """A threshold <= 0 makes every record instantly dead-lettered,
    silently blackholing crash-recovered publications forever."""
    store = StubStore()
    for bad in (0, -3):
        with pytest.raises(ValueError, match="dead_letter_after_attempts"):
            outbox.configure(
                store, JsonEventSerializer(), dead_letter_after_attempts=bad, start_loop=False
            )


# ---------------------------------------------------------------------------
# A failing sweep must not kill the retry loop
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
    """A transient store error during one sweep must only skip that
    sweep — previously it permanently killed the retry-loop task."""
    store = FlakyStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=0.01)

    await asyncio.wait_for(store.survived.wait(), timeout=2)

    task = outbox._retry_task
    assert task is not None and not task.done()


# ---------------------------------------------------------------------------
# _reset_for_testing must cancel the retry task
# ---------------------------------------------------------------------------


async def test_reset_for_testing_cancels_retry_task() -> None:
    """_reset_for_testing() dropped the task reference
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
# Naive datetimes from a nonconforming store must not crash backoff
# ---------------------------------------------------------------------------


def test_backoff_elapsed_tolerates_naive_datetimes() -> None:
    """A custom store may round-trip naive datetimes (SQLite loses
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
# shutdown() window must not allow a second retry loop to spawn
# ---------------------------------------------------------------------------


async def test_shutdown_window_does_not_spawn_second_retry_loop() -> None:
    """shutdown() nulled _retry_task before cancellation completed,
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
# Re-configure must not silently reuse the stale retry loop
# ---------------------------------------------------------------------------


async def test_reconfigure_replaces_retry_loop_and_resweeps_new_store() -> None:
    """configure() with a live retry task must cancel
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
# Retry-loop startup must be single-shot across OS threads/loops
# ---------------------------------------------------------------------------


def test_ensure_retry_loop_single_task_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two event loops on two OS threads (main loop + sync.py's
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
# The in-flight guard must be atomic across OS threads/loops
# ---------------------------------------------------------------------------


def test_inflight_guard_is_atomic_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The documented same-process non-reentrancy guarantee must
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


# ---------------------------------------------------------------------------
# advisory_lock configuration contract
# ---------------------------------------------------------------------------


class _AdvisoryStoreWithoutLookup(StubStore):
    """Advisory-locking capability only: no ``find_by_id``."""

    supports_advisory_lock = True

    async def try_lock_publication(self, publication_id: UUID) -> object | None:
        return object()

    async def unlock_publication(self, handle: object, publication_id: UUID) -> None:
        return None


class _AdvisoryStoreWithLookup(_AdvisoryStoreWithoutLookup):
    async def find_by_id(self, publication_id: UUID) -> EventPublication | None:
        return self.rows.get(publication_id)


def test_advisory_lock_requires_find_by_id() -> None:
    """Under the lock the row is re-read to see what a peer did since the
    sweep's snapshot; a store that cannot look a row up would redeliver from
    the stale snapshot, so configuring it is refused and names the method."""
    with pytest.raises(ConfigurationError, match="find_by_id"):
        outbox.configure(
            _AdvisoryStoreWithoutLookup(),
            JsonEventSerializer(),
            claim_strategy="advisory_lock",
            start_loop=False,
        )


def test_advisory_lock_accepts_a_store_with_find_by_id() -> None:
    outbox.configure(
        _AdvisoryStoreWithLookup(),
        JsonEventSerializer(),
        claim_strategy="advisory_lock",
        start_loop=False,
    )

    assert outbox._claim_strategy == "advisory_lock"


def test_find_by_id_is_not_needed_for_the_other_strategies() -> None:
    outbox.configure(
        _AdvisoryStoreWithoutLookup(),
        JsonEventSerializer(),
        claim_strategy="none",
        start_loop=False,
    )

    assert outbox._claim_strategy == "none"


def test_lock_connection_timeout_is_part_of_the_advisory_protocol() -> None:
    """The timeout a lock attempt can raise is defined beside the protocol
    that documents it, and stays importable from the outbox plugin."""
    from modulith import _claims

    assert issubclass(_claims.LockConnectionTimeout, Exception)
    assert outbox._LockConnectionTimeout is _claims.LockConnectionTimeout
    assert "LockConnectionTimeout" in (
        _claims.AdvisoryLockingStore.try_lock_publication.__doc__ or ""
    )
    assert "find_by_id" in (_claims.AdvisoryLockingStore.__doc__ or "")


# ---------------------------------------------------------------------------
# claim_batch's optional keywords reach only the stores that declare them
# ---------------------------------------------------------------------------


class _ExcludingStore(StubStore):
    def __init__(self) -> None:
        super().__init__()
        self.claim_calls: list[dict[str, Any]] = []

    async def claim_batch(
        self,
        *,
        owner: str,
        batch_size: int,
        lease_seconds: float,
        older_than: timedelta,
        exclude_ids: frozenset[UUID] = frozenset(),
    ) -> list[EventPublication]:
        self.claim_calls.append({"exclude_ids": exclude_ids})
        return []


class _LegacyClaimingStore(StubStore):
    """Declares exactly the four keywords the protocol had before the optional
    ones: a call carrying anything else raises TypeError."""

    def __init__(self) -> None:
        super().__init__()
        self.claim_calls = 0

    async def claim_batch(
        self, *, owner: str, batch_size: int, lease_seconds: float, older_than: timedelta
    ) -> list[EventPublication]:
        self.claim_calls += 1
        return []


class _WildcardClaimingStore(StubStore):
    def __init__(self) -> None:
        super().__init__()
        self.claim_kwargs: list[dict[str, Any]] = []

    async def claim_batch(self, **kwargs: Any) -> list[EventPublication]:
        self.claim_kwargs.append(kwargs)
        return []


async def test_lease_sweep_hands_its_in_flight_ids_to_a_store_that_declares_exclude_ids() -> None:
    """A row this process is delivering must not be claimed again by its own
    sweep, so the sweep passes the in-flight ids to the claim query."""
    store = _ExcludingStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    delivering = uuid4()
    with outbox._inflight_lock:
        outbox._inflight_ids.add(delivering)

    await outbox._sweep(timedelta(0))

    assert store.claim_calls == [{"exclude_ids": frozenset({delivering})}]


async def test_lease_sweep_calls_a_store_without_exclude_ids_as_before() -> None:
    """A third-party store written against the four-keyword protocol keeps
    being swept, with no TypeError, however many sweeps run."""
    store = _LegacyClaimingStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    with outbox._inflight_lock:
        outbox._inflight_ids.add(uuid4())

    await outbox._sweep(timedelta(0))
    await outbox._sweep(timedelta(0))

    assert store.claim_calls == 2


async def test_lease_sweep_does_not_offer_optional_keywords_to_a_catch_all_store() -> None:
    """Only a keyword the store names is passed: a ``**kwargs`` store receives
    the protocol's four and nothing it did not ask for."""
    store = _WildcardClaimingStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)

    await outbox._sweep(timedelta(0))

    assert [set(kwargs) for kwargs in store.claim_kwargs] == [
        {"owner", "batch_size", "lease_seconds", "older_than"}
    ]


def test_the_claiming_store_protocol_documents_exclude_ids() -> None:
    from modulith import _claims

    assert "exclude_ids" in (_claims.ClaimingStore.claim_batch.__doc__ or "")
