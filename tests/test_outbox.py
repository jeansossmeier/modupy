"""Behavioral tests for the transactional outbox plugin.

These exercise the storage-agnostic plugin logic against an in-memory stub
PublicationStore — no real database. The Postgres adapter (and its
crash-recovery integration test) live separately and are Docker-gated.

What is verified here:
  * configure() binds the store and starts the retry loop + crash sweep.
  * Publishing inside a bound transaction persists one publication per
    listener and does NOT dispatch in-memory (dispatch waits for commit).
  * _dispatch_publication delivers to the right listener and completes it
    per the configured completion mode; failures increment attempt_count,
    record last_error, and leave the record incomplete for retry.
  * The retry loop picks up stale incomplete publications.
  * Maintenance APIs (status / force_retry / purge_completed) behave.

The stub store mirrors the real adapter's contract: save() is an
upsert-by-id, and (like the SQLAlchemy adapter) it records the pending
publication id on the bound session's ``info`` dict so the "nothing
dispatches before commit" property is observable.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from modulith import EventPublication, event
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

# ---------------------------------------------------------------------------
# Module-scope event + handlers (module scope so the serializer can resolve
# the fully-qualified class name on the round trip).
# ---------------------------------------------------------------------------


@event
@dataclass(frozen=True)
class OutboxEvent:
    value: int


received: list[int] = []


async def record(event: OutboxEvent) -> None:
    received.append(event.value)


async def boom(event: OutboxEvent) -> None:
    raise ValueError("dispatch failed")


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeSession:
    """Stand-in for a SQLAlchemy AsyncSession — only ``.info`` is used."""

    def __init__(self) -> None:
        self.info: dict[str, object] = {}


class StubStore:
    """In-memory PublicationStore. save() is an upsert-by-id."""

    def __init__(self) -> None:
        self.rows: dict[UUID, EventPublication] = {}
        self.find_incomplete_calls: list[timedelta] = []
        self.completed: list[UUID] = []
        self.deleted: list[UUID] = []
        self.archived: list[UUID] = []

    async def save(self, publication: EventPublication) -> None:
        self.rows[publication.id] = publication
        session = outbox._current_session.get()
        if session is not None:
            pending = session.info.setdefault("_modulith_pending", [])
            pending.append(publication.id)  # type: ignore[union-attr]

    async def mark_complete(self, publication_id: UUID) -> None:
        self.completed.append(publication_id)
        row = self.rows.get(publication_id)
        if row is not None:
            row.completed_at = datetime.now(UTC)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        self.find_incomplete_calls.append(older_than)
        now = datetime.now(UTC)
        return [
            p
            for p in self.rows.values()
            if p.completed_at is None
            and p.published_at is not None
            and (now - p.published_at) >= older_than
        ]

    async def archive(self, publication_id: UUID) -> None:
        self.archived.append(publication_id)
        self.rows.pop(publication_id, None)

    async def delete(self, publication_id: UUID) -> None:
        self.deleted.append(publication_id)
        self.rows.pop(publication_id, None)

    # Duck-typed maintenance extensions (beyond the 5 core Protocol methods).
    async def count_completed(self) -> int:
        return sum(1 for p in self.rows.values() if p.completed_at is not None)

    async def purge_completed(self, older_than: timedelta) -> int:
        now = datetime.now(UTC)
        victims = [
            pid
            for pid, p in self.rows.items()
            if p.completed_at is not None and (now - p.completed_at) >= older_than
        ]
        for pid in victims:
            del self.rows[pid]
        return len(victims)


@pytest.fixture(autouse=True)
def _reset() -> None:
    received.clear()
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    yield
    asyncio.run(outbox.shutdown())
    _runtime._reset_for_testing()
    outbox._reset_for_testing()


def _bootstrap_with_listener(handler) -> None:
    """Bootstrap the runtime and register a single async listener."""
    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(OutboxEvent, handler)


def _make_pub(handler, *, value: int = 1, **overrides) -> EventPublication:
    serializer = JsonEventSerializer()
    defaults = dict(
        id=uuid4(),
        payload=serializer.serialize(OutboxEvent(value=value)),
        event_type=f"{OutboxEvent.__module__}.{OutboxEvent.__qualname__}",
        # Mirror persist(): the listener field is the module-qualified identity,
        # not the bare qualname, so _resolve_listener matches on the round trip.
        listener=outbox._listener_id(handler),
        published_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return EventPublication(**defaults)


# ---------------------------------------------------------------------------
# configure()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_configure_starts_retry_loop_and_crash_sweep() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=0.01)
    # Crash-recovery sweep runs once immediately with older_than=0; the retry
    # loop then polls. Either way find_incomplete fires shortly.
    await asyncio.sleep(0.05)
    assert store.find_incomplete_calls
    assert timedelta(0) in store.find_incomplete_calls  # crash sweep


# ---------------------------------------------------------------------------
# publish inside a transaction → persist, no in-memory dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_in_transaction_persists_one_per_listener() -> None:
    from modulith import publish

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    session = FakeSession()
    token = outbox._current_session.set(session)
    try:
        await publish(OutboxEvent(value=5))
    finally:
        outbox._current_session.reset(token)

    # One publication persisted, targeting the registered listener.
    assert len(store.rows) == 1
    (pub,) = list(store.rows.values())
    # persist() stamps the module-qualified listener identity (not the bare
    # qualname) so same-named listeners in different modules don't collide.
    assert pub.listener == outbox._listener_id(record)
    assert pub.event_type.endswith("OutboxEvent")
    # Queued for after-commit dispatch — but nothing has dispatched yet.
    assert session.info["_modulith_pending"] == [pub.id]
    assert received == []


@pytest.mark.asyncio
async def test_publish_without_session_dispatches_in_memory() -> None:
    """Outbox configured but no transaction bound → normal in-memory dispatch."""
    from modulith import publish

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    await publish(OutboxEvent(value=7))

    assert store.rows == {}  # nothing persisted
    assert received == [7]  # dispatched directly


# ---------------------------------------------------------------------------
# _dispatch_publication
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_success_marks_complete() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=11)
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert received == [11]
    assert store.completed == [pub.id]


@pytest.mark.asyncio
async def test_dispatch_delete_mode_deletes_on_success() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), completion_mode="delete", start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=12)
    await store.save(pub)
    await outbox._dispatch_publication(pub)

    assert store.deleted == [pub.id]
    assert pub.id not in store.rows


@pytest.mark.asyncio
async def test_dispatch_archive_mode_archives_on_success() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), completion_mode="archive", start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=13)
    await store.save(pub)
    await outbox._dispatch_publication(pub)

    assert store.archived == [pub.id]


@pytest.mark.asyncio
async def test_dispatch_failure_increments_count_and_records_error() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(boom)

    pub = _make_pub(boom, value=99)
    assert pub.last_attempt_at is None
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    # Not completed; failure persisted with incremented count + error + the
    # attempt timestamp (so retry backoff is measured from it, not publish time).
    assert store.completed == []
    assert pub.attempt_count == 1
    assert pub.last_error is not None and "dispatch failed" in pub.last_error
    assert pub.completed_at is None
    assert pub.last_attempt_at is not None


@pytest.mark.asyncio
async def test_dispatch_dead_letters_at_threshold() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=3, start_loop=False)
    _bootstrap_with_listener(boom)

    pub = _make_pub(boom, value=1, attempt_count=2)  # next failure → 3 == threshold
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert pub.attempt_count == 3
    # Status counts it as dead-lettered (incomplete past the attempt threshold).
    counts = await outbox.status()
    assert counts["dead_lettered"] == 1
    assert counts["incomplete"] == 0


# ---------------------------------------------------------------------------
# retry loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_loop_processes_incomplete() -> None:
    store = StubStore()
    _bootstrap_with_listener(record)
    # Pre-load a stale incomplete publication (published well in the past so it
    # clears both the staleness threshold and the per-attempt backoff window).
    pub = _make_pub(record, value=42, published_at=datetime.now(UTC) - timedelta(seconds=120))
    store.rows[pub.id] = pub

    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=0.01)

    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)

    assert received == [42]
    assert pub.id in store.completed


@pytest.mark.asyncio
async def test_retry_loop_skips_dead_lettered() -> None:
    store = StubStore()
    _bootstrap_with_listener(boom)
    pub = _make_pub(
        boom,
        value=1,
        attempt_count=10,
        published_at=datetime.now(UTC) - timedelta(seconds=120),
    )
    store.rows[pub.id] = pub

    outbox.configure(
        store,
        JsonEventSerializer(),
        dead_letter_after_attempts=10,
        retry_interval_seconds=0.01,
    )
    await asyncio.sleep(0.1)

    # Dead-lettered: never retried, attempt_count untouched.
    assert pub.attempt_count == 10


# ---------------------------------------------------------------------------
# maintenance APIs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_counts_incomplete_completed_dead_lettered() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=5, start_loop=False)

    incomplete = _make_pub(record, value=1)
    dead = _make_pub(record, value=2, attempt_count=5)
    done = _make_pub(record, value=3)
    done.completed_at = datetime.now(UTC)
    for p in (incomplete, dead, done):
        store.rows[p.id] = p

    counts = await outbox.status()
    assert counts == {"incomplete": 1, "completed": 1, "dead_lettered": 1}


@pytest.mark.asyncio
async def test_force_retry_dispatches_specific_publication() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=77)
    store.rows[pub.id] = pub

    await outbox.force_retry(pub.id)

    assert received == [77]
    assert pub.id in store.completed


@pytest.mark.asyncio
async def test_purge_completed_removes_old_completed() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    old = _make_pub(record, value=1)
    old.completed_at = datetime.now(UTC) - timedelta(days=40)
    store.rows[old.id] = old

    removed = await outbox.purge_completed(timedelta(days=30))

    assert removed == 1
    assert old.id not in store.rows


# ---------------------------------------------------------------------------
# regression: listener identity + backoff anchor (audit findings)
# ---------------------------------------------------------------------------


def test_listener_id_disambiguates_same_qualname_across_modules() -> None:
    # Two callables sharing a __qualname__ but living in different modules must
    # get distinct identities. The old bare-qualname identity collided: one
    # listener's publications would resolve to the other (delivered twice / never).
    async def h(event: OutboxEvent) -> None: ...

    async def g(event: OutboxEvent) -> None: ...

    h.__qualname__ = g.__qualname__ = "on_order_created"
    h.__module__ = "myapp.orders"
    g.__module__ = "myapp.billing"

    assert outbox._listener_id(h) == "myapp.orders.on_order_created"
    assert outbox._listener_id(g) == "myapp.billing.on_order_created"
    assert outbox._listener_id(h) != outbox._listener_id(g)


@pytest.mark.asyncio
async def test_dispatch_resolves_same_qualname_listeners_to_own_handler() -> None:
    # End-to-end: two listeners with the SAME qualname (different modules)
    # registered for one event. Each publication must dispatch to its OWN
    # handler — the collision bug delivered both to whichever registered first.
    seen: list[str] = []

    async def handler_a(event: OutboxEvent) -> None:
        seen.append("a")

    async def handler_b(event: OutboxEvent) -> None:
        seen.append("b")

    handler_a.__qualname__ = handler_b.__qualname__ = "on_event"
    handler_a.__module__ = "pkg.mod_a"
    handler_b.__module__ = "pkg.mod_b"

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(OutboxEvent, handler_a)
    _runtime.event_bus.register(OutboxEvent, handler_b)

    pub_a = _make_pub(handler_a, value=1)
    pub_b = _make_pub(handler_b, value=2)
    await store.save(pub_a)
    await store.save(pub_b)

    await outbox._dispatch_publication(pub_a)
    await outbox._dispatch_publication(pub_b)

    assert seen == ["a", "b"]  # no cross-delivery
    assert store.completed == [pub_a.id, pub_b.id]


def test_backoff_measured_from_last_attempt_not_published_at() -> None:
    # A record published long ago but attempted just now must NOT be retry-
    # eligible: backoff is anchored on the last attempt, not the publish time.
    # The old anchor (published_at) meant a persistently-failing record aged
    # past the cap and was then retried on every single sweep.
    outbox.configure(StubStore(), JsonEventSerializer(), start_loop=False)

    pub = _make_pub(
        record,
        attempt_count=3,  # backoff = min(2 ** (3-1), cap) = 4s
        published_at=datetime.now(UTC) - timedelta(hours=1),
        last_attempt_at=datetime.now(UTC),
    )
    assert outbox._backoff_elapsed(pub) is False

    # Age the last attempt past the 4s window → eligible again.
    pub.last_attempt_at = datetime.now(UTC) - timedelta(seconds=10)
    assert outbox._backoff_elapsed(pub) is True
