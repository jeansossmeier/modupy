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
import logging
import sqlite3
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from modulith import EventPublication, event, listener, publish
from modulith.adapters.postgres_outbox import (
    Base,
    EventPublicationRow,
    PostgresPublicationStore,
    bind_session,
    unbind_session,
)
from modulith.adapters.shm_broker import ShmBroker
from modulith.builtin import outbox
from modulith.config import ConfigurationError
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
        self.saved: list[UUID] = []
        self.find_incomplete_calls: list[timedelta] = []
        self.completed: list[UUID] = []
        self.deleted: list[UUID] = []
        self.archived: list[UUID] = []

    async def save(self, publication: EventPublication) -> None:
        self.saved.append(publication.id)
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

    async def count_archived(self) -> int:
        return len(self.archived)

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


def test_transactional_publish_starts_retry_loop_when_configured_synchronously() -> None:
    """Synchronous startup defers the loop, but first async publish starts it."""
    from modulith import publish

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    _bootstrap_with_listener(record)

    async def scenario() -> None:
        session = FakeSession()
        token = outbox._current_session.set(session)
        try:
            await publish(OutboxEvent(value=8))
        finally:
            outbox._current_session.reset(token)
        assert outbox._retry_task is not None

    asyncio.run(scenario())


def test_transactional_publish_respects_start_loop_false() -> None:
    from modulith import publish

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    async def scenario() -> None:
        session = FakeSession()
        token = outbox._current_session.set(session)
        try:
            await publish(OutboxEvent(value=9))
        finally:
            outbox._current_session.reset(token)

    asyncio.run(scenario())

    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_runtime_shutdown_stops_outbox_retry_loop() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    assert outbox._retry_task is not None

    await _runtime.shutdown()

    assert outbox._retry_task is None


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
async def test_dispatch_deserialize_failure_increments_count_and_records_error() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    # A REGISTERED event type (so the unregistered-type guard lets it through)
    # whose stored bytes are not decodable — the poison-payload case, which is
    # what actually reaches the deserialize step.
    pub = _make_pub(record, payload=b"not json")
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert pub.attempt_count == 1
    assert pub.last_error is not None
    assert "Expecting value" in pub.last_error  # the JSON decode error, verbatim
    assert pub.completed_at is None
    assert received == []  # nothing was delivered
    assert store.saved == [pub.id, pub.id]


@pytest.mark.asyncio
async def test_dispatch_missing_listener_increments_count_and_records_error() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    pub = _make_pub(record, value=31, listener="missing.listener")
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert pub.attempt_count == 1
    assert pub.last_error is not None
    assert "missing.listener" in pub.last_error
    assert pub.completed_at is None
    assert store.saved == [pub.id, pub.id]


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


class _StoreWithOwnDeadLetterThreshold(StubStore):
    """Mimics PostgresPublicationStore's duck-typed dead-letter capability:
    a store MAY expose its own ``dead_letter_after_attempts`` (used to flag
    ``is_dead_lettered`` at save() time) plus an ``_explicit`` marker so
    outbox.configure() can detect conflicting settings vs. silently pick one
    and leave the two out of sync."""

    def __init__(self, *, dead_letter_after_attempts: int | None = None) -> None:
        super().__init__()
        self.dead_letter_after_attempts_explicit = dead_letter_after_attempts is not None
        self.dead_letter_after_attempts = (
            dead_letter_after_attempts if dead_letter_after_attempts is not None else 10
        )


def test_configure_rejects_conflicting_dead_letter_thresholds() -> None:
    """Dead-letter thresholds are unified: a store constructed with its OWN
    explicit dead_letter_after_attempts that disagrees with the value passed
    to outbox.configure() must fail loudly during configuration instead of
    silently leaving the store's is_dead_lettered flag and the plugin's own
    skip-check disagreeing on which rows are dead."""
    store = _StoreWithOwnDeadLetterThreshold(dead_letter_after_attempts=3)

    with pytest.raises(ConfigurationError, match="dead_letter_after_attempts"):
        outbox.configure(
            store, JsonEventSerializer(), dead_letter_after_attempts=5, start_loop=False
        )


def test_configure_unifies_dead_letter_threshold_from_explicit_store_setting() -> None:
    """When only the STORE sets an explicit threshold (configure() doesn't),
    the plugin adopts the store's value rather than silently keeping its own
    unrelated default of 10."""
    store = _StoreWithOwnDeadLetterThreshold(dead_letter_after_attempts=4)

    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    assert outbox._dead_letter_after_attempts == 4


def test_configure_pushes_its_threshold_down_to_the_store() -> None:
    """When only outbox.configure() sets an explicit threshold (the store
    doesn't), the plugin's value is pushed onto the store so a subsequent
    save() flags is_dead_lettered consistently with the plugin's own
    skip-check."""
    store = _StoreWithOwnDeadLetterThreshold()

    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=7, start_loop=False)

    assert store.dead_letter_after_attempts == 7


class _LegacyDeadLetterStore(StubStore):
    """A third-party store predating keyset pagination: exposes
    ``find_dead_lettered()`` with NO pagination kwargs.
    ``list_dead_lettered()`` must fall back to its single unbounded/capped
    call rather than raising TypeError."""

    async def find_dead_lettered(self) -> list[EventPublication]:
        return [p for p in self.rows.values() if p.attempt_count >= 10]


@pytest.mark.asyncio
async def test_list_dead_lettered_falls_back_for_store_without_pagination_kwargs() -> None:
    """Keyset pagination must retain fallback behavior for third-party
    stores whose find_dead_lettered() predates the after/limit kwargs."""
    store = _LegacyDeadLetterStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    pub = _make_pub(record, value=1, attempt_count=10)
    store.rows[pub.id] = pub

    result = await outbox.list_dead_lettered()

    assert [p.id for p in result] == [pub.id]


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
async def test_status_counts_archived_publications_as_completed() -> None:
    """completion_mode="archive" moves a delivered row out of the primary
    table into the store's archive; status() must still count it as
    completed via the store's optional count_archived capability, not report
    a working outbox's throughput as zero."""
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), completion_mode="archive", start_loop=False)
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=1)
    await store.save(pub)

    await outbox._dispatch_publication(pub)

    assert store.archived == [pub.id]
    counts = await outbox.status()
    assert counts == {"incomplete": 0, "completed": 1, "dead_lettered": 0}


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


def test_backoff_survives_attempt_counts_that_overflow_the_exponent() -> None:
    """A huge attempt_count must not raise OverflowError.

    ``2.0 ** 1024`` exceeds the float range. Exponentiating before applying
    the cap made every sweep raise once any row passed 1024 attempts, and the
    raise aborts the whole cycle rather than one row — so every publication
    behind it in the (oldest-attempt-first) ordering stalls forever. Reachable
    because dead_letter_after_attempts accepts arbitrarily large values, which
    is how operators express "effectively never dead-letter".
    """
    outbox.configure(
        StubStore(), JsonEventSerializer(), max_retry_backoff_seconds=300.0, start_loop=False
    )

    pub = _make_pub(
        record,
        attempt_count=100_000,
        last_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    # Still inside the 300s cap → not eligible, but computed, not raised.
    assert outbox._backoff_elapsed(pub) is False

    pub.last_attempt_at = datetime.now(UTC) - timedelta(seconds=301)
    assert outbox._backoff_elapsed(pub) is True


@pytest.mark.asyncio
async def test_sweep_dispatches_rows_behind_a_high_attempt_count_row() -> None:
    """One row with an overflowing attempt_count must not stall the sweep.

    The OverflowError escaped ``_sweep_unclaimed`` before reaching any later
    row, so a single poisoned publication blocked the entire backlog.
    """
    _bootstrap_with_listener(record)
    store = StubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        dead_letter_after_attempts=1_000_000,
        claim_strategy="none",
        start_loop=False,
    )

    stuck = _make_pub(
        record,
        value=1,
        attempt_count=100_000,
        last_attempt_at=datetime.now(UTC) - timedelta(hours=1),
    )
    behind = _make_pub(record, value=2)
    store.rows[stuck.id] = stuck
    store.rows[behind.id] = behind

    await outbox._sweep(timedelta(0))

    assert sorted(received) == [1, 2]


def test_all_advertises_no_private_names() -> None:
    """``__all__`` is the star-import surface, so a leading-underscore entry
    there contradicts itself: the import binds the name while every linter and
    reader treats it as private. ``_current_session`` in particular is bound
    and released through ``modulith.adapters.postgres_outbox.bind_session`` /
    ``unbind_session``, which restore the previous value; advertising the raw
    ContextVar invites callers to set it directly and lose that guarantee.
    """
    assert [n for n in outbox.__all__ if n.startswith("_")] == []


# ---------------------------------------------------------------------------
# claim_strategy wiring in the outbox plugin
# ---------------------------------------------------------------------------


class ClaimingStubStore(StubStore):
    """In-memory ClaimingStore + AdvisoryLockingStore for plugin-level tests.

    Mirrors the real adapter's contract: claim_batch returns copies carrying
    claim_token, complete_claim/fail_claim fence on that token, and
    try_lock_publication is an exclusive in-process lock.
    """

    def __init__(self, *, supports_advisory_lock: bool = True) -> None:
        super().__init__()
        self.supports_advisory_lock = supports_advisory_lock
        self.claims: dict[UUID, tuple[str, str, datetime]] = {}  # id -> owner,token,until
        self.locks: dict[UUID, object] = {}
        self.claim_batch_calls: list[str] = []
        self.renew_calls: list[tuple[UUID, str, float]] = []
        self.complete_claim_calls: list[tuple[UUID, str, str]] = []
        self.fail_claim_calls: list[tuple[UUID, str]] = []
        self.lock_calls: list[UUID] = []

    async def claim_batch(
        self, *, owner: str, batch_size: int, lease_seconds: float, older_than: timedelta
    ) -> list[EventPublication]:
        self.claim_batch_calls.append(owner)
        now = datetime.now(UTC)
        cutoff = now - older_than
        claimed: list[EventPublication] = []
        for pub in sorted(self.rows.values(), key=lambda p: p.published_at or now):
            if pub.completed_at is not None:
                continue
            if pub.published_at is not None and pub.published_at > cutoff:
                continue
            existing = self.claims.get(pub.id)
            if existing is not None and existing[2] > now:
                continue  # active lease held by someone else
            token = uuid4().hex
            until = now + timedelta(seconds=lease_seconds)
            self.claims[pub.id] = (owner, token, until)
            copy_pub = replace(pub, claim_token=token)
            claimed.append(copy_pub)
            if len(claimed) >= batch_size:
                break
        return claimed

    async def renew_claim(self, publication_id: UUID, token: str, lease_seconds: float) -> bool:
        self.renew_calls.append((publication_id, token, lease_seconds))
        entry = self.claims.get(publication_id)
        if entry is None or entry[1] != token:
            return False
        self.claims[publication_id] = (
            entry[0],
            entry[1],
            datetime.now(UTC) + timedelta(seconds=lease_seconds),
        )
        return True

    async def complete_claim(self, publication_id: UUID, token: str, mode: str) -> bool:
        self.complete_claim_calls.append((publication_id, token, mode))
        entry = self.claims.get(publication_id)
        if entry is None or entry[1] != token:
            return False
        if mode == "delete":
            self.deleted.append(publication_id)
            self.rows.pop(publication_id, None)
        elif mode == "archive":
            self.archived.append(publication_id)
            self.rows.pop(publication_id, None)
        else:
            row = self.rows.get(publication_id)
            if row is not None:
                row.completed_at = datetime.now(UTC)
            self.completed.append(publication_id)
        self.claims.pop(publication_id, None)
        return True

    async def fail_claim(self, publication: EventPublication, token: str) -> bool:
        self.fail_claim_calls.append((publication.id, token))
        entry = self.claims.get(publication.id)
        if entry is None or entry[1] != token:
            return False
        self.rows[publication.id] = publication
        self.saved.append(publication.id)
        self.claims.pop(publication.id, None)
        return True

    async def try_lock_publication(self, publication_id: UUID) -> object | None:
        self.lock_calls.append(publication_id)
        if publication_id in self.locks:
            return None
        handle = object()
        self.locks[publication_id] = handle
        return handle

    async def unlock_publication(self, handle: object, publication_id: UUID) -> None:
        if self.locks.get(publication_id) is handle:
            self.locks.pop(publication_id, None)


def test_configure_rejects_invalid_claim_strategy() -> None:
    with pytest.raises(ValueError, match="claim_strategy"):
        outbox.configure(
            StubStore(), JsonEventSerializer(), claim_strategy="optimistic", start_loop=False
        )


def test_configure_none_strategy_warns_about_duplicates(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="modulith.outbox"):
        outbox.configure(
            StubStore(), JsonEventSerializer(), claim_strategy="none", start_loop=False
        )
    assert any("claim_strategy='none'" in r.message for r in caplog.records)


def test_configure_advisory_lock_requires_store_capability() -> None:
    store = ClaimingStubStore(supports_advisory_lock=False)
    with pytest.raises(ConfigurationError, match="advisory_lock"):
        outbox.configure(
            store, JsonEventSerializer(), claim_strategy="advisory_lock", start_loop=False
        )


@pytest.mark.asyncio
async def test_lease_sweep_uses_claim_batch_not_find_incomplete() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store, JsonEventSerializer(), claim_strategy="lease", claim_batch_size=10, start_loop=False
    )
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=7)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    assert store.claim_batch_calls, "lease sweep must call claim_batch"
    assert store.find_incomplete_calls == []
    assert received == [7]
    assert store.complete_claim_calls  # fenced completion, not mark_complete
    assert store.completed == [pub.id]
    # Direct mark_complete must NOT be used when a claim_token is present.
    assert store.completed == [c[0] for c in store.complete_claim_calls]


@pytest.mark.asyncio
async def test_lease_excludes_second_owner_while_first_holds_claim() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=60.0,
        start_loop=False,
    )
    pub = _make_pub(record, value=1)
    await store.save(pub)

    first = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    second = await store.claim_batch(
        owner="worker-b", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    assert [p.id for p in first] == [pub.id]
    assert second == []


@pytest.mark.asyncio
async def test_lease_stale_token_does_not_complete_or_burn_attempts() -> None:
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=1)
    await store.save(pub)
    (claimed,) = await store.claim_batch(
        owner="worker-a", batch_size=1, lease_seconds=60.0, older_than=timedelta(0)
    )
    # Simulate a peer reclaiming the row with a new token before we finish.
    store.claims[claimed.id] = ("worker-b", "peer-token", datetime.now(UTC) + timedelta(hours=1))
    claimed.claim_token = "stale-token"

    await outbox._dispatch_publication(claimed)

    assert claimed.completed_at is None
    assert store.rows[claimed.id].attempt_count == 0
    assert store.complete_claim_calls == [(claimed.id, "stale-token", "update")]


@pytest.mark.asyncio
async def test_lease_renews_during_slow_dispatch() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=0.3,
        start_loop=False,
    )

    async def slow(event: OutboxEvent) -> None:
        await asyncio.sleep(0.2)
        received.append(event.value)

    _bootstrap_with_listener(slow)
    pub = _make_pub(slow, value=3)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    # The first positive renewal is the pre-dispatch re-arm; a renewal BEYOND
    # it is the in-dispatch loop firing at ~lease/3.
    renewals = [c for c in store.renew_calls if c[2] > 0]
    assert len(renewals) > 1, "lease renewal must run at ~lease/3 during slow dispatch"
    assert received == [3]


@pytest.mark.asyncio
async def test_lease_skips_a_row_a_peer_reclaimed_and_dispatches_the_rest() -> None:
    """One claim_batch call leases every row with the SAME expiry while
    dispatch is serial, so a row's lease can run out before its turn and a peer
    sweeper can claim it. The re-arm before dispatch must catch that: skip the
    row rather than deliver it a second time under a dead lease, and carry on
    with the rest of the batch.
    """
    stolen: list[UUID] = []

    class PeerReclaimsBeforeDispatch(ClaimingStubStore):
        """A peer takes the stolen rows over in the window between our
        claim_batch returning them and the sweep reaching them."""

        async def claim_batch(
            self, *, owner: str, batch_size: int, lease_seconds: float, older_than: timedelta
        ) -> list[EventPublication]:
            claimed = await super().claim_batch(
                owner=owner,
                batch_size=batch_size,
                lease_seconds=lease_seconds,
                older_than=older_than,
            )
            peer_until = datetime.now(UTC) + timedelta(hours=1)
            for pub in claimed:
                if pub.id in stolen:
                    self.claims[pub.id] = ("worker-b", "peer-token", peer_until)
            return claimed

    store = PeerReclaimsBeforeDispatch()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=60.0,
        start_loop=False,
    )
    _bootstrap_with_listener(record)
    first = _make_pub(record, value=1, published_at=datetime.now(UTC) - timedelta(seconds=2))
    second = _make_pub(record, value=2, published_at=datetime.now(UTC) - timedelta(seconds=1))
    await store.save(first)
    await store.save(second)
    stolen.append(first.id)

    await outbox._sweep(timedelta(0))

    assert received == [2]  # the first row belongs to the peer now
    assert [c[0] for c in store.complete_claim_calls] == [second.id]
    assert store.rows[first.id].completed_at is None
    assert store.rows[first.id].attempt_count == 0  # skipped, not failed


@pytest.mark.asyncio
async def test_lease_releases_claim_when_backoff_not_elapsed() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=60.0,
        start_loop=False,
    )
    pub = _make_pub(
        record,
        value=1,
        attempt_count=3,
        last_attempt_at=datetime.now(UTC),  # backoff not elapsed
    )
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    # Early release: renew_claim(..., 0.0) so another sweeper can pick it up.
    assert any(c[2] == 0.0 for c in store.renew_calls)
    assert received == []
    assert store.complete_claim_calls == []


@pytest.mark.asyncio
async def test_advisory_lock_skips_when_lock_held() -> None:
    store = ClaimingStubStore(supports_advisory_lock=True)
    outbox.configure(store, JsonEventSerializer(), claim_strategy="advisory_lock", start_loop=False)
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=9)
    await store.save(pub)
    # Pre-hold the lock as if another sweeper owns dispatch.
    store.locks[pub.id] = object()

    await outbox._sweep(timedelta(0))

    assert pub.id in store.lock_calls
    assert received == []
    assert store.completed == []


@pytest.mark.asyncio
async def test_advisory_lock_holds_through_dispatch() -> None:
    store = ClaimingStubStore(supports_advisory_lock=True)
    outbox.configure(store, JsonEventSerializer(), claim_strategy="advisory_lock", start_loop=False)
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=4)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    assert store.lock_calls == [pub.id]
    assert pub.id not in store.locks  # unlocked after dispatch
    assert received == [4]
    assert store.completed == [pub.id]


class PeerRacingLockStore(ClaimingStubStore):
    """Advisory-lock store whose sweep snapshot goes stale: ``find_incomplete``
    returns copies, and a peer changes the stored row just before this
    sweeper's lock attempt succeeds (the peer delivered and unlocked it)."""

    def __init__(self, peer_action: str) -> None:
        super().__init__(supports_advisory_lock=True)
        self.peer_action = peer_action

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return [replace(p) for p in await super().find_incomplete(older_than)]

    async def find_by_id(self, publication_id: UUID) -> EventPublication | None:
        return self.rows.get(publication_id)

    async def try_lock_publication(self, publication_id: UUID) -> object | None:
        row = self.rows[publication_id]
        if self.peer_action == "completed":
            row.completed_at = datetime.now(UTC)
        elif self.peer_action == "dead_lettered":
            row.attempt_count = 3
        elif self.peer_action == "failed":
            row.attempt_count += 1
            row.last_attempt_at = datetime.now(UTC)
        else:
            del self.rows[publication_id]
        return await super().try_lock_publication(publication_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("peer_action", ["completed", "dead_lettered", "failed", "gone"])
async def test_advisory_sweep_rereads_row_after_locking(peer_action: str) -> None:
    """The advisory sweep dispatches from a snapshot read before locking. A
    peer can finish or fail a row in between and release its lock, so after
    locking the sweep must re-read the row, skip it when it is completed,
    dead-lettered, gone, or failed so recently that its backoff has not
    elapsed, and still release the lock."""
    store = PeerRacingLockStore(peer_action)
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="advisory_lock",
        dead_letter_after_attempts=3,
        start_loop=False,
    )
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=13)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    assert store.lock_calls == [pub.id]
    assert store.locks == {}
    assert received == []
    assert store.completed == []


@pytest.mark.asyncio
async def test_advisory_sweep_dispatches_the_reread_row() -> None:
    """When the re-read row is still pending, the sweep delivers that fresh
    copy, so completion and failure bookkeeping act on current state."""
    store = PeerRacingLockStore("none")
    store.try_lock_publication = ClaimingStubStore.try_lock_publication.__get__(store)  # type: ignore[method-assign]
    outbox.configure(store, JsonEventSerializer(), claim_strategy="advisory_lock", start_loop=False)
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=14)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    assert received == [14]
    assert store.completed == [pub.id]
    assert pub.completed_at is not None
    assert store.locks == {}


@pytest.mark.asyncio
async def test_legacy_store_falls_back_to_find_incomplete_under_default_lease() -> None:
    """Third-party stores without ClaimingStore keep the original path."""
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)  # default lease
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=5)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    assert store.find_incomplete_calls == [timedelta(0)]
    assert received == [5]
    assert store.completed == [pub.id]


@pytest.mark.asyncio
async def test_none_strategy_uses_find_incomplete_even_with_claiming_store() -> None:
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="none", start_loop=False)
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=6)
    await store.save(pub)

    await outbox._sweep(timedelta(0))

    assert store.claim_batch_calls == []
    assert store.find_incomplete_calls == [timedelta(0)]
    assert received == [6]


# ---------------------------------------------------------------------------
# Defensive lifecycle, retry, and maintenance behavior
# ---------------------------------------------------------------------------


class CoreOnlyStore:
    """PublicationStore without any optional maintenance capabilities."""

    def __init__(self) -> None:
        self.rows: dict[UUID, EventPublication] = {}

    async def save(self, publication: EventPublication) -> None:
        self.rows[publication.id] = publication

    async def mark_complete(self, publication_id: UUID) -> None:
        row = self.rows.get(publication_id)
        if row is not None:
            row.completed_at = datetime.now(UTC)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        now = datetime.now(UTC)
        return [
            row
            for row in self.rows.values()
            if row.completed_at is None
            and row.published_at is not None
            and now - row.published_at >= older_than
        ]

    async def archive(self, publication_id: UUID) -> None:
        self.rows.pop(publication_id, None)

    async def delete(self, publication_id: UUID) -> None:
        self.rows.pop(publication_id, None)


class MalformedClaimingStore(ClaimingStubStore):
    """Fault-injection store that violates the claim-token return contract."""

    async def claim_batch(
        self, *, owner: str, batch_size: int, lease_seconds: float, older_than: timedelta
    ) -> list[EventPublication]:
        self.claim_batch_calls.append(owner)
        return list(self.rows.values())[:batch_size]


class SuppressedCancellationTask:
    """Expose stop-driven renewal exits by suppressing the final task cancel."""

    def __init__(self, task: asyncio.Task[None]) -> None:
        self.task = task

    def cancel(self) -> bool:
        return False

    def __await__(self) -> Any:
        return self.task.__await__()


def _suppress_created_task_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise defensive stop paths that stdlib Task cancellation preempts.

    The production finally block calls ``stop.set()`` and ``task.cancel()``
    without yielding between them. A standard asyncio task therefore observes
    cancellation first. This narrow fault injection verifies that the renewal
    coroutine also exits correctly if an alternative task implementation lets
    the stop signal win.
    """
    create_task = asyncio.create_task

    def create_suppressed_cancellation_task(coro: Any) -> SuppressedCancellationTask:
        return SuppressedCancellationTask(create_task(coro))

    monkeypatch.setattr(asyncio, "create_task", create_suppressed_cancellation_task)


def test_listener_id_supports_callable_instances_without_qualname() -> None:
    class CallableHandler:
        async def __call__(self, event: OutboxEvent) -> None:
            pass

        def __repr__(self) -> str:
            return "callable-handler"

    handler = CallableHandler()

    assert outbox._listener_id(handler) == "callable-handler"


def test_configure_rejects_invalid_lease_and_batch_values() -> None:
    store = StubStore()

    with pytest.raises(ValueError, match="completion_mode"):
        outbox.configure(store, JsonEventSerializer(), completion_mode="drop", start_loop=False)

    with pytest.raises(ValueError, match="claim_lease_seconds"):
        outbox.configure(store, JsonEventSerializer(), claim_lease_seconds=0, start_loop=False)

    with pytest.raises(ValueError, match="claim_lease_seconds"):
        outbox.configure(
            store, JsonEventSerializer(), claim_lease_seconds=float("inf"), start_loop=False
        )

    with pytest.raises(ValueError, match="claim_batch_size"):
        outbox.configure(store, JsonEventSerializer(), claim_batch_size=True, start_loop=False)


def test_cancel_retry_task_schedules_cancellation_on_a_foreign_loop() -> None:
    loop = asyncio.new_event_loop()
    task = loop.create_task(asyncio.sleep(60))
    outbox._retry_task = task
    try:
        outbox._cancel_retry_task()

        with pytest.raises(asyncio.CancelledError):
            loop.run_until_complete(task)

        assert outbox._retry_task is None
    finally:
        if not task.done():
            task.cancel()
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
        loop.close()


def test_cancel_retry_task_tolerates_an_already_closed_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.new_event_loop()
    loop.close()

    class TaskOnClosedLoop:
        def done(self) -> bool:
            return False

        def get_loop(self) -> asyncio.AbstractEventLoop:
            return loop

        def cancel(self) -> bool:
            raise AssertionError("closed loops cannot schedule cancellation")

    monkeypatch.setattr(outbox, "_retry_task", TaskOnClosedLoop())

    outbox._cancel_retry_task()

    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_ensure_retry_loop_replaces_task_stranded_on_closed_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry task left pointing at a CLOSED loop (e.g. sync.py's
    _run_nested_dispatch ran a transactional publish on a throwaway loop and
    then closed it) must be treated as absent, not merely "not done" — a
    task on a closed loop will never take another step, so leaving it in the
    slot would block every future retry loop for the rest of the process."""
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    class StrandedTask:
        def done(self) -> bool:
            return False

        def get_loop(self) -> asyncio.AbstractEventLoop:
            return closed_loop

    stranded: Any = StrandedTask()
    monkeypatch.setattr(outbox, "_retry_task", stranded)

    async def fake_retry_loop() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(outbox, "_retry_loop", fake_retry_loop)

    outbox._ensure_retry_loop()

    replacement = outbox._retry_task
    assert replacement is not None
    assert replacement is not stranded

    replacement.cancel()
    try:
        await replacement
    except asyncio.CancelledError:
        pass
    outbox._retry_task = None


@pytest.mark.asyncio
async def test_shutdown_clears_task_stranded_on_a_closed_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """outbox.shutdown() must not hang or raise when the retry task is
    stranded on an already-closed loop — it must clear the stale reference
    instead, matching _cancel_retry_task's tolerance for the same state."""
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    class StrandedTask:
        def done(self) -> bool:
            return False

        def get_loop(self) -> asyncio.AbstractEventLoop:
            return closed_loop

        def cancel(self) -> bool:
            raise AssertionError("a task on a closed loop cannot be cancelled")

    monkeypatch.setattr(outbox, "_retry_task", StrandedTask())

    await asyncio.wait_for(outbox.shutdown(), timeout=1)

    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_shutdown_tolerates_loop_closing_between_check_and_cancel_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A narrower race than the fully-closed-loop case above: the foreign
    loop is still open when shutdown() checks ``is_closed()``, but closes by
    the time ``call_soon_threadsafe`` actually runs. That must be tolerated
    the same way, not left to raise RuntimeError out of shutdown()."""

    class RacingLoop:
        def is_closed(self) -> bool:
            return False

        def call_soon_threadsafe(self, callback: Any) -> None:
            raise RuntimeError("Event loop is closed")

    racing_loop = RacingLoop()

    class RacingTask:
        def done(self) -> bool:
            return False

        def get_loop(self) -> Any:
            return racing_loop

        def cancel(self) -> bool:
            raise AssertionError("must not actually be invoked — the loop rejects it first")

    monkeypatch.setattr(outbox, "_retry_task", RacingTask())

    await asyncio.wait_for(outbox.shutdown(), timeout=1)

    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_persist_returns_no_publications_before_runtime_bootstrap() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    publications = await outbox.persist(OutboxEvent(value=1))

    assert publications == []
    assert store.rows == {}


def test_persisted_broker_route_starts_deferred_retry_loop() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    assert outbox._retry_task is None

    async def persist_route() -> None:
        publication = await outbox.persist_broker_route(OutboxEvent(value=2), "test:events")

        assert store.rows == {publication.id: publication}
        assert outbox._retry_task is not None and not outbox._retry_task.done()

    asyncio.run(persist_route())


@pytest.mark.asyncio
async def test_dispatch_without_bootstrapped_bus_records_missing_listener() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    publication = _make_pub(record, value=3)
    await store.save(publication)

    await outbox._dispatch_publication(publication)

    # With no bootstrapped bus there are no registered event types either, so
    # the unregistered-event-type guard now fires before listener resolution
    # would have — still a safely-recorded failure, not a crash.
    assert publication.attempt_count == 1
    assert publication.last_error is not None
    assert "not a registered event type" in publication.last_error
    assert publication.completed_at is None


@pytest.mark.asyncio
async def test_dispatch_rejects_unregistered_event_type_without_instantiating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A forged outbox row naming an arbitrary importable class (the RCE
    vector fixed here: e.g. ``subprocess.Popen``) must be rejected before
    deserialization ever resolves or instantiates that class — regardless of
    whether the configured serializer itself carries an allowlist.
    """
    import subprocess

    spawned: list[tuple[Any, Any]] = []
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: spawned.append((a, k)))

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    forged = EventPublication(
        id=uuid4(),
        payload=b'{"args": ["/bin/echo", "pwned"]}',
        event_type="subprocess.Popen",
        listener=outbox._listener_id(record),
        published_at=datetime.now(UTC),
    )
    await store.save(forged)

    await outbox._dispatch_publication(forged)

    assert spawned == []
    assert forged.attempt_count == 1
    assert forged.completed_at is None
    assert forged.last_error is not None
    assert "not a registered event type" in forged.last_error

    # A legitimately-registered event must still round-trip and dispatch.
    legit = _make_pub(record, value=55)
    await store.save(legit)

    await outbox._dispatch_publication(legit)

    assert received == [55]
    assert legit.completed_at is not None


@pytest.mark.asyncio
async def test_dispatch_with_missing_plugin_manager_preserves_success_and_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fault injection verifies dispatch remains safe without hook infrastructure.

    Normal runtime bootstrap installs the event bus and plugin manager together.
    Keeping the bus while removing the manager exercises the outbox's explicit
    optional-manager guards without replacing listener or store behavior.
    """
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(OutboxEvent, boom)
    monkeypatch.setattr(_runtime, "_plugin_manager", None)

    successful = _make_pub(record, value=4)
    failed = _make_pub(boom, value=5)
    await store.save(successful)
    await store.save(failed)

    await outbox._dispatch_publication(successful)
    await outbox._dispatch_publication(failed)

    assert received == [4]
    assert successful.completed_at is not None
    assert failed.attempt_count == 1
    assert failed.last_error == "dispatch failed"


@pytest.mark.asyncio
async def test_stale_failure_claim_does_not_burn_retry_budget() -> None:
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    _bootstrap_with_listener(boom)
    publication = _make_pub(boom, value=6)
    await store.save(publication)
    (claimed,) = await store.claim_batch(
        owner="worker-a", batch_size=1, lease_seconds=60, older_than=timedelta(0)
    )
    store.claims[claimed.id] = ("worker-b", "new-token", datetime.now(UTC) + timedelta(minutes=1))
    claimed.claim_token = "stale-token"

    await outbox._dispatch_publication(claimed)

    assert claimed.attempt_count == 0
    assert store.rows[claimed.id].attempt_count == 0
    assert store.rows[claimed.id].completed_at is None


@pytest.mark.asyncio
async def test_lease_failure_with_valid_claim_records_attempt_and_dead_letters() -> None:
    """A dispatch failure under a still-valid claim token records the failed
    attempt via fail_claim (not the stale-token abandon path) and still
    reaches the dead-letter threshold check."""
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        dead_letter_after_attempts=1,
        start_loop=False,
    )
    _bootstrap_with_listener(boom)
    publication = _make_pub(boom, value=7)
    await store.save(publication)
    (claimed,) = await store.claim_batch(
        owner="worker-a", batch_size=1, lease_seconds=60, older_than=timedelta(0)
    )

    await outbox._dispatch_publication(claimed)

    assert store.fail_claim_calls == [(claimed.id, claimed.claim_token)]
    assert store.rows[claimed.id].attempt_count == 1


@pytest.mark.asyncio
async def test_broker_completion_failure_records_retryable_state() -> None:
    sent: list[tuple[str, bytes]] = []

    class Broker:
        async def publish(
            self, target: str, payload: bytes, headers: dict[str, str] | None = None
        ) -> None:
            sent.append((target, payload))

        async def close(self) -> None:
            pass

    class FailingCompletionStore(StubStore):
        async def mark_complete(self, publication_id: UUID) -> None:
            raise ConnectionError("completion database unavailable")

    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    _runtime.broker_registry.register("test", Broker())
    store = FailingCompletionStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    publication = EventPublication(
        id=uuid4(),
        payload=b'{"value": 7}',
        event_type=f"{OutboxEvent.__module__}.{OutboxEvent.__qualname__}",
        listener=outbox._BROKER_ROUTE_LISTENER_PREFIX + "test:events",
        published_at=datetime.now(UTC),
    )
    await store.save(publication)

    await outbox._dispatch_publication(publication)

    assert sent == [("events", publication.payload)]
    assert publication.attempt_count == 1
    assert publication.last_error == "completion database unavailable"
    assert publication.completed_at is None


@pytest.mark.asyncio
async def test_broker_route_threads_publication_id_into_headers() -> None:
    received_headers: list[dict[str, str] | None] = []

    class Broker:
        async def publish(
            self, target: str, payload: bytes, headers: dict[str, str] | None = None
        ) -> None:
            received_headers.append(headers)

        async def close(self) -> None:
            pass

    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    _runtime.broker_registry.register("test", Broker())
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    publication = EventPublication(
        id=uuid4(),
        payload=b'{"value": 7}',
        event_type=f"{OutboxEvent.__module__}.{OutboxEvent.__qualname__}",
        listener=outbox._BROKER_ROUTE_LISTENER_PREFIX + "test:events",
        published_at=datetime.now(UTC),
    )
    await store.save(publication)

    await outbox._dispatch_broker_route(publication)

    assert received_headers == [
        {"event_type": publication.event_type, "publication_id": str(publication.id)}
    ]


@pytest.mark.asyncio
async def test_broker_route_persisted_with_padded_target_reaches_normalized_destination() -> None:
    sent: list[tuple[str, bytes]] = []

    class Broker:
        async def publish(
            self, target: str, payload: bytes, headers: dict[str, str] | None = None
        ) -> None:
            sent.append((target, payload))

        async def close(self) -> None:
            pass

    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    _runtime.broker_registry.register("test", Broker())
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    publication = EventPublication(
        id=uuid4(),
        payload=b'{"value": 7}',
        event_type=f"{OutboxEvent.__module__}.{OutboxEvent.__qualname__}",
        listener=outbox._BROKER_ROUTE_LISTENER_PREFIX + " test : events ",
        published_at=datetime.now(UTC),
    )
    await store.save(publication)

    await outbox._dispatch_broker_route(publication)

    assert sent == [("events", b'{"value": 7}')]
    assert publication.last_error is None


@pytest.mark.asyncio
async def test_broker_route_redispatch_is_deduped_by_publication_id(tmp_path: Path) -> None:
    """A re-dispatch of the same publication (crash-recovery retry) must not
    deliver a second time — the SHM broker's consumer-side dedup keys on the
    ``publication_id`` header threaded by ``_dispatch_broker_route``."""
    db_path = tmp_path / "outbox-dedup.db"
    broker = ShmBroker(
        shm_name="outbox-dedup-hints", db_path=str(db_path), max_store_bytes=64 * 1024
    )
    try:
        await broker.subscribe(["events"], "workers")
        _runtime.configure(package="outboxtest", auto_discover=False)
        _runtime.ensure_bootstrapped()
        assert _runtime.broker_registry is not None
        _runtime.broker_registry.register("test", broker)
        store = StubStore()
        outbox.configure(store, JsonEventSerializer(), start_loop=False)
        publication = EventPublication(
            id=uuid4(),
            payload=b'{"value": 7}',
            event_type=f"{OutboxEvent.__module__}.{OutboxEvent.__qualname__}",
            listener=outbox._BROKER_ROUTE_LISTENER_PREFIX + "test:events",
            published_at=datetime.now(UTC),
        )
        await store.save(publication)

        await outbox._dispatch_broker_route(publication)
        await outbox._dispatch_broker_route(publication)  # simulated crash-recovery re-dispatch

        connection = sqlite3.connect(db_path)
        try:
            assert connection.execute("SELECT COUNT(*) FROM shm_publication").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM shm_delivery").fetchone()[0] == 1
        finally:
            connection.close()
    finally:
        await broker.close()
        broker._ring.unlink()


def test_backoff_allows_legacy_publication_without_timestamps() -> None:
    outbox.configure(StubStore(), JsonEventSerializer(), start_loop=False)
    publication = _make_pub(
        record,
        attempt_count=1,
        published_at=None,
        last_attempt_at=None,
    )

    assert outbox._backoff_elapsed(publication) is True


@pytest.mark.asyncio
async def test_unclaimed_sweep_skips_publication_still_in_backoff() -> None:
    store = StubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="none",
        start_loop=False,
    )
    _bootstrap_with_listener(record)
    publication = _make_pub(record, attempt_count=1, last_attempt_at=datetime.now(UTC))
    await store.save(publication)

    await outbox._sweep(timedelta(0))

    assert received == []
    assert publication.completed_at is None


@pytest.mark.asyncio
async def test_malformed_tokenless_claims_are_safe_during_runtime_outage() -> None:
    store = MalformedClaimingStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    publication = _make_pub(record)
    await store.save(publication)

    await outbox._sweep(timedelta(0))

    assert publication.attempt_count == 0
    assert publication.completed_at is None
    assert store.renew_calls == []


@pytest.mark.asyncio
async def test_lease_sweep_releases_dead_lettered_claim() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        dead_letter_after_attempts=3,
        start_loop=False,
    )
    _bootstrap_with_listener(record)
    publication = _make_pub(record, attempt_count=3)
    await store.save(publication)

    await outbox._sweep(timedelta(0))

    assert received == []
    assert any(call[0] == publication.id and call[2] == 0 for call in store.renew_calls)
    assert publication.completed_at is None


@pytest.mark.asyncio
async def test_lease_sweep_releases_early_for_row_still_in_backoff() -> None:
    """A claimed row not yet past its backoff window must be released
    (renewed with a 0-second lease) rather than held for the rest of the
    lease — holding it would block every other sweeper from picking it up
    sooner than this sweeper's own next cycle."""
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    _bootstrap_with_listener(record)
    publication = _make_pub(record, value=1, attempt_count=1, last_attempt_at=datetime.now(UTC))
    await store.save(publication)

    await outbox._sweep(timedelta(0))

    assert received == []
    assert publication.completed_at is None
    assert any(call[0] == publication.id and call[2] == 0 for call in store.renew_calls)


@pytest.mark.asyncio
async def test_malformed_tokenless_claims_skip_unsafe_rows_and_dispatch_eligible() -> None:
    store = MalformedClaimingStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    _bootstrap_with_listener(record)
    backing_off = _make_pub(record, value=8, attempt_count=1, last_attempt_at=datetime.now(UTC))
    eligible = _make_pub(record, value=9)
    dead = _make_pub(record, value=10, attempt_count=10)
    await store.save(backing_off)
    await store.save(eligible)
    await store.save(dead)

    await outbox._sweep(timedelta(0))

    assert received == [9]
    assert backing_off.completed_at is None
    assert eligible.completed_at is not None
    assert dead.attempt_count == 10


@pytest.mark.asyncio
async def test_advisory_sweep_waits_for_runtime_before_locking() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="advisory_lock",
        start_loop=False,
    )
    publication = _make_pub(record)
    await store.save(publication)

    await outbox._sweep(timedelta(0))

    assert store.lock_calls == []
    assert publication.attempt_count == 0


@pytest.mark.asyncio
async def test_advisory_sweep_skips_dead_letters_and_backoff() -> None:
    store = ClaimingStubStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="advisory_lock",
        dead_letter_after_attempts=3,
        start_loop=False,
    )
    _bootstrap_with_listener(record)
    dead = _make_pub(record, value=10, attempt_count=3)
    backing_off = _make_pub(
        record,
        value=11,
        attempt_count=1,
        last_attempt_at=datetime.now(UTC),
    )
    await store.save(dead)
    await store.save(backing_off)

    await outbox._sweep(timedelta(0))

    assert store.lock_calls == []
    assert received == []
    assert dead.attempt_count == 3
    assert backing_off.attempt_count == 1


@pytest.mark.asyncio
async def test_lease_renewal_exits_when_stop_is_already_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _suppress_created_task_cancellation(monkeypatch)
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    _bootstrap_with_listener(record)
    publication = _make_pub(record, value=12)
    await store.save(publication)
    (claimed,) = await store.claim_batch(
        owner="worker", batch_size=1, lease_seconds=60, older_than=timedelta(0)
    )

    await outbox._dispatch_with_lease_renewal(claimed)

    assert received == [12]
    assert store.rows[claimed.id].completed_at is not None


@pytest.mark.asyncio
async def test_lease_renewal_returns_when_stop_wait_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _suppress_created_task_cancellation(monkeypatch)
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)

    async def yielding_handler(event: OutboxEvent) -> None:
        await asyncio.sleep(0)
        received.append(event.value)

    _bootstrap_with_listener(yielding_handler)
    publication = _make_pub(yielding_handler, value=13)
    await store.save(publication)
    (claimed,) = await store.claim_batch(
        owner="worker", batch_size=1, lease_seconds=60, older_than=timedelta(0)
    )

    await outbox._dispatch_with_lease_renewal(claimed)

    assert received == [13]
    assert store.rows[claimed.id].completed_at is not None


@pytest.mark.asyncio
async def test_lost_lease_stops_renewal_without_cancelling_dispatch() -> None:
    lease_lost = asyncio.Event()

    class LosingRenewalStore(ClaimingStubStore):
        """Grants the pre-dispatch re-arm, then loses the lease mid-dispatch —
        the row WAS ours when delivery started, so it must run to completion."""

        async def renew_claim(self, publication_id: UUID, token: str, lease_seconds: float) -> bool:
            self.renew_calls.append((publication_id, token, lease_seconds))
            if len(self.renew_calls) == 1:
                return True
            lease_lost.set()
            return False

    async def wait_for_lease_loss(event: OutboxEvent) -> None:
        await lease_lost.wait()
        received.append(event.value)

    store = LosingRenewalStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=0.03,
        start_loop=False,
    )
    _bootstrap_with_listener(wait_for_lease_loss)
    publication = _make_pub(wait_for_lease_loss, value=14)
    await store.save(publication)

    await asyncio.wait_for(outbox._sweep(timedelta(0)), timeout=1)

    assert lease_lost.is_set()
    assert received == [14]
    assert store.rows[publication.id].completed_at is not None


@pytest.mark.asyncio
async def test_renewal_that_raises_is_retried_and_delivery_completes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A renewal that raises (a database blip) must not end renewals: the
    next renewal still runs, the delivery completes, and the sweep does not
    re-raise the renewal's error after a delivery that succeeded."""
    renewed_after_error = asyncio.Event()

    class BlippingRenewalStore(ClaimingStubStore):
        async def renew_claim(self, publication_id: UUID, token: str, lease_seconds: float) -> bool:
            call = len(self.renew_calls) + 1
            if call == 2:
                self.renew_calls.append((publication_id, token, lease_seconds))
                raise OSError("db blip")
            if call >= 3:
                renewed_after_error.set()
            return await super().renew_claim(publication_id, token, lease_seconds)

    async def wait_for_renewal(event: OutboxEvent) -> None:
        await renewed_after_error.wait()
        received.append(event.value)

    store = BlippingRenewalStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=0.3,
        start_loop=False,
    )
    _bootstrap_with_listener(wait_for_renewal)
    publication = _make_pub(wait_for_renewal, value=15)
    await store.save(publication)

    with caplog.at_level(logging.WARNING, logger="modulith.outbox"):
        await asyncio.wait_for(outbox._sweep(timedelta(0)), timeout=2)

    assert received == [15]
    assert store.rows[publication.id].completed_at is not None
    assert any(r.exc_info and "db blip" in str(r.exc_info[1]) for r in caplog.records)


@pytest.mark.asyncio
async def test_renewal_that_keeps_raising_stops_at_the_lease_deadline() -> None:
    """Renewals that keep raising are retried only until the lease they
    were protecting has expired; the delivery itself still completes and
    is not reported as a failure."""

    class DownRenewalStore(ClaimingStubStore):
        async def renew_claim(self, publication_id: UUID, token: str, lease_seconds: float) -> bool:
            self.renew_calls.append((publication_id, token, lease_seconds))
            raise OSError("db down")

    async def outlives_the_lease(event: OutboxEvent) -> None:
        await asyncio.sleep(1.2)
        received.append(event.value)

    store = DownRenewalStore()
    outbox.configure(
        store,
        JsonEventSerializer(),
        claim_strategy="lease",
        claim_lease_seconds=0.3,
        start_loop=False,
    )
    _bootstrap_with_listener(outlives_the_lease)
    publication = _make_pub(outlives_the_lease, value=16)
    await store.save(publication)
    [claimed] = await store.claim_batch(
        owner="me", batch_size=1, lease_seconds=0.3, older_than=timedelta(0)
    )

    await outbox._dispatch_with_lease_renewal(claimed)

    assert received == [16]
    assert store.rows[publication.id].completed_at is not None
    assert 2 <= len(store.renew_calls) <= 4


@pytest.mark.asyncio
async def test_dispatch_with_lease_renewal_propagates_outer_cancellation() -> None:
    """A cancellation landing on the ENCLOSING task while it is suspended at
    the finally's ``await renew_task`` must propagate, matching
    ``_polling_consumer.PollingConsumer._cancel``'s ``cancelling() > 0``
    re-raise — not be swallowed as if it were merely the renew task's own
    expected exit. Otherwise ``outbox.shutdown()``'s single cancellation is
    discarded here and its busy-poll spins forever.

    None of the stub store/handler calls in this dispatch path perform a
    real suspension, so the enclosing task's first genuine await is the
    finally's ``await renew_task`` — a single ``asyncio.sleep(0)`` runs the
    whole dispatch synchronously up to exactly that point.
    """
    store = ClaimingStubStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="lease", start_loop=False)
    _bootstrap_with_listener(record)
    publication = _make_pub(record, value=99)
    await store.save(publication)
    (claimed,) = await store.claim_batch(
        owner="worker", batch_size=1, lease_seconds=60, older_than=timedelta(0)
    )

    outer = asyncio.create_task(outbox._dispatch_with_lease_renewal(claimed))
    await asyncio.sleep(0)
    outer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await outer
    assert outer.cancelled()


@pytest.mark.asyncio
async def test_shutdown_handles_running_loop_lookup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fault injection verifies the defensive no-running-loop cancellation path.

    Awaiting this coroutine normally guarantees a running event loop. Raising
    once from the lookup exercises the fallback without changing task behavior.
    """
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    task = outbox._retry_task
    assert task is not None
    get_running_loop = asyncio.get_running_loop
    attempts = 0

    def fail_once() -> asyncio.AbstractEventLoop:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("running loop temporarily unavailable")
        return get_running_loop()

    monkeypatch.setattr(asyncio, "get_running_loop", fail_once)

    await outbox.shutdown()

    assert task.cancelled()
    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_shutdown_preserves_concurrently_installed_retry_task() -> None:
    replacement: asyncio.Task[None] | None = None

    async def retiring_task() -> None:
        nonlocal replacement
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            replacement = asyncio.create_task(asyncio.sleep(60))
            outbox._retry_task = replacement
            raise

    original = asyncio.create_task(retiring_task())
    await asyncio.sleep(0)
    outbox._retry_task = original

    await outbox.shutdown()

    assert replacement is not None
    assert outbox._retry_task is replacement
    assert not replacement.done()
    await outbox.shutdown()


class HeldFindStore(ClaimingStubStore):
    """The first ``find_incomplete`` call waits for ``gate`` and records how it ended."""

    def __init__(self, gate: asyncio.Event | None = None) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.gate = gate or asyncio.Event()
        self.outcome: str | None = None

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        if self.outcome is None:
            self.outcome = "running"
            self.entered.set()
            try:
                await self.gate.wait()
            except asyncio.CancelledError:
                self.outcome = "cancelled"
                raise
            self.outcome = "completed"
        return await super().find_incomplete(older_than)


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_strategy", ["none", "advisory_lock"])
async def test_shutdown_lets_an_in_flight_store_call_finish_then_dispatches_nothing(
    claim_strategy: str,
) -> None:
    store = HeldFindStore()
    outbox.configure(
        store, JsonEventSerializer(), claim_strategy=claim_strategy, retry_interval_seconds=60
    )
    _bootstrap_with_listener(record)
    pub = _make_pub(record, value=5)
    await store.save(pub)
    task = outbox._retry_task
    assert task is not None
    await asyncio.wait_for(store.entered.wait(), timeout=1)

    stopping = asyncio.create_task(outbox.shutdown())
    await asyncio.sleep(0.05)
    assert not stopping.done()
    store.gate.set()
    await asyncio.wait_for(stopping, timeout=1)

    assert store.outcome == "completed"
    assert task.done() and not task.cancelled()
    assert outbox._retry_task is None
    assert received == []
    assert store.rows[pub.id].attempt_count == 0
    assert store.lock_calls == []


@pytest.mark.asyncio
async def test_shutdown_between_lease_rows_releases_the_rest_uncharged(tmp_path: Path) -> None:
    entered = asyncio.Event()
    gate = asyncio.Event()

    async def held(event: OutboxEvent) -> None:
        entered.set()
        await gate.wait()
        received.append(event.value)

    _bootstrap_with_listener(held)
    engine, store = await _sqlite_outbox(tmp_path)
    try:
        start = datetime.now(UTC) - timedelta(seconds=10)
        pubs = [
            _make_pub(held, value=v, published_at=start + timedelta(seconds=v)) for v in (1, 2, 3)
        ]
        for pub in pubs:
            await store.save(pub)
        outbox.configure(
            store,
            JsonEventSerializer(),
            claim_strategy="lease",
            claim_lease_seconds=60.0,
            retry_interval_seconds=60,
        )
        task = outbox._retry_task
        assert task is not None
        await asyncio.wait_for(entered.wait(), timeout=5)

        stopping = asyncio.create_task(outbox.shutdown())
        await asyncio.sleep(0.05)
        assert not stopping.done()
        gate.set()
        await asyncio.wait_for(stopping, timeout=5)

        assert task.done() and not task.cancelled()
        assert received == [1]
        async with async_sessionmaker(engine)() as session:
            rows = {
                row.id: row
                for row in (await session.execute(select(EventPublicationRow))).scalars()
            }
        first, *rest = pubs
        assert rows[first.id].completed_at is not None
        assert [
            (rows[p.id].attempt_count, rows[p.id].claim_token, rows[p.id].completed_at)
            for p in rest
        ] == [(0, None, None), (0, None, None)]
    finally:
        await store.dispose()
        await engine.dispose()


@pytest.mark.asyncio
async def test_shutdown_between_lease_rows_keeps_the_claim_of_a_row_delivered_elsewhere(
    tmp_path: Path,
) -> None:
    entered = asyncio.Event()
    gate = asyncio.Event()

    async def held(event: OutboxEvent) -> None:
        entered.set()
        await gate.wait()
        received.append(event.value)

    _bootstrap_with_listener(held)
    engine, store = await _sqlite_outbox(tmp_path)
    try:
        start = datetime.now(UTC) - timedelta(seconds=10)
        first, second, third = [
            _make_pub(held, value=v, published_at=start + timedelta(seconds=v)) for v in (1, 2, 3)
        ]
        for pub in (first, second, third):
            await store.save(pub)
        outbox.configure(
            store,
            JsonEventSerializer(),
            claim_strategy="lease",
            claim_lease_seconds=60.0,
            retry_interval_seconds=60,
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        with outbox._inflight_lock:
            outbox._inflight_ids.add(third.id)

        stopping = asyncio.create_task(outbox.shutdown())
        await asyncio.sleep(0.05)
        gate.set()
        await asyncio.wait_for(stopping, timeout=5)

        assert received == [1]
        async with async_sessionmaker(engine)() as session:
            rows = {
                row.id: row
                for row in (await session.execute(select(EventPublicationRow))).scalars()
            }
        assert (
            rows[second.id].attempt_count,
            rows[second.id].claim_token,
            rows[second.id].completed_at,
        ) == (0, None, None)
        kept = rows[third.id]
        assert kept.claim_token is not None
        assert kept.claim_owner is not None
        assert (kept.attempt_count, kept.dispatch_started, kept.completed_at) == (0, False, None)
    finally:
        with outbox._inflight_lock:
            outbox._inflight_ids.discard(third.id)
        await store.dispose()
        await engine.dispose()


@pytest.mark.asyncio
async def test_shutdown_cancels_a_store_call_that_outlasts_the_grace_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(outbox, "_shutdown_grace_seconds", 0.2)
    store = HeldFindStore()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="none", retry_interval_seconds=60)
    task = outbox._retry_task
    assert task is not None
    await asyncio.wait_for(store.entered.wait(), timeout=1)

    loop = asyncio.get_running_loop()
    began = loop.time()
    await asyncio.wait_for(outbox.shutdown(), timeout=2)
    waited = loop.time() - began

    assert waited >= 0.2
    assert store.outcome == "cancelled"
    assert task.cancelled()
    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_shutdown_cancels_a_sleeping_retry_loop_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(outbox, "_shutdown_grace_seconds", 30.0)
    store = HeldFindStore()
    store.gate.set()
    outbox.configure(store, JsonEventSerializer(), claim_strategy="none", retry_interval_seconds=60)
    task = outbox._retry_task
    assert task is not None
    await asyncio.wait_for(store.entered.wait(), timeout=1)
    await asyncio.sleep(0.05)
    assert store.outcome == "completed"

    await asyncio.wait_for(outbox.shutdown(), timeout=1)

    assert task.cancelled()
    assert outbox._retry_task is None


@pytest.mark.asyncio
async def test_core_store_maintenance_fallbacks_report_public_state() -> None:
    store = CoreOnlyStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    publication = _make_pub(record)
    await store.save(publication)

    counts = await outbox.status()
    purged = await outbox.purge_completed(timedelta(days=1))

    assert counts == {"incomplete": 1, "completed": 0, "dead_lettered": 0}
    assert purged == 0


@pytest.mark.asyncio
async def test_force_retry_direct_lookup_ignores_missing_and_completed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class LookupStore(StubStore):
        async def find_by_id(self, publication_id: UUID) -> EventPublication | None:
            return self.rows.get(publication_id)

    store = LookupStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)
    completed = _make_pub(record, value=15)
    completed.completed_at = datetime.now(UTC)
    store.rows[completed.id] = completed
    incomplete = _make_pub(record, value=20)
    store.rows[incomplete.id] = incomplete

    with caplog.at_level(logging.WARNING, logger="modulith.outbox"):
        await outbox.force_retry(uuid4())
        await outbox.force_retry(completed.id)
        await outbox.force_retry(incomplete.id)

    assert received == [20]
    assert incomplete.completed_at is not None
    assert sum("not found or already complete" in item.message for item in caplog.records) == 2


@pytest.mark.asyncio
async def test_force_retry_legacy_scan_reaches_later_candidate() -> None:
    store = CoreOnlyStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)
    first = _make_pub(record, value=16)
    target = _make_pub(record, value=17)
    await store.save(first)
    await store.save(target)

    await outbox.force_retry(target.id)

    assert received == [17]
    assert first.completed_at is None
    assert target.completed_at is not None


@pytest.mark.asyncio
async def test_force_retry_legacy_scan_warns_for_unknown_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = CoreOnlyStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    with caplog.at_level(logging.WARNING, logger="modulith.outbox"):
        await outbox.force_retry(uuid4())

    assert any("not found or already complete" in item.message for item in caplog.records)


@pytest.mark.asyncio
async def test_list_dead_lettered_accepts_empty_paginated_result() -> None:
    class EmptyPagedStore(StubStore):
        async def find_dead_lettered(
            self, *, after: tuple[datetime, UUID] | None, limit: int
        ) -> list[EventPublication]:
            return []

    store = EmptyPagedStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    assert await outbox.list_dead_lettered() == []


@pytest.mark.asyncio
async def test_list_dead_lettered_pages_across_a_full_page() -> None:
    """A backlog exceeding one page (100 rows) must page through via the
    keyset (after=(published_at, id)) rather than stopping at the first
    full page."""

    class PagedStore(StubStore):
        def __init__(self) -> None:
            super().__init__()
            self.dead: list[EventPublication] = []

        async def find_dead_lettered(
            self, *, after: tuple[datetime, UUID] | None, limit: int
        ) -> list[EventPublication]:
            ordered = sorted(self.dead, key=lambda p: (p.published_at, p.id))
            if after is not None:
                ordered = [p for p in ordered if (p.published_at, p.id) > after]
            return ordered[:limit]

    store = PagedStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    base = datetime.now(UTC) - timedelta(hours=1)
    store.dead = [
        _make_pub(record, value=i, attempt_count=10, published_at=base + timedelta(seconds=i))
        for i in range(101)
    ]

    result = await outbox.list_dead_lettered()

    assert {p.id for p in result} == {p.id for p in store.dead}
    assert len(result) == 101


@pytest.mark.asyncio
async def test_retry_all_dead_lettered_resubmits_with_reset_state() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=2, start_loop=False)
    _bootstrap_with_listener(record)
    dead = _make_pub(record, value=1, attempt_count=2, last_error="boom")
    store.rows[dead.id] = dead

    count = await outbox.retry_all_dead_lettered()

    assert count == 1
    assert received == [1]
    assert store.rows[dead.id].completed_at is not None


# ---------------------------------------------------------------------------
# Listener identity on the durable path: every stored row must name exactly
# one listener, stably across restarts.
# ---------------------------------------------------------------------------

callable_log: list[str] = []


class EmailSender:
    async def __call__(self, evt: OutboxEvent) -> None:
        callable_log.append(f"email {evt.value}")


class SmsSender:
    async def __call__(self, evt: OutboxEvent) -> None:
        callable_log.append(f"sms {evt.value}")


class Notifier:
    async def __call__(self, evt: OutboxEvent) -> None:
        callable_log.append(f"notifier {evt.value}")

    async def handle(self, evt: OutboxEvent) -> None:
        callable_log.append(f"handle {evt.value}")


async def _sqlite_outbox(tmp_path: Path) -> tuple[Any, PostgresPublicationStore]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}", poolclass=NullPool
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    return engine, store


async def _publish_in_session(engine: Any, evt: OutboxEvent) -> None:
    async with async_sessionmaker(engine)() as session:
        token = bind_session(session)
        try:
            await publish(evt)
            await session.commit()
        finally:
            unbind_session(token)


async def _stored_rows(engine: Any) -> list[tuple[str, bool]]:
    async with async_sessionmaker(engine)() as session:
        rows = (
            await session.execute(
                select(EventPublicationRow.listener, EventPublicationRow.completed_at)
            )
        ).all()
    return sorted((row.listener, row.completed_at is not None) for row in rows)


@pytest.mark.asyncio
async def test_durable_publish_delivers_each_callable_instance_listener_once(
    tmp_path: Path,
) -> None:
    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    callable_log.clear()
    listener(EmailSender())
    listener(SmsSender())
    engine, store = await _sqlite_outbox(tmp_path)
    try:
        await _publish_in_session(engine, OutboxEvent(value=1))
        await store.wait_for_dispatch()

        assert sorted(callable_log) == ["email 1", "sms 1"]
        assert await _stored_rows(engine) == [
            (f"{__name__}.EmailSender", True),
            (f"{__name__}.SmsSender", True),
        ]
    finally:
        await store.dispose()
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_methods", [False, True], ids=["instances", "bound-methods"])
async def test_durable_publish_rejects_two_listeners_sharing_a_stored_id(
    tmp_path: Path, bound_methods: bool
) -> None:
    _runtime.configure(package="outboxtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    callable_log.clear()
    first, second = Notifier(), Notifier()
    if bound_methods:
        assert _runtime.event_bus is not None
        _runtime.event_bus.register(OutboxEvent, first.handle)
        _runtime.event_bus.register(OutboxEvent, second.handle)
    else:
        listener(first)
        listener(second)
    engine, store = await _sqlite_outbox(tmp_path)
    try:
        with pytest.raises(ConfigurationError) as excinfo:
            await _publish_in_session(engine, OutboxEvent(value=1))

        message = str(excinfo.value)
        assert repr(first) in message
        assert repr(second) in message
        assert f"{__name__}.Notifier" in message
        assert "distinct class or module-level function" in message
        assert await _stored_rows(engine) == []
        assert callable_log == []
    finally:
        await store.dispose()
        await engine.dispose()


@pytest.mark.asyncio
async def test_persist_accepts_one_listener_registered_twice() -> None:
    outbox.configure(StubStore(), JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(OutboxEvent, record)

    saved = await outbox.persist(OutboxEvent(value=1))

    assert [pub.listener for pub in saved] == [f"{__name__}.record"] * 2


def test_start_runs_crash_sweep_for_store_configured_without_a_loop() -> None:
    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=60)
    before_start = outbox._retry_task

    async def scenario() -> bool:
        outbox.start()
        first = outbox._retry_task
        outbox.start()
        await asyncio.sleep(0.05)
        return first is not None and outbox._retry_task is first

    reused = asyncio.run(scenario())

    assert (before_start, reused, store.find_incomplete_calls) == (None, True, [timedelta(0)])


_ORDERS_APP = {
    "orders": """
        from dataclasses import dataclass

        from modulith import event, listener

        RECEIVED: list[str] = []

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        @listener
        async def on_placed(evt: OrderPlaced) -> None:
            RECEIVED.append(evt.order_id)
    """
}


def test_bootstrap_binds_store_from_outbox_url_and_delivers_after_commit(
    make_fake_app: Any, tmp_path: Path
) -> None:
    from modulith.adapters import postgres_outbox

    url = f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"
    make_fake_app(_ORDERS_APP)
    _runtime.configure(
        package="fakeapp",
        outbox="postgres",
        outbox_url=url,
        outbox_options={"claim_lease_seconds": 7},
    )
    _runtime.ensure_bootstrapped()
    orders = __import__("fakeapp.orders", fromlist=["OrderPlaced"])
    bound = (
        type(outbox._store).__name__,
        outbox._claim_strategy,
        outbox._claim_lease_seconds,
        len(outbox._claim_owner),
    )

    async def scenario() -> list[tuple[str, bool]]:
        engine = create_async_engine(url, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await _publish_in_session(engine, orders.OrderPlaced("o1"))
        assert outbox._store is not None
        await outbox._store.wait_for_dispatch()  # type: ignore[attr-defined]
        # The first transactional publish also starts the retry loop, whose
        # crash sweep may be the one that claims and completes the row.
        deadline = asyncio.get_running_loop().time() + 2.0
        rows = await _stored_rows(engine)
        while not all(done for _, done in rows) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
            rows = await _stored_rows(engine)
        await _runtime.shutdown()
        await engine.dispose()
        return rows

    rows = asyncio.run(scenario())

    assert (bound, orders.RECEIVED, rows) == (
        ("PostgresPublicationStore", "lease", 7.0, 32),
        ["o1"],
        [("fakeapp.orders.on_placed", True)],
    )
    assert postgres_outbox._active_store is None


_FAILING_LISTENER_APP = {
    "orders": """
        from dataclasses import dataclass

        from modulith import event, listener

        ATTEMPTS: list[str] = []

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        @listener
        async def on_placed(evt: OrderPlaced) -> None:
            ATTEMPTS.append(evt.order_id)
            raise RuntimeError("boom")
    """
}

_FAILS_ONCE_APP = {
    "orders": """
        from dataclasses import dataclass

        from modulith import event, listener

        ATTEMPTS: list[str] = []

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        @listener
        async def on_placed(evt: OrderPlaced) -> None:
            ATTEMPTS.append(evt.order_id)
            if len(ATTEMPTS) == 1:
                raise RuntimeError("first attempt fails")
    """
}


def _publish_after_bootstrap_from_outbox_url(
    make_fake_app: Any,
    tmp_path: Path,
    app: dict[str, str],
    options: dict[str, Any],
    *,
    settled: Any,
    timeout: float,
) -> tuple[Any, list[tuple[bool, int, bool]], dict[str, int]]:
    """Bootstrap ``fakeapp`` from ``outbox_url``, publish one OrderPlaced and
    wait until ``settled(rows)`` or ``timeout`` seconds pass. Returns the
    orders module and the rows as ``(completed, attempt_count, dead_lettered)``."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"
    make_fake_app(app)
    _runtime.configure(package="fakeapp", outbox="postgres", outbox_url=url, outbox_options=options)
    _runtime.ensure_bootstrapped()
    orders = __import__("fakeapp.orders", fromlist=["OrderPlaced"])

    async def rows_of(engine: Any) -> list[tuple[bool, int, bool]]:
        async with async_sessionmaker(engine)() as session:
            result = await session.execute(
                select(
                    EventPublicationRow.completed_at,
                    EventPublicationRow.attempt_count,
                    EventPublicationRow.is_dead_lettered,
                )
            )
        return [(done is not None, attempts, dead) for done, attempts, dead in result.all()]

    async def scenario() -> tuple[list[tuple[bool, int, bool]], dict[str, int]]:
        engine = create_async_engine(url, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await _publish_in_session(engine, orders.OrderPlaced("o1"))
        assert outbox._store is not None
        await outbox._store.wait_for_dispatch()  # type: ignore[attr-defined]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        rows = await rows_of(engine)
        while not settled(rows) and loop.time() < deadline:
            await asyncio.sleep(0.01)
            rows = await rows_of(engine)
        counts = await outbox.status()
        await _runtime.shutdown()
        await engine.dispose()
        return rows, counts

    rows, counts = asyncio.run(scenario())
    return orders, rows, counts


def test_outbox_url_dead_letter_after_attempts_dead_letters_after_one_failure(
    make_fake_app: Any, tmp_path: Path
) -> None:
    orders, rows, counts = _publish_after_bootstrap_from_outbox_url(
        make_fake_app,
        tmp_path,
        _FAILING_LISTENER_APP,
        {"dead_letter_after_attempts": 1},
        settled=lambda rows: any(attempts >= 1 for _, attempts, _ in rows),
        timeout=2.0,
    )

    assert (orders.ATTEMPTS, rows, counts) == (
        ["o1"],
        [(False, 1, True)],
        {"incomplete": 0, "completed": 0, "dead_lettered": 1},
    )


def test_outbox_url_completion_mode_delete_leaves_no_row_for_a_delivered_publication(
    make_fake_app: Any, tmp_path: Path
) -> None:
    orders, rows, _ = _publish_after_bootstrap_from_outbox_url(
        make_fake_app,
        tmp_path,
        _ORDERS_APP,
        {"completion_mode": "delete"},
        settled=lambda rows: not rows,
        timeout=2.0,
    )

    assert (orders.RECEIVED, rows) == (["o1"], [])


def test_outbox_url_retry_keys_retry_a_failed_delivery_within_a_second(
    make_fake_app: Any, tmp_path: Path
) -> None:
    orders, rows, _ = _publish_after_bootstrap_from_outbox_url(
        make_fake_app,
        tmp_path,
        _FAILS_ONCE_APP,
        {
            "retry_interval_seconds": 0.05,
            "retry_stale_seconds": 0.05,
            "max_retry_backoff_seconds": 0.05,
        },
        settled=lambda rows: any(done for done, _, _ in rows),
        timeout=0.8,
    )

    assert (orders.ATTEMPTS, [done for done, _, _ in rows]) == (["o1", "o1"], [True])


def test_explicitly_configured_store_wins_over_outbox_url(
    make_fake_app: Any, tmp_path: Path
) -> None:
    from modulith.adapters import postgres_outbox

    store = StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    make_fake_app(_ORDERS_APP)
    _runtime.configure(
        package="fakeapp",
        outbox="postgres",
        outbox_url=f"sqlite+aiosqlite:///{tmp_path / 'app.db'}",
    )
    _runtime.ensure_bootstrapped()

    assert (outbox._store is store, postgres_outbox._active_store) == (True, None)


_SHARED_LISTENER_CLASS_APP = {
    "contracts": """
        from dataclasses import dataclass

        from modulith import event

        RUNS: list[tuple[str, str]] = []

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        class Notifier:
            def __init__(self, name: str) -> None:
                self.name = name

            async def __call__(self, evt: OrderPlaced) -> None:
                RUNS.append((self.name, evt.order_id))
    """,
    "orders": """
        from modulith import listener
        from fakeapp.contracts import Notifier

        listener(Notifier("orders"))
    """,
    "billing": """
        from modulith import listener
        from fakeapp.contracts import RUNS, Notifier, OrderPlaced

        listener(Notifier("billing"))

        @listener
        async def on_placed(evt: OrderPlaced) -> None:
            RUNS.append(("billing.on_placed", evt.order_id))
    """,
}


@pytest.mark.asyncio
async def test_rows_of_one_listener_class_are_attributed_to_each_registering_module(
    make_fake_app: Any,
) -> None:
    make_fake_app(_SHARED_LISTENER_CLASS_APP)
    _runtime.configure(package="fakeapp")
    _runtime.ensure_bootstrapped()
    outbox.configure(StubStore(), JsonEventSerializer(), start_loop=False)
    contracts = __import__("fakeapp.contracts", fromlist=["OrderPlaced"])

    saved = await outbox.persist(contracts.OrderPlaced("o1"))

    assert sorted(pub.listener or "" for pub in saved) == [
        "fakeapp.billing.on_placed",
        "fakeapp.billing:fakeapp.contracts.Notifier",
        "fakeapp.orders:fakeapp.contracts.Notifier",
    ]


def test_start_without_a_bound_store_starts_nothing() -> None:
    async def scenario() -> Any:
        outbox.start()
        return outbox._retry_task

    assert asyncio.run(scenario()) is None
