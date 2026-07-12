"""W2 G02_runtime — A2-r4-168: durable-path broker sends must be commit-gated.

`publish()` on the durable path (outbox store configured + session bound) used
to hand @externalized / cross-module events to the broker *synchronously,
before commit* — a rolled-back transaction could not un-send them (the exact
inconsistency SPEC's outbox section names as the problem the outbox solves).

The fixed contract, pinned here (written failing-first against the pre-fix
code):
  * publish() inside a transaction sends NOTHING to the broker;
  * instead a broker-route publication row is enlisted in the bound session,
    committing (or rolling back) atomically with the business work;
  * the after-commit dispatch path delivers that row to the broker, with the
    same at-least-once retry machinery local listeners get.
"""

from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

import pytest

from modulith import EventPublication, configure, event, externalized, listener
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Each test gets a clean runtime + manifest registry + outbox state."""
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


class FakeBroker:
    """Records publishes; satisfies the Broker protocol (publish + close)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str] | None]] = []
        self.closed = False

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.published.append((target, payload, headers))

    async def close(self) -> None:
        self.closed = True


class RecordingStore:
    """Minimal PublicationStore that records saves and completions."""

    def __init__(self) -> None:
        self.saved: list[EventPublication] = []
        self.completed: list[UUID] = []

    async def save(self, publication: EventPublication) -> None:
        self.saved.append(publication)

    async def mark_complete(self, publication_id: UUID) -> None:
        self.completed.append(publication_id)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return []

    async def archive(self, publication_id: UUID) -> None:
        pass

    async def delete(self, publication_id: UUID) -> None:
        pass


class Session:
    """Only the outbox context binding matters for these tests."""

    def __init__(self) -> None:
        self.info: dict[str, object] = {}


# Module-level so the serializer can resolve them from their fully-qualified
# name (f"{cls.__module__}.{cls.__qualname__}") during after-commit dispatch.


@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


@externalized
@event
@dataclass(frozen=True)
class InvoiceReady:
    invoice_id: str


def _fqn(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _durable_processes_setup() -> tuple[FakeBroker, RecordingStore]:
    configure(package="gatetest", topology="processes", broker="testbroker", auto_discover=False)
    _runtime.ensure_bootstrapped()
    fake = FakeBroker()
    registry = _runtime.broker_registry
    assert registry is not None
    registry.register("testbroker", fake)
    store = RecordingStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    return fake, store


def _broker_routes(store: RecordingStore) -> list[EventPublication]:
    return [
        p
        for p in store.saved
        if (p.listener or "").startswith(outbox._BROKER_ROUTE_LISTENER_PREFIX)
    ]


async def test_durable_publish_defers_broker_send_to_after_commit() -> None:
    """A2-r4-168: inside a transaction, publish() must not touch the broker.

    The route is persisted as a publication row in the bound session; the
    after-commit dispatch delivers it.
    """
    fake, store = _durable_processes_setup()

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(OrderPlaced(order_id="o-tx"))
    finally:
        outbox._current_session.reset(token)

    # Pre-commit: nothing on the wire — a rollback can still discard the row.
    assert fake.published == []
    routes = _broker_routes(store)
    assert len(routes) == 1

    # After-commit dispatch (what the adapter's after_commit hook drives).
    await outbox._dispatch_publication(routes[0])

    assert len(fake.published) == 1
    destination, payload, headers = fake.published[0]
    assert destination == _fqn(OrderPlaced)
    assert b"o-tx" in payload
    assert headers is not None
    assert headers["event_type"] == _fqn(OrderPlaced)
    # The route row is completed like any delivered publication.
    assert routes[0].id in store.completed


async def test_rolled_back_publish_never_reaches_broker() -> None:
    """A2-r4-168: a rollback discards the broker-route row with the business
    transaction — nothing was sent synchronously, so nothing can leak out."""
    fake, _store = _durable_processes_setup()

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(OrderPlaced(order_id="o-rollback"))
    finally:
        outbox._current_session.reset(token)

    # Rollback == the saved rows are never committed, so after-commit dispatch
    # never runs for them. The only requirement on the runtime is that the
    # broker saw nothing at publish() time.
    assert fake.published == []


async def test_externalized_event_with_local_listener_fans_out_after_commit() -> None:
    """A2-r4-168 (fan-out): an @externalized event with a local listener gets
    one listener row AND one broker-route row; both deliver after commit."""
    fake, store = _durable_processes_setup()

    received: list[InvoiceReady] = []

    @listener
    async def on_invoice(evt: InvoiceReady) -> None:
        received.append(evt)

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(InvoiceReady(invoice_id="i-1"))
    finally:
        outbox._current_session.reset(token)

    assert fake.published == []
    assert received == []  # local delivery is commit-gated too (pre-existing)

    routes = _broker_routes(store)
    listener_rows = [p for p in store.saved if all(p is not r for r in routes)]
    assert len(routes) == 1
    assert len(listener_rows) == 1

    for row in store.saved:
        await outbox._dispatch_publication(row)

    assert len(received) == 1
    assert len(fake.published) == 1
    destination, payload, _headers = fake.published[0]
    assert destination == _fqn(InvoiceReady)
    assert b"i-1" in payload


async def test_failed_broker_route_dispatch_is_recorded_for_retry() -> None:
    """A2-r4-168 (durability): a broker-route row whose send fails after
    commit records the attempt (retry-loop food) instead of raising out of
    the after-commit dispatch task."""
    fake, store = _durable_processes_setup()

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(OrderPlaced(order_id="o-retry"))
    finally:
        outbox._current_session.reset(token)

    (route,) = _broker_routes(store)

    # Simulate the broker adapter vanishing between commit and dispatch
    # (process restart before a broker plugin re-registered, say).
    registry = _runtime.broker_registry
    assert registry is not None
    registry._brokers.clear()

    await outbox._dispatch_publication(route)  # must not raise

    assert fake.published == []
    assert route.attempt_count == 1
    assert route.last_error is not None
    assert route.id not in store.completed
