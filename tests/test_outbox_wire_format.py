"""Outbox durable-path wire-format and unbootstrapped-sweep fixes.

R1-F2: broker WIRE format is fixed JSON in v1. The outbox STORAGE serializer
is an extension point (binary Avro/Protobuf/pickle payloads), but the wire
payload handed to a broker must be what the worker consumer's hardwired
``JsonEventSerializer`` can decode — the durable path (persist_broker_route)
must not leak the storage serializer onto the wire.

R1-F1: the retry loop must not burn attempts (and eventually dead-letter)
committed rows while the runtime is un-bootstrapped — an infrastructure
outage is not a poison message. Rows are skipped for the cycle with no
attempt/backoff bookkeeping.
"""

import pickle
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

from modulith import EventPublication, configure, event, externalized
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@pytest.fixture(autouse=True)
def _reset_runtime():
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


class PickleSerializer:
    """A binary EventSerializer, as the payload-is-bytes contract invites."""

    def serialize(self, event: Any) -> bytes:
        return pickle.dumps(event)

    def deserialize(self, data: bytes, event_type: str) -> Any:
        return pickle.loads(data)


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str] | None]] = []

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.published.append((target, payload, headers))

    async def close(self) -> None:
        pass


class RecordingStore:
    def __init__(self) -> None:
        self.saved: list[EventPublication] = []
        self.completed: list[UUID] = []

    async def save(self, publication: EventPublication) -> None:
        if publication not in self.saved:
            self.saved.append(publication)

    async def mark_complete(self, publication_id: UUID) -> None:
        self.completed.append(publication_id)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return [p for p in self.saved if p.completed_at is None]

    async def archive(self, publication_id: UUID) -> None:
        pass

    async def delete(self, publication_id: UUID) -> None:
        pass


class Session:
    def __init__(self) -> None:
        self.info: dict[str, object] = {}


@externalized
@event
@dataclass(frozen=True)
class InvoiceReady:
    invoice_id: str


def _fqn(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _broker_routes(store: RecordingStore) -> list[EventPublication]:
    return [
        p
        for p in store.saved
        if (p.listener or "").startswith(outbox._BROKER_ROUTE_LISTENER_PREFIX)
    ]


def _durable_setup(serializer: Any) -> tuple[FakeBroker, RecordingStore]:
    configure(package="w3r1", topology="processes", broker="testbroker", auto_discover=False)
    _runtime.ensure_bootstrapped()
    fake = FakeBroker()
    registry = _runtime.broker_registry
    assert registry is not None
    registry.register("testbroker", fake)
    store = RecordingStore()
    outbox.configure(store, serializer, start_loop=False)
    return fake, store


# ---------------------------------------------------------------------------
# R1-F2 — serializer symmetry: broker wire format is JSON v1
# ---------------------------------------------------------------------------


async def test_durable_broker_route_uses_json_wire_format_not_storage_serializer() -> None:
    """R1-F2: with a binary STORAGE serializer configured, the payload the
    broker receives from the durable path must still be JSON — the same wire
    format the direct path sends and the worker consumer decodes."""
    fake, store = _durable_setup(PickleSerializer())

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(InvoiceReady(invoice_id="I-9"))
    finally:
        outbox._current_session.reset(token)

    routes = _broker_routes(store)
    assert len(routes) == 1
    await outbox._dispatch_publication(routes[0])

    assert len(fake.published) == 1
    _target, wire_payload, _headers = fake.published[0]
    # The worker consumer hardwires JsonEventSerializer (wire format v1).
    decoded = JsonEventSerializer().deserialize(wire_payload, _fqn(InvoiceReady))
    assert decoded == InvoiceReady(invoice_id="I-9")


async def test_durable_and_direct_paths_produce_identical_wire_payloads() -> None:
    """R1-F2: wire symmetry — the durable path and the direct path put the
    same bytes on the wire for the same event."""
    fake, store = _durable_setup(PickleSerializer())

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(InvoiceReady(invoice_id="I-10"))
    finally:
        outbox._current_session.reset(token)
    routes = _broker_routes(store)
    assert len(routes) == 1
    await outbox._dispatch_publication(routes[0])

    # Direct path: no session bound -> inline broker send.
    await _runtime.publish(InvoiceReady(invoice_id="I-10"))

    assert len(fake.published) == 2
    durable_payload = fake.published[0][1]
    direct_payload = fake.published[1][1]
    assert durable_payload == direct_payload


# ---------------------------------------------------------------------------
# R1-F1 — retry loop must not burn attempts while un-bootstrapped
# ---------------------------------------------------------------------------


async def test_sweep_skips_rows_without_attempt_bookkeeping_when_unbootstrapped() -> None:
    """R1-F1: a sweep against an un-bootstrapped runtime skips every row for
    the cycle — no attempt_count increment, no last_error, no dead-letter."""
    fake, store = _durable_setup(JsonEventSerializer())

    token = outbox._current_session.set(Session())
    try:
        await _runtime.publish(InvoiceReady(invoice_id="I-11"))
    finally:
        outbox._current_session.reset(token)
    routes = _broker_routes(store)
    assert len(routes) == 1
    row = routes[0]

    # Simulate a worker whose bootstrap is failing: state bound (store and
    # serializer configured) but the runtime is not bootstrapped.
    saved_serializer = outbox._serializer
    _runtime._reset_for_testing()
    outbox._store = store
    outbox._serializer = saved_serializer
    outbox._retry_loop_enabled = False

    for _ in range(3):
        await outbox._sweep(timedelta(0))

    assert row.attempt_count == 0, (
        "infrastructure outage must not burn retry attempts "
        f"(attempt_count={row.attempt_count}, last_error={row.last_error!r})"
    )
    assert row.last_error is None
    assert row.completed_at is None  # still pending, retried next cycle
    assert fake.published == []
