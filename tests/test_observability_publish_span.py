"""A failed publish must not leak the OpenTelemetry publish span.

``modulith_before_event_published`` starts the publish span unconditionally;
``modulith_after_event_published`` — the only place that ended it — is
contractually scoped to SUCCESSFUL persistence/dispatch. On the durable
path, ``Runtime.publish`` awaits outbox persist/serialize/broker-route
BETWEEN the two hooks: a store failure (the DB outage the outbox exists
for) propagated and the span was never ended — unexported forever, with the
stale ``_publish_span`` ContextVar mis-parenting the next dispatch span in
the same context. The fix ends the span, records the exception, and resets
the ContextVar on the failure path.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from modulith import ConfigurationError, EventPublication, configure, event
from modulith.builtin import observability, outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@dataclass(frozen=True)
class DurableEvt:
    x: int


class FailingStore:
    """PublicationStore whose save() fails — the DB-down durable path."""

    async def save(self, publication: Any) -> None:
        raise ConnectionError("db down")

    async def mark_complete(self, publication_id: UUID) -> None: ...

    async def find_incomplete(self, older_than: timedelta) -> list[Any]:
        return []

    async def archive(self, publication_id: UUID) -> None: ...

    async def delete(self, publication_id: UUID) -> None: ...


@pytest.fixture(autouse=True)
def _reset_state():
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    outbox._reset_for_testing()
    yield
    outbox._reset_for_testing()
    manifest_module._reset_for_testing()
    _runtime._reset_for_testing()


@pytest.fixture
def span_exporter(monkeypatch: pytest.MonkeyPatch) -> InMemorySpanExporter:
    """Recording tracer backed by an in-memory exporter (per-test isolated)."""
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(observability, "_tracer", provider.get_tracer("test"))
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    return exporter


async def _noop_listener(evt: Any) -> None: ...


def _publish_spans(exporter: InMemorySpanExporter) -> list[Any]:
    return [s for s in exporter.get_finished_spans() if s.name == "modulith.event.publish"]


def _dispatch_spans(exporter: InMemorySpanExporter) -> list[Any]:
    return [s for s in exporter.get_finished_spans() if s.name == "modulith.event.dispatch"]


async def test_durable_persist_failure_ends_publish_span_with_error(
    span_exporter: InMemorySpanExporter,
) -> None:
    """An outbox persist failure between the paired publish
    hooks must still end the publish span (exported, status ERROR, exception
    recorded) and reset the ContextVar."""
    configure(package="modulith_w3r4obs_metatest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(DurableEvt, _noop_listener)
    outbox.configure(FailingStore(), JsonEventSerializer(), start_loop=False)

    token = outbox._current_session.set(object())
    try:
        with pytest.raises(ConnectionError, match="db down"):
            await _runtime.publish(DurableEvt(x=1))
    finally:
        outbox._current_session.reset(token)

    spans = _publish_spans(span_exporter)
    assert len(spans) == 1, "publish span was never ended (leaked, not exported)"
    span = spans[0]
    assert span.status.status_code is StatusCode.ERROR
    assert any(e.name == "exception" for e in span.events)
    # No stale span left to mis-parent the next dispatch in this context.
    assert observability._publish_span.get() is None


async def test_direct_broker_route_failure_ends_publish_span_with_error(
    span_exporter: InMemorySpanExporter,
) -> None:
    """The direct path's inline broker-route failure
    fires between the same paired hooks — the span must end there too."""
    configure(
        package="modulith_w3r4obs_metatest",
        topology="processes",
        broker="ghostscheme",
        auto_discover=False,
    )
    _runtime.ensure_bootstrapped()

    # No local listener + processes topology → the publish routes to the
    # broker inline; no adapter for the scheme → ConfigurationError.
    with pytest.raises(ConfigurationError, match="ghostscheme"):
        await _runtime.publish(DurableEvt(x=2))

    spans = _publish_spans(span_exporter)
    assert len(spans) == 1, "publish span was never ended (leaked, not exported)"
    assert spans[0].status.status_code is StatusCode.ERROR
    assert observability._publish_span.get() is None


@event
@dataclass(frozen=True)
class RetriedEvt:
    x: int


class PendingStore(FailingStore):
    """Store holding one undelivered publication for the crash sweep."""

    def __init__(self, pending: EventPublication) -> None:
        self.pending = pending

    async def save(self, publication: Any) -> None: ...

    async def find_incomplete(self, older_than: timedelta) -> list[Any]:
        return [self.pending] if self.pending.completed_at is None else []

    async def mark_complete(self, publication_id: UUID) -> None:
        self.pending.completed_at = datetime.now(UTC)


async def test_retry_loop_dispatch_span_has_no_parent_from_the_creating_publish(
    span_exporter: InMemorySpanExporter,
) -> None:
    """The retry task is created inside a publish span's context. It must not
    copy that span: its dispatch spans would be parented to a publish span that
    ended long ago."""

    async def listener(evt: RetriedEvt) -> None: ...

    configure(package="modulith_w3r4obs_metatest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(RetriedEvt, listener)
    pending = EventPublication(
        id=uuid4(),
        payload=JsonEventSerializer().serialize(RetriedEvt(x=3)),
        event_type=f"{RetriedEvt.__module__}.{RetriedEvt.__qualname__}",
        listener=outbox._listener_id(listener),
        published_at=datetime.now(UTC),
    )

    observability.modulith_before_event_published(event=RetriedEvt(x=3))
    stale_publish_span = observability._publish_span.get()
    assert stale_publish_span is not None
    outbox.configure(PendingStore(pending), JsonEventSerializer(), retry_interval_seconds=60)
    observability.modulith_after_event_published(event=RetriedEvt(x=3), publication=pending)

    async def dispatched() -> list[Any]:
        while not (spans := _dispatch_spans(span_exporter)):
            await asyncio.sleep(0.005)
        return spans

    (dispatch_span,) = await asyncio.wait_for(dispatched(), timeout=5)
    assert dispatch_span.parent is None
    assert dispatch_span.context.trace_id != stale_publish_span.context.trace_id
