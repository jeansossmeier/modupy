"""A consumed event's dispatch span joins the producer's publish span across a broker.

The producer half publishes under a real OpenTelemetry SDK tracer and sends to a
real broker adapter; the consumer half is the adapter's real consumer, which
hands the message to ``Runtime.dispatch_local``. The publish span has ended
before the consumer runs, so the only link between the two spans is what travelled
in the message headers.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from modulith import EventPublication, configure, event
from modulith._consumer import BrokerConsumer
from modulith.adapters.db_broker import DatabaseBroker, DatabaseConsumer
from modulith.adapters.shm_broker import ShmBroker, ShmConsumer
from modulith.builtin import observability, outbox
from modulith.event_bus import InMemoryEventBus
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class HopEvt:
    x: int


_FQN = f"{HopEvt.__module__}.{HopEvt.__qualname__}"
_SCHEME = "hopbroker"


class _Session:
    def __init__(self) -> None:
        self.info: dict[str, object] = {}


class _RecordingStore:
    def __init__(self) -> None:
        self.saved: list[EventPublication] = []

    async def save(self, publication: EventPublication) -> None:
        self.saved.append(publication)

    async def mark_complete(self, publication_id: UUID) -> None: ...

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return []

    async def archive(self, publication_id: UUID) -> None: ...

    async def delete(self, publication_id: UUID) -> None: ...


@dataclass
class _Hop:
    broker: Any
    sent: list[dict[str, str] | None] = field(default_factory=list)
    delivered: list[int] = field(default_factory=list)


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
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(observability, "_tracer", provider.get_tracer("test"))
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    return exporter


def _spans(exporter: InMemorySpanExporter, name: str) -> list[Any]:
    return [s for s in exporter.get_finished_spans() if s.name == name]


async def _wait_for(predicate: Any, timeout: float = 5.0) -> None:
    async def reached() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(reached(), timeout=timeout)


def _traceparent_of(span: Any) -> str:
    context = span.context
    return f"00-{context.trace_id:032x}-{context.span_id:016x}-{int(context.trace_flags):02x}"


def _record_sends(hop: _Hop) -> None:
    original = hop.broker.publish

    async def publish(target: str, payload: bytes, headers: dict[str, str] | None = None) -> None:
        hop.sent.append(None if headers is None else dict(headers))
        await original(target, payload, headers)

    hop.broker.publish = publish


@asynccontextmanager
async def _hop(scheme: str, tmp_path: Path, **config: Any) -> AsyncIterator[_Hop]:
    """Runtime routing ``HopEvt`` to a real ``scheme`` broker, plus its real consumer."""
    configure(
        package="w4trace_hop", topology="processes", broker=_SCHEME, auto_discover=False, **config
    )
    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None

    serializer = JsonEventSerializer(allowed_event_types=[HopEvt])
    bus = InMemoryEventBus()
    hop = _Hop(broker=None)

    async def handler(evt: HopEvt) -> None:
        hop.delivered.append(evt.x)

    bus.register(HopEvt, handler)

    engine = None
    if scheme == "shm":
        broker: Any = ShmBroker(
            shm_name=f"w4trace-{uuid4().hex[:8]}", db_path=str(tmp_path / "s.db")
        )
        consumer: Any = ShmConsumer(
            broker=broker,
            bus=bus,
            serializer=serializer,
            consumer_name="hop:1",
            group="modulith-hop",
            targets=[_FQN],
            poll_interval_s=0.01,
        )
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'b.db'}", poolclass=NullPool)
        broker = DatabaseBroker(engine=engine)
        consumer = DatabaseConsumer(
            broker=broker,
            bus=bus,
            serializer=serializer,
            consumer_name="hop:1",
            group="modulith-hop",
            targets=[_FQN],
            poll_interval_s=0.01,
        )
    hop.broker = broker
    _record_sends(hop)
    registry.register(_SCHEME, broker)
    await consumer.start()
    try:
        yield hop
    finally:
        await consumer.stop()
        await broker.close()
        if scheme == "shm":
            broker._ring.unlink()
        if engine is not None:
            await engine.dispose()


async def _publish_via_outbox() -> EventPublication:
    store = _RecordingStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    token = outbox._current_session.set(_Session())
    try:
        await _runtime.publish(HopEvt(x=1))
    finally:
        outbox._current_session.reset(token)
    (route,) = store.saved
    assert route.listener is not None
    assert route.listener.startswith(outbox._BROKER_ROUTE_LISTENER_PREFIX)
    return route


def _assert_child_of_publish(exporter: InMemorySpanExporter) -> None:
    (publish_span,) = _spans(exporter, "modulith.event.publish")
    (dispatch_span,) = _spans(exporter, "modulith.event.dispatch")
    assert dispatch_span.context.trace_id == publish_span.context.trace_id
    assert dispatch_span.parent is not None
    assert dispatch_span.parent.span_id == publish_span.context.span_id


SCHEMES = ["shm", "database"]


@pytest.mark.parametrize("scheme", SCHEMES)
async def test_outbox_broker_route_joins_the_consumer_dispatch_span_to_the_publish_span(
    scheme: str, span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    async with _hop(scheme, tmp_path) as hop:
        route = await _publish_via_outbox()
        await outbox._dispatch_publication(route)

        await _wait_for(lambda: _spans(span_exporter, "modulith.event.dispatch"))
        (publish_span,) = _spans(span_exporter, "modulith.event.publish")
        (headers,) = hop.sent
        assert headers is not None
        assert headers["traceparent"] == _traceparent_of(publish_span)
        assert headers["event_type"] == _FQN
        assert headers["publication_id"] == str(route.id)
        _assert_child_of_publish(span_exporter)
        assert hop.delivered == [1]


@pytest.mark.parametrize("scheme", SCHEMES)
async def test_inline_broker_route_joins_the_consumer_dispatch_span_to_the_publish_span(
    scheme: str, span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    async with _hop(scheme, tmp_path) as hop:
        await _runtime.publish(HopEvt(x=1))

        await _wait_for(lambda: _spans(span_exporter, "modulith.event.dispatch"))
        (publish_span,) = _spans(span_exporter, "modulith.event.publish")
        (headers,) = hop.sent
        assert headers is not None
        assert headers["traceparent"] == _traceparent_of(publish_span)
        _assert_child_of_publish(span_exporter)
        assert hop.delivered == [1]


@pytest.mark.parametrize("scheme", SCHEMES)
async def test_tracestate_travels_with_the_traceparent(
    scheme: str, span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    async with _hop(scheme, tmp_path) as hop:
        route = await _publish_via_outbox()
        route.trace_context = {**(route.trace_context or {}), "tracestate": "vendor=opaque"}
        await outbox._dispatch_publication(route)

        await _wait_for(lambda: _spans(span_exporter, "modulith.event.dispatch"))
        (headers,) = hop.sent
        assert headers is not None
        assert headers["tracestate"] == "vendor=opaque"
        _assert_child_of_publish(span_exporter)
        (dispatch_span,) = _spans(span_exporter, "modulith.event.dispatch")
        assert dispatch_span.parent is not None
        assert dispatch_span.parent.trace_state.get("vendor") == "opaque"


@pytest.mark.parametrize("scheme", SCHEMES)
async def test_retried_outbox_send_repeats_identical_headers_and_the_broker_accepts_it(
    scheme: str, span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    async with _hop(scheme, tmp_path) as hop:
        route = await _publish_via_outbox()
        await outbox._dispatch_publication(route)
        await outbox._dispatch_publication(route)

        first, second = hop.sent
        assert first is not None
        assert "traceparent" in first
        assert second == first
        assert route.attempt_count == 0
        assert route.last_error is None


@pytest.mark.parametrize(
    "carrier", [["not", "a", "carrier"], {"traceparent": 7}, {"traceparent": ""}]
)
async def test_unusable_stored_carrier_sends_without_trace_headers(
    carrier: Any, span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    async with _hop("shm", tmp_path) as hop:
        route = await _publish_via_outbox()
        route.trace_context = carrier
        await outbox._dispatch_publication(route)

        await _wait_for(lambda: hop.delivered)
        assert hop.sent == [{"event_type": _FQN, "publication_id": str(route.id)}]
        assert route.last_error is None


@pytest.mark.parametrize("mode", ["observability-off", "otel-unavailable"])
@pytest.mark.parametrize("path", ["outbox", "inline"])
@pytest.mark.parametrize("scheme", SCHEMES)
async def test_no_trace_headers_are_sent_when_tracing_is_off(
    scheme: str,
    path: str,
    mode: str,
    span_exporter: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config: dict[str, Any] = {}
    if mode == "observability-off":
        config["observability"] = False
    else:
        monkeypatch.setattr(observability, "_OTEL_AVAILABLE", False)
        monkeypatch.setattr(observability, "_tracer", None)

    async with _hop(scheme, tmp_path, **config) as hop:
        if path == "outbox":
            route = await _publish_via_outbox()
            await outbox._dispatch_publication(route)
            expected = {"event_type": _FQN, "publication_id": str(route.id)}
        else:
            await _runtime.publish(HopEvt(x=1))
            expected = {"event_type": _FQN}

        await _wait_for(lambda: hop.delivered)
        assert hop.sent == [expected]
        assert hop.delivered == [1]
        assert _spans(span_exporter, "modulith.event.dispatch") == []


@pytest.mark.parametrize("scheme", SCHEMES)
@pytest.mark.parametrize(
    "extra",
    [{}, {"traceparent": "garbage"}, {"tracestate": "vendor=opaque"}],
    ids=["no-trace-headers", "garbage-traceparent", "tracestate-only"],
)
async def test_message_without_a_usable_traceparent_dispatches_with_a_parentless_span(
    scheme: str, extra: dict[str, str], span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    async with _hop(scheme, tmp_path) as hop:
        payload = JsonEventSerializer().serialize(HopEvt(x=1))
        await hop.broker.publish(_FQN, payload, {"event_type": _FQN, **extra})

        await _wait_for(lambda: _spans(span_exporter, "modulith.event.dispatch"))
        (dispatch_span,) = _spans(span_exporter, "modulith.event.dispatch")
        assert dispatch_span.parent is None
        assert hop.delivered == [1]


async def test_redis_consumer_reads_the_trace_headers_from_h_prefixed_fields(
    span_exporter: InMemorySpanExporter,
) -> None:
    configure(package="w4trace_hop", auto_discover=False)
    _runtime.ensure_bootstrapped()
    acked: list[str] = []

    class _Broker:
        async def ack(self, target: str, message_id: str, group: str | None = None) -> None:
            acked.append(message_id)

    bus = InMemoryEventBus()

    async def handler(evt: HopEvt) -> None: ...

    bus.register(HopEvt, handler)
    consumer = BrokerConsumer(
        broker=_Broker(),
        bus=bus,
        serializer=JsonEventSerializer(allowed_event_types=[HopEvt]),
        consumer_name="hop:1",
        group="modulith-hop",
        targets=[_FQN],
    )
    traceparent = f"00-{'ab' * 16}-{'cd' * 8}-01"
    await consumer._dispatch_one(
        _FQN,
        b"1-0",
        {
            b"data": JsonEventSerializer().serialize(HopEvt(x=1)),
            b"h:event_type": _FQN.encode(),
            b"h:traceparent": traceparent.encode(),
            b"h:tracestate": b"vendor=opaque",
        },
    )

    (dispatch_span,) = _spans(span_exporter, "modulith.event.dispatch")
    assert f"{dispatch_span.context.trace_id:032x}" == "ab" * 16
    assert dispatch_span.parent is not None
    assert f"{dispatch_span.parent.span_id:016x}" == "cd" * 8
    assert dispatch_span.parent.trace_state.get("vendor") == "opaque"
    assert acked == ["1-0"]


@pytest.mark.integration
@pytest.mark.parametrize("path", ["outbox", "inline"])
async def test_redis_streams_hop_joins_the_dispatch_span_to_the_publish_span(
    path: str, span_exporter: InMemorySpanExporter, redis_url: str, redis_key_prefix: str
) -> None:
    from modulith.adapters.redis_broker import RedisStreamsBroker

    configure(package="w4trace_hop", topology="processes", broker=_SCHEME, auto_discover=False)
    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None
    broker = RedisStreamsBroker(url=redis_url, stream_prefix=redis_key_prefix, consumer_group="g")
    registry.register(_SCHEME, broker)

    delivered: list[int] = []

    async def handler(evt: HopEvt) -> None:
        delivered.append(evt.x)

    bus = InMemoryEventBus()
    bus.register(HopEvt, handler)
    consumer = BrokerConsumer(
        broker=broker,
        bus=bus,
        serializer=JsonEventSerializer(allowed_event_types=[HopEvt]),
        consumer_name="hop:1",
        group="modulith-hop",
        targets=[_FQN],
        poll_block_ms=50,
        reclaim_min_idle_ms=0,
    )
    try:
        await broker.ensure_group(_FQN, "modulith-hop")
        if path == "outbox":
            await outbox._dispatch_publication(await _publish_via_outbox())
        else:
            await _runtime.publish(HopEvt(x=1))
        await consumer.start()
        await _wait_for(lambda: delivered)
        await _wait_for(lambda: _spans(span_exporter, "modulith.event.dispatch"))
    finally:
        await consumer.stop()
        await broker.close()

    _assert_child_of_publish(span_exporter)
