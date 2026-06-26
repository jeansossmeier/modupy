"""Tests for the built-in OpenTelemetry observability plugin.

The plugin emits two span types around the in-memory event path:

  * ``modulith.event.publish``  — one per ``publish()`` call (before → after)
  * ``modulith.event.dispatch`` — one per listener invocation, a child of the
    publish span, ended on listener completion (error status on failure)

Rather than a hand-rolled mock tracer, these tests inject a *real* SDK
``TracerProvider`` wired to an ``InMemorySpanExporter`` via monkeypatch — so we
assert on genuine recorded spans (names, attributes, parent linkage, status)
without touching the global provider (no cross-test pollution, no double-set
warning). The OTel-absent path is verified by flipping ``_OTEL_AVAILABLE``.
"""

from __future__ import annotations

import asyncio

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from modulith import configure
from modulith.builtin import observability, outbox
from modulith.serializers import JsonEventSerializer


@pytest.fixture(autouse=True)
def _reset_state():
    outbox._reset_for_testing()
    yield
    outbox._reset_for_testing()
    asyncio.set_event_loop(asyncio.new_event_loop())


@pytest.fixture
def span_exporter(monkeypatch) -> InMemorySpanExporter:
    """Inject a recording tracer backed by an in-memory exporter.

    Isolated per-test: a fresh local ``TracerProvider`` (never set as the
    global provider) is wired into the plugin via monkeypatch, so spans are
    captured here and automatically restored on teardown.
    """
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(observability, "_tracer", provider.get_tracer("test"))
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    return exporter


def _spans_by_name(exporter: InMemorySpanExporter, name: str) -> list:
    return [s for s in exporter.get_finished_spans() if s.name == name]


# ---------------------------------------------------------------------------
# Happy path: publish + dispatch spans
# ---------------------------------------------------------------------------


async def test_publish_emits_publish_and_dispatch_spans(make_fake_app, span_exporter) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """,
            "inventory": """
                from modulith import listener
                from fakeapp.orders import OrderPlaced

                seen = []

                @listener
                async def reserve(evt: OrderPlaced) -> None:
                    seen.append(evt)
            """,
        }
    )
    configure(package="fakeapp")
    import fakeapp.orders as orders

    await orders.place("o-1")

    publish_spans = _spans_by_name(span_exporter, "modulith.event.publish")
    dispatch_spans = _spans_by_name(span_exporter, "modulith.event.dispatch")
    assert len(publish_spans) == 1, [s.name for s in span_exporter.get_finished_spans()]
    assert len(dispatch_spans) == 1

    publish_span = publish_spans[0]
    dispatch_span = dispatch_spans[0]
    assert publish_span.attributes["event.type"].endswith("OrderPlaced")
    assert dispatch_span.attributes["event.type"].endswith("OrderPlaced")
    assert dispatch_span.attributes["listener.name"] == "reserve"

    # dispatch span is a child of the publish span (same trace, parent linkage)
    assert dispatch_span.parent is not None
    assert dispatch_span.parent.span_id == publish_span.context.span_id
    assert dispatch_span.context.trace_id == publish_span.context.trace_id


async def test_publish_span_carries_duration(make_fake_app, span_exporter) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class Ping:
                    n: int

                async def go() -> None:
                    await publish(Ping(n=1))
            """
        }
    )
    configure(package="fakeapp")
    import fakeapp.orders as orders

    await orders.go()

    publish_spans = _spans_by_name(span_exporter, "modulith.event.publish")
    assert len(publish_spans) == 1
    assert publish_spans[0].attributes["modulith.duration_ms"] >= 0.0


# ---------------------------------------------------------------------------
# Error path
# ---------------------------------------------------------------------------


async def test_listener_error_marks_dispatch_span_error(make_fake_app, span_exporter) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class Boom:
                    msg: str

                async def detonate() -> None:
                    await publish(Boom(msg="x"))
            """,
            "inventory": """
                from modulith import listener
                from fakeapp.orders import Boom

                @listener
                async def explode(evt: Boom) -> None:
                    raise ValueError("kaboom")
            """,
        }
    )
    configure(package="fakeapp")
    import fakeapp.orders as orders

    with pytest.raises(ValueError, match="kaboom"):
        await orders.detonate()

    dispatch_spans = _spans_by_name(span_exporter, "modulith.event.dispatch")
    assert len(dispatch_spans) == 1
    span = dispatch_spans[0]
    assert span.status.status_code is StatusCode.ERROR
    # the exception was recorded as a span event
    assert any(e.name == "exception" for e in span.events)


# ---------------------------------------------------------------------------
# Silent no-op when OTel is unavailable
# ---------------------------------------------------------------------------


def test_hooks_are_noop_when_otel_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", False)
    monkeypatch.setattr(observability, "_tracer", None)

    # None of these should raise, and no span state should be set.
    observability.modulith_before_event_published(event=object())
    assert observability._publish_span.get() is None


# ---------------------------------------------------------------------------
# Durable path: no publish span (it would leak — after_event_published does
# not fire in the publishing context when the outbox owns dispatch)
# ---------------------------------------------------------------------------


def test_durable_path_skips_publish_span(span_exporter) -> None:
    outbox.configure(store=object(), serializer=JsonEventSerializer(), start_loop=False)
    token = outbox._current_session.set(object())
    try:
        observability.modulith_before_event_published(event=123)
        assert observability._publish_span.get() is None
    finally:
        outbox._current_session.reset(token)

    assert not _spans_by_name(span_exporter, "modulith.event.publish")
