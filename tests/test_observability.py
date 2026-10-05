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

import importlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from modulith import EventPublication, configure
from modulith.builtin import observability, outbox
from modulith.serializers import JsonEventSerializer

from conftest import replace_current_event_loop


@pytest.fixture(autouse=True)
def _reset_state():
    outbox._reset_for_testing()
    yield
    outbox._reset_for_testing()
    replace_current_event_loop()


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
    assert dispatch_span.attributes["listener.name"] == "fakeapp.inventory.reserve"

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
# A span started inside a listener belongs to that listener's dispatch span
# ---------------------------------------------------------------------------

_WORK_APP = {
    "orders": """
        from dataclasses import dataclass
        from modulith import event, publish

        @event
        @dataclass(frozen=True)
        class Work:
            fail: bool

        async def go(fail: bool = False) -> None:
            await publish(Work(fail=fail))
    """,
    "inventory": """
        from modulith import listener
        from modulith.builtin import observability
        from fakeapp.orders import Work

        @listener
        async def do_work(evt: Work) -> None:
            with observability._tracer.start_as_current_span("app.work"):
                if evt.fail:
                    raise ValueError("kaboom")
    """,
}


async def test_span_started_in_listener_is_a_child_of_its_dispatch_span(
    make_fake_app, span_exporter
) -> None:
    make_fake_app(_WORK_APP)
    configure(package="fakeapp")
    import fakeapp.orders as orders

    await orders.go()

    (work,) = _spans_by_name(span_exporter, "app.work")
    (dispatch,) = _spans_by_name(span_exporter, "modulith.event.dispatch")
    assert work.parent is not None
    assert work.parent.span_id == dispatch.context.span_id
    assert work.context.trace_id == dispatch.context.trace_id


async def test_span_started_in_a_raising_listener_is_a_child_of_its_dispatch_span(
    make_fake_app, span_exporter
) -> None:
    make_fake_app(_WORK_APP)
    configure(package="fakeapp")
    import fakeapp.orders as orders

    with pytest.raises(ValueError, match="kaboom"):
        await orders.go(fail=True)

    (work,) = _spans_by_name(span_exporter, "app.work")
    (dispatch,) = _spans_by_name(span_exporter, "modulith.event.dispatch")
    assert work.parent is not None
    assert work.parent.span_id == dispatch.context.span_id


@pytest.mark.parametrize("exception", [None, ValueError("kaboom")], ids=["returned", "raised"])
def test_dispatch_span_is_current_until_the_listener_completes(
    span_exporter, exception: BaseException | None
) -> None:
    event = object()
    publication = EventPublication(
        id=uuid4(),
        payload=b"",
        event_type="x.Y",
        listener="x.listener",
        published_at=datetime.now(UTC),
    )

    observability.modulith_on_listener_dispatch(
        event=event, listener_name="x.listener", publication=publication
    )
    current = trace.get_current_span()
    observability.modulith_on_listener_complete(
        event=event, listener_name="x.listener", publication=publication, exception=exception
    )

    (dispatch,) = _spans_by_name(span_exporter, "modulith.event.dispatch")
    assert current.get_span_context().span_id == dispatch.context.span_id
    assert not trace.get_current_span().get_span_context().is_valid
    assert observability._dispatch_token.get() is None


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
# Durable path: publish span brackets before → after (after_event_published
# fires there too — the event was persisted, per the hookspec contract)
# ---------------------------------------------------------------------------


async def test_durable_path_emits_publish_span(span_exporter) -> None:
    """The durable (outbox) path must emit a publish span.

    ``Runtime.publish`` fires ``modulith_after_event_published`` on the
    durable path right after the event is persisted (the hookspec's
    documented trigger), so the tracing plugin must start the publish span
    unconditionally in ``modulith_before_event_published`` and close it in
    the after-hook. Skipping span creation there made tracing miss every
    transactional publish — the production path the outbox exists for.
    """
    from modulith import publish
    from modulith.runtime import _runtime

    class _FakeStore:
        def __init__(self):
            self.saved = []

        async def save(self, publication):
            self.saved.append(publication)

    class Ping:
        pass

    _runtime._reset_for_testing()
    configure(package="obs_durable_pkg", auto_discover=False)
    outbox.configure(store=_FakeStore(), serializer=JsonEventSerializer(), start_loop=False)
    token = outbox._current_session.set(object())
    try:
        await publish(Ping())
    finally:
        outbox._current_session.reset(token)
        _runtime._reset_for_testing()

    publish_spans = _spans_by_name(span_exporter, "modulith.event.publish")
    assert len(publish_spans) == 1, [s.name for s in span_exporter.get_finished_spans()]
    assert publish_spans[0].attributes["modulith.duration_ms"] >= 0.0
    # The after-hook ended the span and cleared the ContextVar — no leak.
    assert observability._publish_span.get() is None


# ---------------------------------------------------------------------------
# event.module attribute
# ---------------------------------------------------------------------------


async def test_publish_span_carries_calling_module(make_fake_app, span_exporter) -> None:
    """The publish span's documented ``event.module`` attribute
    must name the application module publish() was called from (populated by
    ``_detect_calling_module``'s stack walk)."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class ModPing:
                    n: int

                async def go() -> None:
                    await publish(ModPing(n=1))
            """
        }
    )
    configure(package="fakeapp")
    import fakeapp.orders as orders

    await orders.go()

    (publish_span,) = _spans_by_name(span_exporter, "modulith.event.publish")
    assert publish_span.attributes["event.module"] == "fakeapp.orders"


async def test_publish_span_carries_calling_module_for_modulith_prefixed_app_package(
    make_fake_app, span_exporter
) -> None:
    """An application package whose name merely starts with the literal
    string ``"modulith"`` (but isn't the modulith package itself) must still
    be detected as the calling module, not excluded as an internal frame."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class ModAppPing:
                    n: int

                async def go() -> None:
                    await publish(ModAppPing(n=1))
            """
        },
        package_name="modulithapp",
    )
    configure(package="modulithapp")
    orders = importlib.import_module("modulithapp.orders")

    await orders.go()

    (publish_span,) = _spans_by_name(span_exporter, "modulith.event.publish")
    assert publish_span.attributes["event.module"] == "modulithapp.orders"


def test_publish_span_event_module_falls_back_to_unknown(span_exporter) -> None:
    """When no application package is configured, calling-module
    detection degrades to the documented ``"unknown"`` fallback instead of
    perturbing the publish path."""
    from uuid import uuid4

    from modulith import EventPublication
    from modulith.runtime import _runtime

    _runtime._reset_for_testing()  # config is None → no package to match against

    observability.modulith_before_event_published(event=object())
    observability.modulith_after_event_published(
        event=object(), publication=EventPublication(id=uuid4(), payload=b"")
    )

    (publish_span,) = _spans_by_name(span_exporter, "modulith.event.publish")
    assert publish_span.attributes["event.module"] == "unknown"


# ---------------------------------------------------------------------------
# Instrumenting-library version
# ---------------------------------------------------------------------------


def test_tracer_version_is_not_a_hardcoded_copy() -> None:
    """The instrumenting-library version must come from ``modulith.__version__``.

    A literal here is a silent third copy of the release version, feeding live
    OTel scope metadata: a release bump that misses it ships spans tagged with
    the previous version, and nothing in the build notices. Read the source
    (importing the module yields the resolved value, which cannot detect the
    drift) and assert no version literal was reintroduced.
    """
    import re
    from pathlib import Path

    import modulith
    from modulith.builtin import observability as obs

    source = Path(obs.__file__).read_text(encoding="utf-8")
    literals = re.findall(r'^\s*\w+\s*=\s*"(\d+\.\d+\.\d+[^"]*)"', source, re.MULTILINE)
    assert literals == [], (
        f"modulith/builtin/observability.py hardcodes version literal(s) {literals} — "
        "use modulith.__version__ so the tracer metadata cannot drift"
    )
    assert obs._tracer._instrumenting_library_version == modulith.__version__


# ---------------------------------------------------------------------------
# listener.name: one name per listener, whichever way the event arrived
# ---------------------------------------------------------------------------


class _Reserver:
    async def reserve(self, evt: object) -> None:
        return None


_ORDERS_APP = {
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

        @listener
        async def reserve(evt: OrderPlaced) -> None:
            pass
    """,
}


async def test_dispatch_listener_name_is_the_outbox_listener_id_in_memory(
    make_fake_app, span_exporter
) -> None:
    """In-memory dispatch names the listener by the outbox's stored id, so a
    trace query for one listener finds it on every delivery path."""
    from modulith.runtime import _runtime

    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp")
    import fakeapp.orders as orders

    await orders.place("o-1")

    (span,) = _spans_by_name(span_exporter, "modulith.event.dispatch")
    (handler,) = _runtime.event_bus.listeners_for(orders.OrderPlaced)
    assert span.attributes["listener.name"] == "fakeapp.inventory.reserve"
    assert span.attributes["listener.name"] == outbox._listener_id(handler)


async def test_dispatch_listener_name_matches_for_broker_delivered_events(
    make_fake_app, span_exporter
) -> None:
    """A broker consumer hands events to ``dispatch_local``; its dispatch span
    carries the same listener name as the in-memory path."""
    from modulith.runtime import _runtime

    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp")
    import fakeapp.orders as orders

    _runtime.ensure_bootstrapped()
    await _runtime.dispatch_local(orders.OrderPlaced(order_id="o-2"), _runtime.event_bus)

    (span,) = _spans_by_name(span_exporter, "modulith.event.dispatch")
    assert span.attributes["listener.name"] == "fakeapp.inventory.reserve"


async def test_dispatch_listener_name_carries_owner_prefix_for_bound_methods(
    make_fake_app, span_exporter
) -> None:
    """A bound method is named ``owner:module.Class.method``, exactly as the
    outbox stores it."""
    from modulith.runtime import _runtime

    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp")
    import fakeapp.orders as orders

    _runtime.ensure_bootstrapped()
    handler = _Reserver().reserve
    _runtime.event_bus.register(orders.OrderPlaced, handler)
    _runtime._listener_owners[handler] = "inventory"

    await _runtime.dispatch_local(orders.OrderPlaced(order_id="o-3"), _runtime.event_bus)

    spans = _spans_by_name(span_exporter, "modulith.event.dispatch")
    names = {s.attributes["listener.name"] for s in spans}
    assert f"inventory:{_Reserver.__module__}._Reserver.reserve" in names
    assert outbox._listener_id(handler) in names


# ---------------------------------------------------------------------------
# Configuration.observability
# ---------------------------------------------------------------------------


async def test_observability_false_creates_no_spans(make_fake_app, span_exporter) -> None:
    from modulith.runtime import _runtime

    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp", observability=False)
    import fakeapp.orders as orders

    await orders.place("o-4")

    assert span_exporter.get_finished_spans() == ()
    assert _runtime.plugin_manager.get_plugin("modulith.builtin.observability") is None


async def test_observability_true_creates_spans_when_otel_is_installed(
    make_fake_app, span_exporter
) -> None:
    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp", observability=True)
    import fakeapp.orders as orders

    await orders.place("o-5")

    assert len(_spans_by_name(span_exporter, "modulith.event.dispatch")) == 1


def test_observability_true_without_otel_fails_bootstrap(make_fake_app, monkeypatch) -> None:
    from modulith import ConfigurationError
    from modulith.runtime import _runtime

    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", False)
    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp", observability=True)

    with pytest.raises(ConfigurationError, match=r"pip install 'modupy\[otel\]'"):
        _runtime.ensure_bootstrapped()
    assert _runtime.plugin_manager is None


async def test_observability_unset_without_otel_stays_a_silent_no_op(
    make_fake_app, monkeypatch
) -> None:
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", False)
    monkeypatch.setattr(observability, "_tracer", None)
    make_fake_app(_ORDERS_APP)
    configure(package="fakeapp")
    import fakeapp.orders as orders

    await orders.place("o-6")
