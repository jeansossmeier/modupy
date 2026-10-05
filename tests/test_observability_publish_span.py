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
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.trace import StatusCode
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from modulith import ConfigurationError, EventPublication, configure, event
from modulith.adapters.postgres_outbox import (
    Base,
    EventPublicationRow,
    PostgresPublicationStore,
    bind_session,
    unbind_session,
)
from modulith.builtin import observability, outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer
from modulith.types import EventPublishReceipt


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


# ---------------------------------------------------------------------------
# Outbox rows carry the publish span's trace context, so dispatch spans of
# durably delivered events are children of the publish span that created them.
# ---------------------------------------------------------------------------


@event
@dataclass(frozen=True)
class TracedEvt:
    x: int


class _FlakyListener:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    async def __call__(self, evt: TracedEvt) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("listener down")


class _CapturingStore(FailingStore):
    def __init__(self) -> None:
        self.saved: list[EventPublication] = []

    async def save(self, publication: Any) -> None:
        self.saved.append(publication)


async def _sqlite_store(
    tmp_path: Path, **outbox_options: Any
) -> tuple[Any, PostgresPublicationStore]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}", poolclass=NullPool
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False, **outbox_options)
    return engine, store


async def _publish_committed(engine: Any, x: int = 1) -> None:
    async with async_sessionmaker(engine)() as session:
        token = bind_session(session)
        try:
            await _runtime.publish(TracedEvt(x=x))
            await session.commit()
        finally:
            unbind_session(token)


async def _stored_trace_contexts(engine: Any) -> list[str | None]:
    async with async_sessionmaker(engine)() as session:
        rows = await session.execute(select(EventPublicationRow.trace_context))
        return [row.trace_context for row in rows]


def _register(listener: Any, **config: Any) -> None:
    configure(package="modulith_w4trace_metatest", auto_discover=False, **config)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(TracedEvt, listener)


def _traceparent(context: Any) -> dict[str, str]:
    flags = int(context.trace_flags)
    return {"traceparent": f"00-{context.trace_id:032x}-{context.span_id:016x}-{flags:02x}"}


async def _wait_for_dispatch_spans(exporter: InMemorySpanExporter, count: int) -> list[Any]:
    async def reached() -> list[Any]:
        while len(spans := _dispatch_spans(exporter)) < count:
            await asyncio.sleep(0.005)
        return spans

    return await asyncio.wait_for(reached(), timeout=5)


async def test_after_commit_dispatch_span_is_a_child_of_the_publish_span(
    span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    flaky = _FlakyListener(failures=0)
    _register(flaky)
    engine, store = await _sqlite_store(tmp_path)
    try:
        await _publish_committed(engine)
        await store.wait_for_dispatch()

        (publish_span,) = _publish_spans(span_exporter)
        (dispatch_span,) = _dispatch_spans(span_exporter)
        assert dispatch_span.context.trace_id == publish_span.context.trace_id
        assert dispatch_span.parent is not None
        assert dispatch_span.parent.span_id == publish_span.context.span_id
        assert flaky.calls == 1
    finally:
        await store.dispose()
        await engine.dispose()


async def test_retry_dispatch_span_is_a_child_of_the_publish_span(
    span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    flaky = _FlakyListener(failures=1)
    _register(flaky)
    engine, store = await _sqlite_store(
        tmp_path,
        retry_interval_seconds=0.02,
        retry_stale_seconds=0,
        max_retry_backoff_seconds=0.01,
    )
    try:
        await _publish_committed(engine)
        await store.wait_for_dispatch()
        assert flaky.calls == 1

        outbox._ensure_retry_loop()
        await _wait_for_dispatch_spans(span_exporter, 2)

        (publish_span,) = _publish_spans(span_exporter)
        failed, retried = sorted(_dispatch_spans(span_exporter), key=lambda s: s.start_time)
        assert failed.status.status_code is StatusCode.ERROR
        assert retried.status.status_code is not StatusCode.ERROR
        assert retried.context.trace_id == publish_span.context.trace_id
        assert retried.parent is not None
        assert retried.parent.span_id == publish_span.context.span_id
    finally:
        await outbox.shutdown()
        await store.dispose()
        await engine.dispose()


async def test_every_row_of_one_publish_stores_the_same_carrier(
    span_exporter: InMemorySpanExporter,
) -> None:
    store = _CapturingStore()
    first, second = _FlakyListener(0), _FlakyListener(0)
    configure(package="modulith_w4trace_metatest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(TracedEvt, first)
    _runtime.event_bus.register(TracedEvt, second)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    token = outbox._current_session.set(object())
    try:
        await _runtime.publish(TracedEvt(x=1))
    finally:
        outbox._current_session.reset(token)

    (publish_span,) = _publish_spans(span_exporter)
    assert len(store.saved) == 2
    assert [pub.trace_context for pub in store.saved] == [_traceparent(publish_span.context)] * 2


async def test_broker_route_row_carries_the_publish_span_carrier(
    span_exporter: InMemorySpanExporter,
) -> None:
    store = _CapturingStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    observability.modulith_before_event_published(event=TracedEvt(x=1))
    publish_span = observability._publish_span.get()
    assert publish_span is not None
    try:
        await outbox.persist_broker_route(TracedEvt(x=1), "redis://queue")
    finally:
        observability.modulith_after_event_published(
            event=TracedEvt(x=1), publication=EventPublishReceipt(records=())
        )

    (pub,) = store.saved
    assert pub.trace_context == _traceparent(publish_span.get_span_context())


@pytest.mark.parametrize("mode", ["observability-off", "otel-unavailable"])
async def test_rows_carry_no_trace_context_when_tracing_is_off(
    mode: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    flaky = _FlakyListener(failures=0)
    if mode == "observability-off":
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
        monkeypatch.setattr(observability, "_tracer", provider.get_tracer("test"))
        monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
        _register(flaky, observability=False)
    else:
        monkeypatch.setattr(observability, "_OTEL_AVAILABLE", False)
        monkeypatch.setattr(observability, "_tracer", None)
        _register(flaky)
    engine, store = await _sqlite_store(tmp_path)
    try:
        await _publish_committed(engine)
        await store.wait_for_dispatch()

        assert await _stored_trace_contexts(engine) == [None]
        assert flaky.calls == 1
    finally:
        await store.dispose()
        await engine.dispose()


@pytest.mark.parametrize(
    "carrier",
    [{"traceparent": "garbage"}, {}, ["not", "a", "carrier"], {"traceparent": 7}],
    ids=["malformed", "empty", "not-a-mapping", "non-string-value"],
)
async def test_garbage_trace_context_dispatches_with_a_parentless_span(
    carrier: Any, span_exporter: InMemorySpanExporter, tmp_path: Path
) -> None:
    flaky = _FlakyListener(failures=0)
    _register(flaky)
    engine, store = await _sqlite_store(tmp_path)
    try:
        pub = EventPublication(
            id=uuid4(),
            payload=JsonEventSerializer().serialize(TracedEvt(x=1)),
            event_type=f"{TracedEvt.__module__}.{TracedEvt.__qualname__}",
            listener=outbox._listener_id(flaky),
            published_at=datetime.now(UTC),
            trace_context=carrier,
        )
        async with async_sessionmaker(engine)() as session:
            token = bind_session(session)
            try:
                await store.save(pub)
                await session.commit()
            finally:
                unbind_session(token)

        await outbox.force_retry(pub.id)

        (dispatch_span,) = _dispatch_spans(span_exporter)
        assert dispatch_span.parent is None
        assert flaky.calls == 1
    finally:
        await store.dispose()
        await engine.dispose()


# ---------------------------------------------------------------------------
# A failing OpenTelemetry SDK component never fails publish().
# ---------------------------------------------------------------------------


class _RaisingProcessor(SpanProcessor):
    """A span processor whose ``on_end`` raises, as a broken exporter would."""

    def on_end(self, span: Any) -> None:
        raise RuntimeError("processor down")


class _RaisingSampler(Sampler):
    def should_sample(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("sampler down")

    def get_description(self) -> str:
        return "raising"


async def _publish_with_listener(calls: list[int]) -> None:
    async def listener(evt: DurableEvt) -> None:
        calls.append(evt.x)

    configure(package="modulith_w3r4obs_metatest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(DurableEvt, listener)
    await _runtime.publish(DurableEvt(x=7))


def _warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == "modulith.observability"
    ]


async def test_raising_span_processor_does_not_fail_publish_after_delivery(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    provider = TracerProvider()
    provider.add_span_processor(_RaisingProcessor())
    monkeypatch.setattr(observability, "_tracer", provider.get_tracer("test"))
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    calls: list[int] = []

    with caplog.at_level(logging.WARNING, logger="modulith.observability"):
        await _publish_with_listener(calls)

    assert calls == [7]
    assert _warnings(caplog)
    assert observability._publish_span.get() is None


async def test_raising_sampler_does_not_fail_publish_before_delivery(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    provider = TracerProvider(sampler=_RaisingSampler())
    monkeypatch.setattr(observability, "_tracer", provider.get_tracer("test"))
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    calls: list[int] = []

    with caplog.at_level(logging.WARNING, logger="modulith.observability"):
        await _publish_with_listener(calls)

    assert calls == [7]
    assert _warnings(caplog)
