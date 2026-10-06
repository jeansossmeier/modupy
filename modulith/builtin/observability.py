"""Built-in OpenTelemetry auto-instrumentation.

When OTel is installed and a tracer provider is configured, this plugin
automatically creates spans for:
  - Every cross-module event publication (parent span)
  - Every listener dispatch (child span linked to the publication)

When OTel is NOT installed, the plugin loads but does nothing. No
errors, no warnings — true silent no-op. This matters because most
apps ship with OTel as a soft dependency (configured per environment).
Even when OTel *is* installed but no provider is configured, the proxy
tracer hands back non-recording spans, so the cost is negligible.

Distributed via the `modupy[otel]` extra. The plugin auto-detects
OTel availability at import time via a try/except block.

Spans emitted:

  modulith.event.publish
    attributes:
      event.type           — fully-qualified class name
      event.module         — source module (where publish was called from)
      modulith.duration_ms — wall-clock from before → after publish
    duration: from before_event_published hook to after_event_published

  modulith.event.dispatch
    attributes:
      event.type
      listener.name        — the outbox's stored listener id (module.qualname,
                             ``owner:`` prefix for bound methods/instances)
      publication.id
    parent: the modulith.event.publish span (for outbox deliveries, the one
            named by the row's stored trace context)
    duration: just the listener invocation (dispatch → complete)
    status: ERROR (with recorded exception) when the listener raises

Span lifecycle relies on the paired hooks: ``modulith_before_event_published``
/ ``modulith_after_event_published`` bracket the publish span, and
``modulith_on_listener_dispatch`` / ``modulith_on_listener_complete`` bracket
each dispatch span. ``modulith_on_listener_complete`` always fires (success or
failure), so dispatch spans are guaranteed to end.

Durable (outbox) path: when an outbox store is configured *and* a transaction
session is bound, ``Runtime.publish`` persists the event and returns before any
in-memory dispatch. ``modulith_after_event_published`` fires on this path too —
right after the event is persisted, which is the hookspec's documented trigger
— so the publish span is started unconditionally and, on the durable path,
brackets the persistence step. Listener dispatch happens after the business
transaction commits, in a different context, and a retry runs in the outbox's
own task. Each outbox row therefore stores the W3C trace context of the publish
span that created it (``EventPublication.trace_context``), and the dispatch span
of every delivery, after commit or on retry, is parented to that span.
"""

from __future__ import annotations

import logging
import time
from contextvars import ContextVar, Token
from typing import Any

from modulith import EventPublication, __version__, hookimpl
from modulith.types import EventPublishReceipt

logger = logging.getLogger("modulith.observability")


# ---------------------------------------------------------------------------
# Soft OTel import
# ---------------------------------------------------------------------------

try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    _OTEL_AVAILABLE = True
    _propagator: Any = TraceContextTextMapPropagator()
    # The instrumenting-library version is the package's own version — read it
    # from ``modulith.__version__`` rather than restating the literal here, so
    # a release bump cannot leave the tracer metadata behind. Passed
    # positionally: OTel >= 1.43 removed the ``version=`` keyword.
    _tracer: Any = trace.get_tracer("modulith", __version__)
except ImportError:
    _OTEL_AVAILABLE = False
    _tracer = None
    _propagator = None


# ---------------------------------------------------------------------------
# Per-publication / per-dispatch span context
# ---------------------------------------------------------------------------

# The active publish span, so the listener-dispatch hook can parent its span
# to it. Each listener runs in its own copied context (asyncio.gather wraps
# each coroutine in a Task), so the dispatch span set here is isolated per
# listener and pairs cleanly with the completion hook in the same context.
_publish_span: ContextVar[Any] = ContextVar("_modulith_publish_span", default=None)
_publish_start: ContextVar[float] = ContextVar("_modulith_publish_start", default=0.0)
_dispatch_span: ContextVar[Any] = ContextVar("_modulith_dispatch_span", default=None)
# Token of the OTel context that makes the dispatch span current while the
# listener runs, so a span the listener starts is a child of it.
_dispatch_token: ContextVar[Token[Any] | None] = ContextVar(
    "_modulith_dispatch_token", default=None
)


# ---------------------------------------------------------------------------
# Hook implementations
# ---------------------------------------------------------------------------


@hookimpl
def modulith_before_event_published(event: Any) -> None:
    """Start the publication span (both in-memory and durable paths).

    ``modulith_after_event_published`` fires in this same context on both
    paths — after in-memory dispatch, and on the durable path right after
    the event is persisted to the outbox — so the span started here is
    always ended there.
    """
    if not _OTEL_AVAILABLE:
        return
    try:
        span = _tracer.start_span(
            "modulith.event.publish",
            attributes={
                "event.type": _event_type(event),
                "event.module": _detect_calling_module(),
            },
        )
    except Exception:
        logger.warning("could not start the publish span", exc_info=True)
        return
    _publish_span.set(span)
    _publish_start.set(time.monotonic())


@hookimpl
def modulith_after_event_published(
    event: Any, publication: EventPublication | EventPublishReceipt
) -> None:
    """End the publication span, recording its wall-clock duration.

    ``publication`` may be an ``EventPublication`` (in-memory path) or an
    ``EventPublishReceipt`` (durable path) — this hookimpl never reads
    either's fields; it only ends the span held in the ContextVar.

    A failing span processor must not fail a publish whose delivery already
    happened, so an exception from the OTel calls is logged and dropped.
    """
    if not _OTEL_AVAILABLE:
        return
    span = _publish_span.get()
    if span is None:
        return
    _publish_span.set(None)
    try:
        duration_ms = (time.monotonic() - _publish_start.get()) * 1000
        span.set_attribute("modulith.duration_ms", duration_ms)
        span.end()
    except Exception:
        logger.warning("could not end the publish span", exc_info=True)


@hookimpl
def modulith_on_publish_error(event: Any, exception: BaseException) -> None:
    """End the publish span for a publish that FAILED between the hooks.

    ``modulith_after_event_published`` is contractually scoped to successful
    persistence/dispatch, so when ``Runtime.publish`` fails between the
    paired hooks (outbox persist/serialize/broker-route raised) no hook
    fires and the span started in
    ``modulith_before_event_published`` would leak: never ended (so never
    exported — tracing would go blind exactly during the outages the outbox
    exists for) and left stale in the ContextVar, mis-parenting the next
    dispatch span in the same context. This observe-only hook is the runtime's
    paired signal for that failure; records the exception, sets ERROR status,
    ends the span, and resets the ContextVar. No-op when OTel is absent or no
    span is active (e.g. this plugin is disabled).
    """
    del event  # unused: the active span already carries the event's attributes
    if not _OTEL_AVAILABLE:
        return
    span = _publish_span.get()
    if span is None:
        return
    duration_ms = (time.monotonic() - _publish_start.get()) * 1000
    span.set_attribute("modulith.duration_ms", duration_ms)
    span.record_exception(exception)
    span.set_status(Status(StatusCode.ERROR, str(exception)))
    span.end()
    _publish_span.set(None)


@hookimpl
def modulith_on_listener_dispatch(
    event: Any,
    listener_name: str,
    publication: EventPublication,
) -> None:
    """Start a span for one listener invocation, parented to the publish span.

    An outbox row's stored ``trace_context`` names the publish span that created
    it, which is the only link left after commit or on a retry; without one, the
    live publish span of this context is the parent. The span is made current
    until ``modulith_on_listener_complete``, so a span the listener starts is
    its child.
    """
    if not _OTEL_AVAILABLE:
        return
    if publication.trace_context is not None:
        context = _extract_context(publication.trace_context)
    else:
        parent = _publish_span.get()
        context = trace.set_span_in_context(parent) if parent is not None else None
    span = _tracer.start_span(
        "modulith.event.dispatch",
        context=context,
        attributes={
            "event.type": _event_type(event),
            "listener.name": listener_name,
            "publication.id": str(publication.id),
        },
    )
    _dispatch_span.set(span)
    _dispatch_token.set(otel_context.attach(trace.set_span_in_context(span)))


@hookimpl
def modulith_on_listener_complete(
    event: Any,
    listener_name: str,
    publication: EventPublication,
    exception: BaseException | None,
) -> None:
    """End the listener-dispatch span; mark it errored if the listener raised.

    Detaches the context ``modulith_on_listener_dispatch`` attached, whether
    the listener returned or raised.
    """
    if not _OTEL_AVAILABLE:
        return
    token = _dispatch_token.get()
    _dispatch_token.set(None)
    if token is not None:
        otel_context.detach(token)
    span = _dispatch_span.get()
    if span is None:
        return
    _dispatch_span.set(None)
    if exception is not None:
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))
    span.end()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _publish_trace_context() -> dict[str, str] | None:
    """W3C trace context of the active publish span, for an outbox row to store.

    None when OTel is missing, no publish span is active (the observability
    plugin disabled never starts one) or the span has no valid span context,
    as when no SDK tracer provider is installed. A sampled-out span still
    yields a carrier, so its sampling decision travels with the row.
    """
    span = _publish_span.get()
    if not _OTEL_AVAILABLE or span is None:
        return None
    carrier: dict[str, str] = {}
    _propagator.inject(carrier, trace.set_span_in_context(span))
    return carrier or None


def trace_headers(carrier: Any) -> dict[str, str]:
    """Broker message headers that carry a W3C trace context; empty without one.

    ``tracestate`` is sent only when non-empty. Anything but a mapping of
    strings (a forged outbox row's stored carrier) yields no headers rather
    than failing the send.
    """
    if not isinstance(carrier, dict):
        return {}
    return {
        name: value
        for name in ("traceparent", "tracestate")
        if isinstance(value := carrier.get(name), str) and value
    }


def _extract_context(carrier: Any) -> Any:
    """The OTel context a stored carrier names; None when it names nothing usable."""
    try:
        return _propagator.extract(carrier)
    except Exception:
        logger.debug("unusable stored trace context", exc_info=True)
        return None


def _event_type(event: Any) -> str:
    return f"{type(event).__module__}.{type(event).__qualname__}"


def _detect_calling_module() -> str:
    """Best-effort: which application module called ``publish()``.

    Walks the call stack for the first frame whose module is under the
    configured application package and isn't modulith itself. Returns
    ``"unknown"`` if detection fails — observability must never block or
    perturb business logic over best-effort metadata.
    """
    try:
        from ..runtime import _runtime

        cfg = _runtime.config
        app_package = cfg.package if cfg is not None else None
        if not app_package:
            return "unknown"

        import sys

        depth = 1
        while True:
            try:
                frame = sys._getframe(depth)
            except ValueError:
                break
            name = str(frame.f_globals.get("__name__", ""))
            is_modulith_internal = name == "modulith" or name.startswith("modulith.")
            if (
                name == app_package or name.startswith(app_package + ".")
            ) and not is_modulith_internal:
                return name
            depth += 1
    except Exception:  # pragma: no cover - detection is best-effort only
        logger.debug("calling-module detection failed", exc_info=True)
    return "unknown"


__all__ = [
    "modulith_after_event_published",
    "modulith_before_event_published",
    "modulith_on_listener_complete",
    "modulith_on_listener_dispatch",
    "modulith_on_publish_error",
]
