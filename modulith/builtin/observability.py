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

Distributed via the `modulith[otel]` extra. The plugin auto-detects
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
      listener.name
      publication.id
    parent: the modulith.event.publish span
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
transaction commits, in a different context, so those later dispatch spans are
not parented to the publish span.
"""

from __future__ import annotations

import logging
import time
from contextvars import ContextVar
from typing import Any

from modulith import EventPublication, hookimpl
from modulith.types import EventPublishReceipt

logger = logging.getLogger("modulith.observability")

# Instrumenting-library version recorded on the tracer. Passed positionally —
# OTel >= 1.43 removed the ``version=`` keyword from get_tracer().
_INSTRUMENTING_VERSION = "0.9.0"


# ---------------------------------------------------------------------------
# Soft OTel import
# ---------------------------------------------------------------------------

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
    _tracer: Any = trace.get_tracer("modulith", _INSTRUMENTING_VERSION)
except ImportError:
    _OTEL_AVAILABLE = False
    _tracer = None


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
    span = _tracer.start_span(
        "modulith.event.publish",
        attributes={
            "event.type": _event_type(event),
            "event.module": _detect_calling_module(),
        },
    )
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
    """
    if not _OTEL_AVAILABLE:
        return
    span = _publish_span.get()
    if span is None:
        return
    duration_ms = (time.monotonic() - _publish_start.get()) * 1000
    span.set_attribute("modulith.duration_ms", duration_ms)
    span.end()
    _publish_span.set(None)


@hookimpl
def modulith_on_publish_error(event: Any, exception: BaseException) -> None:
    """End the publish span for a publish that FAILED between the hooks.

    ``modulith_after_event_published`` is contractually scoped to successful
    persistence/dispatch, so when ``Runtime.publish`` fails between the
    paired hooks (outbox persist/serialize/broker-route raised — W3
    R4-W3-02) no hook fires and the span started in
    ``modulith_before_event_published`` leaked: never ended (so never
    exported — tracing went blind exactly during the outages the outbox
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
    """Start a span for one listener invocation, parented to the publish span."""
    if not _OTEL_AVAILABLE:
        return
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


@hookimpl
def modulith_on_listener_complete(
    event: Any,
    listener_name: str,
    publication: EventPublication,
    exception: BaseException | None,
) -> None:
    """End the listener-dispatch span; mark it errored if the listener raised."""
    if not _OTEL_AVAILABLE:
        return
    span = _dispatch_span.get()
    if span is None:
        return
    if exception is not None:
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))
    span.end()
    _dispatch_span.set(None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
            if (name == app_package or name.startswith(app_package + ".")) and not name.startswith(
                "modulith"
            ):
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
