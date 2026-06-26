"""Built-in OpenTelemetry auto-instrumentation.

When OTel is installed and a tracer provider is configured, this plugin
automatically creates spans for:
  - Every cross-module event publication (parent span)
  - Every listener dispatch (child span linked to the publication)
  - Every plugin hook call (debug-level — high volume, opt-in)

When OTel is NOT installed, the plugin loads but does nothing. No
errors, no warnings — true silent no-op. This matters because most
apps ship with OTel as a soft dependency (configured per environment).

Implementation status: SKELETON. ~120 lines when complete.

Distributed via the `modulith[otel]` extra. The plugin auto-detects
OTel availability at import time via a try/except block.

Spans emitted:

  modulith.event.publish
    attributes:
      event.type     — fully-qualified class name
      event.module   — source module (where publish was called from)
      modulith.outbox = true | false
    duration: from before_event_published hook to after_event_published

  modulith.event.dispatch
    attributes:
      event.type
      listener.name
      listener.module
      listener.duration_ms
    parent: linked to modulith.event.publish span
    duration: just the listener invocation
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

from modulith import EventPublication, hookimpl

logger = logging.getLogger("modulith.observability")


# ---------------------------------------------------------------------------
# Soft OTel import
# ---------------------------------------------------------------------------

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
    _tracer = trace.get_tracer("modulith", version="0.1.0")
except ImportError:
    _OTEL_AVAILABLE = False
    _tracer = None


# ---------------------------------------------------------------------------
# Per-publication span context
# ---------------------------------------------------------------------------

# We keep the active span around so the listener-dispatch hook can link
# its span to the publication's span.
_active_span: ContextVar[Any] = ContextVar("_modulith_active_span", default=None)
_publish_start: ContextVar[float] = ContextVar("_modulith_publish_start", default=0.0)


# ---------------------------------------------------------------------------
# Hook implementations
# ---------------------------------------------------------------------------


@hookimpl
def modulith_before_event_published(event: Any) -> None:
    """Start a span for this publication.

    IMPLEMENTATION TODO:
    if not _OTEL_AVAILABLE: return
    span = _tracer.start_span(
        "modulith.event.publish",
        attributes={
            "event.type": f"{type(event).__module__}.{type(event).__qualname__}",
            "event.module": _detect_calling_module(),
        },
    )
    _active_span.set(span)
    _publish_start.set(time.monotonic())
    """
    if not _OTEL_AVAILABLE:
        return
    raise NotImplementedError("Phase 2 — see TODO above")


@hookimpl
def modulith_after_event_published(event: Any, publication: EventPublication) -> None:
    """End the publication span.

    IMPLEMENTATION TODO:
    if not _OTEL_AVAILABLE: return
    span = _active_span.get()
    if span is None: return
    duration_ms = (time.monotonic() - _publish_start.get()) * 1000
    span.set_attribute("modulith.duration_ms", duration_ms)
    span.end()
    _active_span.set(None)
    """
    if not _OTEL_AVAILABLE:
        return
    raise NotImplementedError("Phase 2")


@hookimpl
def modulith_on_listener_dispatch(
    event: Any,
    listener_name: str,
    publication: EventPublication,
) -> None:
    """Start a span for one listener invocation.

    IMPLEMENTATION TODO:
    if not _OTEL_AVAILABLE: return
    parent = _active_span.get()
    span = _tracer.start_span(
        "modulith.event.dispatch",
        context=trace.set_span_in_context(parent) if parent else None,
        attributes={
            "event.type": f"{type(event).__module__}.{type(event).__qualname__}",
            "listener.name": listener_name,
            "publication.id": str(publication.id),
        },
    )

    NOTE: the listener invocation runs synchronously after this hook in
    the event bus. We need a corresponding span.end() somewhere. Two
    options:
      1. Add a modulith_after_listener_dispatch hookspec (cleaner).
      2. Wrap the listener call ourselves via hookwrapper (uglier).

    Decision pending — see SPEC.md §4.1 hookspec list. For v1.1 add
    the after_dispatch hookspec; for v1 use hookwrapper.
    """
    if not _OTEL_AVAILABLE:
        return
    raise NotImplementedError("Phase 2")


@hookimpl
def modulith_on_listener_error(
    event: Any,
    listener_name: str,
    publication: EventPublication,
    exception: BaseException,
) -> None:
    """Mark the dispatch span as errored.

    IMPLEMENTATION TODO:
    if not _OTEL_AVAILABLE: return
    span = trace.get_current_span()  # or maintain our own ContextVar
    span.record_exception(exception)
    span.set_status(Status(StatusCode.ERROR, str(exception)))
    """
    if not _OTEL_AVAILABLE:
        return


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _detect_calling_module() -> str:
    """Best-effort detection of which module called publish().

    IMPLEMENTATION TODO:
    Walk the call stack looking for a frame whose module is under the
    application package and isn't modulith itself. The first such
    module is the publisher.

    Returns "unknown" if detection fails — observability shouldn't
    block business logic over best-effort metadata.
    """
    return "unknown"


__all__ = [
    "modulith_after_event_published",
    "modulith_before_event_published",
    "modulith_on_listener_dispatch",
    "modulith_on_listener_error",
]
