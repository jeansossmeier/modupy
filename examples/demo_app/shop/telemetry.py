"""Optional OpenTelemetry wiring for the demo shop.

Enabled by setting ``MODULITH_DEMO_OTEL=1`` (read by ``shop.main``'s
lifespan). Not imported at all unless that flag is set, so a user who only
installed ``modulith[fastapi,cli]`` (no ``modulith[otel]``) can still run the
demo in every other mode.
"""

from __future__ import annotations

_initialized = False


def init_telemetry() -> None:
    """Configure a console-exporting OTel tracer provider.

    Idempotent/no-op-safe: calling it more than once does not register a
    second provider. Raises a clear, actionable ``ImportError`` if
    ``opentelemetry`` is not installed, pointing at the extra to install.
    """
    global _initialized
    if _initialized:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
    except ImportError as exc:
        raise ImportError(
            "MODULITH_DEMO_OTEL=1 requires the OpenTelemetry SDK. "
            "Install the extra: pip install 'modulith[otel]'"
        ) from exc

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    _initialized = True


__all__ = ["init_telemetry"]
