"""User-facing API: @event, @listener, publish, configure.

These are thin wrappers over the runtime singleton. The whole point of
this module is that users only need to know these four names to be
productive:

    @event       — mark a class as a domain event
    @listener    — register an async function to receive an event type
    publish      — emit an event to all registered listeners
    configure    — override defaults (only when needed)

Everything else (plugins, hooks, protocols) is for adapter authors.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from .runtime import _runtime

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def event(cls: type[T]) -> type[T]:
    """Mark a class as a domain event.

    Sets a marker attribute used by verifier plugins, serializers, and
    observability tooling to identify event types. Beyond the marker,
    an event is a regular Python class — typically a frozen dataclass.

    Example:
        @event
        @dataclass(frozen=True)
        class OrderCreated:
            order_id: str
    """
    cls.__modulith_event__ = True  # type: ignore[attr-defined]
    return cls


def listener(func: F) -> F:
    """Register a listener for a specific event type.

    The event type is inferred from the function's first argument
    annotation. Listeners may be ``async def`` (preferred) or ``def``.
    Sync listeners run in the event loop's default executor so they never
    block the event loop.

    Example:
        @listener
        async def reserve_stock(event: OrderCreated) -> None:
            await stock_service.reserve(event.order_id)

        @listener
        def send_email(event: OrderCreated) -> None:
            mailer.send(event.order_id)   # sync, runs in threadpool

    Errors in registration (missing annotation, etc.) raise TypeError
    with a message explaining what to fix.
    """
    # Peel through functools.wraps chains so a @listener applied on top of
    # another decorator (e.g. timing, logging, cache wrappers) still detects
    # the underlying coroutine correctly. inspect.iscoroutinefunction does
    # NOT follow __wrapped__ in stdlib; doing it explicitly avoids false
    # rejection of legitimately-async listeners.
    unwrapped = inspect.unwrap(func)
    is_async = inspect.iscoroutinefunction(unwrapped)

    # Pull the event type from the first positional parameter's annotation.
    # Use the unwrapped signature for sync functions so functools.wraps
    # chains don't hide the original parameter list.
    sig = inspect.signature(unwrapped if not is_async else func)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(f"@listener {func.__qualname__!r} must accept an event argument")

    event_type = params[0].annotation
    if event_type is inspect.Parameter.empty:
        raise TypeError(
            f"@listener {func.__qualname__!r} must annotate its event "
            f"parameter so modulith knows which event type to route. "
            f"Example: 'async def handler(event: OrderCreated)'"
        )

    if is_async:
        # Async handler: register as-is.
        _runtime.register_listener(event_type, func)
    else:
        # Sync handler: wrap for executor dispatch and register the wrapper.
        # We return the original func so the user's variable stays sync and
        # is directly testable without going through the async machinery.
        from .sync import wrap_sync_listener

        wrapped = wrap_sync_listener(func)  # type: ignore[arg-type]
        _runtime.register_listener(event_type, wrapped)

    # The runtime queues this if bootstrap hasn't happened yet, registers
    # it directly otherwise. Either way, the listener is wired correctly.
    return func


async def publish(event: Any) -> None:
    """Publish an event to all registered listeners.

    In single-process mode this dispatches in-memory via the event bus.
    With outbox or process-per-module mode enabled (via configuration),
    this writes to a durable log first and dispatches asynchronously.

    The user's code is identical in both modes — only configuration changes.
    """
    await _runtime.publish(event)


def configure(**overrides: Any) -> None:
    """Override default configuration before modulith bootstraps.

    Call this once at application startup, before any @listener
    decoration or publish() call. After bootstrap, configuration is
    locked and configure() raises ConfigurationError.

    Most projects don't need this — pyproject.toml and env vars cover
    the typical cases. Use configure() for runtime-computed values
    (DSNs assembled at startup, feature flags from a remote service).

    Example:
        from modulith import configure

        configure(
            package="myapp",
            outbox="postgres",
            outbox_options={"dsn": os.environ["DATABASE_URL"]},
        )
    """
    _runtime.configure(**overrides)
