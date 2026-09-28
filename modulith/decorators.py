"""User-facing API: @event, @listener, publish, configure — plus the
topology-specific @externalized.

These are thin wrappers over the runtime singleton. Users need four
names to be productive in single-process mode:

    @event       — mark a class as a domain event
    @listener    — register an async function to receive an event type
    publish      — emit an event to all registered listeners
    configure    — override defaults (only when needed)

Applications adopting the process-per-module topology use a fifth:

    @externalized — mark an event as routed to the configured broker so
                    workers in other processes can consume it

Everything else (plugins, hooks, protocols) is for adapter authors.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar, overload

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


def externalized(cls: type[T] | None = None, *, target: str | None = None) -> Any:
    """Mark an event as *externalized* — routed to the configured broker so
    workers in other processes can consume it (process-per-module topology).

    Two forms::

        @externalized
        class OrderPlaced: ...        # default target: "{broker}:{module.qualname}"

        @externalized(target="redis-streams:orders.placed")
        class OrderPlaced: ...        # explicit "scheme:destination"

    An externalized event is published to the broker *whether or not it also
    has a local listener* (fan-out across processes). In single-process
    topology this is an inert marker — there is no broker.

    The runtime resolves an event's broker target in priority order:
      1. the ``modulith_resolve_event_target`` hook (dynamic / tenant-aware),
      2. this annotation's explicit ``target`` (static per-event override),
      3. the default scheme ``{broker}:{fully-qualified-event-name}``.
    """

    def wrap(klass: type[T]) -> type[T]:
        klass.__modulith_externalized__ = True  # type: ignore[attr-defined]
        if target is not None:
            klass.__modulith_broker_target__ = target  # type: ignore[attr-defined]
        return klass

    # Bare ``@externalized`` passes the class as the first positional arg;
    # ``@externalized(target=...)`` passes cls=None and returns the wrapper.
    if cls is None:
        return wrap
    return wrap(cls)


def _resolve_event_type(func: Callable[..., Any], target: Callable[..., Any]) -> type:
    """Resolve the event type from a listener's first parameter annotation.

    Two annotation forms must both work:

      * Eager (no ``from __future__ import annotations``): the annotation is
        already the class object — returned as-is.
      * Stringized (PEP 563, the modern default used across this project and
        the SPEC/README examples): the annotation is a *string* like
        ``'OrderCreated'``. It is resolved against the function's own module
        globals via ``inspect.get_annotations(eval_str=True)``.

    ``func`` is used only for error messages; ``target`` is the object whose
    signature and ``__globals__`` we read (the unwrapped function for sync
    handlers, so functools.wraps chains don't hide the parameter list).
    """
    sig = inspect.signature(target)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(f"@listener {func.__qualname__!r} must accept an event argument")

    first = params[0]
    annotation = first.annotation
    if annotation is inspect.Parameter.empty:
        raise TypeError(
            f"@listener {func.__qualname__!r} must annotate its event "
            f"parameter so modulith knows which event type to route. "
            f"Example: 'async def handler(event: OrderCreated)'"
        )

    if isinstance(annotation, str):
        # PEP 563 stored the annotation as a string. Un-stringize it against
        # the function's module globals. Names defined only in local scope
        # (e.g. an event class nested in a function) are invisible here — that
        # is an inherent PEP 563 limitation, surfaced as a clear TypeError
        # rather than a downstream ``str has no attribute __name__`` crash.
        #
        # ONLY this annotation is evaluated. inspect.get_annotations(eval_str=True)
        # evaluates the whole dict — return type and every other parameter — so
        # a TYPE_CHECKING-only import on a second parameter (the sanctioned way
        # to break a runtime import cycle; see builtin.verifier) raised NameError
        # here and got reported as an unresolvable *event* annotation, blaming
        # the wrong parameter and rejecting a listener the bus would have called
        # perfectly well (it invokes handlers with the event alone).
        try:
            annotation = eval(annotation, getattr(target, "__globals__", {}))
        except (NameError, AttributeError, SyntaxError) as exc:
            raise TypeError(
                f"@listener {func.__qualname__!r} annotates its event parameter "
                f"as {first.annotation!r}, but modulith could not resolve that "
                f"name to a class. Define the event type at module scope so its "
                f"annotation resolves (classes in local scope are invisible "
                f"under 'from __future__ import annotations')."
            ) from exc

    if not isinstance(annotation, type):
        # Either an eager non-class annotation, or a string that resolved to a
        # non-class. The bus keys on ``type(event)``, so a non-class can never
        # be a routing key — fail fast with a clear message instead of a dead
        # listener that silently never fires.
        raise TypeError(
            f"@listener {func.__qualname__!r} could not resolve its event "
            f"annotation to a class (got {annotation!r})."
        )
    return annotation


def _normalize_listener_targets(targets: object) -> tuple[str, ...]:
    """Validate static broker targets attached to a listener."""
    if isinstance(targets, str) or not isinstance(targets, list | tuple):
        raise TypeError("broker_targets must be a list or tuple of 'scheme:destination' strings")

    normalized: list[str] = []
    for target in targets:
        if type(target) is not str:
            raise TypeError(
                f"broker_targets must contain only 'scheme:destination' strings; got {target!r}"
            )
        scheme, separator, destination = target.partition(":")
        if not separator or not scheme.strip() or not destination.strip():
            raise TypeError(
                "broker_targets must contain non-empty 'scheme:destination' "
                f"strings; got {target!r}"
            )
        normalized.append(f"{scheme.strip()}:{destination.strip()}")
    return tuple(normalized)


# Both call forms are overloaded so a decorated function keeps its own
# signature downstream. With only the ``-> Any`` implementation signature,
# type checkers erased every ``@listener`` function to ``Any`` — silently
# disabling all checking on calls to it despite the shipped ``py.typed`` — and
# the parameterized form tripped ``untyped-decorator`` under ``mypy --strict``.
# ``register`` returns the original undecorated ``handler``, so ``F -> F`` is
# exact rather than a convenient lie.
@overload
def listener(func: F) -> F: ...


@overload
def listener(*, broker_targets: list[str] | tuple[str, ...] = ()) -> Callable[[F], F]: ...


def listener(
    func: F | None = None,
    *,
    broker_targets: list[str] | tuple[str, ...] = (),
) -> Any:
    """Register a listener for a specific event type.

    The event type is inferred from the function's first argument
    annotation. Listeners may be ``async def`` (preferred) or ``def``.
    Sync listeners run in the event loop's default executor so they never
    block the event loop. Note that multiple sync listeners for the same
    event run concurrently on separate executor threads — see
    ``modulith.sync.wrap_sync_listener`` for the sharp edges around shared
    sessions/resources.

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
    normalized_targets = _normalize_listener_targets(broker_targets)

    def register(handler: F) -> F:
        # functools.wraps chains can hide an async target behind a sync wrapper.
        unwrapped = inspect.unwrap(handler)
        target_is_async = inspect.iscoroutinefunction(unwrapped)
        resolve_target = unwrapped
        if not target_is_async:
            call = getattr(unwrapped, "__call__", None)  # noqa: B004
            if call is not None and inspect.iscoroutinefunction(call):
                target_is_async = True
                resolve_target = call
        event_type = _resolve_event_type(handler, resolve_target)
        registered: Callable[..., Any]

        if target_is_async and inspect.iscoroutinefunction(handler):
            registered = handler
        elif target_is_async:

            @functools.wraps(handler)
            async def async_adapter(*args: Any, **kwargs: Any) -> Any:
                result = handler(*args, **kwargs)
                if inspect.isawaitable(result):
                    return await result
                return result

            async_adapter.__modulith_sync_wrapped__ = handler  # type: ignore[attr-defined]
            registered = async_adapter
        else:
            from .sync import wrap_sync_listener

            registered = wrap_sync_listener(handler)

        if registered is not handler and not hasattr(handler, "__qualname__"):
            # A callable instance has no __qualname__, so functools.wraps leaves
            # the adapter's own and every instance in a module would share one
            # outbox listener id. Its class name is distinct and restart-stable.
            registered.__qualname__ = type(handler).__qualname__

        registered.__modulith_broker_targets__ = normalized_targets  # type: ignore[union-attr]
        _runtime.register_listener(event_type, registered)
        return handler

    if func is None:
        return register
    return register(func)


def bootstrap() -> None:
    """Eagerly run the runtime's one-time bootstrap. Idempotent.

    Normally bootstrap is *lazy* — the first ``@listener`` registration or
    ``publish()`` call triggers it. Most applications never need to call
    this directly. Call it explicitly at startup when something depends on
    bootstrap having already happened before the first publish — most
    notably the outbox's crash-recovery sweep, which skips every pending
    row for the cycle while ``event_bus`` is still ``None`` (an
    un-bootstrapped runtime has no bus to resolve listeners against). An
    embedding app that configures a durable outbox and wants stranded
    publications from a previous crash retried immediately at startup,
    rather than waiting for the first publish(), should call
    ``bootstrap()`` right after ``configure()``.

    Safe to call any number of times — after the first call, subsequent
    calls are a no-op fast path (same guarantee as ``publish()``'s implicit
    bootstrap).

    Example:
        from modulith import bootstrap, configure

        configure(package="myapp", outbox="postgres")
        bootstrap()  # crash-recovery sweep can dispatch immediately
    """
    _runtime.ensure_bootstrapped()


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
    (e.g. flags derived from your deploy environment at startup).

    Example:
        from modulith import configure

        configure(
            package="myapp",
            outbox="postgres",
            production=os.environ.get("ENV") == "prod",
        )

    Dict-valued fields like ``outbox_options`` are accepted and
    validated but currently reserved — the runtime does not read them
    yet. Outbox tuning (e.g. ``completion_mode``) is passed to
    ``modulith.builtin.outbox.configure()`` instead.
    """
    _runtime.configure(**overrides)
