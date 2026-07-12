"""In-memory async event bus.

The default bus for single-process mode. Listeners register against an
event type; publish() dispatches to all matching listeners concurrently.

This is intentionally minimal — durability, retries, and observability
are layered on *around* it rather than wrapped over publish(): the outbox
plugin and the runtime's hook-firing dispatch path
(``Runtime._dispatch_with_hooks``) read registrations via
``listeners_for()`` and drive delivery themselves, while ``publish()``
here is the direct entry point for the cross-process consumer and for
embedders driving a bus directly. Keeping the bus simple means each
concern stays in its own well-defined layer.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


def _require_async_handler(event_type: type, handler: Callable[..., Any]) -> None:
    """Enforce the bus contract that handlers are awaitable when called.

    Accepts ``async def`` functions (including ``functools.partial`` chains
    over them) and callables whose ``__call__`` is a coroutine function.
    Anything else is rejected here, at registration time, with an error that
    names the handler — accepting it would poison the whole
    ``asyncio.gather()`` batch at publish time with a TypeError that names
    neither the offending handler nor the violated contract.
    """
    if asyncio.iscoroutinefunction(handler):
        return
    call = getattr(handler, "__call__", None)  # noqa: B004 - duck-typed check
    if call is not None and asyncio.iscoroutinefunction(call):
        return
    raise TypeError(
        f"listener {getattr(handler, '__qualname__', repr(handler))!r} for event "
        f"type {event_type.__name__!r} must be async (an `async def` function). "
        f"Wrap sync callables with modulith.sync.wrap_sync_listener, or use the "
        f"@listener decorator, which wraps them automatically."
    )


class InMemoryEventBus:
    """Async event bus that dispatches in-process to async listeners.

    Each event type maps to a list of async callables. publish() runs
    them concurrently and waits for all to complete. Failures in one
    listener don't prevent others from running, but the first exception
    (in registration order) is re-raised after all listeners finish.

    Registration and introspection are guarded by a lock so dynamic
    post-bootstrap registration from another thread can never corrupt an
    in-flight dispatch or a snapshot read.
    """

    def __init__(self) -> None:
        # Map event type -> list of async listener callables.
        # Using defaultdict so register() can append without checking.
        self._handlers: dict[type, list[Callable[..., Any]]] = defaultdict(list)
        # Guards _handlers: register() inserts new dict keys, and both the
        # dispatch snapshot in publish() and the introspection methods
        # iterate the dict — an unguarded concurrent insert crashes the
        # iteration with "dictionary changed size during iteration".
        self._lock = threading.Lock()

    def register(self, event_type: type, handler: Callable[..., Any]) -> None:
        """Add a listener for a specific event type.

        Multiple listeners for the same type are allowed and all run.
        The handler must be async (an `async def` function, or a callable
        whose ``__call__`` is one); sync callables are rejected with a
        TypeError naming the handler — wrap them with
        ``modulith.sync.wrap_sync_listener`` or use ``@listener``.
        """
        _require_async_handler(event_type, handler)
        with self._lock:
            self._handlers[event_type].append(handler)
        logger.debug(
            "registered listener %s for %s",
            getattr(handler, "__qualname__", repr(handler)),
            event_type.__name__,
        )

    async def publish(self, event: Any) -> None:
        """Dispatch an event to all registered listeners.

        Listeners run concurrently via asyncio.gather. Exceptions in any
        listener are logged. After all listeners complete, the first
        exception **in registration order** (not necessarily the first to
        occur chronologically — listeners run concurrently) is re-raised
        so the caller knows something failed.
        """
        # Look up by exact type. Subclass dispatch is intentionally not
        # supported — explicit registration keeps routing predictable.
        # Snapshot under the lock: dispatch targets the listeners registered
        # at publish time; a concurrent register() must not be swept into
        # (or corrupt) an in-flight dispatch.
        with self._lock:
            handlers = list(self._handlers.get(type(event), []))
        if not handlers:
            return

        async def _invoke(handler: Callable[..., Any]) -> None:
            # Calling the handler *inside* a wrapper coroutine turns a
            # synchronous call-time error (wrong arity, a non-async callable
            # that slipped past register()) into a normal per-listener
            # failure captured by return_exceptions, instead of aborting the
            # whole dispatch before any sibling listener runs.
            await handler(event)

        # gather() with return_exceptions captures errors without
        # cancelling sibling listeners. Each handler runs to completion
        # regardless of what its peers do.
        results = await asyncio.gather(
            *(_invoke(h) for h in handlers),
            return_exceptions=True,
        )

        # Log every failure so debugging doesn't depend on which one
        # happened to be raised.
        first_error: BaseException | None = None
        for handler, result in zip(handlers, results, strict=False):
            if isinstance(result, BaseException):
                logger.error(
                    "listener %s failed for %s: %s",
                    getattr(handler, "__qualname__", repr(handler)),
                    type(event).__name__,
                    result,
                )
                if first_error is None:
                    first_error = result

        # Re-raise so the caller sees the failure. Production code paths
        # using the outbox plugin will catch and retry; direct callers
        # see the exception.
        if first_error is not None:
            raise first_error

    def listeners_for(self, event_type: type) -> list[Callable[..., Any]]:
        """Return registered listeners for an event type — useful for tests."""
        with self._lock:
            return list(self._handlers.get(event_type, []))

    def registered_event_types(self) -> list[type]:
        """Event types that have at least one registered listener.

        Used by the cross-process consumer to derive the broker streams a
        worker must subscribe to — exactly the events its local listeners
        consume. Returns a snapshot taken under the lock, so a concurrent
        register() of a new event type can't crash the iteration.
        """
        with self._lock:
            return [event_type for event_type, handlers in self._handlers.items() if handlers]

    def clear(self) -> None:
        """Remove all registered listeners.

        Not used by the runtime itself — test isolation goes through
        ``Runtime._reset_for_testing``, which discards the whole bus and
        builds a fresh one on the next bootstrap. Kept for embedders that
        drive an ``InMemoryEventBus`` directly.
        """
        with self._lock:
            self._handlers.clear()
