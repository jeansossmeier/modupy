"""In-memory async event bus.

The default bus for single-process mode. Listeners register against an
event type; publish() dispatches to all matching listeners concurrently.

This is intentionally minimal — durability, retries, and observability
are layered on by plugins (the outbox plugin wraps publish, the
observability plugin wraps dispatch). Keeping the bus simple means each
concern stays in its own well-defined plugin.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class InMemoryEventBus:
    """Async event bus that dispatches in-process to async listeners.

    Each event type maps to a list of async callables. publish() runs
    them concurrently and waits for all to complete. Failures in one
    listener don't prevent others from running, but the first exception
    is re-raised after all listeners finish.
    """

    def __init__(self) -> None:
        # Map event type -> list of async listener callables.
        # Using defaultdict so register() can append without checking.
        self._handlers: dict[type, list[Callable[..., Any]]] = defaultdict(list)

    def register(self, event_type: type, handler: Callable[..., Any]) -> None:
        """Add a listener for a specific event type.

        Multiple listeners for the same type are allowed and all run.
        The handler must be async (an `async def` function).
        """
        self._handlers[event_type].append(handler)
        logger.debug(
            "registered listener %s for %s",
            getattr(handler, "__qualname__", repr(handler)),
            event_type.__name__,
        )

    async def publish(self, event: Any) -> None:
        """Dispatch an event to all registered listeners.

        Listeners run concurrently via asyncio.gather. Exceptions in any
        listener are logged. The first exception is re-raised after all
        listeners complete so the caller knows something failed.
        """
        # Look up by exact type. Subclass dispatch is intentionally not
        # supported — explicit registration keeps routing predictable.
        handlers = self._handlers.get(type(event), [])
        if not handlers:
            return

        # gather() with return_exceptions captures errors without
        # cancelling sibling listeners. Each handler runs to completion
        # regardless of what its peers do.
        results = await asyncio.gather(
            *(h(event) for h in handlers),
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
        return list(self._handlers.get(event_type, []))

    def clear(self) -> None:
        """Remove all registered listeners. Used between tests."""
        self._handlers.clear()
