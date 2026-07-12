"""W2 G02_runtime — behavioral regression tests for InMemoryEventBus + BrokerRegistry.

Each test cites the audit finding id it reproduces. These tests were written
failing-first against the pre-fix code (strict TDD).
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass

import pytest

from modulith.brokers import BrokerRegistry
from modulith.event_bus import InMemoryEventBus


@dataclass(frozen=True)
class BusEvent:
    n: int


# ---------------------------------------------------------------------------
# A3-r3-130 — eager gather() argument evaluation
# ---------------------------------------------------------------------------


async def test_call_time_error_does_not_skip_sibling_listeners() -> None:
    """A3-r3-130: a handler that raises synchronously at call time (wrong
    arity) must become a per-listener failure — it must NOT abort the whole
    dispatch before any sibling listener runs."""
    bus = InMemoryEventBus()
    ran: list[str] = []

    async def listener_1(event: BusEvent) -> None:
        ran.append("l1")

    async def bad_arity(event: BusEvent, extra_required: int) -> None:  # pragma: no cover
        ran.append("bad")

    async def listener_3(event: BusEvent) -> None:
        ran.append("l3")

    bus.register(BusEvent, listener_1)
    bus.register(BusEvent, bad_arity)
    bus.register(BusEvent, listener_3)

    # The bad-arity handler's TypeError is re-raised (first error in
    # registration order), but both siblings must have run.
    with pytest.raises(TypeError, match="extra_required"):
        await bus.publish(BusEvent(n=1))

    assert ran == ["l1", "l3"]


# ---------------------------------------------------------------------------
# A3-r4-171 — register() must enforce its "handler must be async" contract
# ---------------------------------------------------------------------------


def test_register_rejects_sync_handler_with_clear_error() -> None:
    """A3-r4-171: registering a plain sync callable must fail loudly at
    registration time, naming the handler — not poison the whole gather()
    batch at publish time with an opaque asyncio TypeError."""
    bus = InMemoryEventBus()

    def plain_sync_handler(event: BusEvent) -> None:  # pragma: no cover
        pass

    with pytest.raises(TypeError, match="plain_sync_handler"):
        bus.register(BusEvent, plain_sync_handler)

    # Nothing was registered.
    assert bus.listeners_for(BusEvent) == []


def test_register_accepts_wrapped_sync_and_async_handlers() -> None:
    """A3-r4-171: the documented entry points (async def, wrap_sync_listener
    output) must keep working."""
    from modulith.sync import wrap_sync_listener

    bus = InMemoryEventBus()

    async def async_handler(event: BusEvent) -> None:  # pragma: no cover
        pass

    def sync_handler(event: BusEvent) -> None:  # pragma: no cover
        pass

    bus.register(BusEvent, async_handler)
    bus.register(BusEvent, wrap_sync_listener(sync_handler))
    assert len(bus.listeners_for(BusEvent)) == 2


async def test_runtime_register_listener_rejects_sync_handler() -> None:
    """A3-r4-171: the pre-bootstrap queueing path must validate too, so the
    error surfaces at the registration call site, not mid-bootstrap."""
    from modulith.runtime import Runtime

    rt = Runtime()

    def plain_sync_handler(event: BusEvent) -> None:  # pragma: no cover
        pass

    with pytest.raises(TypeError, match="plain_sync_handler"):
        rt.register_listener(BusEvent, plain_sync_handler)
    assert rt._pending_listeners == []


# ---------------------------------------------------------------------------
# A3-r4-170 / A3-r5-205 — unsynchronized _handlers access
# ---------------------------------------------------------------------------


def test_registered_event_types_safe_under_concurrent_register() -> None:
    """A3-r4-170: registered_event_types() must not crash with 'dictionary
    changed size during iteration' while another thread registers new event
    types. (A3-r5-205's adjudicated fix — snapshot under a lock — is the same
    mechanism, exercised here.)"""
    bus = InMemoryEventBus()

    async def handler(event: object) -> None:  # pragma: no cover
        pass

    errors: list[BaseException] = []
    stop = threading.Event()

    def registrar() -> None:
        try:
            for i in range(20_000):
                event_type = type(f"DynEvent{i}", (), {})
                bus.register(event_type, handler)
        finally:
            stop.set()

    t = threading.Thread(target=registrar)
    t.start()
    try:
        while not stop.is_set():
            try:
                bus.registered_event_types()
                bus.listeners_for(BusEvent)
            except BaseException as exc:
                errors.append(exc)
                break
    finally:
        t.join(timeout=30)

    assert errors == []


# ---------------------------------------------------------------------------
# A1-r3-127 — close_all() must survive a cancelled broker close
# ---------------------------------------------------------------------------


async def test_close_all_continues_after_cancelled_close() -> None:
    """A1-r3-127: a broker whose close() raises asyncio.CancelledError must
    not abort cleanup of brokers registered after it."""

    class OkBroker:
        def __init__(self) -> None:
            self.closed = False

        async def publish(
            self, target: str, payload: bytes, headers: dict[str, str] | None = None
        ) -> None:  # pragma: no cover
            pass

        async def close(self) -> None:
            self.closed = True

    class CancelledBroker(OkBroker):
        async def close(self) -> None:  # pragma: no cover - body raises
            raise asyncio.CancelledError()

    registry = BrokerRegistry()
    first = OkBroker()
    cancelled = CancelledBroker()
    last = OkBroker()
    registry.register("first", first)
    registry.register("cancelled", cancelled)
    registry.register("last", last)

    await registry.close_all()

    assert first.closed is True
    assert last.closed is True
