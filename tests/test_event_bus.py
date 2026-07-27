"""Behavioral regression tests for InMemoryEventBus + BrokerRegistry.

Each test names the behaviour it pins, and was written failing-first against
code that did not yet have it (strict TDD).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass

import pytest

from modulith.brokers import BrokerRegistry, DuplicateBrokerError
from modulith.event_bus import InMemoryEventBus


@dataclass(frozen=True)
class BusEvent:
    n: int


# ---------------------------------------------------------------------------
# Eager gather() argument evaluation
# ---------------------------------------------------------------------------


async def test_call_time_error_does_not_skip_sibling_listeners() -> None:
    """A handler that raises synchronously at call time (wrong
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
# register() must enforce its "handler must be async" contract
# ---------------------------------------------------------------------------


def test_register_rejects_sync_handler_with_clear_error() -> None:
    """Registering a plain sync callable must fail loudly at
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
    """The documented entry points (async def, wrap_sync_listener
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
    """The pre-bootstrap queueing path must validate too, so the
    error surfaces at the registration call site, not mid-bootstrap."""
    from modulith.runtime import Runtime

    rt = Runtime()

    def plain_sync_handler(event: BusEvent) -> None:  # pragma: no cover
        pass

    with pytest.raises(TypeError, match="plain_sync_handler"):
        rt.register_listener(BusEvent, plain_sync_handler)
    assert rt._pending_listeners == []


# ---------------------------------------------------------------------------
# Unsynchronized _handlers access
# ---------------------------------------------------------------------------


def test_registered_event_types_safe_under_concurrent_register() -> None:
    """registered_event_types() must not crash with 'dictionary changed size
    during iteration' while another thread registers new event types. The fix
    — snapshotting the dict under the bus lock — also covers the sibling
    listeners_for() read exercised here."""
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
# close_all() must survive a cancelled broker close
# ---------------------------------------------------------------------------


async def test_close_all_continues_after_cancelled_close() -> None:
    """A broker whose close() raises asyncio.CancelledError must
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


# ---------------------------------------------------------------------------
# BrokerRegistry partial-failure / replacement contracts and bus introspection
# ---------------------------------------------------------------------------


class _RecordingBroker:
    """Minimal Broker double recording publishes and close() calls."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.closed = False

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.published.append((target, payload))

    async def close(self) -> None:
        self.closed = True


async def test_close_all_logs_and_continues_after_raising_close(caplog) -> None:
    """A broker whose close() raises a plain Exception must not abort
    cleanup of brokers registered after it, and the error must be logged
    rather than propagated — the partial-failure contract close_all()'s
    docstring documents."""

    class _FailingBroker(_RecordingBroker):
        async def close(self) -> None:
            raise RuntimeError("redis connection already dead")

    registry = BrokerRegistry()
    first = _RecordingBroker()
    failing = _FailingBroker()
    last = _RecordingBroker()
    registry.register("first", first)
    registry.register("failing", failing)
    registry.register("last", last)

    with caplog.at_level(logging.ERROR, logger="modulith.brokers"):
        await registry.close_all()  # must not raise

    assert first.closed is True
    assert last.closed is True
    logged = [r.getMessage() for r in caplog.records if "failed to close cleanly" in r.getMessage()]
    assert any("'failing'" in message for message in logged)


def test_unregister_is_a_noop_for_an_absent_scheme() -> None:
    """unregister() on a scheme that was never registered is the
    documented silent no-op (0/1 boundary)."""
    registry = BrokerRegistry()

    registry.unregister("ghost")  # must not raise

    assert registry.schemes() == []


async def test_unregister_then_register_replaces_the_broker() -> None:
    """The sanctioned replace sequence — unregister, then register —
    must succeed and route subsequent publishes to the replacement broker
    (re-registering without unregister stays a loud DuplicateBrokerError)."""
    registry = BrokerRegistry()
    original = _RecordingBroker()
    replacement = _RecordingBroker()
    registry.register("kafka", original)

    with pytest.raises(DuplicateBrokerError):
        registry.register("kafka", replacement)

    registry.unregister("kafka")
    registry.register("kafka", replacement)

    await registry.publish("kafka:orders", b"payload")

    assert replacement.published == [("orders", b"payload")]
    assert original.published == []


def test_registered_event_types_lists_exactly_the_listened_types() -> None:
    """registered_event_types() feeds the worker's deserialization
    allowlist (modulith/_worker.py) and its broker-stream subscriptions
    (modulith/_consumer.py), so its output must be exactly the event types
    with at least one registered listener — no duplicates, no strays."""

    @dataclass(frozen=True)
    class OtherEvent:
        s: str

    @dataclass(frozen=True)
    class NeverListened:
        s: str

    bus = InMemoryEventBus()

    async def handler(event: object) -> None:  # pragma: no cover
        pass

    bus.register(BusEvent, handler)
    bus.register(BusEvent, handler)  # a second listener must not duplicate the type
    bus.register(OtherEvent, handler)

    types = bus.registered_event_types()

    assert set(types) == {BusEvent, OtherEvent}
    assert len(types) == 2
    assert NeverListened not in types


def test_clear_empties_listeners_and_registered_event_types() -> None:
    """clear() removes every listener, emptying both listeners_for()
    and registered_event_types() — the documented embedder-facing reset."""
    bus = InMemoryEventBus()

    async def handler(event: object) -> None:  # pragma: no cover
        pass

    bus.register(BusEvent, handler)
    bus.clear()

    assert bus.listeners_for(BusEvent) == []
    assert bus.registered_event_types() == []


async def test_publish_logs_every_failing_listener_and_reraises_the_first(caplog) -> None:
    """With several failing listeners, publish() logs each failure
    individually (debugging must not depend on which exception happens to be
    re-raised), re-raises the first in registration order, and still runs the
    healthy sibling."""
    bus = InMemoryEventBus()
    ran: list[str] = []

    async def fails_first(event: BusEvent) -> None:
        raise ValueError("first failure")

    async def healthy(event: BusEvent) -> None:
        ran.append("healthy")

    async def fails_second(event: BusEvent) -> None:
        raise RuntimeError("second failure")

    bus.register(BusEvent, fails_first)
    bus.register(BusEvent, healthy)
    bus.register(BusEvent, fails_second)

    with caplog.at_level(logging.ERROR, logger="modulith.event_bus"):
        with pytest.raises(ValueError, match="first failure"):
            await bus.publish(BusEvent(n=1))

    assert ran == ["healthy"]
    failures = [r.getMessage() for r in caplog.records if "failed for BusEvent" in r.getMessage()]
    assert any("fails_first" in message for message in failures)
    assert any("fails_second" in message for message in failures)
