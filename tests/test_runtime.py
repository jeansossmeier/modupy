"""Behavioral regression tests for the Runtime singleton.

Each test cites the audit finding id it reproduces. Written failing-first
against the pre-fix code (strict TDD; verified red on the base revision).

NOTE: no `from __future__ import annotations` — some tests use @listener with
locally-defined event classes, whose annotations must stay real objects.
"""

import asyncio
import contextlib
import logging
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

from modulith import ConfigurationError, EventPublication, configure, event, hookimpl, listener
from modulith.brokers import BrokerRegistry
from modulith.event_bus import InMemoryEventBus
from modulith.runtime import Runtime, _runtime
from modulith.serializers import JsonEventSerializer


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Each test gets a clean runtime + manifest registry."""
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


class FakeBroker:
    """Records publishes; satisfies the Broker protocol (publish + close)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str] | None]] = []
        self.closed = False

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.published.append((target, payload, headers))

    async def close(self) -> None:
        self.closed = True


class StubStore:
    """Minimal PublicationStore for outbox module-state tests."""

    def __init__(self) -> None:
        self.saved: list[EventPublication] = []

    async def save(self, publication: EventPublication) -> None:
        self.saved.append(publication)

    async def mark_complete(self, publication_id: UUID) -> None:
        pass

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return []

    async def archive(self, publication_id: UUID) -> None:
        pass

    async def delete(self, publication_id: UUID) -> None:
        pass


# ---------------------------------------------------------------------------
# A2-r5-203 — ensure_bootstrapped() must be genuinely retryable
# ---------------------------------------------------------------------------


_LISTENER_MODULE = """
    from dataclasses import dataclass
    from modulith import event, listener

    @event
    @dataclass(frozen=True)
    class OrderCreated:
        order_id: str

    received = []

    @listener
    async def on_created(event: OrderCreated) -> None:
        received.append(event)
"""


async def test_failed_bootstrap_retry_keeps_already_imported_listeners(make_fake_app) -> None:
    """A2-r5-203: a bootstrap that fails AFTER discovery has imported modules
    must not strand those modules' listeners — a retry (once the transient
    failure clears) must deliver events to them. The old in-place bootstrap
    flushed-and-cleared the pending queue into a bus it then discarded, so
    the retry 'succeeded' with zero listeners and publishes went nowhere."""

    class FlakyModuleLoadHook:
        """Fails the first bootstrap attempt at step 6.6 (after discovery)."""

        def __init__(self) -> None:
            self.failed_once = False

        @hookimpl
        def modulith_after_module_load(self, module: Any) -> None:
            if not self.failed_once:
                self.failed_once = True
                raise RuntimeError("transient startup failure (first attempt only)")

    make_fake_app({"orders": _LISTENER_MODULE})
    configure(package="fakeapp")
    _runtime._extra_plugins.append(FlakyModuleLoadHook())

    with pytest.raises(RuntimeError, match="transient startup failure"):
        _runtime.ensure_bootstrapped()
    assert _runtime._bootstrapped is False

    # Retry: the transient failure has cleared; the module was already
    # imported by attempt 1 (its @listener decorator will NOT re-fire).
    _runtime.ensure_bootstrapped()
    assert _runtime._bootstrapped is True

    import fakeapp.orders as orders

    from modulith import publish

    await publish(orders.OrderCreated(order_id="o-retry"))
    assert [e.order_id for e in orders.received] == ["o-retry"]


def test_register_brokers_hook_sees_resolved_config_during_bootstrap() -> None:
    """A2-r5-203 (regression found in remediation): making bootstrap atomic
    must NOT hide the resolved configuration from the broker-registration
    hook — adapter hookimpls (the shipped redis-streams broker,
    adapters/redis_broker.py) read it lazily via ``_runtime.config`` to
    decide whether/how to register. Holding config in a local until the
    commit point silently turned registration into a no-op: schemes() came
    back empty and every cross-process publish failed."""

    seen: dict[str, Any] = {}

    class ConfigReadingBrokerPlugin:
        @hookimpl
        def modulith_register_brokers(self, registry: Any) -> None:
            # Mirrors adapters/redis_broker.py's lazy config read.
            seen["config"] = _runtime.config
            if _runtime.config is not None and _runtime.config.broker == "cfgbroker":
                registry.register("cfgbroker", FakeBroker())

    configure(package="cfgtest", topology="processes", broker="cfgbroker", auto_discover=False)
    _runtime._extra_plugins.append(ConfigReadingBrokerPlugin())
    _runtime.ensure_bootstrapped()

    assert seen["config"] is not None, "hook fired before config was visible"
    registry = _runtime.broker_registry
    assert registry is not None
    assert "cfgbroker" in registry.schemes()


def test_failed_bootstrap_rolls_back_early_config_install() -> None:
    """A2-r5-203 companion: the early config install that the broker hook
    needs (test above) must be rolled back when a later step fails, so a
    failed bootstrap still leaves the runtime pristine for a clean retry."""

    class AlwaysFailingModuleLoad:
        @hookimpl
        def modulith_register_brokers(self, registry: Any) -> None:
            raise RuntimeError("boom at step 4.5")

    configure(package="cfgtest", auto_discover=False)
    _runtime._extra_plugins.append(AlwaysFailingModuleLoad())

    with pytest.raises(RuntimeError, match=r"boom at step 4\.5"):
        _runtime.ensure_bootstrapped()

    assert _runtime._bootstrapped is False
    assert _runtime.config is None  # documented: "None before bootstrap"


# ---------------------------------------------------------------------------
# A2-r1-5 / S1-r1-44 — reentrant runtime use during bootstrap must not deadlock
# ---------------------------------------------------------------------------


def test_configure_and_rebootstrap_during_bootstrap_raise_instead_of_deadlocking(
    make_fake_app,
) -> None:
    """A2-r1-5 / S1-r1-44: configure() or ensure_bootstrapped() reached from
    plugin/module code running inside bootstrap (same thread, lock held) used
    to deadlock the process forever on the non-reentrant lock. It must now
    raise a clear ConfigurationError and let bootstrap proceed."""
    recorded: dict[str, str] = {}

    class ReentrantHook:
        @hookimpl
        def modulith_register_brokers(self, registry: Any) -> None:
            try:
                _runtime.configure(some_option="value")
            except ConfigurationError as exc:
                recorded["configure"] = str(exc)
            try:
                _runtime.ensure_bootstrapped()
            except ConfigurationError as exc:
                recorded["ensure_bootstrapped"] = str(exc)

    make_fake_app({"orders": _LISTENER_MODULE})
    configure(package="fakeapp")
    _runtime._extra_plugins.append(ReentrantHook())

    # Bootstrap on a daemon thread with a bounded join: under the old plain
    # Lock this deadlocked forever; the join turns that into a test failure
    # instead of a hung pytest process.
    t = threading.Thread(target=_runtime.ensure_bootstrapped, daemon=True)
    t.start()
    t.join(timeout=10)

    assert not t.is_alive(), "bootstrap deadlocked on reentrant runtime use"
    assert "bootstrapping" in recorded["configure"]
    assert "re-entered" in recorded["ensure_bootstrapped"]
    assert _runtime._bootstrapped is True


# ---------------------------------------------------------------------------
# A2-r2-74 / S1-r2-101 / A2-r5-204 — register_listener vs bootstrap races
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RtEvent:
    n: int


def test_register_listener_cannot_land_in_consumed_pending_list() -> None:
    """A2-r2-74 / S1-r2-101: a register_listener() racing bootstrap's
    pending-listener flush must never append to the already-flushed list
    (which silently lost the listener forever). With the fix the registration
    blocks on the runtime lock until 'bootstrap' completes, then registers
    directly on the live bus. (A2-r5-204's crash — bootstrap iterating the
    handler dict while a concurrent registration mutates it — is excluded by
    the same lock.)"""
    rt = Runtime()
    entered_append = threading.Event()
    allow_append = threading.Event()

    class GatedList(list):
        """Parks the racing thread exactly at the lost-update window."""

        def append(self, item):
            entered_append.set()
            assert allow_append.wait(5)
            super().append(item)

    rt._pending_listeners = GatedList()

    async def handler(event: RtEvent) -> None:  # pragma: no cover - never dispatched
        pass

    racer = threading.Thread(target=lambda: rt.register_listener(RtEvent, handler), daemon=True)

    with rt._lock:  # simulate _bootstrap() holding the runtime lock
        racer.start()
        # Old code: the racer bypasses the lock and parks inside append().
        # Fixed code: the racer blocks on the lock; this wait times out.
        entered_append.wait(0.5)
        # Simulate bootstrap completing: flush + clear pending, publish state.
        bus = InMemoryEventBus()
        for event_type, h in list(rt._pending_listeners):
            bus.register(event_type, h)
        rt._pending_listeners.clear()
        rt._event_bus = bus
        rt._bootstrapped = True

    allow_append.set()
    racer.join(timeout=5)
    assert not racer.is_alive()

    assert rt._event_bus.listeners_for(RtEvent) == [handler]
    assert list(rt._pending_listeners) == []


# ---------------------------------------------------------------------------
# A2-r3-128 / A2-r4-169 — unregistered broker schemes must fail loudly + uniformly
# ---------------------------------------------------------------------------


_PUBLISHER_MODULE = """
    from dataclasses import dataclass
    from modulith import event, publish

    @event
    @dataclass(frozen=True)
    class OrderPlaced:
        order_id: str

    async def place(order_id: str) -> None:
        await publish(OrderPlaced(order_id=order_id))
"""


async def test_unregistered_default_scheme_raises_config_error(make_fake_app, caplog) -> None:
    """A2-r3-128: a typo'd/uninstalled default broker scheme in a cross-process
    topology must not silently drop every cross-process event forever — the
    publish raises ConfigurationError and bootstrap warns about the mismatch."""
    make_fake_app({"orders": _PUBLISHER_MODULE})
    # 'redis-stream' is a one-character typo of the shipped 'redis-streams'.
    configure(package="fakeapp", topology="processes", broker="redis-stream")
    with caplog.at_level(logging.WARNING, logger="modulith"):
        _runtime.ensure_bootstrapped()

    assert any(
        "no broker adapter is registered" in record.getMessage() for record in caplog.records
    )

    import fakeapp.orders as orders

    with pytest.raises(ConfigurationError, match="redis-stream"):
        await orders.place("o-1")


_EXPLICIT_TARGET_MODULE = """
    from dataclasses import dataclass
    from modulith import event, externalized, publish

    @externalized(target="kafka:orders.placed")
    @event
    @dataclass(frozen=True)
    class OrderPlaced:
        order_id: str

    async def place(order_id: str) -> None:
        await publish(OrderPlaced(order_id=order_id))
"""


async def test_unregistered_explicit_target_scheme_raises_config_error(make_fake_app) -> None:
    """A2-r4-169: an @externalized(target=...) scheme with no registered broker
    must fail with the SAME loud ConfigurationError as the default-scheme case
    — not an undocumented UnknownBrokerError leaking out of publish()."""
    make_fake_app({"orders": _EXPLICIT_TARGET_MODULE})
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None
    registry.register("testbroker", FakeBroker())

    import fakeapp.orders as orders

    with pytest.raises(ConfigurationError, match="kafka"):
        await orders.place("o-1")


# ---------------------------------------------------------------------------
# A1-r1-2 — hook-facing publications must carry the real payload bytes
# ---------------------------------------------------------------------------


async def test_in_memory_dispatch_hook_publications_carry_real_payload() -> None:
    """A1-r1-2: the EventPublications handed to the lifecycle hooks on the
    in-memory dispatch path carried a hardcoded b'' payload, defeating the
    documented dead-letter/audit use cases. They must carry the event's
    serialized bytes."""

    class _Capture:
        def __init__(self) -> None:
            self.payloads: dict[str, bytes] = {}

        @hookimpl
        def modulith_after_event_published(self, event: Any, publication: Any) -> None:
            self.payloads["after"] = publication.payload

        @hookimpl
        def modulith_on_listener_dispatch(
            self, event: Any, listener_name: str, publication: Any
        ) -> None:
            self.payloads["dispatch"] = publication.payload

        @hookimpl
        def modulith_on_listener_complete(
            self, event: Any, listener_name: str, publication: Any, exception: Any
        ) -> None:
            self.payloads["complete"] = publication.payload

    capture = _Capture()
    configure(package="payloadtest", auto_discover=False)
    _runtime._extra_plugins.append(capture)

    @event
    @dataclass(frozen=True)
    class Ping:
        x: int

    seen: list[Ping] = []

    @listener
    async def on_ping(evt: Ping) -> None:
        seen.append(evt)

    from modulith import publish

    await publish(Ping(x=7))

    assert seen
    assert capture.payloads, "hooks never fired"
    for name, payload in capture.payloads.items():
        assert b'"x":7' in payload, f"{name} hook got payload {payload!r}"


# ---------------------------------------------------------------------------
# S1-r1-46 — shutdown() must drain the store's in-flight dispatch tasks
# ---------------------------------------------------------------------------


async def test_shutdown_waits_for_inflight_store_dispatch() -> None:
    """S1-r1-46: Runtime.shutdown() must drain the active publication store's
    in-flight after-commit dispatch tasks (duck-typed wait_for_dispatch())
    instead of abandoning mid-flight listener execution."""
    from modulith.builtin import outbox

    class DrainableStore(StubStore):
        def __init__(self) -> None:
            super().__init__()
            self.waited = False

        async def wait_for_dispatch(self) -> None:
            self.waited = True

    store = DrainableStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    try:
        await _runtime.shutdown()
    finally:
        outbox._reset_for_testing()

    assert store.waited is True


# ---------------------------------------------------------------------------
# A2-r2-75 — _reset_for_testing() must not leak brokers or outbox state
# ---------------------------------------------------------------------------


def test_reset_for_testing_closes_registered_brokers() -> None:
    """A2-r2-75 (broker half): the reset every test fixture calls must close
    registered brokers — the exact leak shutdown() exists to prevent."""
    rt = Runtime()
    registry = BrokerRegistry()
    fake = FakeBroker()
    registry.register("fake", fake)
    rt._broker_registry = registry
    rt._bootstrapped = True

    rt._reset_for_testing()

    assert fake.closed is True


async def test_reset_for_testing_clears_outbox_state_and_cancels_retry_task() -> None:
    """A2-r2-75 (outbox half): the reset must clear modulith.builtin.outbox's
    module state and cancel its retry task, so one test's durable-path
    configuration can't bleed into the next 'fresh' runtime."""
    from modulith.builtin import outbox

    outbox.configure(StubStore(), JsonEventSerializer(), start_loop=True)
    task = outbox._retry_task
    assert task is not None and not task.done()

    rt = Runtime()
    rt._reset_for_testing()

    assert outbox._store is None
    assert outbox._retry_task is None
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert task.cancelled() or task.done()
