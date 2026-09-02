"""Behavioral regression tests for the Runtime singleton.

Each test's docstring states the contract it pins.

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

from modulith import (
    ConfigurationError,
    EventPublication,
    configure,
    event,
    externalized,
    hookimpl,
    listener,
)
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
# ensure_bootstrapped() must be genuinely retryable
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
    """A bootstrap that fails AFTER discovery has imported modules
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
    """Making bootstrap atomic
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
    """The early config install that the broker hook
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
# Reentrant runtime use during bootstrap must not deadlock
# ---------------------------------------------------------------------------


def test_configure_and_rebootstrap_during_bootstrap_raise_instead_of_deadlocking(
    make_fake_app,
) -> None:
    """configure() or ensure_bootstrapped() reached from
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
# register_listener vs bootstrap races
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RtEvent:
    n: int


def test_register_listener_cannot_land_in_consumed_pending_list() -> None:
    """A register_listener() racing bootstrap's
    pending-listener flush must never append to the already-flushed list
    (which silently lost the listener forever). With the fix the registration
    blocks on the runtime lock until 'bootstrap' completes, then registers
    directly on the live bus. (A second crash mode — bootstrap iterating the
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
# Unregistered broker schemes must fail loudly + uniformly
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
    """A typo'd/uninstalled default broker scheme in a cross-process
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
    """An @externalized(target=...) scheme with no registered broker
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
# Hook-facing publications must carry the real payload bytes
# ---------------------------------------------------------------------------


async def test_in_memory_dispatch_hook_publications_carry_real_payload() -> None:
    """The EventPublications handed to the lifecycle hooks on the
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


def test_is_builtin_plugin_module() -> None:
    """The builtin-module classifier is exact: it recognizes the package's
    own modules — and nothing that merely borrows its name. The old
    ``startswith("modulith.")`` check let a plugin physically placed under
    a ``modulith.*`` name masquerade as built-in and silently starve it of
    payload bytes; ``modulith`` is a regular package, so only its own
    directories can be real top-level subpackages."""
    from modulith.runtime import _is_builtin_plugin_module

    assert _is_builtin_plugin_module("modulith")
    assert _is_builtin_plugin_module("modulith.runtime")
    assert _is_builtin_plugin_module("modulith.builtin.outbox")
    assert _is_builtin_plugin_module("modulith.adapters.db_broker")
    # External by construction:
    assert not _is_builtin_plugin_module("")
    assert not _is_builtin_plugin_module("builtins")
    assert not _is_builtin_plugin_module("modulith_extras.thing")
    assert not _is_builtin_plugin_module("modulith.plugins.custom")
    assert not _is_builtin_plugin_module("myapp.plugins.audit")


async def test_payload_produced_for_modulith_named_external_hookimpl() -> None:
    """A third-party hookimpl that merely names its module ``modulith.*``
    (never a real top-level subpackage of this regular package) must still
    receive real payload bytes. The old prefix check classified it as a
    built-in, silently handing its dead-letter/audit logic ``b""``."""
    payloads: list[bytes] = []

    class _Spoofed:
        @hookimpl
        def modulith_after_event_published(self, event: Any, publication: Any) -> None:
            payloads.append(publication.payload)

    _Spoofed.modulith_after_event_published.__module__ = "modulith.plugins.custom"

    configure(package="spoofedpayload", auto_discover=False)
    _runtime._extra_plugins.append(_Spoofed())

    @event
    @dataclass(frozen=True)
    class Probe:
        x: int

    from modulith import publish

    await publish(Probe(x=9))

    assert payloads, "hooks never fired"
    assert b'"x":9' in payloads[0], f"got payload {payloads[0]!r}"


# ---------------------------------------------------------------------------
# shutdown() must drain the store's in-flight dispatch tasks
# ---------------------------------------------------------------------------


async def test_shutdown_waits_for_inflight_store_dispatch() -> None:
    """Runtime.shutdown() must drain the active publication store's
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
# _reset_for_testing() must not leak brokers or outbox state
# ---------------------------------------------------------------------------


def test_reset_for_testing_closes_registered_brokers() -> None:
    """Broker half: the reset every test fixture calls must close
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
    """Outbox half: the reset must clear modulith.builtin.outbox's
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


async def test_teardown_test_resources_never_cancels_retry_task_directly() -> None:
    """When a loop is running, _teardown_test_resources must not call
    retry_task.cancel() directly — that is a non-threadsafe mutation on a
    task that may live on another thread's loop (sync.py's daemon loop).
    outbox._reset_for_testing(), which always runs next, already performs the
    same cancellation through the threadsafe outbox._cancel_retry_task()."""
    from modulith.builtin import outbox

    class _FakeForeignLoop:
        def __init__(self) -> None:
            self.threadsafe_calls: list[Any] = []

        def call_soon_threadsafe(self, callback: Any) -> None:
            self.threadsafe_calls.append(callback)

    class _FakeForeignTask:
        def __init__(self, loop: _FakeForeignLoop) -> None:
            self._loop = loop
            self.direct_cancel_calls = 0

        def done(self) -> bool:
            return False

        def cancel(self) -> bool:
            self.direct_cancel_calls += 1
            return True

        def get_loop(self) -> _FakeForeignLoop:
            return self._loop

    fake_loop = _FakeForeignLoop()
    fake_task = _FakeForeignTask(fake_loop)
    outbox._retry_task = fake_task  # type: ignore[assignment]

    rt = Runtime()
    try:
        rt._teardown_test_resources()
    finally:
        outbox._reset_for_testing()

    assert fake_task.direct_cancel_calls == 0
    assert fake_loop.threadsafe_calls == [fake_task.cancel]


# ---------------------------------------------------------------------------
# A failed bootstrap must close provisional brokers
# ---------------------------------------------------------------------------


def test_failed_bootstrap_closes_provisional_brokers(make_fake_app) -> None:
    """bootstrap-leak: brokers registered at step 4.5 (modulith_register_brokers)
    are provisional until bootstrap commits — a LATER step failing (discovery,
    manifest verification, modulith_after_module_load) must not leave those
    already-registered brokers' connections open. The old bootstrap only
    rolled back ``self._config``, never closing the local ``broker_registry``
    it had already built and populated."""

    class RegisterThenFail:
        def __init__(self, broker: FakeBroker) -> None:
            self._broker = broker

        @hookimpl
        def modulith_register_brokers(self, registry: Any) -> None:
            registry.register("provisional", self._broker)

        @hookimpl
        def modulith_after_module_load(self, module: Any) -> None:
            raise RuntimeError("boom after brokers were registered")

    make_fake_app({"orders": ""})
    broker = FakeBroker()
    configure(package="fakeapp")
    _runtime._extra_plugins.append(RegisterThenFail(broker))

    with pytest.raises(RuntimeError, match="boom after brokers were registered"):
        _runtime.ensure_bootstrapped()

    assert _runtime._bootstrapped is False
    assert broker.closed is True, "provisionally-registered broker leaked past the failed bootstrap"


# ---------------------------------------------------------------------------
# shutdown() processes local/broker failures
# independently, raising an ExceptionGroup only when BOTH fail
# ---------------------------------------------------------------------------


class _FailingWaitStore(StubStore):
    """A store whose wait_for_dispatch() fails — the 'local' shutdown step."""

    async def wait_for_dispatch(self) -> None:
        raise ConnectionError("store connection dropped during drain")


class _FailingCloseRegistry(BrokerRegistry):
    """A broker registry whose close_all() fails — the 'broker' shutdown step."""

    async def close_all(self) -> None:
        raise RuntimeError("broker transport refused to close")


async def test_shutdown_closes_brokers_even_when_local_drain_fails() -> None:
    """simultaneous-error (local half): a failing store.wait_for_dispatch()
    must not skip broker close — local and broker cleanup are independent
    steps, mirroring the same guarantee _worker.py's lifespan teardown owes
    consumer.stop() vs runtime.shutdown()."""
    from modulith.builtin import outbox

    outbox.configure(_FailingWaitStore(), JsonEventSerializer(), start_loop=False)
    registry = BrokerRegistry()
    fake = FakeBroker()
    registry.register("fake", fake)
    _runtime._broker_registry = registry
    try:
        with pytest.raises(ConnectionError, match="store connection dropped"):
            await _runtime.shutdown()
    finally:
        outbox._reset_for_testing()

    assert fake.closed is True, "broker close was skipped because the local drain failed"


async def test_shutdown_raises_exception_group_when_local_and_broker_both_fail() -> None:
    """simultaneous-error (both halves): when BOTH the local drain and the
    broker close fail, shutdown() must surface both errors — not silently
    drop one in favor of the other."""
    from modulith.builtin import outbox

    outbox.configure(_FailingWaitStore(), JsonEventSerializer(), start_loop=False)
    _runtime._broker_registry = _FailingCloseRegistry()
    try:
        with pytest.raises(ExceptionGroup) as excinfo:
            await _runtime.shutdown()
    finally:
        outbox._reset_for_testing()

    causes = {type(exc) for exc in excinfo.value.exceptions}
    assert causes == {ConnectionError, RuntimeError}


# ---------------------------------------------------------------------------
# Bootstrap's pending-listener queue vs. the hooks that fire after the flush
# ---------------------------------------------------------------------------


async def test_listener_registered_from_after_module_load_hook_survives(make_fake_app) -> None:
    """A plugin registering a listener from modulith_after_module_load — the
    documented "react to module load completion" hook — runs on the bootstrap
    thread AFTER the pending-listener flush. Its registration therefore lands
    in the pending list the commit point clears one statement later, and the
    listener was destroyed with no error, no warning and no log line. The
    commit point must drain that tail before clearing it."""

    @dataclass(frozen=True)
    class LateEvent:
        n: int

    received: list[int] = []

    async def late_handler(evt: LateEvent) -> None:
        received.append(evt.n)

    class LateRegistrar:
        @hookimpl
        def modulith_after_module_load(self, module: Any) -> None:
            _runtime.register_listener(LateEvent, late_handler)

    make_fake_app({"orders": ""})
    configure(package="fakeapp")
    _runtime._extra_plugins.append(LateRegistrar())
    _runtime.ensure_bootstrapped()

    assert _runtime.event_bus is not None
    # Exactly one registration — the tail drain must not re-flush the whole
    # queue, since the bus appends without dedupe.
    assert _runtime.event_bus.listeners_for(LateEvent) == [late_handler]

    from modulith import publish

    await publish(LateEvent(n=3))
    assert received == [3]


# ---------------------------------------------------------------------------
# Fan-out publish: a failing broker route must not swallow listener diagnostics
# ---------------------------------------------------------------------------


async def test_broker_route_failure_does_not_swallow_listener_failure(caplog) -> None:
    """The fan-out broker route is awaited between the listener gather and the
    per-listener error logging, and it re-raises. A broker outage therefore
    dropped the listener results on the floor — never logged, never raised —
    so the operator had no record that local processing failed too. The
    diagnostics must land before the route can propagate."""

    class FailingBroker(FakeBroker):
        async def publish(self, target, payload, headers=None):
            raise ConnectionError("redis unreachable")

    @event
    @externalized
    @dataclass(frozen=True)
    class FanOut:
        n: int

    configure(package="routefail", auto_discover=False, topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None
    registry.register("testbroker", FailingBroker())

    @listener
    async def on_fanout(evt: FanOut) -> None:
        raise ValueError("bad order total")

    from modulith import publish

    with caplog.at_level(logging.ERROR, logger="modulith"):
        # The fail-loud broker contract still wins the propagation.
        with pytest.raises(ConnectionError, match="redis unreachable"):
            await publish(FanOut(n=1))

    assert any("bad order total" in record.message for record in caplog.records), (
        "the listener failure vanished when the broker route raised"
    )


# ---------------------------------------------------------------------------
# The publish span must be closed on every escape from dispatch
# ---------------------------------------------------------------------------


async def test_cancelled_in_memory_publish_fires_the_publish_error_hook() -> None:
    """Cancellation while the listeners run escapes BETWEEN the paired publish
    hooks: modulith_after_event_published never fires, so without a matching
    modulith_on_publish_error the observability publish span opened in the
    before hook leaks — never ended, therefore never exported, and its child
    dispatch spans point at a parent the backend never receives."""

    class _PublishHooks:
        def __init__(self) -> None:
            self.errors: list[BaseException] = []
            self.after = 0

        @hookimpl
        def modulith_on_publish_error(self, event: Any, exception: BaseException) -> None:
            self.errors.append(exception)

        @hookimpl
        def modulith_after_event_published(self, event: Any, publication: Any) -> None:
            self.after += 1

    @event
    @dataclass(frozen=True)
    class Slow:
        n: int

    hooks = _PublishHooks()
    configure(package="canceltest", auto_discover=False)
    _runtime._extra_plugins.append(hooks)

    started = asyncio.Event()

    @listener
    async def on_slow(evt: Slow) -> None:
        started.set()
        await asyncio.sleep(30)

    from modulith import publish

    task = asyncio.create_task(publish(Slow(n=1)))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert hooks.after == 0, "the after hook fired for a publish that never completed"
    assert [type(exc) for exc in hooks.errors] == [asyncio.CancelledError]


# ---------------------------------------------------------------------------
# The in-memory payload is produced only for hooks that can actually read it
# ---------------------------------------------------------------------------


async def test_in_memory_publish_skips_payload_nobody_can_read(monkeypatch) -> None:
    """The hook-facing payload exists for third-party dead-letter/audit
    hookimpls; not one of modulith's own reads it. A full recursive dataclass
    walk dominated the cost of every in-memory publish, so with only built-in
    plugins registered the bytes must not be produced at all. (Its converse —
    real bytes as soon as an outside hookimpl is registered — is pinned by
    test_in_memory_dispatch_hook_publications_carry_real_payload above.)"""
    serialized: list[Any] = []
    real_serialize = JsonEventSerializer.serialize

    def _counting(self, evt):
        serialized.append(evt)
        return real_serialize(self, evt)

    monkeypatch.setattr(JsonEventSerializer, "serialize", _counting)

    @event
    @dataclass(frozen=True)
    class Quiet:
        x: int

    configure(package="payloadskip", auto_discover=False)

    seen: list[Any] = []

    @listener
    async def on_quiet(evt: Quiet) -> None:
        seen.append(evt)

    from modulith import publish

    await publish(Quiet(x=1))

    assert len(seen) == 1
    assert serialized == [], "serialized a payload no registered hookimpl can read"


# ---------------------------------------------------------------------------
# strict_boundaries — the dev warn-only escape hatch stops at production
# ---------------------------------------------------------------------------


def test_dev_warn_only_cannot_disarm_strict_boundaries_in_production(
    make_fake_app, monkeypatch
) -> None:
    """MODULITH_DEV_WARN_ONLY is an internal signal single-process `modulith
    dev` sets for itself, undocumented and with no other legitimate producer.
    `modulith dev` never runs in production, so a value inherited from a
    container image, a CI job or a copied shell profile must not turn the
    strict_boundaries gate into a log line there."""
    monkeypatch.setenv("MODULITH_DEV_WARN_ONLY", "1")
    make_fake_app(
        {
            "orders": "from fakeapp.inventory import _internal",
            "inventory": "",
        },
        extra_files={"inventory/_internal/__init__.py": "# private submodule\n"},
    )
    # outbox="memory" is the explicit production opt-in; without it config
    # validation raises first and never reaches the boundary gate.
    configure(package="fakeapp", strict_boundaries=True, production=True, outbox="memory")

    with pytest.raises(ConfigurationError, match="boundary violations detected"):
        _runtime.ensure_bootstrapped()
