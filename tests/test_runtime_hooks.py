"""Tests for the runtime's plugin-hook wiring around publish().

The runtime is the integration point that makes the outbox and
observability plugins functional: it must invoke the event-lifecycle
hooks (before/after publish, per-listener dispatch/complete/error) around
in-memory dispatch, and it must create a BrokerRegistry and run the
modulith_register_brokers hook at bootstrap.

These behaviors are storage-agnostic: with no outbox configured, publish
still dispatches in-memory AND fires the observability-shaped hooks.
"""

from __future__ import annotations

import pytest

from modulith import BrokerRegistry, hookimpl
from modulith.runtime import _runtime


class Recorder:
    """A plugin that records every lifecycle hook invocation, in order."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    @hookimpl
    def modulith_before_event_published(self, event) -> None:
        self.calls.append(("before", type(event).__name__))

    @hookimpl
    def modulith_after_event_published(self, event, publication) -> None:
        self.calls.append(("after", type(event).__name__))

    @hookimpl
    def modulith_on_listener_dispatch(self, event, listener_name, publication) -> None:
        self.calls.append(("dispatch", listener_name))

    @hookimpl
    def modulith_on_listener_complete(self, event, listener_name, publication, exception) -> None:
        self.calls.append(("complete", listener_name, exception is not None))

    @hookimpl
    def modulith_on_listener_error(self, event, listener_name, publication, exception) -> None:
        self.calls.append(("error", listener_name, type(exception).__name__))

    @hookimpl
    def modulith_register_brokers(self, registry) -> None:
        self.calls.append(("register_brokers", type(registry).__name__))

    @hookimpl
    def modulith_after_module_load(self, module) -> None:
        self.calls.append(("module_load", module.name))


def _install_recorder(monkeypatch) -> Recorder:
    """Make bootstrap register a Recorder so it sees the whole lifecycle."""
    import modulith.runtime as rt

    recorder = Recorder()
    original = rt.create_plugin_manager

    def patched(**kwargs):
        pm = original(**kwargs)
        pm.register(recorder, name="test-recorder")
        return pm

    monkeypatch.setattr(rt, "create_plugin_manager", patched)
    return recorder


@pytest.mark.asyncio
async def test_publish_fires_lifecycle_hooks_in_order(fake_app, monkeypatch) -> None:
    recorder = _install_recorder(monkeypatch)
    # Pin the package: this test publishes directly (not from inside app
    # code), so call-stack auto-detection would otherwise pick up "tests".
    _runtime.configure(package="fakeapp")

    from fakeapp.orders import OrderCreated  # type: ignore[import-not-found]

    await _runtime.publish(OrderCreated(order_id="o1"))

    names = [c[0] for c in recorder.calls]
    # before precedes dispatch precedes complete precedes after.
    assert names.index("before") < names.index("dispatch")
    assert names.index("dispatch") < names.index("complete")
    assert names.index("complete") < names.index("after")
    assert ("dispatch", "reserve_stock") in recorder.calls
    assert ("complete", "reserve_stock", False) in recorder.calls

    # And the listener actually ran (in-memory dispatch still happened).
    from fakeapp.inventory import received  # type: ignore[import-not-found]

    assert len(received) == 1


@pytest.mark.asyncio
async def test_listener_error_fires_error_and_complete_then_reraises(
    make_fake_app, monkeypatch
) -> None:
    recorder = _install_recorder(monkeypatch)
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class Boom:
                    x: int
            """,
            "handlers": """
                from modulith import listener
                from fakeapp.orders import Boom

                @listener
                async def explode(event: Boom) -> None:
                    raise ValueError("kaboom")
            """,
        }
    )
    _runtime.configure(package="fakeapp")

    from fakeapp.orders import Boom  # type: ignore[import-not-found]

    with pytest.raises(ValueError, match="kaboom"):
        await _runtime.publish(Boom(x=1))

    assert ("error", "explode", "ValueError") in recorder.calls
    # complete still fires on error, carrying the exception flag.
    assert ("complete", "explode", True) in recorder.calls


@pytest.mark.asyncio
async def test_no_listeners_still_fires_before_and_after(make_fake_app, monkeypatch) -> None:
    recorder = _install_recorder(monkeypatch)
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class Lonely:
                    x: int
            """,
        }
    )
    _runtime.configure(package="fakeapp")
    from fakeapp.orders import Lonely  # type: ignore[import-not-found]

    await _runtime.publish(Lonely(x=1))

    names = [c[0] for c in recorder.calls]
    assert "before" in names
    assert "after" in names
    assert "dispatch" not in names  # no listeners


def test_bootstrap_creates_broker_registry_and_calls_register_brokers(
    fake_app, monkeypatch
) -> None:
    recorder = _install_recorder(monkeypatch)

    _runtime.ensure_bootstrapped()

    assert isinstance(_runtime.broker_registry, BrokerRegistry)
    assert ("register_brokers", "BrokerRegistry") in recorder.calls


def test_bootstrap_fires_after_module_load_once_per_module(make_fake_app, monkeypatch) -> None:
    """modulith_after_module_load fires once per discovered module (#6/#16/#19).

    Regression: the hookspec was declared but never invoked, so plugins
    implementing it (startup metrics, module-scoped resources) silently
    never ran.
    """
    recorder = _install_recorder(monkeypatch)
    make_fake_app({"orders": "", "inventory": ""})
    _runtime.configure(package="fakeapp")

    _runtime.ensure_bootstrapped()

    loaded = sorted(c[1] for c in recorder.calls if c[0] == "module_load")
    assert loaded == ["inventory", "orders"]


@pytest.mark.asyncio
async def test_durable_publish_fires_after_event_published(make_fake_app, monkeypatch) -> None:
    """The post-publish hook fires on the durable outbox path too (#4).

    Previously it fired only on in-memory dispatch, so metrics/tracing plugins
    missed every transactional publish — the production path the outbox exists
    for. Here a bound session routes the publish through persist(); the hook
    must still fire.
    """
    from modulith.builtin import outbox
    from modulith.serializers import JsonEventSerializer

    class _StubStore:
        def __init__(self) -> None:
            self.saved: list = []

        async def save(self, publication) -> None:
            self.saved.append(publication)

        async def mark_complete(self, publication_id) -> None: ...
        async def find_incomplete(self, older_than):
            return []

        async def archive(self, publication_id) -> None: ...
        async def delete(self, publication_id) -> None: ...

    recorder = _install_recorder(monkeypatch)
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class Durable:
                    x: int

                @listener
                async def on_durable(evt: Durable) -> None:
                    pass
            """,
        }
    )
    _runtime.configure(package="fakeapp")

    store = _StubStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    try:
        from fakeapp.orders import Durable  # type: ignore[import-not-found]

        session = type("S", (), {"info": {}})()
        token = outbox._current_session.set(session)
        try:
            await _runtime.publish(Durable(x=1))
        finally:
            outbox._current_session.reset(token)
    finally:
        await outbox.shutdown()
        outbox._reset_for_testing()

    # Persisted on the durable path, and the post-publish hook fired.
    assert len(store.saved) == 1
    assert ("after", "Durable") in recorder.calls


async def test_shutdown_closes_registered_brokers(fake_app) -> None:
    """Runtime.shutdown() closes every registered broker (#21).

    BrokerRegistry.close_all() previously had no caller — a redis client's
    connection leaked for the process lifetime.
    """

    class _ClosableBroker:
        def __init__(self) -> None:
            self.closed = False

        async def publish(self, target, payload, headers=None) -> None: ...
        async def close(self) -> None:
            self.closed = True

    _runtime.ensure_bootstrapped()
    assert _runtime.broker_registry is not None
    broker = _ClosableBroker()
    _runtime.broker_registry.register("closable", broker)

    await _runtime.shutdown()

    assert broker.closed is True
