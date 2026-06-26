"""Smoke tests for the plugin contract and dispatch registry.

These verify the core mechanics work end-to-end:

  1. Plugins register and their hooks are invoked.
  2. The broker registry routes by URI scheme correctly.
  3. The plugin manager handles missing entry points gracefully.

Run with: pytest tests/
"""

from __future__ import annotations

import asyncio

import pytest

from modulith import (
    BrokerRegistry,
    DuplicateBrokerError,
    UnknownBrokerError,
    create_plugin_manager,
    hookimpl,
)

# ---------------------------------------------------------------------------
# Plugin manager tests
# ---------------------------------------------------------------------------


class _RecordingPlugin:
    """Test plugin that captures hook invocations for assertion."""

    def __init__(self) -> None:
        self.brokers_called_with: list[BrokerRegistry] = []

    @hookimpl
    def modulith_register_brokers(self, registry: BrokerRegistry) -> None:
        self.brokers_called_with.append(registry)


def test_extra_plugin_is_registered_and_invoked() -> None:
    """A plugin passed via extra_plugins receives hook calls."""
    plugin = _RecordingPlugin()
    pm = create_plugin_manager(
        extra_plugins=[plugin],
        load_builtins=False,
        load_entrypoints=False,
    )

    registry = BrokerRegistry()
    pm.hook.modulith_register_brokers(registry=registry)

    assert plugin.brokers_called_with == [registry]


def test_plugin_manager_handles_missing_entry_points() -> None:
    """Discovery succeeds even with no entry points installed."""
    # Real test environment has no "modulith" entry points registered,
    # so this exercises the empty-discovery code path.
    pm = create_plugin_manager(
        load_builtins=False,
        load_entrypoints=True,
    )
    # No assertions needed — the test is that this doesn't raise.
    assert pm is not None


# ---------------------------------------------------------------------------
# Broker registry tests
# ---------------------------------------------------------------------------


class _StubBroker:
    """Minimal Broker-conforming object for dispatch testing."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, dict | None]] = []

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.calls.append((target, payload, headers))

    async def close(self) -> None:
        pass


def test_registry_dispatches_to_correct_broker_by_scheme() -> None:
    """Each scheme routes to its own registered broker."""
    registry = BrokerRegistry()
    kafka = _StubBroker()
    sqs = _StubBroker()

    registry.register("kafka", kafka)
    registry.register("sqs", sqs)

    asyncio.run(registry.publish("kafka:orders.created", b"a"))
    asyncio.run(registry.publish("sqs:notifications-queue", b"b"))

    assert kafka.calls == [("orders.created", b"a", None)]
    assert sqs.calls == [("notifications-queue", b"b", None)]


def test_registry_rejects_duplicate_scheme_registration() -> None:
    """Two plugins registering the same scheme raises a clear error."""
    registry = BrokerRegistry()
    registry.register("kafka", _StubBroker())

    with pytest.raises(DuplicateBrokerError):
        registry.register("kafka", _StubBroker())


def test_registry_raises_on_unknown_scheme() -> None:
    """Publishing to an unregistered scheme fails with a helpful error."""
    registry = BrokerRegistry()

    with pytest.raises(UnknownBrokerError) as exc_info:
        asyncio.run(registry.publish("kafka:topic", b"x"))

    # Message should mention the scheme so users can debug typos quickly.
    assert "kafka" in str(exc_info.value)


def test_registry_rejects_target_without_destination() -> None:
    """Targets must include a destination after the scheme."""
    registry = BrokerRegistry()
    registry.register("kafka", _StubBroker())

    with pytest.raises(ValueError):
        asyncio.run(registry.publish("kafka:", b"x"))


def test_registry_destination_can_contain_colons() -> None:
    """Targets split on the FIRST colon — destinations may have more."""
    registry = BrokerRegistry()
    broker = _StubBroker()
    registry.register("amqp", broker)

    # AMQP routing keys often look like "exchange:routing.key" — the
    # first colon is the scheme separator, the rest is destination.
    asyncio.run(registry.publish("amqp:my-exchange:routing.key", b"x"))

    assert broker.calls == [("my-exchange:routing.key", b"x", None)]
