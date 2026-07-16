"""Tests for the pluggable cross-process consumer contract.

The producer side (``Broker`` / ``BrokerRegistry`` / ``modulith_register_brokers``)
has a symmetric consumer side: the ``Consumer`` protocol, the ``ConsumerRegistry``
that maps a URI scheme to a factory, and the ``modulith_register_consumers`` hook
adapters use to register that factory. In process-per-module topology the worker
builds one ``Consumer`` per module from a ``ConsumerSpec`` and only start()/stop()s
it — each adapter owns its own poll/claim loop.

These are the registry/contract unit tests. The Redis adapter's concrete
``BrokerConsumer`` loop is exercised in test_consumer.py and the e2e suites.
"""

from __future__ import annotations

from typing import Any

import pytest

from modulith import (
    BrokerRegistry,
    Consumer,
    ConsumerRegistry,
    ConsumerSpec,
    DuplicateConsumerError,
    UnknownConsumerError,
)


class _FakeConsumer:
    """Minimal object satisfying the Consumer protocol (async start/stop)."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


def _spec(scheme: str, broker_registry: BrokerRegistry | None = None) -> ConsumerSpec:
    return ConsumerSpec(
        scheme=scheme,
        module_name="orders",
        group="modulith-orders",
        consumer_name="orders:123",
        targets=("fakeapp.orders.OrderCreated",),
        bus=object(),
        serializer=object(),
        broker_registry=broker_registry or BrokerRegistry(),
    )


# ---------------------------------------------------------------------------
# ConsumerRegistry
# ---------------------------------------------------------------------------


def test_register_and_build_invokes_factory_with_spec() -> None:
    registry = ConsumerRegistry()
    seen: list[ConsumerSpec] = []

    def factory(spec: ConsumerSpec) -> Consumer:
        seen.append(spec)
        return _FakeConsumer()

    registry.register("database", factory)
    spec = _spec("database")
    consumer = registry.build("database", spec)

    assert isinstance(consumer, _FakeConsumer)
    assert seen == [spec]  # the exact spec object was threaded through


def test_duplicate_registration_raises() -> None:
    registry = ConsumerRegistry()
    registry.register("database", lambda spec: _FakeConsumer())
    with pytest.raises(DuplicateConsumerError, match="already has a consumer factory"):
        registry.register("database", lambda spec: _FakeConsumer())


def test_unknown_scheme_raises_keyerror_subclass() -> None:
    registry = ConsumerRegistry()
    with pytest.raises(UnknownConsumerError, match="no consumer registered for scheme"):
        registry.get("nope")
    # Subclass of KeyError so existing except-KeyError handlers still catch it.
    assert issubclass(UnknownConsumerError, KeyError)


def test_build_unknown_scheme_raises() -> None:
    registry = ConsumerRegistry()
    with pytest.raises(UnknownConsumerError):
        registry.build("nope", _spec("nope"))


def test_build_rejects_spec_for_a_different_scheme() -> None:
    registry = ConsumerRegistry()
    seen: list[ConsumerSpec] = []

    def factory(spec: ConsumerSpec) -> Consumer:
        seen.append(spec)
        return _FakeConsumer()

    registry.register("database", factory)

    with pytest.raises(ValueError, match="does not match"):
        registry.build("database", _spec("redis-streams"))

    assert seen == []


def test_unregister_removes_and_is_noop_when_absent() -> None:
    registry = ConsumerRegistry()
    registry.register("database", lambda spec: _FakeConsumer())
    registry.unregister("database")
    assert "database" not in registry.schemes()
    # No-op, does not raise.
    registry.unregister("database")


def test_schemes_returns_sorted() -> None:
    registry = ConsumerRegistry()
    registry.register("redis-streams", lambda spec: _FakeConsumer())
    registry.register("database", lambda spec: _FakeConsumer())
    assert registry.schemes() == ["database", "redis-streams"]


# ---------------------------------------------------------------------------
# Consumer protocol
# ---------------------------------------------------------------------------


def test_consumer_protocol_is_runtime_checkable() -> None:
    assert isinstance(_FakeConsumer(), Consumer)

    class _NoStop:
        async def start(self) -> None: ...

    # Missing stop() → not a Consumer.
    assert not isinstance(_NoStop(), Consumer)


def test_broker_consumer_satisfies_consumer_protocol() -> None:
    from modulith._consumer import BrokerConsumer

    consumer = BrokerConsumer(
        broker=object(),
        bus=object(),
        serializer=object(),
        consumer_name="orders:1",
        group="modulith-orders",
        targets=["fakeapp.orders.OrderCreated"],
    )
    assert isinstance(consumer, Consumer)


# ---------------------------------------------------------------------------
# Redis adapter's consumer factory registration
# ---------------------------------------------------------------------------


def test_redis_register_consumers_noop_when_broker_not_redis(make_fake_app: Any) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.adapters.redis_broker import modulith_register_consumers
    from modulith.runtime import _runtime

    configure(package="fakeapp", broker="memory")
    _runtime.ensure_bootstrapped()

    registry = ConsumerRegistry()
    modulith_register_consumers(registry=registry)
    assert "redis-streams" not in registry.schemes()


def test_redis_register_consumers_registers_when_configured(
    make_fake_app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.adapters.redis_broker import modulith_register_consumers
    from modulith.runtime import _runtime

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    configure(package="fakeapp", broker="redis-streams")
    _runtime.ensure_bootstrapped()

    registry = ConsumerRegistry()
    modulith_register_consumers(registry=registry)
    assert "redis-streams" in registry.schemes()


def test_runtime_bootstrap_registers_redis_consumer_factory(
    make_fake_app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Normal bootstrap wires the Redis consumer factory through the built-in
    plugin path, not just a direct hook call — process topology relies on it."""
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    configure(package="fakeapp", broker="redis-streams")
    _runtime.ensure_bootstrapped()

    assert _runtime.consumer_registry is not None
    assert "redis-streams" in _runtime.consumer_registry.schemes()


def test_redis_factory_reuses_registered_broker_object() -> None:
    """The consumer factory pulls its backend from the broker registry rather
    than opening a second connection — one backend serves both halves."""
    from modulith._consumer import BrokerConsumer
    from modulith.adapters.redis_broker import _make_redis_consumer

    broker = object()
    broker_registry = BrokerRegistry()
    broker_registry.register("redis-streams", broker)  # type: ignore[arg-type]

    consumer = _make_redis_consumer(_spec("redis-streams", broker_registry))

    assert isinstance(consumer, BrokerConsumer)
    # Same object the broker hook registered — no new client.
    assert consumer._broker is broker
