"""Cross-process event routing (process-per-module topology).

In single-process topology, ``publish()`` dispatches in-memory. In
``topology="processes"``, an event with NO local listener is a cross-module
event: the runtime serializes it and routes it to the configured broker (so a
worker in another process can consume it). An event WITH a local listener stays
in-process. These tests drive that decision with a fake broker registered into
the runtime's broker registry — no real broker required.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from uuid import UUID

import pytest

from modulith import EventPublication, configure
from modulith._consumer import consumer_targets
from modulith.builtin import outbox
from modulith.runtime import _PUBLISHER_MODULE_HEADER, _runtime
from modulith.serializers import JsonEventSerializer
from tests.test_broker_consumer import FakePollingBroker, _make_polling_consumer
from tests.test_consumer import FakeConsumerBroker
from tests.test_consumer import _make_consumer as _make_redis_consumer


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


class Store:
    """Minimal PublicationStore used to activate the transactional path."""

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


class Session:
    """Only the outbox context binding matters for this runtime test."""

    def __init__(self) -> None:
        self.info: dict[str, object] = {}


def _register_fake_broker(scheme: str) -> FakeBroker:
    fake = FakeBroker()
    registry = _runtime.broker_registry
    assert registry is not None  # bootstrapped
    registry.register(scheme, fake)
    return fake


async def test_event_without_local_listener_routes_to_broker(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-1")

    assert len(fake.published) == 1
    # The registry strips the scheme (brokers.py: scheme:destination) and hands
    # the broker the destination — the FULLY-QUALIFIED event name (module +
    # qualname), matching the event_type header. Bare qualname would collide
    # across modules that share a class name (e.g. orders.Created vs
    # billing.Created → one stream).
    destination, payload, headers = fake.published[0]
    assert destination == "fakeapp.orders.OrderPlaced"
    assert b"o-1" in payload
    # The fully-qualified event type rides in headers so a consumer in another
    # process can resolve the class to deserialize.
    assert headers is not None
    assert headers["event_type"] == "fakeapp.orders.OrderPlaced"


async def test_transactional_event_without_local_listener_still_routes_to_broker(
    make_fake_app,
) -> None:
    """The durable local-listener outbox must not swallow remote-only events.

    This test used to assert the broker received
    the payload synchronously inside publish() — i.e. BEFORE the business
    transaction committed, which a rollback could not un-send. The fixed
    contract is commit-gated: publish() persists a broker-route publication
    row in the bound session (atomic with the business work) and the
    after-commit dispatch delivers it to the broker.
    """
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    store = Store()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    token = outbox._current_session.set(Session())
    try:
        await orders.place("o-tx")
    finally:
        outbox._current_session.reset(token)

    # Commit-gated: nothing on the wire at publish() time — a rollback could
    # not un-send a broker message, so the send must wait for the commit.
    assert fake.published == []
    # No local listeners, so the only row is the deferred broker route,
    # enlisted in the bound session (atomic with the business transaction).
    assert len(store.saved) == 1
    route = store.saved[0]
    assert route.listener is not None
    assert route.listener.startswith(outbox._BROKER_ROUTE_LISTENER_PREFIX)

    # After-commit dispatch (driven by the adapter's after_commit hook).
    await outbox._dispatch_publication(route)

    assert len(fake.published) == 1
    destination, payload, headers = fake.published[0]
    assert destination == "fakeapp.orders.OrderPlaced"
    assert b"o-tx" in payload
    assert headers is not None
    assert headers["event_type"] == "fakeapp.orders.OrderPlaced"


async def test_event_with_local_listener_stays_in_process(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                seen = []

                @listener
                async def on_placed(evt: OrderPlaced) -> None:
                    seen.append(evt)

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-2")

    assert len(orders.seen) == 1  # dispatched locally
    assert fake.published == []  # NOT routed to broker


async def test_single_topology_never_routes_to_broker(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="single", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-3")  # no local listener, but single topology

    assert fake.published == []


# ---------------------------------------------------------------------------
# @externalized + dynamic routing (resolve_event_target hook)
# ---------------------------------------------------------------------------


async def test_externalized_event_with_local_listener_also_routes_to_broker(make_fake_app) -> None:
    """An @externalized event fans out: dispatched locally AND sent to the broker.

    Regression (#20/#22): routing used to fire only when there was NO local
    listener, so a fan-out event consumed both locally and cross-process
    silently lost its remote deliveries. @externalized now forces broker
    routing independent of local-listener presence.
    """
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, externalized, listener, publish

                @externalized
                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                seen = []

                @listener
                async def on_placed(evt: OrderPlaced) -> None:
                    seen.append(evt)

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-9")

    assert len(orders.seen) == 1  # dispatched locally
    assert len(fake.published) == 1  # AND routed to the broker (fan-out)
    destination, _, _ = fake.published[0]
    assert destination == "fakeapp.orders.OrderPlaced"


_EXTERNALIZED_ORDERS = """
    from dataclasses import dataclass
    from modulith import event, externalized, listener, publish

    @externalized
    @event
    @dataclass(frozen=True)
    class OrderPlaced:
        order_id: str

    @listener
    async def on_placed(evt: OrderPlaced) -> None:
        pass

    async def place(order_id: str) -> None:
        await publish(OrderPlaced(order_id=order_id))
"""


async def test_direct_broker_send_names_the_publishing_module_in_a_header(make_fake_app) -> None:
    make_fake_app({"orders": _EXTERNALIZED_ORDERS})
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module("fakeapp.orders")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-1")

    assert [headers for _, _, headers in fake.published] == [
        {"event_type": "fakeapp.orders.OrderPlaced", _PUBLISHER_MODULE_HEADER: "fakeapp.orders"}
    ]


async def test_direct_broker_send_outside_any_module_carries_no_publisher_header(
    make_fake_app,
) -> None:
    make_fake_app({"orders": _EXTERNALIZED_ORDERS})
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-1")

    assert [headers for _, _, headers in fake.published] == [
        {"event_type": "fakeapp.orders.OrderPlaced"}
    ]


async def _publish_in_transaction(orders, store: Store) -> EventPublication:
    """Publish inside a bound session; return the broker-route row."""
    token = outbox._current_session.set(Session())
    try:
        await orders.place("o-tx")
    finally:
        outbox._current_session.reset(token)
    (route,) = [
        p
        for p in store.saved
        if (p.listener or "").startswith(outbox._BROKER_ROUTE_LISTENER_PREFIX)
    ]
    return route


async def test_outbox_broker_route_names_the_publishing_module_in_a_header(make_fake_app) -> None:
    make_fake_app({"orders": _EXTERNALIZED_ORDERS})
    store = Store()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module("fakeapp.orders")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    route = await _publish_in_transaction(orders, store)
    await outbox._dispatch_publication(route)

    assert [headers for _, _, headers in fake.published] == [
        {
            "event_type": "fakeapp.orders.OrderPlaced",
            "publication_id": str(route.id),
            _PUBLISHER_MODULE_HEADER: "fakeapp.orders",
        }
    ]


async def test_outbox_broker_route_keeps_the_publisher_when_a_sibling_worker_sends_it(
    make_fake_app,
) -> None:
    """Any worker may send a broker-route row, so the stamp lives in the row."""
    make_fake_app({"orders": _EXTERNALIZED_ORDERS})
    store = Store()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module("fakeapp.orders")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    route = await _publish_in_transaction(orders, store)
    _runtime.host_module("fakeapp.billing")
    await outbox._dispatch_publication(route)

    (_, _, headers) = fake.published[0]
    assert headers is not None
    assert headers[_PUBLISHER_MODULE_HEADER] == "fakeapp.orders"


async def test_outbox_broker_route_outside_any_module_carries_no_publisher_header(
    make_fake_app,
) -> None:
    make_fake_app({"orders": _EXTERNALIZED_ORDERS})
    store = Store()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    route = await _publish_in_transaction(orders, store)
    await outbox._dispatch_publication(route)

    assert [headers for _, _, headers in fake.published] == [
        {"event_type": "fakeapp.orders.OrderPlaced", "publication_id": str(route.id)}
    ]


_TWO_LISTENER_APP = {
    "orders": """
        from dataclasses import dataclass
        from modulith import event, externalized, listener, publish

        @externalized
        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        seen = []

        @listener
        async def on_placed(evt: OrderPlaced) -> None:
            seen.append(evt.order_id)

        async def place(order_id: str) -> None:
            await publish(OrderPlaced(order_id=order_id))
    """,
    "billing": """
        from modulith import listener
        from fakeapp.orders import OrderPlaced

        seen = []

        @listener
        async def bill(evt: OrderPlaced) -> None:
            seen.append(evt.order_id)
    """,
}

_DELIVERY_PATHS = ["redis", "polling-json-headers", "polling-dict-headers"]


async def _deliver(path: str, target: str, payload: bytes, headers: dict[str, str]) -> None:
    """Hand one broker message to the consumer that serves ``path``."""
    bus = _runtime.event_bus
    assert bus is not None  # bootstrapped
    if path == "redis":
        redis_fields = {b"data": payload}
        redis_fields.update({f"h:{k}".encode(): v.encode() for k, v in headers.items()})
        redis_consumer = _make_redis_consumer(FakeConsumerBroker(), bus, targets=[target])
        await redis_consumer._dispatch_one(target, b"1-0", redis_fields)
        return
    polling_consumer = _make_polling_consumer(
        FakePollingBroker(), poll_interval_s=1.0, max_attempts=3, idle_wait=asyncio.sleep, bus=bus
    )
    await polling_consumer._dispatch_one(
        {
            "id": "row-1",
            "target": target,
            "event_type": headers["event_type"],
            "payload": payload,
            "headers": json.dumps(headers) if path == "polling-json-headers" else dict(headers),
        }
    )


@pytest.mark.parametrize("path", _DELIVERY_PATHS)
async def test_externalized_event_does_not_rerun_the_publishers_own_listener(
    make_fake_app, path: str
) -> None:
    """The publisher's worker ran its own listener at publish; its consumer must not repeat it."""
    make_fake_app(_TWO_LISTENER_APP)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module("fakeapp.orders")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-1")
    assert orders.seen == ["o-1"]
    ((target, payload, headers),) = fake.published
    assert headers is not None

    await _deliver(path, target, payload, headers)

    assert orders.seen == ["o-1"]


@pytest.mark.parametrize("path", _DELIVERY_PATHS)
async def test_delivery_without_a_publisher_header_runs_every_listener(
    make_fake_app, path: str
) -> None:
    """An unhosted publisher or a message sent before the header existed names no module."""
    make_fake_app(_TWO_LISTENER_APP)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module("fakeapp.orders")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-1")
    ((target, payload, headers),) = fake.published
    assert headers is not None
    unstamped = {k: v for k, v in headers.items() if k != _PUBLISHER_MODULE_HEADER}

    await _deliver(path, target, payload, unstamped)

    assert orders.seen == ["o-1", "o-1"]


@pytest.mark.parametrize("path", _DELIVERY_PATHS)
async def test_sibling_replica_of_the_publisher_module_skips_its_own_listeners(
    make_fake_app, path: str
) -> None:
    """The header names the module, so a replica that never published skips too."""
    make_fake_app(_TWO_LISTENER_APP)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module("fakeapp.orders")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-1")
    ((target, payload, headers),) = fake.published
    assert headers is not None
    orders.seen.clear()  # a replica's process has no record of the publisher's run

    await _deliver(path, target, payload, headers)

    assert orders.seen == []


@pytest.mark.parametrize("path", _DELIVERY_PATHS)
async def test_listeners_of_other_modules_still_run_from_the_broker_copy(
    make_fake_app, path: str
) -> None:
    """Only the publishing module's listeners are skipped; a process owning both runs the rest."""
    make_fake_app(_TWO_LISTENER_APP)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.billing as billing
    import fakeapp.orders as orders

    _runtime.host_module("fakeapp.orders")
    await orders.place("o-1")
    ((target, payload, headers),) = fake.published
    assert headers is not None
    orders.seen.clear()
    _runtime._hosted_module = None  # a consumer process owning both modules' listeners

    await _deliver(path, target, payload, headers)

    assert (orders.seen, billing.seen) == ([], ["o-1"])


async def test_externalized_explicit_target_overrides_default(make_fake_app) -> None:
    """@externalized(target="scheme:dest") routes to that exact destination (#22)."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, externalized, publish

                @externalized(target="testbroker:custom.stream")
                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-10")

    assert len(fake.published) == 1
    destination, _, _ = fake.published[0]
    assert destination == "custom.stream"  # registry strips the "testbroker:" scheme


@pytest.mark.parametrize("subscription_source", ["manifest", "config", "listener"])
async def test_static_event_target_is_consumed_under_every_subscription_source(
    make_fake_app,
    subscription_source: str,
) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, externalized, listener, publish

                @externalized(target="testbroker:custom.stream")
                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                @listener
                async def on_placed(event: OrderPlaced) -> None:
                    pass

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(
        package="fakeapp",
        topology="processes",
        broker="testbroker",
        subscription_source=subscription_source,
    )
    _runtime.ensure_bootstrapped()
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-static")

    assert _runtime.event_bus is not None
    assert _runtime.config is not None
    assert consumer_targets(_runtime.event_bus, _runtime.config, "orders") == ["custom.stream"]
    assert [destination for destination, _, _ in fake.published] == ["custom.stream"]


async def test_resolve_event_target_hook_overrides_routing(make_fake_app) -> None:
    """A modulith_resolve_event_target plugin wins over the default target (#3/#5).

    The event is neither @externalized nor listener-less here, yet the dynamic
    hook routes it — proving the hook is consulted and takes priority.
    """
    from modulith import hookimpl

    class _Router:
        @hookimpl
        def modulith_resolve_event_target(self, event) -> str:
            return "testbroker:dynamic.dest"

    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime._plugin_manager.register(_Router(), name="dynamic-router")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-11")

    assert len(fake.published) == 1
    destination, _, _ = fake.published[0]
    assert destination == "dynamic.dest"


async def test_whitespace_padded_hook_target_routes_to_normalized_destination(
    make_fake_app,
) -> None:
    from modulith import hookimpl

    class _Router:
        @hookimpl
        def modulith_resolve_event_target(self, event) -> str:
            return " testbroker : dynamic.dest "

    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime._plugin_manager.register(_Router(), name="padded-router")
    fake = _register_fake_broker("testbroker")

    import fakeapp.orders as orders

    await orders.place("o-12")

    assert [destination for destination, _, _ in fake.published] == ["dynamic.dest"]


# ---------------------------------------------------------------------------
# Broker publish failure — producer-side fake that CAN fail
# ---------------------------------------------------------------------------


class RaisingBroker:
    """Broker whose publish() always fails — e.g. Redis unreachable. Every
    other producer-side fake in this file unconditionally succeeds, so
    without this one the failure path of cross-process routing has no
    coverage."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:
        raise self._exc

    async def close(self) -> None:  # pragma: no cover - registry contract
        pass


async def test_broker_publish_failure_propagates_to_publisher(make_fake_app) -> None:
    """Pins the CURRENT (de-facto) contract: on the direct (non-durable)
    path, a broker publish failure propagates uncaught out of ``publish()``
    into the caller's own business logic — runtime._maybe_route_to_broker
    wraps ``registry.publish()`` in nothing (runtime.py:451). Whether that is
    the *intended* contract (vs. a modulith-specific BrokerPublishError or
    graceful degradation) is an OPEN design decision; this
    test makes the behavior visible so a deliberate change shows up here.
    """
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None  # bootstrapped
    registry.register("testbroker", RaisingBroker(ConnectionError("redis unreachable")))

    import fakeapp.orders as orders

    with pytest.raises(ConnectionError, match="redis unreachable"):
        await orders.place("o-boom")  # the caller's business logic sees the failure
