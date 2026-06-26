"""Cross-process event routing (process-per-module topology).

In single-process topology, ``publish()`` dispatches in-memory. In
``topology="processes"``, an event with NO local listener is a cross-module
event: the runtime serializes it and routes it to the configured broker (so a
worker in another process can consume it). An event WITH a local listener stays
in-process. These tests drive that decision with a fake broker registered into
the runtime's broker registry — no real broker required.
"""

from __future__ import annotations

from modulith import configure
from modulith.runtime import _runtime


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
