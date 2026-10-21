"""One worker process hosting several modules (``own_process``).

The modules a deployment does not give their own process share one worker.
Their listeners run in-process for that worker's publishes, every event it
publishes still crosses to the broker for the modules in other processes, and
each co-hosted module consumes the broker through its own consumer group, so a
consumer must see only its own module's listeners. These tests drive that with
a fake broker, as ``test_cross_process`` does for one module per worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

import pytest

from modulith import configure, event, listener
from modulith._consumer import consumer_targets
from modulith.builtin import outbox
from modulith.runtime import _PUBLISHER_MODULE_HEADER, _runtime
from modulith.serializers import JsonEventSerializer
from modulith.supervisor import derive_specs_from_config
from tests.test_broker_consumer import FakePollingBroker, _make_polling_consumer
from tests.test_consumer import FakeConsumerBroker
from tests.test_consumer import _make_consumer as _make_redis_consumer
from tests.test_cross_process import Session, Store, _register_fake_broker

_APP = {
    "orders": """
        from dataclasses import dataclass
        from modulith import event, listener, publish

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        @event
        @dataclass(frozen=True)
        class ShipmentBooked:
            order_id: str

        seen = []

        @listener
        async def on_shipped(evt: ShipmentBooked) -> None:
            seen.append(evt.order_id)

        async def place(order_id: str) -> None:
            await publish(OrderPlaced(order_id=order_id))
    """,
    "billing": """
        from modulith import listener
        from fakeapp.orders import OrderPlaced, ShipmentBooked

        seen = []
        shipped = []

        @listener
        async def bill(evt: OrderPlaced) -> None:
            seen.append(evt.order_id)

        @listener
        async def on_shipped(evt: ShipmentBooked) -> None:
            shipped.append(evt.order_id)
    """,
}

_COHOSTED = ("fakeapp.orders", "fakeapp.billing")

_FLAKY_BILLING = """
        from modulith import listener
        from fakeapp.orders import OrderPlaced

        attempts = []

        @listener
        async def bill(evt: OrderPlaced) -> None:
            attempts.append(evt.order_id)
            if len(attempts) == 1:
                raise RuntimeError("payment provider down")
    """


@event
@dataclass(frozen=True)
class AuditTrail:
    entry: str


def _host_orders_and_billing(make_fake_app):
    make_fake_app(_APP)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module(*_COHOSTED)
    import fakeapp.billing as billing
    import fakeapp.orders as orders

    return orders, billing


async def test_an_event_a_cohosted_module_listens_to_still_crosses_to_the_broker(
    make_fake_app,
) -> None:
    """One module per worker keeps such an event in-process; a shared worker
    cannot tell which of its modules published, so a module elsewhere would miss it."""
    orders, billing = _host_orders_and_billing(make_fake_app)
    fake = _register_fake_broker("testbroker")

    await orders.place("o-1")

    assert billing.seen == ["o-1"]
    assert [headers for _, _, headers in fake.published] == [
        {
            "event_type": "fakeapp.orders.OrderPlaced",
            _PUBLISHER_MODULE_HEADER: "fakeapp.orders,fakeapp.billing",
        }
    ]


async def test_transactional_route_names_every_cohosted_module(make_fake_app) -> None:
    make_fake_app(_APP)
    store = Store()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module(*_COHOSTED)
    _register_fake_broker("testbroker")
    import fakeapp.billing  # noqa: F401
    import fakeapp.orders as orders

    token = outbox._current_session.set(Session())
    try:
        await orders.place("o-tx")
    finally:
        outbox._current_session.reset(token)

    route = next(
        p
        for p in store.saved
        if (p.listener or "").startswith(outbox._BROKER_ROUTE_LISTENER_PREFIX)
    )
    assert route.trace_context is not None
    assert route.trace_context[_PUBLISHER_MODULE_HEADER] == "fakeapp.orders,fakeapp.billing"
    listener_rows = [p.listener for p in store.saved if p is not route]
    assert listener_rows == ["fakeapp.billing.bill"]


_LOCAL_ONLY_ORDERS = """
        from dataclasses import dataclass
        from modulith import event, listener, publish

        @event
        @dataclass(frozen=True)
        class Recalculated:
            handle: object

        seen = []

        @listener
        async def on_recalculated(evt: Recalculated) -> None:
            seen.append(evt.handle)

        async def recalculate(handle: object) -> None:
            await publish(Recalculated(handle=handle))
    """

_IDLE_BILLING = """
        seen = []
    """


class _OpaqueSerializer:
    """A storage serializer that stores anything, unlike the JSON wire format."""

    def serialize(self, event: object) -> bytes:
        return b"opaque"

    def deserialize(self, data: bytes, event_type: str) -> object:
        raise NotImplementedError


async def test_an_event_the_wire_cannot_carry_stays_with_its_cohosted_listeners(
    make_fake_app, caplog: pytest.LogCaptureFixture
) -> None:
    """One module per worker never routes an event its own listener handles,
    so such an event needs no JSON form; a shared worker routes it only when it has one."""
    make_fake_app({"orders": _LOCAL_ONLY_ORDERS, "billing": _IDLE_BILLING})
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module(*_COHOSTED)
    fake = _register_fake_broker("testbroker")
    import fakeapp.billing  # noqa: F401
    import fakeapp.orders as orders

    handle = object()
    with caplog.at_level(logging.WARNING, logger="modulith"):
        await orders.recalculate(handle)
        await orders.recalculate(handle)

    assert orders.seen == [handle, handle]
    assert fake.published == []
    assert caplog.text.count("fakeapp.orders.Recalculated cannot be sent") == 1


async def test_a_transactional_event_the_wire_cannot_carry_stays_with_its_cohosted_listeners(
    make_fake_app,
) -> None:
    make_fake_app({"orders": _LOCAL_ONLY_ORDERS, "billing": _IDLE_BILLING})
    store = Store()
    outbox.configure(store, _OpaqueSerializer(), start_loop=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module(*_COHOSTED)
    _register_fake_broker("testbroker")
    import fakeapp.billing  # noqa: F401
    import fakeapp.orders as orders

    token = outbox._current_session.set(Session())
    try:
        await orders.recalculate(object())
    finally:
        outbox._current_session.reset(token)

    assert [p.listener for p in store.saved] == ["fakeapp.orders.on_recalculated"]


async def test_an_externalized_event_the_wire_cannot_carry_still_fails_its_cohosted_publish(
    make_fake_app,
) -> None:
    orders_module = """
        from dataclasses import dataclass
        from modulith import event, externalized, listener, publish

        @externalized
        @event
        @dataclass(frozen=True)
        class Recalculated:
            handle: object

        @listener
        async def on_recalculated(evt: Recalculated) -> None:
            pass

        async def recalculate(handle: object) -> None:
            await publish(Recalculated(handle=handle))
    """
    make_fake_app({"orders": orders_module, "billing": _IDLE_BILLING})
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module(*_COHOSTED)
    _register_fake_broker("testbroker")
    import fakeapp.billing  # noqa: F401
    import fakeapp.orders as orders

    with pytest.raises(TypeError, match="cannot JSON-serialize"):
        await orders.recalculate(object())


@pytest.mark.parametrize("path", ["redis", "polling"])
async def test_the_broker_copy_of_its_own_publish_reruns_no_cohosted_listener(
    make_fake_app, path: str
) -> None:
    orders, billing = _host_orders_and_billing(make_fake_app)
    fake = _register_fake_broker("testbroker")
    await orders.place("o-1")
    ((target, payload, headers),) = fake.published
    assert headers is not None

    await _deliver(path, "fakeapp.billing", target, payload, headers)

    assert billing.seen == ["o-1"]


@pytest.mark.parametrize("path", ["redis", "polling"])
async def test_a_cohosted_listener_that_fails_at_publish_is_retried_from_the_broker_copy(
    make_fake_app, path: str
) -> None:
    """Without a transaction the publish runs a co-hosted module's listener
    in-process and raises its failure, as one process would. The broker copy
    then leaves that module out of its header, so the module's own consumer
    retries it, as it would were the module in a worker of its own."""
    make_fake_app({**_APP, "billing": _FLAKY_BILLING})
    configure(package="fakeapp", topology="processes", broker="testbroker")
    _runtime.ensure_bootstrapped()
    _runtime.host_module(*_COHOSTED)
    import fakeapp.billing as billing
    import fakeapp.orders as orders

    fake = _register_fake_broker("testbroker")

    with pytest.raises(RuntimeError, match="payment provider down"):
        await orders.place("o-1")
    ((target, payload, headers),) = fake.published
    assert headers is not None
    assert headers[_PUBLISHER_MODULE_HEADER] == "fakeapp.orders"

    await _deliver(path, "fakeapp.billing", target, payload, headers)

    assert billing.attempts == ["o-1", "o-1"]


def test_only_a_shared_worker_leaves_a_failed_module_out_of_the_header() -> None:
    _runtime.host_module("fakeapp.orders")
    assert _runtime.publisher_modules({"fakeapp.orders"}) == "fakeapp.orders"

    _runtime.host_module(*_COHOSTED)
    assert _runtime.publisher_modules({"fakeapp.billing"}) == "fakeapp.orders"
    assert _runtime.publisher_modules(set(_COHOSTED)) is None


def test_a_shared_worker_refuses_a_zero_count_for_any_of_its_modules() -> None:
    config = {
        "package": "fakeapp",
        "modules": ["billing", "orders", "shipping"],
        "own_process": ["shipping"],
        "workers": {"billing": 2, "orders": 0},
    }
    with pytest.raises(ValueError, match="orders must be >= 1, got 0"):
        derive_specs_from_config(config)


def test_an_untagged_listener_belongs_to_the_first_cohosted_module_only(make_fake_app) -> None:
    """A listener no module package registered (a plugin's, say) counts as the
    first hosted module's, so exactly one co-hosted consumer group consumes it."""
    _host_orders_and_billing(make_fake_app)

    @listener
    async def audit(evt: AuditTrail) -> None:
        pass

    bus = _runtime.event_bus
    cfg = _runtime.config
    assert bus is not None and cfg is not None
    fqn = f"{AuditTrail.__module__}.{AuditTrail.__qualname__}"
    assert fqn in consumer_targets(_runtime.module_bus(bus, "fakeapp.orders"), cfg, "orders")
    assert fqn not in consumer_targets(_runtime.module_bus(bus, "fakeapp.billing"), cfg, "billing")


@pytest.mark.parametrize("path", ["redis", "polling"])
async def test_each_cohosted_consumer_runs_only_its_own_modules_listeners(
    make_fake_app, path: str
) -> None:
    orders, billing = _host_orders_and_billing(make_fake_app)
    payload = JsonEventSerializer().serialize(orders.ShipmentBooked(order_id="o-9"))
    headers = {
        "event_type": "fakeapp.orders.ShipmentBooked",
        _PUBLISHER_MODULE_HEADER: "fakeapp.shipping",
    }

    await _deliver(path, "fakeapp.orders", "testbroker:shipped", payload, headers)
    assert (orders.seen, billing.shipped) == (["o-9"], [])

    await _deliver(path, "fakeapp.billing", "testbroker:shipped", payload, headers)
    assert (orders.seen, billing.shipped) == (["o-9"], ["o-9"])


def test_a_cohosted_modules_subscriptions_cover_only_its_own_listeners(make_fake_app) -> None:
    _host_orders_and_billing(make_fake_app)
    bus = _runtime.event_bus
    cfg = _runtime.config
    assert bus is not None and cfg is not None

    orders_targets = consumer_targets(_runtime.module_bus(bus, "fakeapp.orders"), cfg, "orders")
    billing_targets = consumer_targets(_runtime.module_bus(bus, "fakeapp.billing"), cfg, "billing")

    assert sorted(orders_targets) == ["fakeapp.orders.ShipmentBooked"]
    assert sorted(billing_targets) == [
        "fakeapp.orders.OrderPlaced",
        "fakeapp.orders.ShipmentBooked",
    ]


def test_the_worker_owns_the_listeners_of_every_cohosted_module(make_fake_app) -> None:
    _host_orders_and_billing(make_fake_app)

    assert outbox._local_listener_ids() == frozenset(
        {"fakeapp.orders.on_shipped", "fakeapp.billing.bill", "fakeapp.billing.on_shipped"}
    )


async def _deliver(
    path: str, module_package: str, target: str, payload: bytes, headers: dict[str, str]
) -> None:
    """Hand one broker message to ``module_package``'s consumer in this worker."""
    bus = _runtime.event_bus
    assert bus is not None
    view = _runtime.module_bus(bus, module_package)
    if path == "redis":
        fields = {b"data": payload}
        fields.update({f"h:{k}".encode(): v.encode() for k, v in headers.items()})
        consumer = _make_redis_consumer(FakeConsumerBroker(), view, targets=[target])
        await consumer._dispatch_one(target, b"1-0", fields)
        return
    polling = _make_polling_consumer(
        FakePollingBroker(), poll_interval_s=1.0, max_attempts=3, idle_wait=asyncio.sleep, bus=view
    )
    await polling._dispatch_one(
        {
            "id": "row-1",
            "target": target,
            "event_type": headers["event_type"],
            "payload": payload,
            "headers": json.dumps(headers),
        }
    )
