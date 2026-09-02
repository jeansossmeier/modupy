"""Zero-infra example tests for the demo shop, using the modulith pytest plugin.

Runs against the real ``shop`` package (auto-discovered, default in-memory
bus) — no Docker, no external services. Demonstrates the ``scenario`` and
``modulith_app`` fixtures the plugin ships (auto-loaded via the ``pytest11``
entry point) as living documentation for anyone building their own modulith
app's tests.
"""

from __future__ import annotations

from modulith.testing import ModulithTestApp, Scenario


def _bootstrap_shop() -> None:
    """Configure and bootstrap the real ``shop`` package for a test."""
    from modulith import configure
    from modulith.runtime import _runtime

    configure(package="shop", auto_discover=True)
    _runtime.ensure_bootstrapped()


def test_order_placed_triggers_stock_reserved_via_scenario(scenario: Scenario) -> None:
    """``scenario.publish(...).expect_event(...).within(...)`` across modules."""
    from shop.contracts.events import OrderPlaced, StockReserved

    _bootstrap_shop()

    result = (
        scenario.publish(OrderPlaced(order_id="s-1", customer_id="cust-1", total=9.99))
        .expect_event(StockReserved)
        .matching(lambda e: e.order_id == "s-1")
        .within(seconds=2)
    )

    assert isinstance(result, StockReserved)
    assert result.order_id == "s-1"


async def test_place_order_is_captured_by_modulith_app(modulith_app: ModulithTestApp) -> None:
    """``modulith_app`` captures every event ``place_order`` publishes."""
    from shop.contracts.events import OrderPlaced
    from shop.orders import place_order

    _bootstrap_shop()

    order_id = await place_order(customer_id="c-9", total=3.5)

    captured = modulith_app.published_events_of_type(OrderPlaced)
    assert any(evt.order_id == order_id for evt in captured)


async def test_reserve_stock_dedup_survives_1000_order_boundary(
    modulith_app: ModulithTestApp,
) -> None:
    """At-least-once redelivery is deduped even after 1000 intervening orders."""
    from shop.contracts.events import OrderPlaced, StockReserved
    from shop.inventory import reserve_stock
    from shop.inventory import reserved as reserved_deque
    from shop.orders import place_order

    _bootstrap_shop()

    target_event = OrderPlaced(order_id="target-123", customer_id="c-target", total=1.0)
    await reserve_stock(target_event)
    modulith_app.reset()

    for i in range(1000):
        await place_order(customer_id=f"c-{i}", total=float(i))

    target_exists_after_eviction = any(
        evt.order_id == target_event.order_id for evt in reserved_deque
    )
    assert not target_exists_after_eviction, (
        "Target should be evicted from bounded deque after 1000 intervening orders"
    )

    modulith_app.reset()

    await reserve_stock(target_event)

    redelivered_stock_reserved = modulith_app.published_events_of_type(StockReserved)
    target_reservations = [
        evt for evt in redelivered_stock_reserved if evt.order_id == target_event.order_id
    ]
    assert len(target_reservations) == 0, (
        "Redelivered event should be deduped and not republish StockReserved"
    )
