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
