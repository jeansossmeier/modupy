from modulith.testing import ModulithTestApp


async def test_each_event_is_recorded_once_even_when_redelivered_and_read_in_order(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import (
        OrderCancelled,
        OrderConfirmed,
        OrderPlaced,
        ShipmentBooked,
    )
    from marketplace.notifications import notifications_for
    from marketplace.notifications.handlers import (
        on_order_cancelled,
        on_order_confirmed,
        on_order_placed,
        on_shipment_booked,
    )

    placed = OrderPlaced(
        order_id="o-1",
        customer_id="alice",
        sku="SKU-MUG",
        quantity=2,
        total_cents=2400,
        country="DE",
    )
    confirmed = OrderConfirmed(
        order_id="o-1",
        customer_id="alice",
        sku="SKU-MUG",
        quantity=2,
        total_cents=2400,
        country="DE",
    )
    booked = ShipmentBooked(
        order_id="o-1", customer_id="alice", carrier="DHL", tracking_number="TRK-o-1"
    )

    for handler, event in [
        (on_order_placed, placed),
        (on_order_placed, placed),
        (on_order_confirmed, confirmed),
        (on_shipment_booked, booked),
        (on_shipment_booked, booked),
    ]:
        await handler(event)
    await on_order_placed(
        OrderPlaced(
            order_id="o-2",
            customer_id="bob",
            sku="SKU-MUG",
            quantity=5,
            total_cents=6000,
            country="DE",
        )
    )
    await on_order_cancelled(
        OrderCancelled(order_id="o-2", customer_id="bob", reason="out of stock")
    )

    assert await notifications_for("o-1") == [
        {"kind": "received", "message": "order received"},
        {"kind": "confirmed", "message": "order confirmed"},
        {"kind": "shipped", "message": "shipped with DHL, tracking TRK-o-1"},
    ]
    assert await notifications_for("o-2") == [
        {"kind": "received", "message": "order received"},
        {"kind": "cancelled", "message": "order cancelled: out of stock"},
    ]
    assert await notifications_for("o-9") == []


async def test_notifications_read_in_lifecycle_order_whatever_order_they_arrived(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import OrderConfirmed, OrderPlaced
    from marketplace.notifications import notifications_for
    from marketplace.notifications.handlers import on_order_confirmed, on_order_placed

    await on_order_confirmed(
        OrderConfirmed(
            order_id="o-1",
            customer_id="alice",
            sku="SKU-MUG",
            quantity=2,
            total_cents=2400,
            country="DE",
        )
    )
    await on_order_placed(
        OrderPlaced(
            order_id="o-1",
            customer_id="alice",
            sku="SKU-MUG",
            quantity=2,
            total_cents=2400,
            country="DE",
        )
    )

    assert [found["kind"] for found in await notifications_for("o-1")] == ["received", "confirmed"]
