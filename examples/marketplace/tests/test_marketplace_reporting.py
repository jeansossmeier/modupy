from modulith.testing import ModulithTestApp


async def test_the_summary_counts_orders_by_what_happened_to_them(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import OrderCancelled, OrderConfirmed, ShipmentBooked
    from marketplace.reporting import summary
    from marketplace.reporting.handlers import (
        on_order_cancelled,
        on_order_confirmed,
        on_shipment_booked,
    )

    assert await summary() == {"confirmed": 0, "cancelled": 0, "shipped": 0, "revenue_cents": 0}

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
    await on_order_confirmed(
        OrderConfirmed(
            order_id="o-2",
            customer_id="bob",
            sku="SKU-MUG",
            quantity=1,
            total_cents=1200,
            country="DE",
        )
    )
    await on_shipment_booked(
        ShipmentBooked(order_id="o-1", customer_id="alice", carrier="DHL", tracking_number="T-1")
    )
    await on_order_cancelled(OrderCancelled(order_id="o-3", customer_id="carol", reason="x"))

    assert await summary() == {
        "confirmed": 2,
        "cancelled": 1,
        "shipped": 1,
        "revenue_cents": 3600,
    }


async def test_a_redelivered_event_changes_nothing_and_arrival_order_does_not_matter(
    marketplace: ModulithTestApp,
) -> None:
    from sqlalchemy import func, select

    from marketplace.contracts import OrderConfirmed, ShipmentBooked
    from marketplace.db import engine
    from marketplace.reporting import summary
    from marketplace.reporting.handlers import on_order_confirmed, on_shipment_booked
    from marketplace.reporting.tables import report

    confirmed = OrderConfirmed(
        order_id="o-1",
        customer_id="alice",
        sku="SKU-MUG",
        quantity=2,
        total_cents=2400,
        country="DE",
    )
    booked = ShipmentBooked(
        order_id="o-1", customer_id="alice", carrier="DHL", tracking_number="T-1"
    )

    await on_shipment_booked(booked)
    await on_order_confirmed(confirmed)
    once = await summary()
    await on_shipment_booked(booked)
    await on_order_confirmed(confirmed)

    async with engine().connect() as connection:
        rows = await connection.scalar(select(func.count()).select_from(report))
    assert rows == 1
    assert (
        await summary()
        == once
        == {
            "confirmed": 1,
            "cancelled": 0,
            "shipped": 1,
            "revenue_cents": 2400,
        }
    )
