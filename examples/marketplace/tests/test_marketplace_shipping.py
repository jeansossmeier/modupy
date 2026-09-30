from typing import Any

import pytest
from modulith.testing import ModulithTestApp


def confirmed(order_id: str, country: str) -> Any:
    from marketplace.contracts import OrderConfirmed

    return OrderConfirmed(
        order_id=order_id,
        customer_id="alice",
        sku="SKU-MUG",
        quantity=2,
        total_cents=2400,
        country=country,
    )


async def test_a_confirmed_order_requests_one_shipment_even_when_redelivered(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.shipping import ShipmentRequested
    from marketplace.shipping.handlers import on_order_confirmed

    event = confirmed("o-1", "DE")

    await on_order_confirmed(event)
    await on_order_confirmed(event)

    assert marketplace.published_events == [ShipmentRequested(order_id="o-1", country="DE")]


async def test_a_zone_books_the_shipment_once_even_when_redelivered(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import ShipmentBooked
    from marketplace.shipping import ShipmentRequested
    from marketplace.shipping.handlers import book_carrier, on_order_confirmed

    await on_order_confirmed(confirmed("o-1", "DE"))
    requested = ShipmentRequested(order_id="o-1", country="DE")

    await book_carrier(requested)
    await book_carrier(requested)

    assert marketplace.published_events_of_type(ShipmentBooked) == [
        ShipmentBooked(
            order_id="o-1", customer_id="alice", carrier="DHL", tracking_number="TRK-o-1"
        )
    ]


async def test_a_country_without_a_zone_cannot_be_booked(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import ShipmentBooked
    from marketplace.shipping import ShipmentRequested
    from marketplace.shipping.handlers import book_carrier, on_order_confirmed

    await on_order_confirmed(confirmed("o-1", "NZ"))

    with pytest.raises(LookupError, match="NZ"):
        await book_carrier(ShipmentRequested(order_id="o-1", country="NZ"))

    assert marketplace.published_events_of_type(ShipmentBooked) == []
