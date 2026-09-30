from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest
from modulith.testing import ModulithTestApp

SIBLINGS = [
    "marketplace.catalog",
    "marketplace.inventory",
    "marketplace.notifications",
    "marketplace.payments",
    "marketplace.reporting",
    "marketplace.shipping",
]


async def test_the_order_total_uses_the_catalog_price(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=SIBLINGS):
        from marketplace import catalog
        from marketplace.contracts import OrderPlaced
        from marketplace.orders import place_order, status_of

        catalog.price_of = AsyncMock(return_value=1200)

        await place_order("o-1", "alice", "SKU-MUG", 3, "tok_visa", "DE")

        assert marketplace.published_events == [
            OrderPlaced(
                order_id="o-1",
                customer_id="alice",
                sku="SKU-MUG",
                quantity=3,
                total_cents=3600,
                country="DE",
            )
        ]
        assert await status_of("o-1") == {"order_id": "o-1", "status": "placed", "reason": None}
        catalog.price_of.assert_awaited_once_with("SKU-MUG")


async def test_reserved_stock_requests_payment_once_even_when_redelivered(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=SIBLINGS):
        from marketplace import catalog
        from marketplace.contracts import PaymentRequested, StockReserved
        from marketplace.orders import place_order, status_of
        from marketplace.orders.handlers import on_stock_reserved

        catalog.price_of = AsyncMock(return_value=1200)
        await place_order("o-1", "alice", "SKU-MUG", 3, "tok_visa", "DE")
        reserved = StockReserved(order_id="o-1", sku="SKU-MUG", quantity=3)

        await on_stock_reserved(reserved)
        await on_stock_reserved(reserved)

        assert marketplace.published_events_of_type(PaymentRequested) == [
            PaymentRequested(order_id="o-1", amount_cents=3600, card_token="tok_visa")
        ]
        assert (await status_of("o-1"))["status"] == "reserved"


async def test_a_captured_payment_confirms_the_order_once_even_when_redelivered(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=SIBLINGS):
        from marketplace import catalog
        from marketplace.contracts import OrderConfirmed, PaymentCaptured, StockReserved
        from marketplace.orders import place_order, status_of
        from marketplace.orders.handlers import on_payment_captured, on_stock_reserved

        catalog.price_of = AsyncMock(return_value=1200)
        await place_order("o-1", "alice", "SKU-MUG", 3, "tok_visa", "DE")
        captured = PaymentCaptured(order_id="o-1", amount_cents=3600)

        await on_payment_captured(captured)
        assert marketplace.published_events_of_type(OrderConfirmed) == []

        await on_stock_reserved(StockReserved(order_id="o-1", sku="SKU-MUG", quantity=3))
        await on_payment_captured(captured)
        await on_payment_captured(captured)

        assert marketplace.published_events_of_type(OrderConfirmed) == [
            OrderConfirmed(
                order_id="o-1",
                customer_id="alice",
                sku="SKU-MUG",
                quantity=3,
                total_cents=3600,
                country="DE",
            )
        ]
        assert await status_of("o-1") == {"order_id": "o-1", "status": "confirmed", "reason": None}


async def test_released_stock_cancels_the_order_once_even_when_redelivered(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=SIBLINGS):
        from marketplace import catalog
        from marketplace.contracts import (
            OrderCancelled,
            PaymentCaptured,
            StockReleased,
            StockReserved,
        )
        from marketplace.orders import place_order, status_of
        from marketplace.orders.handlers import (
            on_payment_captured,
            on_stock_released,
            on_stock_reserved,
        )

        catalog.price_of = AsyncMock(return_value=1200)
        await place_order("o-1", "alice", "SKU-MUG", 3, "tok_declined", "DE")
        await on_stock_reserved(StockReserved(order_id="o-1", sku="SKU-MUG", quantity=3))
        released = StockReleased(order_id="o-1", sku="SKU-MUG", quantity=3)

        await on_stock_released(released)
        await on_stock_released(released)
        await on_payment_captured(PaymentCaptured(order_id="o-1", amount_cents=3600))

        assert marketplace.published_events_of_type(OrderCancelled) == [
            OrderCancelled(order_id="o-1", customer_id="alice", reason="payment declined")
        ]
        assert await status_of("o-1") == {
            "order_id": "o-1",
            "status": "cancelled",
            "reason": "payment declined",
        }


async def test_an_order_for_an_unlisted_sku_is_refused_and_stores_nothing(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=SIBLINGS):
        from marketplace import catalog
        from marketplace.orders import place_order, status_of

        catalog.price_of = AsyncMock(side_effect=LookupError("unknown sku 'SKU-NOPE'"))

        with pytest.raises(LookupError, match="SKU-NOPE"):
            await place_order("o-1", "alice", "SKU-NOPE", 1, "tok_visa", "DE")

        assert marketplace.published_events == []
        with pytest.raises(LookupError, match="o-1"):
            await status_of("o-1")
