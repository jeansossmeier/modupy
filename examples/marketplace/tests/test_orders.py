from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest
from modulith.testing import ModulithTestApp


async def test_the_order_total_uses_the_catalog_price(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=["marketplace.catalog"]):
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


async def test_an_order_for_an_unlisted_sku_is_refused_and_stores_nothing(
    marketplace: ModulithTestApp, modulith_module: Callable[..., Any]
) -> None:
    with modulith_module("marketplace.orders", mock_modules=["marketplace.catalog"]):
        from marketplace import catalog
        from marketplace.orders import place_order, status_of

        catalog.price_of = AsyncMock(side_effect=LookupError("unknown sku 'SKU-NOPE'"))

        with pytest.raises(LookupError, match="SKU-NOPE"):
            await place_order("o-1", "alice", "SKU-NOPE", 1, "tok_visa", "DE")

        assert marketplace.published_events == []
        with pytest.raises(LookupError, match="o-1"):
            await status_of("o-1")
