from modulith.testing import ModulithTestApp


async def test_listing_a_product_sets_its_stock_and_a_redelivery_does_not_reset_it(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import OrderPlaced, ProductListed
    from marketplace.inventory import stock_of
    from marketplace.inventory.handlers import on_order_placed, on_product_listed

    listed = ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=5)
    await on_product_listed(listed)
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

    await on_product_listed(listed)

    assert await stock_of("SKU-MUG") == 3


async def test_an_order_within_stock_reserves_it_once_even_when_redelivered(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import OrderPlaced, ProductListed, StockReserved
    from marketplace.inventory import stock_of
    from marketplace.inventory.handlers import on_order_placed, on_product_listed

    await on_product_listed(
        ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=5)
    )
    placed = OrderPlaced(
        order_id="o-1",
        customer_id="alice",
        sku="SKU-MUG",
        quantity=2,
        total_cents=2400,
        country="DE",
    )

    await on_order_placed(placed)
    await on_order_placed(placed)

    assert marketplace.published_events == [
        StockReserved(order_id="o-1", sku="SKU-MUG", quantity=2)
    ]
    assert await stock_of("SKU-MUG") == 3


async def test_an_order_beyond_stock_is_rejected_once_even_when_redelivered(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.contracts import OrderPlaced, ProductListed, StockRejected
    from marketplace.inventory import stock_of
    from marketplace.inventory.handlers import on_order_placed, on_product_listed

    await on_product_listed(
        ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=1)
    )
    placed = OrderPlaced(
        order_id="o-1",
        customer_id="alice",
        sku="SKU-MUG",
        quantity=2,
        total_cents=2400,
        country="DE",
    )

    await on_order_placed(placed)
    await on_order_placed(placed)

    assert marketplace.published_events == [
        StockRejected(order_id="o-1", sku="SKU-MUG", requested=2, available=1)
    ]
    assert await stock_of("SKU-MUG") == 1
