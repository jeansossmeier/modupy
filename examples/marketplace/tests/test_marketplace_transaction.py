import pytest
from modulith.testing import ModulithTestApp

from .conftest import Delivered


async def test_transaction_commits_business_rows_and_outbox_rows_together(
    marketplace: ModulithTestApp, delivered: Delivered
) -> None:
    from modulith import listener, publish
    from sqlalchemy import func, select

    from marketplace.catalog.tables import product
    from marketplace.contracts import ProductListed
    from marketplace.db import engine, transaction

    received: list[ProductListed] = []

    @listener
    async def on_listed(event: ProductListed) -> None:
        received.append(event)

    listed = ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=10)
    async with transaction() as session:
        await session.execute(
            product.insert().values(sku="SKU-MUG", name="Stoneware mug", price_cents=1200)
        )
        await publish(listed)
        assert received == []

    counts = await delivered(1)
    async with engine().connect() as connection:
        stored = await connection.scalar(select(func.count()).select_from(product))

    assert stored == 1
    assert received == [listed]
    assert counts == {"incomplete": 0, "completed": 1, "dead_lettered": 0}


async def test_transaction_rolls_back_business_rows_and_outbox_rows_on_error(
    marketplace: ModulithTestApp, delivered: Delivered
) -> None:
    from modulith import listener, publish
    from sqlalchemy import func, select

    from marketplace.catalog.tables import product
    from marketplace.contracts import ProductListed
    from marketplace.db import engine, transaction

    received: list[ProductListed] = []

    @listener
    async def on_listed(event: ProductListed) -> None:
        received.append(event)

    with pytest.raises(RuntimeError, match="boom"):
        async with transaction() as session:
            await session.execute(
                product.insert().values(sku="SKU-MUG", name="Stoneware mug", price_cents=1200)
            )
            await publish(
                ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=10)
            )
            raise RuntimeError("boom")

    counts = await delivered(0)
    async with engine().connect() as connection:
        stored = await connection.scalar(select(func.count()).select_from(product))

    assert stored == 0
    assert received == []
    assert counts == {"incomplete": 0, "completed": 0, "dead_lettered": 0}


async def test_a_publish_after_the_transaction_is_no_longer_transactional(
    marketplace: ModulithTestApp, delivered: Delivered
) -> None:
    from modulith import listener, publish

    from marketplace.contracts import ProductListed
    from marketplace.db import transaction

    received: list[ProductListed] = []

    @listener
    async def on_listed(event: ProductListed) -> None:
        received.append(event)

    listed = ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=10)
    async with transaction():
        await publish(listed)
    await delivered(1)

    await publish(listed)

    assert received == [listed, listed]
    assert await delivered(1) == {"incomplete": 0, "completed": 1, "dead_lettered": 0}
