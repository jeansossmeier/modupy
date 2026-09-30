import pytest
from modulith.testing import ModulithTestApp

from .conftest import run_python


async def test_listing_a_product_stores_it_and_publishes_product_listed(
    marketplace: ModulithTestApp,
) -> None:
    from marketplace.catalog import list_product, price_of
    from marketplace.contracts import ProductListed

    await list_product("SKU-MUG", "Stoneware mug", 1200, 10)

    assert marketplace.published_events == [
        ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=10)
    ]
    assert await price_of("SKU-MUG") == 1200


async def test_price_of_returns_each_listed_price(marketplace: ModulithTestApp) -> None:
    from marketplace.catalog import list_product, price_of

    await list_product("SKU-MUG", "Stoneware mug", 1200, 10)
    await list_product("SKU-TEA", "Tea tin", 850, 4)

    assert await price_of("SKU-TEA") == 850
    assert await price_of("SKU-MUG") == 1200


async def test_price_of_an_unlisted_sku_is_a_lookup_error(marketplace: ModulithTestApp) -> None:
    from marketplace.catalog import price_of

    with pytest.raises(LookupError, match="SKU-NOPE"):
        await price_of("SKU-NOPE")


async def test_the_catalog_command_lists_a_product_and_leaves_nothing_undelivered(
    database: str,
) -> None:
    listed = run_python("-m", "marketplace.catalog", "SKU-MUG", "Stoneware mug", "1200", "10")
    status = run_python("-m", "modulith", "outbox", "status")

    assert listed.returncode == 0, listed.stderr
    assert status.returncode == 0, status.stderr
    assert "incomplete:    0" in status.stdout.splitlines()
