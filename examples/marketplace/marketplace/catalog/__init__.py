from modulith import publish
from sqlalchemy import select

from marketplace.catalog.tables import product
from marketplace.contracts import ProductListed
from marketplace.db import engine, transaction


async def list_product(sku: str, name: str, price_cents: int, stock: int) -> None:
    async with transaction() as session:
        await session.execute(product.insert().values(sku=sku, name=name, price_cents=price_cents))
        await publish(ProductListed(sku=sku, name=name, price_cents=price_cents, stock=stock))


async def price_of(sku: str) -> int:
    async with engine().connect() as connection:
        price: int | None = await connection.scalar(
            select(product.c.price_cents).where(product.c.sku == sku)
        )
    if price is None:
        raise LookupError(f"unknown sku {sku!r}")
    return price
