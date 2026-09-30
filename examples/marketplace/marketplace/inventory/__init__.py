from sqlalchemy import select

from marketplace.contracts import StockRejected, StockReleased, StockReserved
from marketplace.db import engine
from marketplace.inventory.tables import stock


async def stock_of(sku: str) -> int:
    async with engine().connect() as connection:
        on_hand: int | None = await connection.scalar(
            select(stock.c.on_hand).where(stock.c.sku == sku)
        )
    return on_hand or 0


__all__ = ["StockRejected", "StockReleased", "StockReserved", "router", "stock_of"]

# Process-per-module mode serves HTTP by mounting the `router` attribute of each
# module package, so the router is re-exported here.
from marketplace.inventory.api import router as router  # noqa: E402
