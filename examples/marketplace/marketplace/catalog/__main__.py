import asyncio
import sys

from modulith import bootstrap
from modulith.builtin import outbox

from marketplace.catalog import list_product
from marketplace.db import engine


async def run(sku: str, name: str, price_cents: int, stock: int) -> None:
    try:
        await list_product(sku, name, price_cents, stock)
        async with asyncio.timeout(30):
            while (await outbox.status())["incomplete"]:
                await asyncio.sleep(0.1)
    finally:
        await outbox.shutdown()
        await engine().dispose()


def main(argv: list[str]) -> None:
    sku, name, price_cents, stock = argv
    bootstrap()
    asyncio.run(run(sku, name, int(price_cents), int(stock)))


if __name__ == "__main__":
    main(sys.argv[1:])
