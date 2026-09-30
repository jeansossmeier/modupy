from fastapi import APIRouter

from marketplace.inventory import stock_of

router = APIRouter()


@router.get("/{sku}")
async def get_stock(sku: str) -> dict[str, str | int]:
    return {"sku": sku, "on_hand": await stock_of(sku)}
