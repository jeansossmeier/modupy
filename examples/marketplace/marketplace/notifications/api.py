from fastapi import APIRouter

from marketplace.notifications import notifications_for

router = APIRouter()


@router.get("/{order_id}")
async def get_notifications(order_id: str) -> list[dict[str, str]]:
    found: list[dict[str, str]] = await notifications_for(order_id)
    return found
