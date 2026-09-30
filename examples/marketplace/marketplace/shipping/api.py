from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from marketplace.shipping import set_zone, shipment_of

router = APIRouter()


class Zone(BaseModel):
    carrier: str


@router.put("/zones/{country}")
async def put_zone(country: str, body: Zone) -> dict[str, str]:
    await set_zone(country, body.carrier)
    return {"country": country, "carrier": body.carrier}


@router.get("/{order_id}")
async def get_shipment(order_id: str) -> dict[str, str | None]:
    try:
        shipped: dict[str, str | None] = await shipment_of(order_id)
    except LookupError as unknown:
        raise HTTPException(status_code=404, detail=str(unknown)) from unknown
    return shipped
