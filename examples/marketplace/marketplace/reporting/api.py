from fastapi import APIRouter

from marketplace.reporting import summary

router = APIRouter()


@router.get("/summary")
async def get_summary() -> dict[str, int]:
    totals: dict[str, int] = await summary()
    return totals
