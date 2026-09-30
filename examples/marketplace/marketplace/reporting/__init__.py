from typing import Any

from sqlalchemy import case, func, select

from marketplace.db import engine
from marketplace.reporting.tables import report


def total_where(flag: Any, amount: Any = 1) -> Any:
    return func.coalesce(func.sum(case((flag.is_(True), amount), else_=0)), 0)


async def summary() -> dict[str, int]:
    async with engine().connect() as connection:
        row = (
            await connection.execute(
                select(
                    total_where(report.c.confirmed),
                    total_where(report.c.cancelled),
                    total_where(report.c.shipped),
                    total_where(report.c.confirmed, report.c.total_cents),
                )
            )
        ).one()
    confirmed, cancelled, shipped, revenue_cents = row
    return {
        "confirmed": int(confirmed),
        "cancelled": int(cancelled),
        "shipped": int(shipped),
        "revenue_cents": int(revenue_cents),
    }


__all__ = ["router", "summary"]

# Process-per-module mode serves HTTP by mounting the `router` attribute of each
# module package, so the router is re-exported here.
from marketplace.reporting.api import router as router  # noqa: E402
