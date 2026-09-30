from sqlalchemy import case, select

from marketplace.db import engine
from marketplace.notifications.tables import notification

# Each event reaches this module independently, so arrival order says nothing
# about what happened first; the lifecycle stage does.
LIFECYCLE = {"received": 0, "confirmed": 1, "cancelled": 2, "shipped": 3}


async def notifications_for(order_id: str) -> list[dict[str, str]]:
    async with engine().connect() as connection:
        rows = await connection.execute(
            select(notification.c.kind, notification.c.message)
            .where(notification.c.order_id == order_id)
            .order_by(case(LIFECYCLE, value=notification.c.kind))
        )
    return [{"kind": row.kind, "message": row.message} for row in rows]


__all__ = ["notifications_for", "router"]

# Process-per-module mode serves HTTP by mounting the `router` attribute of each
# module package, so the router is re-exported here.
from marketplace.notifications.api import router as router  # noqa: E402
