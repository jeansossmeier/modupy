from modulith import publish
from modulith.builtin.outbox import bind_session, unbind_session

from shop.contracts.events import OrderPlaced
from shop.database import sessionmaker
from shop.orders.models import Order


class DuplicateOrderError(Exception):
    pass


async def place_order(order_id: str, customer_id: str, total: float) -> None:
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            if await session.get(Order, order_id) is not None:
                raise DuplicateOrderError(order_id)
            await publish(OrderPlaced(order_id=order_id, customer_id=customer_id, total=total))
            session.add(Order(order_id=order_id, customer_id=customer_id, total=total))
            await session.commit()
        finally:
            unbind_session(token)


async def get_order(order_id: str) -> Order | None:
    async with sessionmaker() as session:
        return await session.get(Order, order_id)


# Re-export the router onto the module package. Process-per-module mode serves
# HTTP by mounting each module package's `router` attribute under /<module>.
from shop.orders.api import router as router  # noqa: E402
