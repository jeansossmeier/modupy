from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived

_orders: dict[str, str] = {}  # order_id -> customer_id
_fulfilled: set[str] = set()


async def create_order(customer_id: str) -> str:
    order_id = f"ord-{len(_orders) + 1}"
    _orders[order_id] = customer_id  # your real persistence goes here
    await publish(OrderCreated(order_id=order_id))
    return order_id


def is_fulfilled(order_id: str) -> bool:
    return order_id in _fulfilled


@listener
async def on_payment(event: PaymentReceived) -> None:
    """Cross-module communication via events, not direct calls."""
    _fulfilled.add(event.order_id)  # your real fulfilment goes here


# Re-export the router onto the module package. Process-per-module mode serves
# HTTP by mounting each module package's `router` attribute under /<module>.
from myapp.orders.api import router as router  # noqa: E402
