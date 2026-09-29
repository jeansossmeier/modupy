from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived


@listener
async def charge(event: OrderCreated) -> None:
    await publish(PaymentReceived(order_id=event.order_id))  # your real charge goes here
