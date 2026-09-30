from modulith import listener, publish
from sqlalchemy import exists, insert, literal, select, update

from marketplace.contracts import PaymentCaptured, PaymentDeclined, PaymentRequested
from marketplace.db import transaction
from marketplace.payments._internal import gateway
from marketplace.payments.tables import payment


@listener
async def on_payment_requested(event: PaymentRequested) -> None:
    async with transaction() as session:
        first_delivery = await session.execute(
            insert(payment)
            .from_select(
                ["order_id", "amount_cents", "status"],
                select(
                    literal(event.order_id), literal(event.amount_cents), literal("pending")
                ).where(~exists().where(payment.c.order_id == event.order_id)),
            )
            .returning(payment.c.order_id)
        )
        if first_delivery.first() is None:
            return

        charge = gateway.charge(event.card_token, event.amount_cents)
        await session.execute(
            update(payment)
            .where(payment.c.order_id == event.order_id)
            .values(status="captured" if charge.captured else "declined", reason=charge.reason)
        )
        if charge.captured:
            await publish(PaymentCaptured(order_id=event.order_id, amount_cents=event.amount_cents))
        else:
            await publish(PaymentDeclined(order_id=event.order_id, reason=charge.reason or ""))
