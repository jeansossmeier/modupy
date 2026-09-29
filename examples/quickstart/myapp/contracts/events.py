from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str


@event
@dataclass(frozen=True)
class PaymentReceived:
    order_id: str
