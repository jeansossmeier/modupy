from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
    customer_id: str
    total: float


@event
@dataclass(frozen=True)
class StockReserved:
    order_id: str
