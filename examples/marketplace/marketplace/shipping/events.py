from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class ShipmentRequested:
    order_id: str
    country: str
