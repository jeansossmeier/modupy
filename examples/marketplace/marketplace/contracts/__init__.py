from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class ProductListed:
    sku: str
    name: str
    price_cents: int
    stock: int


@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
    customer_id: str
    sku: str
    quantity: int
    total_cents: int
    country: str


@event
@dataclass(frozen=True)
class StockReserved:
    order_id: str
    sku: str
    quantity: int


@event
@dataclass(frozen=True)
class StockRejected:
    order_id: str
    sku: str
    requested: int
    available: int


@event
@dataclass(frozen=True)
class StockReleased:
    order_id: str
    sku: str
    quantity: int


@event
@dataclass(frozen=True)
class PaymentRequested:
    order_id: str
    amount_cents: int
    card_token: str


@event
@dataclass(frozen=True)
class PaymentCaptured:
    order_id: str
    amount_cents: int


@event
@dataclass(frozen=True)
class PaymentDeclined:
    order_id: str
    reason: str


@event
@dataclass(frozen=True)
class OrderConfirmed:
    order_id: str
    customer_id: str
    sku: str
    quantity: int
    total_cents: int
    country: str


@event
@dataclass(frozen=True)
class OrderCancelled:
    order_id: str
    customer_id: str
    reason: str


@event
@dataclass(frozen=True)
class ShipmentBooked:
    order_id: str
    customer_id: str
    carrier: str
    tracking_number: str
