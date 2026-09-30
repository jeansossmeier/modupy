from modulith import declare_module

from marketplace.shipping import handlers

declare_module(
    publishes=["ShipmentRequested", "ShipmentBooked"],
    consumes=["OrderConfirmed", "ShipmentRequested"],
    listeners=[handlers.on_order_confirmed, handlers.book_carrier],
    owns_tables=["shipping_shipment", "shipping_zone"],
    declared_dependencies=["contracts"],
)
