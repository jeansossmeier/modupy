from modulith import declare_module

from marketplace.notifications import handlers

declare_module(
    consumes=["OrderPlaced", "OrderConfirmed", "OrderCancelled", "ShipmentBooked"],
    listeners=[
        handlers.on_order_placed,
        handlers.on_order_confirmed,
        handlers.on_order_cancelled,
        handlers.on_shipment_booked,
    ],
    owns_tables=["notifications_notification"],
    declared_dependencies=["contracts"],
)
