from modulith import declare_module

from marketplace.reporting import handlers

declare_module(
    consumes=["OrderConfirmed", "OrderCancelled", "ShipmentBooked"],
    listeners=[
        handlers.on_order_confirmed,
        handlers.on_order_cancelled,
        handlers.on_shipment_booked,
    ],
    owns_tables=["reporting_order"],
    declared_dependencies=["contracts"],
)
