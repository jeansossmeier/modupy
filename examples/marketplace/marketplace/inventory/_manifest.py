from modulith import declare_module

from marketplace.inventory import handlers

declare_module(
    publishes=["StockReserved", "StockRejected", "StockReleased"],
    consumes=["ProductListed", "OrderPlaced", "PaymentDeclined"],
    listeners=[handlers.on_product_listed, handlers.on_order_placed, handlers.on_payment_declined],
    owns_tables=["inventory_stock", "inventory_reservation"],
    declared_dependencies=["contracts"],
)
