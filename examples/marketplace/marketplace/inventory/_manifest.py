from modulith import declare_module

from marketplace.inventory import handlers

declare_module(
    publishes=["StockReserved", "StockRejected"],
    consumes=["ProductListed", "OrderPlaced"],
    listeners=[handlers.on_product_listed, handlers.on_order_placed],
    owns_tables=["inventory_stock", "inventory_reservation"],
    declared_dependencies=["contracts"],
)
