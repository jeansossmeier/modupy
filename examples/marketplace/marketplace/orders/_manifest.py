from modulith import declare_module

from marketplace.orders import handlers

declare_module(
    publishes=["OrderPlaced", "OrderCancelled"],
    consumes=["StockRejected"],
    listeners=[handlers.on_stock_rejected],
    owns_tables=["orders_order"],
    declared_dependencies=["catalog"],
)
