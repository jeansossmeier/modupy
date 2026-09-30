from modulith import declare_module

from marketplace.orders import handlers

declare_module(
    publishes=["OrderPlaced", "PaymentRequested", "OrderConfirmed", "OrderCancelled"],
    consumes=["StockReserved", "StockRejected", "PaymentCaptured", "StockReleased"],
    listeners=[
        handlers.on_stock_reserved,
        handlers.on_stock_rejected,
        handlers.on_payment_captured,
        handlers.on_stock_released,
    ],
    owns_tables=["orders_order"],
    declared_dependencies=["catalog"],
)
