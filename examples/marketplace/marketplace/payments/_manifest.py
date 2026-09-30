from modulith import declare_module

from marketplace.payments import handlers

declare_module(
    publishes=["PaymentCaptured", "PaymentDeclined"],
    consumes=["PaymentRequested"],
    listeners=[handlers.on_payment_requested],
    owns_tables=["payments_payment"],
    declared_dependencies=["contracts"],
)
