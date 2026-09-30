from modulith import declare_module

from shop.notifications import notify_customer

declare_module(
    consumes=["StockReserved"],
    listeners=[notify_customer],
    owns_tables=["notifications_notification"],
    declared_dependencies=["contracts"],
)
