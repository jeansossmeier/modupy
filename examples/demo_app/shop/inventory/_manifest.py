from modulith import declare_module

from shop.inventory import reserve_stock

declare_module(
    consumes=["OrderPlaced"],
    publishes=["StockReserved"],
    listeners=[reserve_stock],
    owns_tables=["inventory_reservation"],
    declared_dependencies=["contracts"],
)
