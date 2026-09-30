from modulith import declare_module

declare_module(
    publishes=["OrderPlaced"],
    owns_tables=["orders_order"],
    declared_dependencies=["contracts"],
)
