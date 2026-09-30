from modulith import declare_module

declare_module(
    publishes=["ProductListed"],
    owns_tables=["catalog_product"],
    declared_dependencies=["contracts"],
)
