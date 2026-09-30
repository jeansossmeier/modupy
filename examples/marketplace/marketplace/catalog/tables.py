from sqlalchemy import Column, Integer, String, Table

from marketplace.db import metadata

product = Table(
    "catalog_product",
    metadata,
    Column("sku", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("price_cents", Integer, nullable=False),
)
