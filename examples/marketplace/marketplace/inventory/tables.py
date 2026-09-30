from sqlalchemy import Column, Integer, String, Table

from marketplace.db import metadata

stock = Table(
    "inventory_stock",
    metadata,
    Column("sku", String, primary_key=True),
    Column("on_hand", Integer, nullable=False),
)

reservation = Table(
    "inventory_reservation",
    metadata,
    Column("order_id", String, primary_key=True),
    Column("sku", String, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("status", String, nullable=False),
)
