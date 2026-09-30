from sqlalchemy import Column, Integer, String, Table

from marketplace.db import metadata

order = Table(
    "orders_order",
    metadata,
    Column("order_id", String, primary_key=True),
    Column("customer_id", String, nullable=False),
    Column("sku", String, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("total_cents", Integer, nullable=False),
    Column("country", String, nullable=False),
    Column("card_token", String, nullable=False),
    Column("status", String, nullable=False),
    Column("reason", String, nullable=True),
)
