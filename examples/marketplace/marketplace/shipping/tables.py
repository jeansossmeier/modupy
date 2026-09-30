from sqlalchemy import Column, String, Table

from marketplace.db import metadata

shipment = Table(
    "shipping_shipment",
    metadata,
    Column("order_id", String, primary_key=True),
    Column("customer_id", String, nullable=False),
    Column("country", String, nullable=False),
    Column("status", String, nullable=False),
    Column("carrier", String, nullable=True),
    Column("tracking_number", String, nullable=True),
)

zone = Table(
    "shipping_zone",
    metadata,
    Column("country", String, primary_key=True),
    Column("carrier", String, nullable=False),
)
