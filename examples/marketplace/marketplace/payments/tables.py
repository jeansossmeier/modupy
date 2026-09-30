from sqlalchemy import Column, Integer, String, Table

from marketplace.db import metadata

payment = Table(
    "payments_payment",
    metadata,
    Column("order_id", String, primary_key=True),
    Column("amount_cents", Integer, nullable=False),
    Column("status", String, nullable=False),
    Column("reason", String, nullable=True),
)
