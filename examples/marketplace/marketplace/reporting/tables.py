from sqlalchemy import Boolean, Column, Integer, String, Table, false

from marketplace.db import metadata

report = Table(
    "reporting_order",
    metadata,
    Column("order_id", String, primary_key=True),
    Column("confirmed", Boolean, nullable=False, server_default=false()),
    Column("cancelled", Boolean, nullable=False, server_default=false()),
    Column("shipped", Boolean, nullable=False, server_default=false()),
    Column("total_cents", Integer, nullable=False, server_default="0"),
)
