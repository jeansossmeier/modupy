from sqlalchemy import Column, Integer, String, Table

from marketplace.db import metadata

notification = Table(
    "notifications_notification",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("order_id", String, nullable=False, index=True),
    Column("kind", String, nullable=False),
    Column("message", String, nullable=False),
)
