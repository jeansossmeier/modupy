from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Reservation(Base):
    __tablename__ = "inventory_reservation"

    order_id: Mapped[str] = mapped_column(String, primary_key=True)
