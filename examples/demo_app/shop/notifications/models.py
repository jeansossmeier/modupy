from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Notification(Base):
    __tablename__ = "notifications_notification"

    order_id: Mapped[str] = mapped_column(String, primary_key=True)
