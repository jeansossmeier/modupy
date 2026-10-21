from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Ticket(Base):
    __tablename__ = "tickets_ticket"

    ticket_id: Mapped[str] = mapped_column(String, primary_key=True)
    subject: Mapped[str] = mapped_column(String)
    body: Mapped[str] = mapped_column(String)
    category: Mapped[str | None] = mapped_column(String, default=None)
    priority: Mapped[str | None] = mapped_column(String, default=None)
    reply: Mapped[str | None] = mapped_column(String, default=None)
