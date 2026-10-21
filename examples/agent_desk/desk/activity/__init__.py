"""A log of what happened to each ticket, fed only by events."""

import contextlib

from fastapi import APIRouter
from modulith import listener
from sqlalchemy import Integer, String, UniqueConstraint, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from desk.contracts import TicketOpened, TicketTriaged
from desk.database import sessionmaker


class Base(DeclarativeBase):
    pass


class Entry(Base):
    __tablename__ = "activity_entry"
    __table_args__ = (UniqueConstraint("ticket_id", "what"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticket_id: Mapped[str] = mapped_column(String)
    what: Mapped[str] = mapped_column(String)


async def _record(ticket_id: str, what: str) -> None:
    async with sessionmaker() as session:
        session.add(Entry(ticket_id=ticket_id, what=what))
        # Delivery is at least once, so a redelivered event finds its entry.
        with contextlib.suppress(IntegrityError):
            await session.commit()


@listener
async def on_opened(event: TicketOpened) -> None:
    await _record(event.ticket_id, "opened")


@listener
async def on_triaged(event: TicketTriaged) -> None:
    await _record(event.ticket_id, f"triaged as {event.category}/{event.priority}")


router = APIRouter()


@router.get("/{ticket_id}")
async def read_activity(ticket_id: str) -> dict[str, str | list[str]]:
    async with sessionmaker() as session:
        rows = await session.scalars(
            select(Entry.what).where(Entry.ticket_id == ticket_id).order_by(Entry.id)
        )
        return {"ticket_id": ticket_id, "activity": list(rows)}
