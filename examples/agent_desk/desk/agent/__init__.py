"""Runs the triage graph on every new ticket and reports back with an event."""

import os

from modulith import listener, publish
from modulith.builtin.outbox import bind_session, unbind_session
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from desk.agent.graph import graph
from desk.contracts import TicketOpened, TicketTriaged
from desk.database import sessionmaker

TIMEOUT = float(os.environ.get("DESK_AGENT_TIMEOUT", "30"))
# 1 keeps the session open across the model calls: the mistake that
# benchmark.py --hold-session measures.
HOLD_SESSION = os.environ.get("DESK_AGENT_HOLD_SESSION") == "1"


class Base(DeclarativeBase):
    pass


class Triage(Base):
    __tablename__ = "agent_triage"

    ticket_id: Mapped[str] = mapped_column(String, primary_key=True)
    category: Mapped[str] = mapped_column(String)
    priority: Mapped[str] = mapped_column(String)


@listener(timeout=TIMEOUT)
async def triage(event: TicketOpened) -> None:
    async with sessionmaker() as session:
        # Delivery is at least once, so a redelivered ticket is not triaged twice.
        if await session.get(Triage, event.ticket_id) is not None:
            return
        if HOLD_SESSION:
            # The session's pooled connection stays checked out while the model answers.
            state = await graph.ainvoke({"subject": event.subject, "body": event.body})
    if not HOLD_SESSION:
        # No session stays open across the model calls, which can take seconds.
        state = await graph.ainvoke({"subject": event.subject, "body": event.body})
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            session.add(
                Triage(
                    ticket_id=event.ticket_id,
                    category=state["category"],
                    priority=state["priority"],
                )
            )
            await publish(
                TicketTriaged(
                    ticket_id=event.ticket_id,
                    category=state["category"],
                    priority=state["priority"],
                    reply=state["reply"],
                )
            )
            await session.commit()
        finally:
            unbind_session(token)
