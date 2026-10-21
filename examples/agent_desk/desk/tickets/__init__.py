from modulith import listener, publish
from modulith.builtin.outbox import bind_session, unbind_session
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from desk.contracts import TicketOpened, TicketTriaged
from desk.database import sessionmaker
from desk.tickets.models import Ticket


class DuplicateTicketError(Exception):
    pass


async def open_ticket(ticket_id: str, subject: str, body: str) -> None:
    """Store the ticket and its TicketOpened event in one transaction.

    The bound session puts the event in the outbox table, so the agent's work
    is committed with the ticket and is retried until it succeeds.
    """
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            # Inserting first lets the primary key reject a duplicate, even one
            # from a request racing this one, before the event is published.
            session.add(Ticket(ticket_id=ticket_id, subject=subject, body=body))
            await session.flush()
            await publish(TicketOpened(ticket_id=ticket_id, subject=subject, body=body))
            await session.commit()
        except IntegrityError:
            raise DuplicateTicketError(ticket_id) from None
        finally:
            unbind_session(token)


async def get_ticket(ticket_id: str) -> Ticket | None:
    async with sessionmaker() as session:
        return await session.get(Ticket, ticket_id)


@listener
async def record_triage(event: TicketTriaged) -> None:
    async with sessionmaker() as session:
        await session.execute(
            update(Ticket)
            .where(Ticket.ticket_id == event.ticket_id)
            .values(category=event.category, priority=event.priority, reply=event.reply)
        )
        await session.commit()


# Process-per-module mode serves each module package's `router` under /<module>.
from desk.tickets.api import router as router  # noqa: E402
