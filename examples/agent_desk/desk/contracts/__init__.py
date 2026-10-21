"""The events the desk's modules agree on."""

from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class TicketOpened:
    ticket_id: str
    subject: str
    body: str


@event
@dataclass(frozen=True)
class TicketTriaged:
    ticket_id: str
    category: str
    priority: str
    reply: str
