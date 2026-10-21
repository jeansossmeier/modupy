from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from desk.tickets import DuplicateTicketError, get_ticket, open_ticket

router = APIRouter()


class NewTicket(BaseModel):
    ticket_id: str
    subject: str
    body: str


@router.post("")
async def post_ticket(body: NewTicket) -> dict[str, str]:
    try:
        await open_ticket(body.ticket_id, body.subject, body.body)
    except DuplicateTicketError:
        raise HTTPException(status_code=409, detail="ticket already exists") from None
    return {"ticket_id": body.ticket_id}


@router.get("/{ticket_id}")
async def read_ticket(ticket_id: str) -> dict[str, str | None]:
    ticket = await get_ticket(ticket_id)
    if ticket is None:
        raise HTTPException(status_code=404, detail="unknown ticket")
    return {
        "ticket_id": ticket.ticket_id,
        "category": ticket.category,
        "priority": ticket.priority,
        "reply": ticket.reply,
    }
