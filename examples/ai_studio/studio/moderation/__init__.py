"""Moderates every new review with a local model and reports the verdict with an event."""

from modulith import listener, publish

from studio.contracts import ReviewModerated, ReviewPosted
from studio.database import transaction
from studio.moderation.model import is_abusive


@listener
async def moderate(event: ReviewPosted) -> None:
    # Local inference is synchronous: it holds the event loop's thread while it
    # runs, and a crash in its native code takes the process down. No timeout
    # stops either, so moderation runs in a process of its own (own_process).
    allowed = not is_abusive(event.text)
    async with transaction():
        await publish(ReviewModerated(review_id=event.review_id, allowed=allowed))
