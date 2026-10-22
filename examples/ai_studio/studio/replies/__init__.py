"""Drafts a reply to every new review with the reply graph and reports it with an event."""

import os

from modulith import listener, publish

from studio.contracts import ReplyDrafted, ReviewPosted
from studio.database import transaction
from studio.replies.graph import graph

TIMEOUT = float(os.environ.get("STUDIO_REPLIES_TIMEOUT", "30"))


@listener(timeout=TIMEOUT)
async def draft_reply(event: ReviewPosted) -> None:
    # No session stays open across the model call, which can take seconds.
    state = await graph.ainvoke(
        {"product": event.product, "rating": event.rating, "text": event.text}
    )
    async with transaction():
        await publish(ReplyDrafted(review_id=event.review_id, reply=state["reply"]))
