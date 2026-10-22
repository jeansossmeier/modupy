"""The events the studio's modules agree on."""

from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class ReviewPosted:
    review_id: str
    product: str
    rating: int
    text: str


@event
@dataclass(frozen=True)
class ReviewModerated:
    review_id: str
    allowed: bool


@event
@dataclass(frozen=True)
class ReplyDrafted:
    review_id: str
    reply: str
