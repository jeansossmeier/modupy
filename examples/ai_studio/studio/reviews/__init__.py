"""The shop's reviews: the business module the AI modules work for."""

from modulith import listener, publish
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from studio.contracts import ReplyDrafted, ReviewModerated, ReviewPosted
from studio.database import sessionmaker, transaction
from studio.reviews.models import Review


class DuplicateReviewError(Exception):
    pass


async def post_review(review_id: str, product: str, rating: int, text: str) -> None:
    """Store the review and its ReviewPosted event in one transaction."""
    try:
        async with transaction() as session:
            # Inserting first lets the primary key reject a duplicate before the
            # event is published.
            session.add(Review(review_id=review_id, product=product, rating=rating, text=text))
            await session.flush()
            await publish(
                ReviewPosted(review_id=review_id, product=product, rating=rating, text=text)
            )
    except IntegrityError:
        raise DuplicateReviewError(review_id) from None


async def get_review(review_id: str) -> Review | None:
    async with sessionmaker() as session:
        return await session.get(Review, review_id)


async def published_reviews(product: str) -> list[tuple[int, str]]:
    """The rating and text of each published review of ``product``."""
    async with sessionmaker() as session:
        rows = await session.execute(
            select(Review.rating, Review.text).where(
                Review.product == product, Review.status == "published"
            )
        )
        return [(rating, text) for rating, text in rows]


async def _set(review_id: str, **values: str) -> None:
    async with sessionmaker() as session:
        await session.execute(update(Review).where(Review.review_id == review_id).values(values))
        await session.commit()


@listener
async def record_verdict(event: ReviewModerated) -> None:
    await _set(event.review_id, status="published" if event.allowed else "hidden")


@listener
async def record_reply(event: ReplyDrafted) -> None:
    await _set(event.review_id, reply=event.reply)


# Process-per-module mode serves each module package's `router` under /<module>.
from studio.reviews.api import router as router  # noqa: E402
