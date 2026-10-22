from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from studio.reviews import DuplicateReviewError, get_review, post_review

router = APIRouter()


class NewReview(BaseModel):
    review_id: str
    product: str
    rating: int = Field(ge=1, le=5)
    text: str


@router.post("")
async def submit_review(body: NewReview) -> dict[str, str]:
    try:
        await post_review(body.review_id, body.product, body.rating, body.text)
    except DuplicateReviewError:
        raise HTTPException(status_code=409, detail="review already exists") from None
    return {"review_id": body.review_id}


@router.get("/{review_id}")
async def read_review(review_id: str) -> dict[str, str | int | None]:
    review = await get_review(review_id)
    if review is None:
        raise HTTPException(status_code=404, detail="unknown review")
    return {
        "review_id": review.review_id,
        "product": review.product,
        "rating": review.rating,
        "status": review.status,
        "reply": review.reply,
    }
