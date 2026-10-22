"""Summarizes a product's published reviews on request, within a deadline."""

import asyncio
import os

from fastapi import APIRouter, HTTPException

from studio.reviews import published_reviews
from studio.summaries.model import summarize

TIMEOUT = float(os.environ.get("STUDIO_SUMMARIES_TIMEOUT", "10"))

router = APIRouter()


@router.get("/{product}")
async def read_summary(product: str) -> dict[str, str]:
    reviews = await published_reviews(product)
    if not reviews:
        raise HTTPException(status_code=404, detail="no published reviews")
    try:
        # Someone is waiting on this request, so a model call past the deadline
        # is cancelled and answered with a 504.
        async with asyncio.timeout(TIMEOUT):
            summary = await summarize(product, reviews)
    except TimeoutError:
        raise HTTPException(
            status_code=504, detail=f"the model did not answer within {TIMEOUT:g}s"
        ) from None
    return {"product": product, "summary": summary}
