"""A stand-in for a hosted model's summary, so the studio runs without an API key.

It counts the reviews that liked the product. ``STUDIO_SUMMARIES_FAULT`` makes
every call fail on demand, as ``studio/faults.py`` describes. A call to any
provider's SDK can replace ``summarize``; the route does not change.
"""

from studio.faults import fail, fault_for

FAULT = fault_for("summaries")


async def summarize(product: str, reviews: list[tuple[int, str]]) -> str:
    await fail(FAULT)
    liked = sum(rating >= 4 for rating, _text in reviews)
    return f"{liked} of {len(reviews)} published reviews of the {product} are positive."
