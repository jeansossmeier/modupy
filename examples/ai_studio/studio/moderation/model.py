"""A stand-in for a local moderation model, so the studio runs without one.

It flags abuse by keyword. ``STUDIO_MODERATION_FAULT`` makes every call fail on
demand, as ``studio/faults.py`` describes. A model loaded into this process can
replace ``is_abusive``; the listener does not change.
"""

from studio.faults import fail_now, fault_for

FAULT = fault_for("moderation")
ABUSIVE = ("idiot", "scam", "stupid")


def is_abusive(text: str) -> bool:
    fail_now(FAULT)
    words = text.lower()
    return any(word in words for word in ABUSIVE)
