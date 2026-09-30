from dataclasses import dataclass


@dataclass(frozen=True)
class Charge:
    captured: bool
    reason: str | None = None


def charge(card_token: str, amount_cents: int) -> Charge:
    if card_token == "tok_declined":
        return Charge(captured=False, reason="card declined")
    return Charge(captured=True)
