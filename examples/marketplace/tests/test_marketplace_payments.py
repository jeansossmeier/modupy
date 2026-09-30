import pytest
from modulith.testing import ModulithTestApp


async def test_a_capturing_token_is_charged_and_captured_once_even_when_redelivered(
    marketplace: ModulithTestApp, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlalchemy import select

    from marketplace.contracts import PaymentCaptured, PaymentRequested
    from marketplace.db import engine
    from marketplace.payments._internal import gateway
    from marketplace.payments.handlers import on_payment_requested
    from marketplace.payments.tables import payment

    charges: list[tuple[str, int]] = []
    real_charge = gateway.charge

    def counting_charge(card_token: str, amount_cents: int) -> gateway.Charge:
        charges.append((card_token, amount_cents))
        return real_charge(card_token, amount_cents)

    monkeypatch.setattr(gateway, "charge", counting_charge)
    requested = PaymentRequested(order_id="o-1", amount_cents=2400, card_token="tok_visa")

    await on_payment_requested(requested)
    await on_payment_requested(requested)

    async with engine().connect() as connection:
        rows = (await connection.execute(select(payment))).all()
    assert charges == [("tok_visa", 2400)]
    assert marketplace.published_events == [PaymentCaptured(order_id="o-1", amount_cents=2400)]
    assert [(row.order_id, row.amount_cents, row.status, row.reason) for row in rows] == [
        ("o-1", 2400, "captured", None)
    ]


async def test_a_declined_token_is_recorded_and_declined_once_even_when_redelivered(
    marketplace: ModulithTestApp,
) -> None:
    from sqlalchemy import select

    from marketplace.contracts import PaymentDeclined, PaymentRequested
    from marketplace.db import engine
    from marketplace.payments.handlers import on_payment_requested
    from marketplace.payments.tables import payment

    requested = PaymentRequested(order_id="o-2", amount_cents=2400, card_token="tok_declined")

    await on_payment_requested(requested)
    await on_payment_requested(requested)

    async with engine().connect() as connection:
        rows = (await connection.execute(select(payment))).all()
    assert marketplace.published_events == [PaymentDeclined(order_id="o-2", reason="card declined")]
    assert [(row.order_id, row.status, row.reason) for row in rows] == [
        ("o-2", "declined", "card declined")
    ]


def test_the_gateway_declines_only_the_declining_token(modulith_app: ModulithTestApp) -> None:
    from marketplace.payments._internal.gateway import Charge, charge

    assert charge("tok_declined", 100) == Charge(captured=False, reason="card declined")
    assert charge("tok_visa", 100) == Charge(captured=True)
