"""FastAPI router for the orders module.

Mounted by ``shop.main`` with ``prefix="/orders"`` in single-process mode, and
by ``modulith._worker`` under the same ``/orders`` prefix in process-per-module
mode (it mounts every module's ``router`` under ``/<module_name>``). The route
below is declared at the router root (``""``) precisely so both mounts produce
the same final URL: ``POST /orders``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from shop.orders import place_order

router = APIRouter()


class PlaceOrderRequest(BaseModel):
    customer_id: str
    total: float


async def get_session(request: Request) -> AsyncIterator[Any]:
    """Provide a bound outbox session, or ``None`` in the zero-config default.

    ``request.app.state.sessionmaker`` is ``None`` unless ``shop.main``'s
    lifespan wired the durable outbox (``MODULITH_OUTBOX`` != "memory" or
    ``MODULITH_DB_URL`` set). When it's set, this dependency binds a session
    to the outbox's context var for the lifetime of the request and always
    resets the binding. FastAPI runs a request-scoped ``yield`` dependency's
    teardown after the response has been sent, so a commit there would
    acknowledge the order before knowing it persisted: the route commits the
    order itself (see ``post_order``). The commit after ``yield`` only covers
    publishes made after the route's commit, such as from ``BackgroundTasks``,
    and is skipped when the route raises.

    Typed ``Any`` (rather than ``AsyncSession | None``) deliberately: FastAPI
    resolves this callable's annotations via forward-ref evaluation against its
    own module globals at route-registration time, so a ``TYPE_CHECKING``-only
    import would raise ``NameError`` there — not just on the default in-memory
    path this dependency exists to keep sqlalchemy off of.
    """
    sessionmaker = getattr(request.app.state, "sessionmaker", None)
    if sessionmaker is None:
        yield None
        return

    from modulith.adapters.postgres_outbox import bind_session, unbind_session

    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            yield session
            await session.commit()
        finally:
            unbind_session(token)


# Module-level singleton so the dependency is constructed once, not on every
# request (and so ruff's B008 — "no function calls in argument defaults" —
# doesn't fire on Depends(get_session) below).
_session_dependency = Depends(get_session)


@router.post("")
async def post_order(req: PlaceOrderRequest, session: Any = _session_dependency) -> dict[str, str]:
    """Place an order; the event chain fans out to the other modules.

    In durable mode the commit runs here, on the still-bound session, so a
    failed commit becomes an error response instead of a 200 for an order
    that was rolled back, and the outbox's after-commit dispatch still fires.
    """
    order_id = await place_order(customer_id=req.customer_id, total=req.total, session=session)
    if session is not None:
        await session.commit()
    return {"order_id": order_id}
