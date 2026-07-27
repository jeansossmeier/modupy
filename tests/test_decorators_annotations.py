"""Regression: @listener must resolve PEP 563 string annotations.

Under ``from __future__ import annotations`` (PEP 563) — the modern default
used across this very project and shown throughout the SPEC/README examples —
a listener's event-parameter annotation is stored as a *string*
(``'OrderShipped'``), not the class object. ``@listener`` must resolve that
string back to the class so the event bus can route by ``type(event)``.

Without resolution, the string flows into ``InMemoryEventBus.register`` and
crashes on ``event_type.__name__`` (``str`` has no ``__name__``), AND dispatch
would never match because the bus keys on the real class. These tests pin the
resolution behavior for both the async and sync listener paths.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.asyncio
async def test_async_listener_resolves_pep563_string_annotation(make_fake_app) -> None:
    """A module using PEP 563 registers and dispatches by the resolved class."""
    make_fake_app(
        {
            "orders": """
                from __future__ import annotations

                from dataclasses import dataclass

                from modulith import event, listener, publish

                @event
                @dataclass(frozen=True)
                class OrderShipped:
                    order_id: str

                received: list[OrderShipped] = []

                @listener
                async def on_shipped(event: OrderShipped) -> None:
                    received.append(event)

                async def ship(order_id: str) -> None:
                    await publish(OrderShipped(order_id=order_id))
            """,
        }
    )

    from modulith.runtime import _runtime

    _runtime.configure(package="fakeapp")
    # Bootstrap imports the module, firing @listener. Under the current bug the
    # string annotation flows into register() and raises AttributeError here.
    _runtime.ensure_bootstrapped()

    from fakeapp.orders import (  # type: ignore[import-not-found]
        OrderShipped,
        on_shipped,
        received,
        ship,
    )

    assert _runtime.event_bus is not None
    # Registered under the resolved CLASS, not the string "OrderShipped".
    assert on_shipped in _runtime.event_bus.listeners_for(OrderShipped)

    await ship("o-42")
    assert len(received) == 1
    assert received[0].order_id == "o-42"


@pytest.mark.asyncio
async def test_sync_listener_resolves_pep563_string_annotation(make_fake_app) -> None:
    """The sync-listener path resolves string annotations too (wrapped handler)."""
    make_fake_app(
        {
            "billing": """
                from __future__ import annotations

                from dataclasses import dataclass

                from modulith import event, listener, publish

                @event
                @dataclass(frozen=True)
                class Invoiced:
                    amount: int

                seen: list[int] = []

                @listener
                def on_invoiced(event: Invoiced) -> None:  # sync listener
                    seen.append(event.amount)

                async def invoice(amount: int) -> None:
                    await publish(Invoiced(amount=amount))
            """,
        }
    )

    from modulith.runtime import _runtime

    _runtime.configure(package="fakeapp")
    _runtime.ensure_bootstrapped()

    from fakeapp.billing import (  # type: ignore[import-not-found]
        Invoiced,
        invoice,
        seen,
    )

    assert _runtime.event_bus is not None
    # One wrapped handler registered under the resolved class.
    assert len(_runtime.event_bus.listeners_for(Invoiced)) == 1

    await invoice(99)
    assert seen == [99]


def test_unresolvable_string_annotation_raises_clear_error(make_fake_app) -> None:
    """A string annotation that names nothing resolvable gives a clear TypeError.

    PEP 563 stores the annotation as a string; if the name cannot be resolved
    against the function's globals (e.g. an event type defined in local scope),
    modulith cannot route it. The failure must be an explanatory TypeError, not
    an opaque ``AttributeError: 'str' object has no attribute '__name__'``.

    The error surfaces at *import* time — ``@listener`` runs when the module is
    imported, which is what a developer hits running their app. (Bulk discovery
    deliberately catches and logs per-module import failures rather than
    aborting the whole walk — see ``builtin.discovery`` and
    ``tests/test_discovery.py``; so this contract is pinned at the import layer
    where it actually fires, not through ``ensure_bootstrapped``.)
    """
    pkg = make_fake_app(
        {
            "broken": """
                from __future__ import annotations

                from modulith import listener

                @listener
                async def on_ghost(event: NeverDefinedEvent) -> None:  # noqa: F821
                    ...
            """,
        }
    )

    with pytest.raises(TypeError, match="could not resolve"):
        importlib.import_module(f"{pkg}.broken")


@pytest.mark.asyncio
async def test_type_checking_only_annotation_on_another_parameter_is_tolerated(
    make_fake_app,
) -> None:
    """Only the EVENT parameter's annotation is resolved.

    Resolving the whole annotation dict (``inspect.get_annotations(eval_str=
    True)``) evaluated the return type and every other parameter too, so a
    ``TYPE_CHECKING``-only import on a second parameter — the framework's own
    sanctioned way to break a runtime import cycle, see ``builtin.verifier`` —
    raised NameError and was reported as an unresolvable *event* annotation.
    That blamed the wrong parameter AND rejected, at import time, a listener
    the bus invokes with the event alone and which runs perfectly well.
    """
    make_fake_app(
        {
            "orders": """
                from __future__ import annotations

                from dataclasses import dataclass
                from typing import TYPE_CHECKING

                from modulith import event, listener, publish

                if TYPE_CHECKING:
                    from nowhere.at.all import Session

                @event
                @dataclass(frozen=True)
                class Ping:
                    n: int

                seen: list[int] = []

                @listener
                async def on_ping(event: Ping, session: Session | None = None) -> None:
                    seen.append(event.n)

                async def ping(n: int) -> None:
                    await publish(Ping(n=n))
            """,
        }
    )

    from modulith.runtime import _runtime

    _runtime.configure(package="fakeapp")
    _runtime.ensure_bootstrapped()

    from fakeapp.orders import (  # type: ignore[import-not-found]
        Ping,
        on_ping,
        ping,
        seen,
    )

    assert _runtime.event_bus is not None
    assert on_ping in _runtime.event_bus.listeners_for(Ping)

    await ping(7)
    assert seen == [7]
