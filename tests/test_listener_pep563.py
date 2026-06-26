"""Regression test for D7: @listener under PEP 563.

@listener reads the event type from the listener's parameter annotation at
decoration time. With ``from __future__ import annotations`` active — which
modulith's own modules all use, and which is a near-ubiquitous modern idiom —
that annotation is a *string* at runtime, not the class. The decorator must
resolve it to the real class (via typing.get_type_hints over the function's
module globals) so the listener registers under the event TYPE. publish()
routes by the event's actual type, so a string registration key would silently
never match and the listener would never fire.

This module intentionally does NOT use ``from __future__ import annotations``
itself; the PEP 563 behaviour under test lives inside the exec'd user-module
source, whose functions therefore carry stringized annotations exactly as a
real user's module would.
"""

import pytest

from modulith.runtime import _runtime

# A user module that opts into PEP 563, with the event class and listener at
# module level (real-world shape — get_type_hints resolves names via globals).
_USER_MODULE_SRC = (
    "from __future__ import annotations\n"
    "from dataclasses import dataclass\n"
    "from modulith import event, listener\n"
    "\n"
    "@event\n"
    "@dataclass(frozen=True)\n"
    "class OrderCreated:\n"
    "    order_id: str\n"
    "\n"
    "@listener\n"
    "def on_order(evt: OrderCreated) -> None:\n"
    "    pass\n"
)


@pytest.fixture(autouse=True)
def _reset_runtime():
    _runtime._reset_for_testing()
    yield
    _runtime._reset_for_testing()


def test_listener_resolves_pep563_string_annotation() -> None:
    """A PEP 563 listener registers under the resolved class, not the string."""
    ns: dict[str, object] = {}
    exec(compile(_USER_MODULE_SRC, "<pep563_user_module>", "exec"), ns)

    pending = _runtime._pending_listeners
    assert pending, "listener did not register"
    event_type, _handler = pending[-1]

    assert not isinstance(event_type, str), (
        f"@listener registered under the string {event_type!r} (PEP 563 not "
        "resolved) — publish() routes by type, so this listener never fires"
    )
    assert event_type is ns["OrderCreated"], (
        f"@listener registered under {event_type!r}, not the resolved class"
    )
