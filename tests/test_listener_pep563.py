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

import functools
from typing import Any

import pytest

from modulith import listener
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


_CALLABLE_FORMS_SRC = (
    "from __future__ import annotations\n"
    "from dataclasses import dataclass\n"
    "from modulith import event\n"
    "\n"
    "@event\n"
    "@dataclass(frozen=True)\n"
    "class OrderCreated:\n"
    "    order_id: str\n"
    "\n"
    "async def handler(prefix: str, evt: OrderCreated) -> None:\n"
    "    pass\n"
    "\n"
    "class SyncCallable:\n"
    "    def __call__(self, evt: OrderCreated) -> None:\n"
    "        pass\n"
    "\n"
    "class Unresolvable:\n"
    "    def __call__(self, evt: NoSuchEvent) -> None:\n"
    "        pass\n"
    "\n"
    "class Unannotated:\n"
    "    def __call__(self, evt) -> None:\n"
    "        pass\n"
    "\n"
    "class NoParams:\n"
    "    def __call__(self) -> None:\n"
    "        pass\n"
)


def _forms_namespace() -> dict[str, Any]:
    ns: dict[str, Any] = {}
    exec(compile(_CALLABLE_FORMS_SRC, "<pep563_forms_module>", "exec"), ns)
    return ns


def test_listener_accepts_partial_of_async_function() -> None:
    """A partial has no __globals__; they come from the wrapped function."""
    ns = _forms_namespace()
    single = functools.partial(ns["handler"], "x")
    nested = functools.partial(functools.partial(ns["handler"], "x"))
    for target in (single, nested):
        _runtime._reset_for_testing()
        listener(target)
        event_type, registered = _runtime._pending_listeners[-1]
        assert event_type is ns["OrderCreated"]
        assert registered is target


def test_listener_accepts_sync_callable_instance() -> None:
    """A sync callable instance reads globals from its class's __call__."""
    ns = _forms_namespace()
    listener(ns["SyncCallable"]())
    event_type, _registered = _runtime._pending_listeners[-1]
    assert event_type is ns["OrderCreated"]


def test_listener_accepts_partial_of_sync_callable_instance() -> None:
    """The partial chain ends at a callable instance, whose class supplies globals."""
    ns = _forms_namespace()
    listener(functools.partial(ns["SyncCallable"]()))
    event_type, _registered = _runtime._pending_listeners[-1]
    assert event_type is ns["OrderCreated"]


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("Unresolvable", "could not resolve that name"),
        ("Unannotated", "must annotate its event"),
        ("NoParams", "must accept an event argument"),
    ],
)
def test_registration_errors_for_object_without_qualname_are_type_errors(
    name: str, message: str
) -> None:
    """The error path formats a name even when the target has no __qualname__."""
    instance = _forms_namespace()[name]()
    assert not hasattr(instance, "__qualname__")
    with pytest.raises(TypeError, match=message):
        listener(instance)
