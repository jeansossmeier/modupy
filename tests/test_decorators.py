"""Decorator behavior tests.

Covers @event and @listener edge cases that aren't exercised by the
zero-config integration tests:
  - acceptance paths (async, sync, functools.wraps chains)
  - rejection paths (missing arg, missing annotation)
  - decorator-composition paths (functools.wraps, lru_cache, custom wrappers)

The runtime singleton is reset before each test so listener registrations
don't leak between cases.
"""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass

import pytest

from modulith import event, listener
from modulith.manifest import Manifest, verify_manifest
from modulith.runtime import _runtime


# Event types are defined at MODULE scope on purpose. This file uses
# ``from __future__ import annotations`` (PEP 563), so a listener's event
# annotation reaches @listener as the *string* "E" — @listener resolves it
# against the function's module globals. A class defined in local (function)
# scope would be invisible to that resolution; module scope is the supported
# pattern for PEP 563 (see tests/test_sync.py for the no-future-import
# alternative used with local-scope events).
@event
@dataclass(frozen=True)
class E:
    pass


@pytest.fixture(autouse=True)
def _reset_runtime() -> None:
    """Each test gets a clean runtime so listener registrations don't leak."""
    _runtime._reset_for_testing()
    yield
    _runtime._reset_for_testing()


# ---------------------------------------------------------------------------
# @event
# ---------------------------------------------------------------------------


def test_event_marks_class() -> None:
    @event
    @dataclass(frozen=True)
    class OrderCreated:
        order_id: str

    assert OrderCreated.__modulith_event__ is True


# ---------------------------------------------------------------------------
# @listener — happy paths
# ---------------------------------------------------------------------------


def test_listener_accepts_async_function() -> None:
    @listener
    async def handler(evt: E) -> None:
        pass

    assert _runtime._pending_listeners or _runtime._event_bus is not None
    registered = _runtime._pending_listeners[0][1]
    assert registered.__modulith_broker_targets__ == ()


def test_listener_accepts_parameterized_broker_targets() -> None:
    @listener(
        broker_targets=[
            "redis-streams:orders",
            "amqp:events:orders",
        ]
    )
    async def handler(evt: E) -> None:
        pass

    registered = _runtime._pending_listeners[0][1]
    assert registered is handler
    assert registered.__modulith_broker_targets__ == (
        "redis-streams:orders",
        "amqp:events:orders",
    )


def test_listener_accepts_wrapped_async_function() -> None:
    """Regression test for B1: @functools.wraps-decorated async listeners
    must register correctly even though the wrapper hides the coroutine
    nature from naive isinstance/iscoroutinefunction checks."""

    def timing(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            return await f(*args, **kwargs)

        return wrapped

    @listener
    @timing
    async def handler(evt: E) -> None:
        pass

    # If the bug is present, the line above raises TypeError before reaching
    # this assertion. Reaching here means @listener accepted the wrapped form.
    assert callable(handler)


def test_listener_accepts_double_wrapped_async_function() -> None:
    """Multiple decorator layers (each preserving __wrapped__) must work."""

    def deco_one(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            return await f(*args, **kwargs)

        return wrapped

    def deco_two(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            return await f(*args, **kwargs)

        return wrapped

    @listener
    @deco_one
    @deco_two
    async def handler(evt: E) -> None:
        pass

    assert callable(handler)


def test_listener_accepts_sync_function() -> None:
    """T1.2.4: sync functions with a valid event annotation are now accepted."""

    @listener
    def handler(evt: E) -> None:
        pass

    # Returns the original sync function (not the async wrapper).
    assert callable(handler)
    assert not inspect.iscoroutinefunction(handler)


def test_listener_accepts_wrapped_sync_function() -> None:
    """A sync wrapper around a sync target is accepted (executor dispatch)."""

    def naive_wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def sync_target(evt: E) -> None:
        pass

    # Must not raise — sync listeners are now accepted.
    result = listener(naive_wrap(sync_target))
    assert callable(result)


@pytest.mark.asyncio
async def test_sync_wrapper_around_async_listener_registers_async_adapter() -> None:
    received: list[E] = []

    def sync_wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @listener(broker_targets=("redis-streams:orders",))
    @sync_wrap
    async def handler(evt: E) -> None:
        received.append(evt)

    registered = _runtime._pending_listeners[0][1]
    assert not inspect.iscoroutinefunction(handler)
    assert inspect.iscoroutinefunction(registered)
    assert registered.__modulith_broker_targets__ == ("redis-streams:orders",)

    event_instance = E()
    await registered(event_instance)
    assert received == [event_instance]


def test_sync_wrapper_around_async_listener_preserves_manifest_identity() -> None:
    def sync_wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @listener
    @sync_wrap
    async def handler(evt: E) -> None:
        pass

    registered = _runtime._pending_listeners[0][1]
    manifest = Manifest(package="not.importable", listeners=(handler,))

    assert verify_manifest(manifest, {registered}) == []


@pytest.mark.parametrize(
    "broker_targets",
    [
        "redis-streams:orders",
        ("",),
        ("redis-streams",),
        (":orders",),
        ("redis-streams:",),
        (1,),
    ],
)
def test_listener_rejects_invalid_broker_targets(broker_targets: object) -> None:
    with pytest.raises(TypeError, match="broker_targets"):

        @listener(broker_targets=broker_targets)  # type: ignore[arg-type]
        async def handler(evt: E) -> None:
            pass


# ---------------------------------------------------------------------------
# @listener — rejection paths
# ---------------------------------------------------------------------------


def test_listener_rejects_missing_event_arg() -> None:
    with pytest.raises(TypeError, match="must accept an event argument"):

        @listener
        async def handler() -> None:
            pass


def test_listener_rejects_unannotated_event_arg() -> None:
    with pytest.raises(TypeError, match="annotate"):

        @listener
        async def handler(evt) -> None:  # type: ignore[no-untyped-def]
            pass
