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
from modulith.runtime import _runtime


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
    @event
    @dataclass(frozen=True)
    class E:
        pass

    @listener
    async def handler(evt: E) -> None:
        pass

    assert _runtime._pending_listeners or _runtime._event_bus is not None


def test_listener_accepts_wrapped_async_function() -> None:
    """Regression test for B1: @functools.wraps-decorated async listeners
    must register correctly even though the wrapper hides the coroutine
    nature from naive isinstance/iscoroutinefunction checks."""

    @event
    @dataclass(frozen=True)
    class E:
        pass

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

    @event
    @dataclass(frozen=True)
    class E:
        pass

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

    @event
    @dataclass(frozen=True)
    class E:
        pass

    @listener
    def handler(evt: E) -> None:
        pass

    # Returns the original sync function (not the async wrapper).
    assert callable(handler)
    assert not inspect.iscoroutinefunction(handler)


def test_listener_accepts_wrapped_sync_function() -> None:
    """A sync wrapper around a sync target is accepted (executor dispatch)."""

    @event
    @dataclass(frozen=True)
    class E:
        pass

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
