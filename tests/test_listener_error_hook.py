"""A raising modulith_on_listener_error hookimpl (A1-r1-1)
must never mask the listener's own exception or skip the paired
modulith_on_listener_complete call.

Two dispatch paths fire these hooks — the in-memory path
(``Runtime._dispatch_with_hooks``) and the durable outbox path
(``outbox._dispatch_publication``). Both are protected at the origin: the
plugin manager's ``_ObserveContractShield`` wraps the listener lifecycle
hooks and swallows hookimpl exceptions, so every call site (including future
ones) honors the hookspec's observe-only contract. These tests pin that
guarantee end-to-end through both paths.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from modulith import EventPublication, configure, event, hookimpl, listener
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Each test gets a clean runtime + manifest registry + outbox state."""
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


# Module-level so the serializer can resolve the class from its
# fully-qualified name on the durable path.
@event
@dataclass(frozen=True)
class Boom:
    x: int


class _RaisingErrorHookPlugin:
    """Observability plugin whose error hook itself blows up (e.g. a dead
    alerting webhook) — the exact scenario the hookspec says must be
    swallowed."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.complete_exceptions: list[BaseException | None] = []

    @hookimpl
    def modulith_on_listener_error(
        self, event: Any, listener_name: str, publication: Any, exception: Any
    ) -> None:
        self.calls.append("error")
        raise RuntimeError("alerting webhook failed")

    @hookimpl
    def modulith_on_listener_complete(
        self, event: Any, listener_name: str, publication: Any, exception: Any
    ) -> None:
        self.calls.append("complete")
        self.complete_exceptions.append(exception)


async def test_raising_error_hook_preserves_original_and_fires_complete() -> None:
    """A1-r1-1 (in-memory path): the listener's ValueError propagates — not
    the hook's RuntimeError — and modulith_on_listener_complete still fires,
    carrying the listener's own exception."""
    plugin = _RaisingErrorHookPlugin()
    configure(package="hooktest", auto_discover=False)
    _runtime._extra_plugins.append(plugin)

    @listener
    async def explode(evt: Boom) -> None:
        raise ValueError("kaboom-original")

    with pytest.raises(ValueError, match="kaboom-original"):
        await _runtime.publish(Boom(x=1))

    assert plugin.calls == ["error", "complete"]
    assert len(plugin.complete_exceptions) == 1
    assert isinstance(plugin.complete_exceptions[0], ValueError)


async def test_raising_error_hook_does_not_escape_durable_dispatch() -> None:
    """A1-r1-1 (durable-path twin site, outbox._dispatch_publication): a
    raising error hookimpl must not propagate out of the after-commit
    dispatch, must not skip modulith_on_listener_complete, and must not
    prevent the failed attempt from being recorded for retry."""
    plugin = _RaisingErrorHookPlugin()
    configure(package="hooktest", auto_discover=False)
    _runtime._extra_plugins.append(plugin)
    _runtime.ensure_bootstrapped()

    @listener
    async def explode(evt: Boom) -> None:
        raise ValueError("kaboom-original")

    saved: list[EventPublication] = []

    class Store:
        async def save(self, publication: EventPublication) -> None:
            saved.append(publication)

        async def mark_complete(self, publication_id: Any) -> None:
            pass

        async def find_incomplete(self, older_than: Any) -> list[EventPublication]:
            return []

        async def archive(self, publication_id: Any) -> None:
            pass

        async def delete(self, publication_id: Any) -> None:
            pass

    serializer = JsonEventSerializer()
    outbox.configure(Store(), serializer, start_loop=False)

    bus = _runtime.event_bus
    assert bus is not None
    (handler,) = bus.listeners_for(Boom)
    pub = EventPublication(
        id=uuid4(),
        payload=serializer.serialize(Boom(x=2)),
        event_type=f"{Boom.__module__}.{Boom.__qualname__}",
        listener=outbox._listener_id(handler),
        published_at=datetime.now(UTC),
    )

    # Must not raise: the hook's RuntimeError is swallowed by the shield.
    await outbox._dispatch_publication(pub)

    assert plugin.calls == ["error", "complete"]
    assert isinstance(plugin.complete_exceptions[0], ValueError)
    # The failed attempt was still recorded (retry-loop food).
    assert pub.attempt_count == 1
    assert pub.last_error is not None
    assert "kaboom-original" in pub.last_error
    assert saved and saved[0] is pub
