"""Behavioral tests for the public ``modulith.bootstrap()`` API and the
shape of the ``modulith`` top-level namespace.

``bootstrap()`` exists because *lazy* bootstrap alone is not enough: it is
triggered implicitly by the first ``@listener`` registration or ``publish()``
call (``runtime.py``'s ``ensure_bootstrapped()``, reached privately by
``_worker.py``), so an embedding app that wants the outbox crash-recovery
sweep to dispatch at startup would have no public trigger. The sweep skips
every row for the cycle while the runtime is un-bootstrapped (``event_bus``
is ``None`` — see ``modulith/builtin/outbox.py``'s ``_sweep`` guard, pinned
by
``tests/test_outbox_wire_format.py::test_sweep_skips_rows_without_attempt_bookkeeping_when_unbootstrapped``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from modulith import EventPublication, configure, event
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Each test gets a clean runtime + manifest registry."""
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


@event
@dataclass(frozen=True)
class RecoveredEvent:
    value: int


received: list[int] = []


def test_publish_sync_timeout_is_exported_without_future_receipt_type() -> None:
    import modulith
    from modulith.sync import PublishSyncTimeout

    assert modulith.PublishSyncTimeout is PublishSyncTimeout
    assert "PublishSyncTimeout" in modulith.__all__
    assert "EventPublishReceipt" not in modulith.__all__


def test_no_public_attribute_escapes_the_declared_api() -> None:
    """Every public (non-underscore) attribute of ``modulith`` must be in
    ``__all__``. A stray one — an unaliased stdlib import, say — shows up in
    ``dir(modulith)`` and editor completion beside real modulith names, so a
    user writes ``except modulith.SomethingError:`` around a modulith call and
    gets a handler that can never fire."""
    import types as _types

    import modulith

    leaked = sorted(
        name
        for name, value in vars(modulith).items()
        if not name.startswith("_")
        and not isinstance(value, _types.ModuleType)
        and name not in modulith.__all__
    )

    assert leaked == []


async def record(recovered: RecoveredEvent) -> None:
    received.append(recovered.value)


class StubStore:
    """Minimal in-memory PublicationStore — enough to drive a sweep."""

    def __init__(self) -> None:
        self.rows: dict[UUID, EventPublication] = {}

    async def save(self, publication: EventPublication) -> None:
        self.rows[publication.id] = publication

    async def mark_complete(self, publication_id: UUID) -> None:
        row = self.rows.get(publication_id)
        if row is not None:
            row.completed_at = datetime.now(UTC)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        now = datetime.now(UTC)
        return [
            p
            for p in self.rows.values()
            if p.completed_at is None
            and p.published_at is not None
            and (now - p.published_at) >= older_than
        ]

    async def archive(self, publication_id: UUID) -> None:
        pass

    async def delete(self, publication_id: UUID) -> None:
        pass


def test_bootstrap_is_exported_and_bootstraps_the_runtime_eagerly() -> None:
    """bootstrap() is a public top-level export that flips the same
    observable state ensure_bootstrapped() does — without requiring a
    @listener registration or publish() call first."""
    from modulith import bootstrap

    assert _runtime._bootstrapped is False
    assert _runtime.event_bus is None

    bootstrap()

    assert _runtime._bootstrapped is True
    assert _runtime.event_bus is not None


def test_bootstrap_is_idempotent(make_fake_app: Any) -> None:
    """A second call is a safe no-op — no re-discovery, no exception —
    exactly like calling ensure_bootstrapped() twice."""
    from modulith import bootstrap

    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderCreated:
                    order_id: str
            """
        }
    )
    configure(package="fakeapp")

    bootstrap()
    modules_after_first = _runtime.modules
    bootstrap()  # must not raise, must not re-run discovery

    assert _runtime.modules == modules_after_first
    assert len(_runtime.modules) == 1


async def test_bootstrap_eagerly_unblocks_crash_recovery_sweep_before_any_publish() -> None:
    """The confirmed gap: an app that calls bootstrap() at startup — before
    any publish() — gets a live event_bus immediately, so a publication a
    PREVIOUS (crashed) process left incomplete is actually retried by the
    crash-recovery sweep instead of being skipped for the whole cycle (the
    un-bootstrapped counterpart is pinned by test_outbox_wire_format.py's
    test_sweep_skips_rows_without_attempt_bookkeeping_when_unbootstrapped)."""
    from modulith import bootstrap

    received.clear()
    configure(package="bootstraptest", auto_discover=False)
    store = StubStore()
    stale = EventPublication(
        id=uuid4(),
        payload=JsonEventSerializer().serialize(RecoveredEvent(value=42)),
        event_type=f"{RecoveredEvent.__module__}.{RecoveredEvent.__qualname__}",
        listener=outbox._listener_id(record),
        published_at=datetime.now(UTC) - timedelta(seconds=5),
    )
    store.rows[stale.id] = stale
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    # No publish() has happened yet in this process — the runtime is cold.
    assert _runtime.event_bus is None

    bootstrap()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(RecoveredEvent, record)

    await outbox._sweep(timedelta(0))

    assert received == [42]
    assert stale.completed_at is not None
