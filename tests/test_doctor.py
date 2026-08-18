"""Tests for `modulith doctor` — the operational/architectural health check.

``run_doctor()`` bootstraps the app and runs six independent checks against
*real* runtime state (real discovery, real AST scanning of the discovered
modules' source, the real outbox API). Each check is exercised through the
public ``run_doctor()`` entrypoint against a fake app shaped to trigger the
condition under test, then the specific HealthCheck is pulled out by name and
asserted on. The CLI wiring is verified through typer's ``CliRunner``.

The checks are pure-ish but two touch process/global state: schema drift writes
``.modulith-schemas.json`` into the cwd (isolated per-test by ``make_fake_app``
chdir'ing into ``tmp_path``), and outbox health calls ``asyncio.run`` (which
clears the thread's current loop — restored on teardown, mirroring test_cli.py).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from typer.testing import CliRunner

from modulith import EventPublication, configure
from modulith.adapters._shm_ring import ShmRing
from modulith.builtin import outbox
from modulith.cli import app
from modulith.doctor import (
    HealthCheck,
    HealthReport,
    render_report,
    run_doctor,
)
from modulith.serializers import JsonEventSerializer

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_state():
    """Isolate manifest + outbox module globals around each test.

    ``make_fake_app`` resets the runtime singleton but not the manifest
    registry or the outbox plugin's globals. The outbox check calls
    ``asyncio.run`` (in the wired-store path), which leaves the thread with no
    current event loop — harmless in production (one process per command) but
    poisonous to later sync tests under pytest, so re-establish a fresh loop on
    teardown.
    """
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    outbox._reset_for_testing()
    yield
    manifest_module._reset_for_testing()
    outbox._reset_for_testing()
    asyncio.set_event_loop(asyncio.new_event_loop())


def _check(report: HealthReport, name: str) -> HealthCheck:
    for c in report.checks:
        if c.name == name:
            return c
    raise AssertionError(f"no check named {name!r}; have {[c.name for c in report.checks]}")


class _DeadLetterStore:
    """Minimal PublicationStore exposing only what ``outbox.status()`` reads."""

    def __init__(self, pubs: list[EventPublication]) -> None:
        self.pubs = {p.id: p for p in pubs}

    async def save(self, publication: EventPublication) -> None:  # pragma: no cover - unused
        self.pubs[publication.id] = publication

    async def mark_complete(self, publication_id: UUID) -> None:  # pragma: no cover - unused
        pass

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return [p for p in self.pubs.values() if p.completed_at is None]

    async def archive(self, publication_id: UUID) -> None:  # pragma: no cover - unused
        pass

    async def delete(self, publication_id: UUID) -> None:  # pragma: no cover - unused
        pass

    async def count_completed(self) -> int:
        return sum(1 for p in self.pubs.values() if p.completed_at is not None)


class _CountStore:
    """Store that reports counts via the unbounded count_* capabilities only.

    ``find_incomplete`` returns [] so any check still reading it for counts
    would see zero — proving doctor uses count_open/count_dead_lettered.
    """

    def __init__(self, *, open_: int, dead: int, completed: int = 0) -> None:
        self._open, self._dead, self._completed = open_, dead, completed

    async def save(self, publication: EventPublication) -> None:  # pragma: no cover - unused
        pass

    async def mark_complete(self, publication_id: UUID) -> None:  # pragma: no cover - unused
        pass

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return []

    async def archive(self, publication_id: UUID) -> None:  # pragma: no cover - unused
        pass

    async def delete(self, publication_id: UUID) -> None:  # pragma: no cover - unused
        pass

    async def count_open(self) -> int:
        return self._open

    async def count_dead_lettered(self) -> int:
        return self._dead

    async def count_completed(self) -> int:
        return self._completed


# ---------------------------------------------------------------------------
# Boundary health
# ---------------------------------------------------------------------------


def test_boundary_health_ok_for_independent_modules(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})
    configure(package="fakeapp")

    report = run_doctor()

    assert _check(report, "boundary health").status == "ok"


def test_boundary_health_error_on_internal_import(make_fake_app) -> None:
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "boundary health")
    assert check.status == "error"
    assert any("private" in d for d in check.details)


# ---------------------------------------------------------------------------
# Process-split readiness
# ---------------------------------------------------------------------------


def test_split_readiness_high_when_event_driven(make_fake_app) -> None:
    # A single self-contained module that publishes and has no cross-module
    # imports → 100% of interactions are event-shaped.
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """
        }
    )
    configure(package="fakeapp")

    report = run_doctor()

    assert _check(report, "process-split readiness").status == "ok"


def test_split_readiness_warns_on_direct_coupling(make_fake_app) -> None:
    # orders reaches directly into inventory's public API (no events at all).
    # Readiness is an informational maturity metric (SPEC/
    # MIGRATION_GUIDE frame it as "are you ready to split?"), so a low score
    # caps at "warn" — it must never fail the doctor CI gate on its own.
    make_fake_app({"orders": "from fakeapp.inventory import thing\n", "inventory": "thing = 1\n"})
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "process-split readiness")
    assert check.status == "warn"
    assert any("orders" in d for d in check.details)


def test_split_readiness_shared_tables_block_microservice_tier(make_fake_app) -> None:
    # A ForeignKey into a table owned by another module is cross-module
    # coupling the import/publish ratio can't see on its own — at 100%
    # event-driven the report would otherwise call this "microservice-ready"
    # while a shared table still blocks a clean extraction.
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish
                import sqlalchemy as sa

                customer_id = sa.Column(sa.ForeignKey("customers.id"))

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                async def place(order_id: str) -> None:
                    await publish(OrderPlaced(order_id=order_id))
            """,
            "customers": "",
        },
        extra_files={
            "customers/_manifest.py": """
                from modulith.manifest import declare_module

                declare_module(owns_tables=["customers"])
            """
        },
    )
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "process-split readiness")
    assert check.status == "ok"
    assert "100%" in check.summary
    assert "process-split ready" in check.summary
    assert "microservice-ready" not in check.summary
    assert any("1 cross-module table reference(s)" in d for d in check.details)


def test_split_readiness_details_unprefixed_tables(make_fake_app) -> None:
    # When a module defines tables without the module prefix, the details
    # should include an informational line: "<mod>: N table(s) not prefixed <mod>_"
    make_fake_app(
        {
            "orders": """
                from sqlalchemy import Integer, String
                from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

                class Base(DeclarativeBase):
                    pass

                class OrderRow(Base):
                    __tablename__ = "order_items"  # not prefixed with orders_
                    id: Mapped[int] = mapped_column(Integer, primary_key=True)

                class OrderDetailRow(Base):
                    __tablename__ = "order_details"  # not prefixed
                    id: Mapped[int] = mapped_column(Integer, primary_key=True)
            """,
            "inventory": """
                from sqlalchemy import Integer
                from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

                class Base(DeclarativeBase):
                    pass

                class StockRow(Base):
                    __tablename__ = "inventory_stock"  # correctly prefixed
                    id: Mapped[int] = mapped_column(Integer, primary_key=True)
            """,
        }
    )
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "process-split readiness")
    # Should have details for orders with 2 unprefixed tables
    assert any("orders" in d and "2 table(s) not prefixed orders_" in d for d in check.details)
    # Should NOT have details for inventory (all tables are correctly prefixed)
    assert not any("inventory" in d and "not prefixed" in d for d in check.details)


# ---------------------------------------------------------------------------
# Schema drift
# ---------------------------------------------------------------------------


def test_schema_drift_records_baseline_on_first_run(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """
        }
    )
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "schema drift")
    assert check.status == "ok"
    assert "recorded" in check.summary
    assert Path(".modulith-schemas.json").exists()


def test_schema_drift_warns_when_event_changes(make_fake_app, tmp_path) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """
        }
    )
    configure(package="fakeapp")

    first = run_doctor()
    assert _check(first, "schema drift").status == "ok"

    # Add a field to the event — its fingerprint changes on the next scan.
    (tmp_path / "fakeapp" / "orders" / "__init__.py").write_text(
        "from dataclasses import dataclass\n"
        "from modulith import event\n\n"
        "@event\n"
        "@dataclass(frozen=True)\n"
        "class OrderPlaced:\n"
        "    order_id: str\n"
        "    total: float\n"
    )

    second = run_doctor()
    drift = _check(second, "schema drift")
    assert drift.status == "warn"
    assert any("OrderPlaced" in d for d in drift.details)


# ---------------------------------------------------------------------------
# Outbox health
# ---------------------------------------------------------------------------


def test_outbox_health_ok_in_memory_mode(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp")  # default outbox == "memory"

    report = run_doctor()

    check = _check(report, "outbox health")
    assert check.status == "ok"
    assert "outbox" in check.summary.lower()


def test_outbox_health_warns_on_dead_letters(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", outbox="postgres")
    now = datetime.now(UTC)
    dead = EventPublication(
        id=uuid4(),
        payload=b"{}",
        event_type="fakeapp.orders.Boom",
        listener="handler",
        published_at=now,
        attempt_count=10,  # >= default dead-letter threshold
    )
    outbox.configure(
        store=_DeadLetterStore([dead]),
        serializer=JsonEventSerializer(),
        start_loop=False,
    )

    report = run_doctor()

    check = _check(report, "outbox health")
    assert check.status == "warn"
    assert "dead-lettered" in check.summary


# ---------------------------------------------------------------------------
# Listener registration
# ---------------------------------------------------------------------------


def test_listener_registration_ok_when_manifest_matches(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                @listener
                async def on_order_placed(evt: OrderPlaced) -> None:
                    pass
            """
        },
        extra_files={
            "orders/_manifest.py": """
                from modulith.manifest import declare_module
                from fakeapp.orders import on_order_placed

                declare_module(listeners=[on_order_placed])
            """
        },
    )
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "listener registration")
    assert check.status == "ok"
    assert "1 declared listener" in check.summary


def test_listener_registration_error_when_declared_listener_missing(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """
        },
        extra_files={
            "orders/_manifest.py": """
                from modulith.manifest import declare_module

                async def orphan_listener(e: object) -> None:
                    pass

                declare_module(listeners=[orphan_listener])
            """
        },
    )
    # verify_manifests=False so bootstrap doesn't abort — we want doctor to
    # be the one that surfaces the unregistered listener.
    configure(package="fakeapp", verify_manifests=False)

    report = run_doctor()

    check = _check(report, "listener registration")
    assert check.status == "error"
    assert any("orphan_listener" in d for d in check.details)


# ---------------------------------------------------------------------------
# SHM notifier
# ---------------------------------------------------------------------------


class _RingBroker:
    """A Broker whose only interesting feature is the hint ring it carries.

    ``ShmBroker`` holds its notifier as ``_ring``; the doctor check duck-types
    on that rather than importing the adapter, so this stands in for it while
    the ring itself stays a real ``ShmRing``.
    """

    def __init__(self, ring: ShmRing) -> None:
        self._ring = ring

    async def publish(
        self, target: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> None:  # pragma: no cover - the doctor check never publishes
        pass

    async def close(self) -> None:
        self._ring.close()


def _register_ring(ring: ShmRing) -> None:
    """Attach a hint-ring-carrying broker to the bootstrapped runtime."""
    from modulith.runtime import _runtime

    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None
    registry.register("shm", _RingBroker(ring))


def test_shm_notifier_ok_when_no_shm_broker_is_registered(make_fake_app) -> None:
    """The check must stay silent for the (overwhelmingly common) deployments
    that use a different broker or none at all."""
    make_fake_app({"orders": ""})
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "shm notifier")
    assert check.status == "ok"
    assert "no shm broker" in check.summary


def test_shm_notifier_ok_when_the_hint_ring_attached(make_fake_app, tmp_path) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp")
    ring = ShmRing(tmp_path / "attached.hint", 64, create=True)
    assert ring.available  # premise: a genuinely live notifier
    _register_ring(ring)

    try:
        report = run_doctor()
    finally:
        ring.close()

    check = _check(report, "shm notifier")
    assert check.status == "ok"
    assert "1 hint ring(s) attached" in check.summary


def test_shm_notifier_warns_when_the_hint_ring_could_not_attach(make_fake_app, tmp_path) -> None:
    """A hint file is never re-created and attaching demands an exact capacity
    match, so raising ``shm_capacity`` on a deployment whose file already
    exists leaves the notifier permanently dead: delivery still works (SQLite
    is authoritative) but every consumer drops to its safety poll. The only
    trace used to be a single WARNING at broker construction, invisible to
    anyone looking at an already-running process."""
    make_fake_app({"orders": ""})
    configure(package="fakeapp")
    path = tmp_path / "capacity-mismatch.hint"
    existing = ShmRing(path, 64, create=True)  # the file the deployment already has
    after_capacity_change = ShmRing(path, 128, create=True)
    assert not after_capacity_change.available
    _register_ring(after_capacity_change)

    try:
        report = run_doctor()
    finally:
        after_capacity_change.close()
        existing.close()

    check = _check(report, "shm notifier")
    assert check.status == "warn"
    assert any("capacity=128" in d for d in check.details)
    assert any(str(path) in d for d in check.details)


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def test_render_report_shows_icons_details_and_overall() -> None:
    report = HealthReport(
        checks=[
            HealthCheck("boundary health", "ok", "0 violation(s)"),
            HealthCheck("schema drift", "warn", "1 changed", ["orders.OrderPlaced"]),
            HealthCheck("listener registration", "error", "1 issue(s)", ["missing handler"]),
        ]
    )

    text = render_report(report)

    assert "✓ boundary health" in text
    assert "⚠ schema drift" in text
    assert "✗ listener registration" in text
    assert "    orders.OrderPlaced" in text  # detail indented
    assert "overall: error" in text


def test_overall_status_is_worst_of_checks() -> None:
    assert HealthReport([HealthCheck("a", "ok", "")]).overall_status == "ok"
    assert (
        HealthReport([HealthCheck("a", "ok", ""), HealthCheck("b", "warn", "")]).overall_status
        == "warn"
    )
    assert (
        HealthReport([HealthCheck("a", "warn", ""), HealthCheck("b", "error", "")]).overall_status
        == "error"
    )


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------


def test_doctor_cli_runs_and_reports(make_fake_app, monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""})

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0, result.output
    assert "modulith doctor" in result.output
    assert "boundary health" in result.output


def test_doctor_cli_exits_nonzero_on_error(make_fake_app, monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 1
    assert "boundary health" in result.output


# ---------------------------------------------------------------------------
# regression: audit findings
# ---------------------------------------------------------------------------


def test_boundary_health_warns_on_warning_only_violations(make_fake_app) -> None:
    # Data-ownership WARNINGs with zero ERRORs must read as 'warn', not 'ok' —
    # warnings used to be invisible in the overall status.
    make_fake_app(
        {
            "orders": "",
            "reporting": """
                from sqlalchemy import MetaData, Table
                metadata = MetaData()
                orders_table = Table("orders", metadata)
            """,
        },
        extra_files={
            "orders/_manifest.py": """
                from modulith.manifest import declare_module
                declare_module(owns_tables=["orders"])
            """
        },
    )
    configure(package="fakeapp")

    check = _check(run_doctor(), "boundary health")
    assert check.status == "warn"
    assert "warning" in check.summary
    assert any("orders" in d for d in check.details)


def test_split_readiness_does_not_penalize_contracts_imports(make_fake_app) -> None:
    # Importing the shared contracts module is the prescribed pattern, not
    # coupling. Without the exemption this scored 0% → 'error'.
    make_fake_app(
        {
            "contracts": "from dataclasses import dataclass\n",
            "orders": "from fakeapp.contracts import dataclass\n",
        }
    )
    configure(package="fakeapp")

    check = _check(run_doctor(), "process-split readiness")
    assert check.status == "ok"
    assert "no cross-module interactions" in check.summary


def test_schema_drift_detects_default_value_change(make_fake_app, tmp_path) -> None:
    # Adding a default (required → optional) is a wire-compatibility change even
    # though the annotation text is unchanged — the fingerprint must catch it.
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """
        }
    )
    configure(package="fakeapp")
    assert _check(run_doctor(), "schema drift").status == "ok"

    (tmp_path / "fakeapp" / "orders" / "__init__.py").write_text(
        "from dataclasses import dataclass\n"
        "from modulith import event\n\n"
        "@event\n"
        "@dataclass(frozen=True)\n"
        "class OrderPlaced:\n"
        '    order_id: str = ""\n'
    )

    drift = _check(run_doctor(), "schema drift")
    assert drift.status == "warn"
    assert any("OrderPlaced" in d for d in drift.details)


def test_outbox_health_uses_unbounded_counts_for_large_backlog(make_fake_app) -> None:
    # status() must read the store's unbounded count_open, not the capped
    # find_incomplete (which returns [] here) — a real backlog → 'warn'.
    make_fake_app({"orders": ""})
    configure(package="fakeapp", outbox="postgres")
    outbox.configure(
        store=_CountStore(open_=50_000, dead=0),
        serializer=JsonEventSerializer(),
        start_loop=False,
    )

    check = _check(run_doctor(), "outbox health")
    assert check.status == "warn"
    assert "50000 incomplete" in check.summary


def test_outbox_health_errors_on_large_dead_letter_pile(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", outbox="postgres")
    outbox.configure(
        store=_CountStore(open_=0, dead=500),
        serializer=JsonEventSerializer(),
        start_loop=False,
    )

    check = _check(run_doctor(), "outbox health")
    assert check.status == "error"
    assert "500 dead-lettered" in check.summary


# ---------------------------------------------------------------------------
# regression: W2 audit fixes (G06_cli)
# ---------------------------------------------------------------------------


def test_split_readiness_ok_at_80_percent(make_fake_app) -> None:
    """MIGRATION_GUIDE documents '80%+' as split-ready — the 80%
    boundary must be inclusive 'ok', not 'warn'."""
    make_fake_app(
        {
            # 1 direct cross-module import + 4 publish calls → exactly 80%.
            "orders": (
                "from fakeapp.inventory import thing\n"
                "from modulith import publish\n\n"
                "async def go() -> None:\n" + "".join(f"    await publish({i})\n" for i in range(4))
            ),
            "inventory": "thing = 1\n",
        }
    )
    configure(package="fakeapp")

    check = _check(run_doctor(), "process-split readiness")

    assert check.status == "ok"
    assert "80%" in check.summary
    assert "process-split ready" in check.summary


def test_split_readiness_names_microservice_tier_at_95_percent(make_fake_app) -> None:
    """MIGRATION_GUIDE's 95%+ 'microservice-ready' tier must be
    visible in the report, distinct from plain 80%+ split-readiness."""
    make_fake_app(
        {
            # 1 direct cross-module import + 19 publish calls → exactly 95%.
            "orders": (
                "from fakeapp.inventory import thing\n"
                "from modulith import publish\n\n"
                "async def go() -> None:\n"
                + "".join(f"    await publish({i})\n" for i in range(19))
            ),
            "inventory": "thing = 1\n",
        }
    )
    configure(package="fakeapp")

    check = _check(run_doctor(), "process-split readiness")

    assert check.status == "ok"
    assert "95%" in check.summary
    assert "microservice-ready" in check.summary


def test_corrupt_schema_cache_is_detected_and_not_overwritten(make_fake_app) -> None:
    """A corrupt (malformed JSON) schema cache was silently treated as empty
    and immediately overwritten — the corruption evidence vanished and the
    check reported 'ok'. It must surface the corruption and leave the file
    untouched so it can be inspected."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """
        }
    )
    configure(package="fakeapp")
    cache_path = Path(".modulith-schemas.json")
    cache_path.write_text("{not valid json", encoding="utf-8")

    report = run_doctor()

    check = _check(report, "schema drift")
    assert check.status != "ok"
    assert cache_path.read_text(encoding="utf-8") == "{not valid json"


def test_removed_event_is_flagged_by_schema_drift(make_fake_app, tmp_path) -> None:
    """An event that existed in the cached baseline but no longer exists in
    the code is a schema-drift signal too (e.g. a consumer still deployed
    against the old schema) — dropping it from the current scan must not
    silently drop it from the report."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """
        }
    )
    configure(package="fakeapp")
    first = run_doctor()
    assert _check(first, "schema drift").status == "ok"

    # The event is removed entirely from the source.
    (tmp_path / "fakeapp" / "orders" / "__init__.py").write_text("", encoding="utf-8")

    second = run_doctor()
    drift = _check(second, "schema drift")
    assert drift.status == "warn"
    assert any("OrderPlaced" in d for d in drift.details)


def test_outbox_health_check_is_bounded_by_a_timeout(make_fake_app, monkeypatch) -> None:
    """An unresponsive outbox store (network partition, stalled connection
    pool) must not hang the whole ``doctor`` command forever — the query
    needs an explicit timeout that turns into a reported error."""
    import modulith.doctor as doctor_module

    class _HangingStore:
        async def save(self, publication):  # pragma: no cover - unused
            pass

        async def mark_complete(self, publication_id):  # pragma: no cover - unused
            pass

        async def find_incomplete(self, older_than):  # pragma: no cover - unused
            return []

        async def archive(self, publication_id):  # pragma: no cover - unused
            pass

        async def delete(self, publication_id):  # pragma: no cover - unused
            pass

        async def count_open(self) -> int:
            await asyncio.sleep(10)
            return 0

        async def count_dead_lettered(self) -> int:
            return 0

    monkeypatch.setattr(doctor_module, "_OUTBOX_HEALTH_TIMEOUT", 0.05)
    make_fake_app({"orders": ""})
    configure(package="fakeapp", outbox="postgres")
    outbox.configure(store=_HangingStore(), serializer=JsonEventSerializer(), start_loop=False)

    report = run_doctor()

    check = _check(report, "outbox health")
    assert check.status == "error"
    assert "timed out" in check.summary.lower()


def test_doctor_cli_passes_with_low_readiness_score(make_fake_app, monkeypatch) -> None:
    """A low readiness score is an informational maturity signal —
    it renders as 'warn' and must not fail the doctor CI gate on its own."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "from fakeapp.inventory import thing\n", "inventory": "thing = 1\n"})

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0, result.output
    assert "⚠ process-split readiness" in result.output


# ---------------------------------------------------------------------------
# Actuator token
# ---------------------------------------------------------------------------


def test_actuator_check_ok_for_single_process_topology(make_fake_app) -> None:
    """The actuator proxy only exists under topology='processes' — nothing to
    warn about for the default single-process topology."""
    make_fake_app({"orders": ""})
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "actuator token")
    assert check.status == "ok"


def test_actuator_check_ok_when_mode_is_disabled(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(
        package="fakeapp", topology="processes", broker="testbroker", actuator_mode="disabled"
    )

    report = run_doctor()

    check = _check(report, "actuator token")
    assert check.status == "ok"


def test_actuator_check_ok_when_mode_is_open(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", topology="processes", broker="testbroker", actuator_mode="open")

    report = run_doctor()

    check = _check(report, "actuator token")
    assert check.status == "ok"


def test_actuator_check_ok_when_token_is_configured(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_ACTUATOR_TOKEN", "s3cret")
    configure(package="fakeapp", topology="processes", broker="testbroker")

    report = run_doctor()

    check = _check(report, "actuator token")
    assert check.status == "ok"


def test_actuator_check_warns_when_token_mode_has_no_token(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.delenv("MODULITH_ACTUATOR_TOKEN", raising=False)
    configure(package="fakeapp", topology="processes", broker="testbroker", actuator_mode="token")

    report = run_doctor()

    check = _check(report, "actuator token")
    assert check.status == "warn"
    assert "refuse to start" in check.summary


def test_actuator_check_warns_when_auto_mode_has_no_token(make_fake_app, monkeypatch) -> None:
    """'auto' mode with no token leaves /_modulith/* unmounted under the
    conditions 'modulith run' hits by default (0.0.0.0 bind) — the doctor
    check must name both, since 'refuse to start' would be the wrong fix here
    (the app *starts*; only the actuator proxy is skipped)."""
    make_fake_app({"orders": ""})
    monkeypatch.delenv("MODULITH_ACTUATOR_TOKEN", raising=False)
    configure(package="fakeapp", topology="processes", broker="testbroker")

    report = run_doctor()

    check = _check(report, "actuator token")
    assert check.status == "warn"
    assert "UNMOUNTED" in check.summary
    assert "0.0.0.0" in check.summary


# ---------------------------------------------------------------------------
# Single-host broker
# ---------------------------------------------------------------------------


def test_single_host_broker_ok_for_single_process_topology(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "ok"


def test_single_host_broker_ok_for_host_independent_broker(make_fake_app, monkeypatch) -> None:
    """A broker scheme that isn't the embedded-sqlite 'database' or 'shm' is
    host-independent — even under a detected container runtime, there is
    nothing to warn about."""
    make_fake_app({"orders": ""})
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    configure(package="fakeapp", topology="processes", broker="testbroker")

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "ok"


def test_single_host_broker_warns_for_sqlite_under_kubernetes(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    configure(
        package="fakeapp",
        topology="processes",
        broker="database",
        broker_options={"url": f"sqlite+aiosqlite:///{tmp_path}/test.db"},
    )

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "warn"


def test_single_host_broker_errors_for_sqlite_under_kubernetes_in_production(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    configure(
        package="fakeapp",
        topology="processes",
        broker="database",
        broker_options={"url": f"sqlite+aiosqlite:///{tmp_path}/test.db"},
        production=True,
        outbox="memory",
    )

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "error"


def test_single_host_broker_warns_for_sqlite_under_docker_marker(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    from modulith import doctor as doctor_module

    make_fake_app({"orders": ""})
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    marker = tmp_path / "dockerenv-marker"
    marker.write_text("")
    monkeypatch.setattr(doctor_module, "_CONTAINER_MARKERS", (marker,))
    monkeypatch.setattr(doctor_module, "_CGROUP_PATH", tmp_path / "no-such-cgroup-file")
    configure(
        package="fakeapp",
        topology="processes",
        broker="database",
        broker_options={"url": f"sqlite+aiosqlite:///{tmp_path}/test.db"},
    )

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "warn"


def test_single_host_broker_ok_when_no_container_runtime_detected(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    """Neutralise all three detection sources — env, marker files, and the
    cgroup path — since CI itself may run inside a container and would
    otherwise leak a false positive into this negative test."""
    from modulith import doctor as doctor_module

    make_fake_app({"orders": ""})
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(doctor_module, "_CONTAINER_MARKERS", ())
    monkeypatch.setattr(doctor_module, "_CGROUP_PATH", tmp_path / "no-such-cgroup-file")
    configure(
        package="fakeapp",
        topology="processes",
        broker="database",
        broker_options={"url": f"sqlite+aiosqlite:///{tmp_path}/test.db"},
    )

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "ok"


def test_single_host_broker_warns_for_shm_under_container(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    make_fake_app({"orders": ""})
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    configure(
        package="fakeapp",
        topology="processes",
        broker="shm",
        broker_options={"state_dir": str(tmp_path)},
    )

    report = run_doctor()

    check = _check(report, "single-host broker")
    assert check.status == "warn"


# ---------------------------------------------------------------------------
# Redis retention
# ---------------------------------------------------------------------------


def test_redis_retention_ok_when_broker_is_not_redis(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp")

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "ok"


def test_redis_retention_ok_for_default_maxlen(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", topology="processes", broker="redis-streams")

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "ok"


def test_redis_retention_warns_for_low_maxlen(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(
        package="fakeapp",
        topology="processes",
        broker="redis-streams",
        broker_options={"max_stream_len": 100},
    )

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "warn"
    assert any("reclaim_min_idle_ms=60000" in d for d in check.details)


# ---------------------------------------------------------------------------
# Redis retention — live backlog
# ---------------------------------------------------------------------------


class _FakeRedisScanClient:
    """A minimal async Redis double exposing only the two commands the live
    backlog check issues: SCAN (via ``scan_iter``) to enumerate streams, and
    XINFO GROUPS per stream for pending/lag counts. Mirrors the hand-rolled
    ``FakeRedis`` convention in test_redis_broker.py rather than depending on
    the (not installed) ``fakeredis`` package."""

    def __init__(self, streams: dict[str, list[dict]]) -> None:
        self._streams = streams

    async def scan_iter(self, *, match: str, _type: str = "STREAM"):
        for key in self._streams:
            yield key.encode()

    async def xinfo_groups(self, key: bytes | str) -> list[dict]:
        name = key.decode() if isinstance(key, bytes) else key
        return self._streams[name]

    async def aclose(self) -> None:  # pragma: no cover - broker teardown only
        pass


class _RaisingRedisClient:
    """Simulates a connection failure on the very first Redis command."""

    async def scan_iter(self, *, match: str, _type: str = "STREAM"):
        raise ConnectionError("connection refused")
        yield  # pragma: no cover - unreachable; keeps this an async generator

    async def aclose(self) -> None:  # pragma: no cover - broker teardown only
        pass


def _register_fake_redis_broker(
    client, *, max_stream_len: int, stream_prefix: str = "modulith.events"
):
    """Bootstrap, then swap the real redis-streams broker for one wrapping a
    fake client — the real one (built lazily by ``modulith_register_brokers``,
    no eager connection) is harmless but must be replaced to control both
    ``_max_stream_len`` and what the live backlog query sees."""
    from modulith.adapters.redis_broker import RedisStreamsBroker
    from modulith.runtime import _runtime

    _runtime.ensure_bootstrapped()
    registry = _runtime.broker_registry
    assert registry is not None
    registry.unregister("redis-streams")
    broker = RedisStreamsBroker(
        client=client, stream_prefix=stream_prefix, max_stream_len=max_stream_len
    )
    registry.register("redis-streams", broker)
    return broker


def test_redis_retention_live_warns_at_sixty_percent_backlog(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", topology="processes", broker="redis-streams")
    client = _FakeRedisScanClient(
        {"modulith.events.orders": [{"name": "modulith", "pending": 600, "lag": 0}]}
    )
    _register_fake_redis_broker(client, max_stream_len=1000)

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "warn"
    assert any("60%" in d for d in check.details)


def test_redis_retention_live_errors_at_near_full_backlog(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    configure(package="fakeapp", topology="processes", broker="redis-streams")
    client = _FakeRedisScanClient(
        {"modulith.events.orders": [{"name": "modulith", "pending": 950, "lag": 0}]}
    )
    _register_fake_redis_broker(client, max_stream_len=1000)

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "error"


def test_redis_retention_live_ok_when_query_fails(make_fake_app) -> None:
    """A connection failure during the live check must not fail the doctor
    CI gate on its own — the static maxlen sizing check already ran and
    passed, so the check falls back to that result plus a note."""
    make_fake_app({"orders": ""})
    configure(package="fakeapp", topology="processes", broker="redis-streams")
    _register_fake_redis_broker(_RaisingRedisClient(), max_stream_len=10_000)

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "ok"
    assert any("skipped" in d for d in check.details)


def test_redis_retention_live_ignores_dead_letter_stream(make_fake_app) -> None:
    """The DLQ stream (``<stream>.dead``) is bounded independently and is
    expected to run near its own cap — counting it against the primary
    stream's backlog would falsely flag every deployment with any dead
    letters at all."""
    make_fake_app({"orders": ""})
    configure(package="fakeapp", topology="processes", broker="redis-streams")
    client = _FakeRedisScanClient(
        {
            "modulith.events.orders": [{"name": "modulith", "pending": 100, "lag": 0}],
            "modulith.events.orders.dead": [{"name": "modulith", "pending": 9500, "lag": 0}],
        }
    )
    _register_fake_redis_broker(client, max_stream_len=10_000)

    report = run_doctor()

    check = _check(report, "redis retention")
    assert check.status == "ok"
