"""State-machine tests for the SQLite-authoritative SHM queue."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, cast

import pytest

from modulith import ConfigurationError
from modulith.adapters import _shm_publications
from modulith.adapters._shm_coldstore import ShmColdStore
from modulith.adapters._shm_schema import (
    _configure_max_page_count,
    immediate_transaction,
    open_database,
)
from modulith.adapters._shm_store import (
    _UNCAPPED_PAGES,
    ClaimToken,
    PublishResult,
    SqliteQueueStore,
)


async def _published_store(
    path: Path,
    *,
    groups: tuple[str, ...] = ("g1",),
    completion_mode: str = "delete",
    **options,
) -> ShmColdStore:
    store = ShmColdStore(
        str(path),
        completion_mode=completion_mode,
        **options,
    )
    for group in groups:
        await store.subscribe(["events.Created"], group)
    result = await store.publish(
        "events.Created",
        b'{"id": 1}',
        {"event_type": "events.Created"},
        publication_id="publication-1",
    )
    assert result.publication_id == "publication-1"
    return store


def _rows(path: Path, sql: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return list(conn.execute(sql))
    finally:
        conn.close()


async def test_publish_atomically_creates_publication_and_group_deliveries(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fanout.db"
    store = await _published_store(path, groups=("g1", "g2"))
    try:
        first = await store.claim("g1", consumer_name="worker-1")
        second = await store.claim("g2", consumer_name="worker-2")

        assert first[0]["message_id"] == second[0]["message_id"] == "publication-1"
        assert first[0]["claim_token"].delivery_id != second[0]["claim_token"].delivery_id
        assert len(_rows(path, "SELECT id FROM shm_publication")) == 1
        assert len(_rows(path, "SELECT id FROM shm_delivery")) == 2
        assert first[0]["sequence"] == 1
    finally:
        await store.close()


@pytest.mark.parametrize(
    "headers",
    [
        {"event_type": "events.Created", "trace": "original"},
        {},
    ],
    ids=["populated-headers", "empty-headers"],
)
async def test_exact_publication_replay_is_idempotent(
    tmp_path: Path,
    headers: dict[str, str],
) -> None:
    path = tmp_path / "duplicate.db"
    store = ShmColdStore(str(path))
    try:
        await store.subscribe(["events.Created"], "g1")
        first = await store.publish(
            "events.Created",
            b"first",
            headers,
            publication_id="duplicate",
        )
        duplicate = await store.publish(
            "events.Created",
            b"first",
            headers,
            publication_id="duplicate",
        )

        assert duplicate == first
        claimed = await store.claim("g1", consumer_name="worker")
        assert len(claimed) == 1
        assert claimed[0]["payload"] == b"first"
        assert await store.ack(claimed[0]["claim_token"], consumer_name="worker")

        replay = await store.publish(
            "events.Created",
            b"first",
            headers,
            publication_id="duplicate",
        )

        assert replay == first
        assert await store.claim("g1", consumer_name="worker") == []
        assert len(_rows(path, "SELECT id FROM shm_publication")) == 1
        assert _rows(path, "SELECT id FROM shm_delivery") == []
    finally:
        await store.close()


@pytest.mark.parametrize(
    ("target", "payload", "headers"),
    [
        (
            "events.Changed",
            b"first",
            {"event_type": "events.Created", "trace": "original"},
        ),
        (
            "events.Created",
            b"first",
            {"event_type": "events.Changed", "trace": "original"},
        ),
        (
            "events.Created",
            b"second",
            {"event_type": "events.Created", "trace": "original"},
        ),
        (
            "events.Created",
            b"first",
            {"event_type": "events.Created", "trace": "changed"},
        ),
    ],
    ids=["target", "event-type", "payload", "headers"],
)
async def test_conflicting_publication_replay_fails_without_creating_delivery(
    tmp_path: Path,
    target: str,
    payload: bytes,
    headers: dict[str, str],
) -> None:
    path = tmp_path / "conflict.db"
    store = ShmColdStore(str(path))
    try:
        await store.subscribe(["events.Created"], "original-group")
        await store.subscribe(["events.Changed"], "conflicting-group")
        await store.publish(
            "events.Created",
            b"first",
            {"event_type": "events.Created", "trace": "original"},
            publication_id="duplicate",
        )

        with pytest.raises(ValueError, match="conflicting immutable fields"):
            await store.publish(
                target,
                payload,
                headers,
                publication_id="duplicate",
            )

        deliveries = _rows(
            path,
            "SELECT consumer_group FROM shm_delivery ORDER BY consumer_group",
        )
        assert [row["consumer_group"] for row in deliveries] == ["original-group"]
        assert len(_rows(path, "SELECT id FROM shm_publication")) == 1
    finally:
        await store.close()


async def test_negative_schema_version_fails_without_migration(tmp_path: Path) -> None:
    path = tmp_path / "corrupt-version.db"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = -1")
    conn.close()
    store = ShmColdStore(str(path))

    with pytest.raises(RuntimeError, match="unsupported or corrupt schema version -1"):
        await store.get_subscriptions()
    await store.close()

    assert _rows(path, "PRAGMA user_version")[0][0] == -1
    assert _rows(path, "SELECT name FROM sqlite_master WHERE name LIKE 'shm_%'") == []


async def test_publication_insert_without_a_row_rolls_back(tmp_path: Path) -> None:
    path = tmp_path / "suppressed-insert.db"
    store = ShmColdStore(str(path))
    try:
        await store.get_subscriptions()
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TRIGGER suppress_publication_insert
            BEFORE INSERT ON shm_publication
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """
        )
        conn.close()

        with pytest.raises(RuntimeError, match="did not produce a row"):
            await store.publish("events.Created", b"{}", publication_id="suppressed")

        assert _rows(path, "SELECT id FROM shm_publication") == []
        assert _rows(path, "SELECT id FROM shm_delivery") == []
    finally:
        await store.close()


def test_store_exhaustion_rolls_back_publish_with_actionable_error(tmp_path: Path) -> None:
    path = tmp_path / "full.db"
    store = SqliteQueueStore(
        str(path),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
        max_payload_bytes=8192,
        max_store_bytes=1,
    )
    try:
        store.subscribe(["events.Created"], "g1")
        for index in range(100):
            before = (
                len(_rows(path, "SELECT id FROM shm_publication")),
                len(_rows(path, "SELECT id FROM shm_delivery")),
            )
            try:
                store.publish(
                    "events.Created",
                    b"x" * 8192,
                    {"event_type": "events.Created"},
                    f"publication-{index}",
                )
            except ConfigurationError as error:
                message = str(error)
                assert "max_store_bytes" in message
                assert "orphan_retention_seconds" in message
                assert "bytes per publication" in message
                assert "disk space" in message
                # Pruning cannot help: drained publications stay until their retention ends.
                assert "prune completed publications" not in message
                assert "backlog" in message
                assert "retired group" in message
                assert "modulith broker drop-group" in message
                assert "completion_mode" in message
                assert "retention_age_seconds" in message
                assert "restart every process" in message
                assert (
                    "(the retired-group warning appears at the next modulith run "
                    "--topology processes start)"
                ) in message
                assert "groups are named modulith-<module>" in message
                # Rows already stored keep their stamped expiry, so a shorter
                # retention frees nothing in a store that is already full.
                assert "shorten" not in message
                assert not store._conn.in_transaction
                assert (
                    len(_rows(path, "SELECT id FROM shm_publication")),
                    len(_rows(path, "SELECT id FROM shm_delivery")),
                ) == before
                break
        else:
            pytest.fail("bounded SQLite store did not report exhaustion")
    finally:
        store.close()


@pytest.mark.parametrize("max_store_bytes", [256 * 1024, 1024 * 1024])
def test_consumers_drain_a_backlog_that_filled_the_store(
    tmp_path: Path, max_store_bytes: int
) -> None:
    path = tmp_path / "backlog.db"
    store = SqliteQueueStore(
        str(path),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=1e-6,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
        max_store_bytes=max_store_bytes,
    )
    groups = ["modulith-inventory", "modulith-notifications"]
    try:
        for group in groups:
            store.subscribe(["events.Created"], group)
        published = 0
        with pytest.raises(ConfigurationError, match="max_store_bytes"):
            while True:
                store.publish(
                    "events.Created",
                    b'{"order_id":"%08d","total":19.99}' % published,
                    {"event_type": "events.Created"},
                    None,
                )
                published += 1
        assert published >= 100
        assert not store._conn.in_transaction

        for group in groups:
            consumer = f"{group}:worker-0123456789abcdef0123456789abcdef"
            while store.group_backlog()[group]:
                rows = store.claim(group, 100, consumer, 30.0)
                if not rows:
                    time.sleep(0.05)
                    continue
                first, *rest = rows
                assert store.fail(first["claim_token"], "ValueError: bad payload", 3, consumer)
                if rest:
                    assert store.dead_letter(rest.pop()["claim_token"], "poison", consumer)
                for row in rest:
                    assert store.ack(row["claim_token"], consumer, None)
                store.prune(1e-9, 1000)
        assert store.group_backlog() == dict.fromkeys(groups, 0)
        assert store.read_pragma("page_count") * store.read_pragma("page_size") <= max_store_bytes

        store.publish("events.Created", b"{}", {"event_type": "events.Created"}, "after-drain")
    finally:
        store.close()


def _bounded_store(path: Path, max_store_bytes: int, **options) -> SqliteQueueStore:
    settings = {
        "synchronous": "NORMAL",
        "completion_mode": "delete",
        "orphan_retention_seconds": 86400.0,
        "retry_backoff_base_seconds": 1e-6,
        "retry_backoff_cap_seconds": 1e-6,
        **options,
    }
    return SqliteQueueStore(str(path), max_store_bytes=max_store_bytes, **settings)


def _publish_until_refused(store: SqliteQueueStore, target: str = "events.Created") -> int:
    published = 0
    with pytest.raises(ConfigurationError, match="max_store_bytes"):
        while True:
            store.publish(target, b'{"order_id":"%08d"}' % published, None, None)
            published += 1
    assert published >= 100
    return published


_LONG_CONSUMER = "modulith-orders:worker-" + "0123456789abcdef" * 4


@pytest.mark.parametrize("completion_mode", ["delete", "mark"])
def test_consumers_drain_a_full_store_in_either_completion_mode(
    tmp_path: Path, completion_mode: str
) -> None:
    store = _bounded_store(tmp_path / "full.db", 4 * 1024 * 1024, completion_mode=completion_mode)
    try:
        store.subscribe(["events.Created"], "orders")
        published = _publish_until_refused(store)
        acked = 0
        while rows := store.claim("orders", 100, _LONG_CONSUMER, 30.0):
            for row in rows:
                assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
                acked += 1
            store.prune(3 * 86400.0, 1000)
        assert acked == published
        assert store.group_backlog() == {"orders": 0}
    finally:
        store.close()


def test_a_listener_failing_every_delivery_until_dead_letter_drains_a_full_store(
    tmp_path: Path,
) -> None:
    store = _bounded_store(tmp_path / "failing.db", 1024 * 1024)
    error = "ConnectionError: inventory database unavailable " + "x" * 250
    try:
        store.subscribe(["events.Created"], "orders")
        published = _publish_until_refused(store)
        dead = 0
        while rows := store.claim("orders", 100, _LONG_CONSUMER, 30.0):
            for row in rows:
                assert store.fail(row["claim_token"], error, 2, _LONG_CONSUMER)
                dead += row["attempts"] == 1
        assert dead == published
        assert store.group_backlog() == {"orders": 0}
        assert store.prune(1e-9, 1000) == min(published, 1000)
    finally:
        store.close()


def test_a_full_prune_batch_commits_in_a_full_store(tmp_path: Path) -> None:
    group = "orders-consumer-group-long-name"
    store = _bounded_store(tmp_path / "prune.db", 4 * 1024 * 1024, completion_mode="mark")
    try:
        store.subscribe(["events.Created"], group)
        published = _publish_until_refused(store)
        while rows := store.claim(group, 100, "c", 30.0):
            for row in rows:
                assert store.ack(row["claim_token"], "c", None)
        pruned = 0
        while batch := store.prune(1e-9, 1000):
            pruned += batch
        assert pruned == published
    finally:
        store.close()


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "modulith.adapters.shm" and record.levelno == logging.WARNING
    ]


def test_a_group_whose_backlog_filled_the_store_replays_a_new_target_and_drains(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = _bounded_store(tmp_path / "replay.db", 4 * 1024 * 1024)
    try:
        store.subscribe(["events.Busy"], "busy")
        for index in range(800):
            store.publish("events.Late", b'{"late":%d}' % index, None, None)
        published = _publish_until_refused(store, "events.Busy")

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            assert store.subscribe(["events.Busy", "events.Late"], "busy") == 0
        [warning] = _warnings(caplog)
        assert "'busy'" in warning
        assert "'events.Late'" in warning
        assert "replayed 0 and skipped 800" in warning
        assert store.get_subscriptions()["events.Late"] == ["busy"]
        assert store.group_backlog() == {"busy": published}

        acked = 0
        while rows := store.claim("busy", 100, _LONG_CONSUMER, 30.0):
            for row in rows:
                assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
                acked += 1
        assert acked == published
        store.publish("events.Busy", b"{}", None, None)
    finally:
        store.close()


def _used_and_budget_pages(store: SqliteQueueStore, max_store_bytes: int) -> tuple[int, int]:
    conn = store._conn
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    used = int(conn.execute("PRAGMA page_count").fetchone()[0]) - int(
        conn.execute("PRAGMA freelist_count").fetchone()[0]
    )
    max_pages = max_store_bytes // page_size
    return used, max_pages - min(_shm_publications.CONSUMER_RESERVE_PAGES, max_pages // 8)


def test_a_replay_into_a_nearly_full_store_stops_at_the_publish_budget(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(tmp_path / "bounded.db", max_store_bytes)
    try:
        for index in range(3000):
            store.publish("events.T", b'{"t":%d}' % index, None, None)
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        index = 0
        while used < budget - 40:
            store.publish("events.U", b'{"u":%d}' % index, None, None)
            index += 1
            used, _ = _used_and_budget_pages(store, max_store_bytes)

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            replayed = store.subscribe(["events.T"], "late")

        assert 0 < replayed < 3000
        assert _used_and_budget_pages(store, max_store_bytes)[0] <= budget
        assert store.get_subscriptions()["events.T"] == ["late"]
        [warning] = _warnings(caplog)
        assert "'late'" in warning
        assert "'events.T'" in warning
        assert f"replayed {replayed} and skipped {3000 - replayed}" in warning
        store.publish("events.V", b"{}", None, None)

        claimed: list[bytes] = []
        while rows := store.claim("late", 100, _LONG_CONSUMER, 30.0):
            for row in rows:
                claimed.append(bytes(row["payload"]))
                assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
        assert claimed == [b'{"t":%d}' % index for index in range(replayed)]
        assert store.subscribe(["events.T"], "late") == 0
    finally:
        store.close()


def test_a_replay_leaves_room_for_a_publish_that_fit_before_it(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(tmp_path / "probe.db", max_store_bytes)
    try:
        store.subscribe(["events.T"], "early")
        for index in range(3000):
            store.publish("events.T", b'{"t":%d}' % index, None, None)
        while rows := store.claim("early", 100, _LONG_CONSUMER, 30.0):
            for row in rows:
                assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        index = 0
        while used < budget - 3:
            store.publish("events.U", b'{"u":%d}' % index, None, None)
            index += 1
            used, _ = _used_and_budget_pages(store, max_store_bytes)

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            store.subscribe(["events.T"], "late")

        assert _used_and_budget_pages(store, max_store_bytes)[0] <= budget
        assert store.get_subscriptions()["events.T"] == ["early", "late"]
        assert len(_warnings(caplog)) == 1
        store.publish("events.V", b"{}", None, None)
    finally:
        store.close()


def test_a_replay_within_the_publish_budget_logs_no_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = _bounded_store(tmp_path / "roomy.db", 4 * 1024 * 1024)
    try:
        for index in range(20):
            store.publish("events.Late", b'{"late":%d}' % index, None, None)
        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            assert store.subscribe(["events.Late"], "late") == 20
        assert _warnings(caplog) == []
    finally:
        store.close()


def _drain(store: SqliteQueueStore, group: str) -> list[bytes]:
    payloads: list[bytes] = []
    while rows := store.claim(group, 100, _LONG_CONSUMER, 30.0):
        for row in rows:
            payloads.append(bytes(row["payload"]))
            assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
    return payloads


def _hold_then_drop_target(store: SqliteQueueStore, held: int, acked: int) -> None:
    store.subscribe(["events.T"], "g")
    for index in range(held):
        store.publish("events.T", b'{"t":%d}' % index, None, None)
    for row in store.claim("g", acked, _LONG_CONSUMER, 30.0):
        assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
    store.subscribe([], "g")


def test_re_adding_a_target_the_group_still_holds_in_a_full_store_logs_no_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = _bounded_store(tmp_path / "held.db", 2 * 1024 * 1024)
    try:
        _hold_then_drop_target(store, held=10, acked=3)
        _publish_until_refused(store, "events.U")

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            assert store.subscribe(["events.T"], "g") == 0

        assert _warnings(caplog) == []
        assert store.group_backlog() == {"g": 7}
    finally:
        store.close()


def test_a_cut_replay_counts_only_the_publications_the_group_lacks_as_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = _bounded_store(tmp_path / "lacks.db", 2 * 1024 * 1024)
    try:
        _hold_then_drop_target(store, held=10, acked=3)
        for index in range(5):
            store.publish("events.T", b'{"missed":%d}' % index, None, None)
        _publish_until_refused(store, "events.U")

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            assert store.subscribe(["events.T"], "g") == 0

        [warning] = _warnings(caplog)
        assert "replayed 0 and skipped 5 " in warning
    finally:
        store.close()


def test_a_cut_replay_warning_states_in_minutes_when_the_earliest_skipped_publication_expires(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = _bounded_store(tmp_path / "deadline.db", 2 * 1024 * 1024)
    try:
        _hold_then_drop_target(store, held=10, acked=3)
        for index in range(5):
            store.publish("events.T", b'{"missed":%d}' % index, None, None)
        _publish_until_refused(store, "events.U")
        now = time.time()
        for index, minutes in enumerate([50, 40, 10, 30, 20]):
            store._conn.execute(
                "UPDATE shm_publication SET retained_until=? WHERE payload=?",
                (now + minutes * 60 + 30, b'{"missed":%d}' % index),
            )
        store._conn.commit()

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            assert store.subscribe(["events.T"], "g") == 0

        [warning] = _warnings(caplog)
        assert "expires in about 10 minutes" in warning
        assert "restart every process within that time" in warning
    finally:
        store.close()


def test_the_cut_replay_recovery_delivers_every_publication_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "recover.db"
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(path, max_store_bytes)
    try:
        for index in range(3000):
            store.publish("events.T", b'{"t":%d}' % index, None, None)
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        index = 0
        while used < budget - 40:
            store.publish("events.U", b'{"u":%d}' % index, None, None)
            index += 1
            used, _ = _used_and_budget_pages(store, max_store_bytes)
        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            replayed = store.subscribe(["events.T"], "late")
        [warning] = _warnings(caplog)
        assert 0 < replayed < 3000
        # The recovery the warning gives: drain first, then drop-group and restart.
        assert warning.index("drain") < warning.index("drop-group")
        # The replayed publications are the oldest, so they expire first.
        store._conn.execute(
            "UPDATE shm_publication SET retained_until=0 WHERE target='events.T' "
            "AND sequence IN (SELECT sequence FROM shm_publication "
            "WHERE target='events.T' ORDER BY sequence LIMIT ?)",
            (replayed,),
        )
        store._conn.commit()
        delivered = _drain(store, "late")
        assert store.drop_group("late", ["events.T"]) == (1, 0)
    finally:
        store.close()

    store = _bounded_store(path, 16 * 1024 * 1024)
    try:
        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            assert store.subscribe(["events.T"], "late") == 3000 - replayed
        delivered += _drain(store, "late")
    finally:
        store.close()
    assert delivered == [b'{"t":%d}' % index for index in range(3000)]


def test_a_replay_frees_the_write_lock_between_batches_and_keeps_publication_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "batches.db"
    subscriber = _bounded_store(path, 4 * 1024 * 1024)
    sibling = _bounded_store(path, 4 * 1024 * 1024)
    # A zero busy timeout makes the sibling's write fail the moment the subscriber holds the lock.
    sibling._conn.execute("PRAGMA busy_timeout=0")
    monkeypatch.setattr(_shm_publications, "REPLAY_BATCH_ROWS", 10)
    monkeypatch.setattr(_shm_publications, "REPLAY_BATCH_PAUSE_SECONDS", 0)
    subscribed_in_window: list[bool] = []
    transactions = [0]

    def transaction(conn: sqlite3.Connection) -> AbstractContextManager[None]:
        if conn is subscriber._conn:
            transactions[0] += 1
            if transactions[0] > 1:  # the first transaction only reconciles subscriptions
                subscribed_in_window.append(bool(sibling.get_subscriptions()))
                sibling.publish("events.T", b"{}", None, f"live{transactions[0]:02d}")
        return immediate_transaction(conn)

    try:
        for index in range(25):
            sibling.publish("events.T", b"{}", None, f"p{index:02d}")
        monkeypatch.setattr(_shm_publications, "immediate_transaction", transaction)

        replayed = subscriber.subscribe(["events.T"], "g")

        before_subscription = subscribed_in_window.count(False)
        assert subscribed_in_window[:2] == [False, False], "a batch did not release the lock"
        assert replayed == 25 + before_subscription
        assert sibling.get_subscriptions() == {"events.T": ["g"]}
        in_sequence = [
            row["id"]
            for row in sibling._conn.execute("SELECT id FROM shm_publication ORDER BY sequence")
        ]
        claimed = subscriber.claim("g", 100, _LONG_CONSUMER, 30.0)
        assert [row["message_id"] for row in claimed] == in_sequence
    finally:
        subscriber.close()
        sibling.close()


def test_a_sibling_waiting_on_the_write_lock_gets_in_while_a_replay_is_still_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "contended.db"
    subscriber = _bounded_store(path, 4 * 1024 * 1024)
    for index in range(80):
        subscriber.publish("events.T", b"{}", None, f"p{index:02d}")
    monkeypatch.setattr(_shm_publications, "REPLAY_BATCH_ROWS", 20)
    sibling_ready = threading.Event()
    replay_holds_the_lock = threading.Event()
    outcome: dict[str, Any] = {}

    def sibling_publishes() -> None:
        sibling = _bounded_store(path, 4 * 1024 * 1024)  # opened on the thread that uses it
        try:
            sibling_ready.set()
            replay_holds_the_lock.wait(10)
            sibling.publish("events.T", b"{}", None, "sibling")
            outcome["subscriptions_when_it_returned"] = sibling.get_subscriptions()
        except sqlite3.Error as error:
            outcome["error"] = error
        finally:
            sibling.close()

    sibling_thread = threading.Thread(target=sibling_publishes)
    real_insert_delivery = _shm_publications._insert_delivery
    started = [False]

    def insert_delivery_while_the_sibling_waits(*arguments: Any) -> int:
        if not started[0]:
            started[0] = True
            replay_holds_the_lock.set()
            time.sleep(0.05)  # the sibling's write now waits on the lock this batch holds
        return real_insert_delivery(*arguments)

    monkeypatch.setattr(
        _shm_publications, "_insert_delivery", insert_delivery_while_the_sibling_waits
    )
    try:
        sibling_thread.start()
        assert sibling_ready.wait(10)
        replayed = subscriber.subscribe(["events.T"], "g")
        sibling_thread.join()
    finally:
        subscriber.close()

    assert "error" not in outcome
    assert outcome["subscriptions_when_it_returned"] == {}, "the sibling waited out the replay"
    assert replayed == 81


def test_a_replay_cut_after_several_batches_counts_every_skipped_publication(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(tmp_path / "cut-batches.db", max_store_bytes)
    monkeypatch.setattr(_shm_publications, "REPLAY_BATCH_ROWS", 100)
    monkeypatch.setattr(_shm_publications, "REPLAY_BATCH_PAUSE_SECONDS", 0)
    try:
        for index in range(3000):
            store.publish("events.T", b'{"t":%d}' % index, None, None)
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        index = 0
        while used < budget - 40:
            store.publish("events.U", b'{"u":%d}' % index, None, None)
            index += 1
            used, _ = _used_and_budget_pages(store, max_store_bytes)
        # The earliest expiry belongs to the newest publication, far past the batch that is cut.
        store._conn.execute(
            "UPDATE shm_publication SET retained_until=? WHERE target='events.T' AND sequence="
            "(SELECT MAX(sequence) FROM shm_publication WHERE target='events.T')",
            (time.time() + 630,),
        )
        store._conn.commit()

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            replayed = store.subscribe(["events.T"], "late")

        [warning] = _warnings(caplog)
        assert 100 < replayed < 3000
        assert f"replayed {replayed} and skipped {3000 - replayed} " in warning
        assert "expires in about 10 minutes" in warning
        assert store.get_subscriptions()["events.T"] == ["late"]
        assert store.group_backlog() == {"late": replayed}
    finally:
        store.close()


def test_draining_a_cut_replay_in_mark_mode_keeps_room_for_a_publish_that_fit_before_it(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(tmp_path / "mark.db", max_store_bytes, completion_mode="mark")
    try:
        for index in range(3000):
            store.publish("events.T", b'{"t":%d}' % index, None, None)
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        index = 0
        while used < budget - 40:
            store.publish("events.U", b'{"u":%d}' % index, None, None)
            index += 1
            used, _ = _used_and_budget_pages(store, max_store_bytes)
        store.publish("events.V", b"{}", None, None)

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            replayed = store.subscribe(["events.T"], "late")
        assert 0 < replayed < 3000
        assert len(_warnings(caplog)) == 1
        assert len(_drain(store, "late")) == replayed

        store.publish("events.V", b"{}", None, None)
    finally:
        store.close()


def _run_before_the_write_lock(
    monkeypatch: pytest.MonkeyPatch, store: SqliteQueueStore, action: Callable[[], None]
) -> None:
    """Run action once, after store starts a write and before it holds the write lock."""
    pending = [action]

    def transaction(conn: sqlite3.Connection) -> AbstractContextManager[None]:
        if conn is store._conn and pending:
            pending.pop()()
        return immediate_transaction(conn)

    monkeypatch.setattr(_shm_publications, "immediate_transaction", transaction)


def test_a_publish_that_takes_the_write_lock_after_a_subscribe_is_claimed_after_the_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "race.db"
    publisher = _bounded_store(path, 4 * 1024 * 1024)
    subscriber = _bounded_store(path, 4 * 1024 * 1024)
    clock = [1000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    replayed: list[int] = []

    def subscribe_while_the_publisher_waits() -> None:
        clock[0] += 10
        replayed.append(subscriber.subscribe(["events.T"], "g"))

    try:
        for index in (1, 2, 3):
            clock[0] += 1
            publisher.publish("events.T", b"{}", None, f"p{index}")
        _run_before_the_write_lock(monkeypatch, publisher, subscribe_while_the_publisher_waits)

        publisher.publish("events.T", b"{}", None, "p4")

        assert replayed == [3]
        clock[0] += 100
        claimed = subscriber.claim("g", 10, _LONG_CONSUMER, 30.0)
        assert [row["message_id"] for row in claimed] == ["p1", "p2", "p3", "p4"]
    finally:
        publisher.close()
        subscriber.close()


def test_a_subscribe_that_waits_for_the_write_lock_replays_only_what_is_retained_once_it_has_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _bounded_store(tmp_path / "wait.db", 4 * 1024 * 1024, orphan_retention_seconds=5.0)
    clock = [1000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])

    def lock_wait_lasts_ten_seconds() -> None:
        clock[0] += 10

    try:
        store.publish("events.T", b"{}", None, "short-lived")
        _run_before_the_write_lock(monkeypatch, store, lock_wait_lasts_ten_seconds)

        assert store.subscribe(["events.T"], "g") == 0

        assert store.claim("g", 10, _LONG_CONSUMER, 30.0) == []
    finally:
        store.close()


def test_a_mark_mode_replay_takes_half_the_room_left_after_the_previous_targets_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(tmp_path / "freed.db", max_store_bytes, completion_mode="mark")
    try:
        for _ in range(150):
            store.publish("a.old", b"z" * 2000, None, None)
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        while used < budget - 60:
            store.publish("b.live", b"%08d" % used, None, None)
            used, _ = _used_and_budget_pages(store, max_store_bytes)
        store._conn.execute("UPDATE shm_publication SET retained_until=0 WHERE target='a.old'")
        store._conn.commit()
        used_before_subscribe, _ = _used_and_budget_pages(store, max_store_bytes)
        publish_limit = budget - _shm_publications.REPLAY_PUBLISH_HEADROOM_PAGES

        # (target, pages used before its replay, pages used after it, publications skipped)
        replays: list[tuple[str, int, int, int]] = []
        real_replay = _shm_publications._replay

        def recording_replay(
            conn: sqlite3.Connection, target: str, group: str, now: float, page_limit: int
        ) -> tuple[int, int, float | None]:
            used_before = _shm_publications._used_pages(conn)
            result = real_replay(conn, target, group, now, page_limit)
            replays.append((target, used_before, _shm_publications._used_pages(conn), result[1]))
            return result

        monkeypatch.setattr(_shm_publications, "_replay", recording_replay)
        store.subscribe(["a.old", "b.live"], "g")

        assert [target for target, *_ in replays] == ["a.old", "b.live"]
        _, used_before, used_after, skipped = replays[1]
        assert used_before < used_before_subscribe - 40, "the expiry freed too few pages"
        assert skipped > 0, "the page limit did not cut the replay short"
        assert used_after - used_before <= (publish_limit - used_before) // 2
    finally:
        store.close()


def test_a_mark_mode_replay_of_several_targets_adds_at_most_half_the_room_left(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    max_store_bytes = 2 * 1024 * 1024
    store = _bounded_store(tmp_path / "targets.db", max_store_bytes, completion_mode="mark")
    try:
        used, budget = _used_and_budget_pages(store, max_store_bytes)
        index = 0
        while used < budget - 60:
            store.publish(("a.live", "b.live")[index % 2], b"%08d" % index, None, None)
            index += 1
            used, _ = _used_and_budget_pages(store, max_store_bytes)
        publish_limit = budget - _shm_publications.REPLAY_PUBLISH_HEADROOM_PAGES

        with caplog.at_level(logging.WARNING, logger="modulith.adapters.shm"):
            store.subscribe(["a.live", "b.live"], "g")

        assert len(_warnings(caplog)) == 2, "each target's replay should be cut short"
        used_after, _ = _used_and_budget_pages(store, max_store_bytes)
        assert used_after - used <= (publish_limit - used) // 2
    finally:
        store.close()


def test_the_uncapped_page_count_fits_the_32_bit_pragma_parser(tmp_path: Path) -> None:
    # SQLite 3.31.1 and older parse PRAGMA max_page_count as a signed 32-bit
    # int and treat a larger value as a query that leaves the cap unchanged.
    assert _UNCAPPED_PAGES <= 2**31 - 1
    store = _bounded_store(tmp_path / "uncap.db", 256 * 1024)
    try:
        store._conn.execute(f"PRAGMA max_page_count={_UNCAPPED_PAGES}")
        assert store.read_pragma("max_page_count") == _UNCAPPED_PAGES
    finally:
        store.close()


def _fsync_pragmas(conn: sqlite3.Connection) -> tuple[int, int]:
    return (
        conn.execute("PRAGMA fullfsync").fetchone()[0],
        conn.execute("PRAGMA checkpoint_fullfsync").fetchone()[0],
    )


def test_full_synchronous_also_turns_on_the_full_fsync_pragmas(tmp_path: Path) -> None:
    conn = open_database(str(tmp_path / "full.db"), "FULL", 1024 * 1024)
    try:
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert _fsync_pragmas(conn) == (1, 1)
    finally:
        conn.close()


def test_normal_synchronous_leaves_the_full_fsync_pragmas_off(tmp_path: Path) -> None:
    conn = open_database(str(tmp_path / "normal.db"), "NORMAL", 1024 * 1024)
    try:
        assert _fsync_pragmas(conn) == (0, 0)
    finally:
        conn.close()


class _SiblingGrowsOnPageCount:
    """A real connection whose page_count read is followed by a sibling's insert."""

    def __init__(self, conn: sqlite3.Connection, sibling: sqlite3.Connection) -> None:
        self._conn = conn
        self._sibling = sibling

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        cursor = self._conn.execute(sql, *parameters)
        if sql == "PRAGMA page_count":
            self._sibling.execute("INSERT INTO filler VALUES (randomblob(512 * 1024))")
        return cursor


def test_a_file_that_grows_under_a_sibling_while_opening_does_not_fail_the_cap(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "sibling.db")
    conn = open_database(path, "NORMAL", 1024 * 1024)
    sibling = sqlite3.connect(path, isolation_level=None, timeout=5.0)
    try:
        sibling.execute("CREATE TABLE filler(data BLOB)")
        pages_before = conn.execute("PRAGMA page_count").fetchone()[0]
        _configure_max_page_count(_SiblingGrowsOnPageCount(conn, sibling), 1024)  # type: ignore[arg-type]
        assert conn.execute("PRAGMA page_count").fetchone()[0] > pages_before
        assert (
            conn.execute("PRAGMA max_page_count").fetchone()[0]
            >= (conn.execute("PRAGMA page_count").fetchone()[0])
        )
    finally:
        sibling.close()
        conn.close()


class _CapRefusingConnection:
    """A real connection whose SQLite refuses every cap above the file size."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        if sql.startswith("PRAGMA max_page_count="):
            sql = "PRAGMA max_page_count=1"
        return self._conn.execute(sql, *parameters)


def test_a_cap_below_the_requested_page_count_still_fails_the_open(tmp_path: Path) -> None:
    conn = open_database(str(tmp_path / "refused.db"), "NORMAL", 1024 * 1024)
    try:
        with pytest.raises(ConfigurationError, match="could not enforce SHM max_store_bytes"):
            _configure_max_page_count(_CapRefusingConnection(conn), 4 * 1024 * 1024)  # type: ignore[arg-type]
    finally:
        conn.close()


class _CapLiftIgnoringConnection:
    """A real connection whose SQLite reads an over-range cap as a query."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        if sql == f"PRAGMA max_page_count={_UNCAPPED_PAGES}":
            sql = "PRAGMA max_page_count"
        return self._conn.execute(sql, *parameters)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _grow_past_the_limit(store: SqliteQueueStore) -> Callable[[], None]:
    conn = store._conn
    conn.execute("CREATE TABLE filler(data BLOB)")

    def operation() -> None:
        with conn:
            conn.execute("INSERT INTO filler VALUES (randomblob(512 * 1024))")

    return operation


def test_a_consumer_write_grows_the_store_past_its_limit(tmp_path: Path) -> None:
    store = _bounded_store(tmp_path / "grow.db", 256 * 1024)
    try:
        store._consumer_write(_grow_past_the_limit(store))
        assert store.read_pragma("page_count") * store.read_pragma("page_size") > 256 * 1024
    finally:
        store.close()


def test_a_consumer_write_names_the_sqlite_version_when_the_cap_cannot_be_lifted(
    tmp_path: Path,
) -> None:
    store = _bounded_store(tmp_path / "stuck.db", 256 * 1024)
    try:
        operation = _grow_past_the_limit(store)
        real_conn = store._conn
        store._conn = cast(sqlite3.Connection, _CapLiftIgnoringConnection(real_conn))
        with pytest.raises(sqlite3.OperationalError, match="full") as raised:
            store._consumer_write(operation)
        notes = getattr(raised.value, "__notes__", [])
        assert any(f"SQLite {sqlite3.sqlite_version}" in note for note in notes)
        store._conn = real_conn
        assert store.read_pragma("page_count") * store.read_pragma("page_size") <= 256 * 1024
    finally:
        store.close()


class _CapRestoreFailingConnection:
    """A real connection whose PRAGMAs that restore the page cap raise while ``failing``."""

    def __init__(self, conn: sqlite3.Connection, failing_pragma: str) -> None:
        self._conn = conn
        self._failing_pragma = failing_pragma
        self.failing = True

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        lifts_the_cap = sql == f"PRAGMA max_page_count={_UNCAPPED_PAGES}"
        if self.failing and sql.startswith(self._failing_pragma) and not lifts_the_cap:
            raise sqlite3.OperationalError("disk I/O error")
        return self._conn.execute(sql, *parameters)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


_CAP_RESTORING_PRAGMAS = ["PRAGMA page_size", "PRAGMA max_page_count="]


def _store_that_cannot_restore_its_cap(
    path: Path, failing_pragma: str
) -> tuple[SqliteQueueStore, Callable[[], str], _CapRestoreFailingConnection]:
    store = _bounded_store(path, 256 * 1024)
    grow = _grow_past_the_limit(store)

    def operation() -> str:
        grow()
        return "committed"

    flaky = _CapRestoreFailingConnection(store._conn, failing_pragma)
    store._conn = cast(sqlite3.Connection, flaky)
    return store, operation, flaky


def _shm_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "modulith.adapters.shm" and record.levelno == logging.ERROR
    ]


@pytest.mark.parametrize("failing_pragma", _CAP_RESTORING_PRAGMAS)
def test_a_consumer_write_returns_its_committed_result_when_the_cap_restore_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, failing_pragma: str
) -> None:
    path = tmp_path / "restore.db"
    store, operation, _ = _store_that_cannot_restore_its_cap(path, failing_pragma)
    try:
        assert store._consumer_write(operation) == "committed"

        [error] = _shm_errors(caplog)
        assert "max_page_count" in error
        assert "disk I/O error" in error
        assert _rows(path, "SELECT COUNT(*) AS n FROM filler")[0]["n"] == 1
    finally:
        store.close()


@pytest.mark.parametrize("failing_pragma", _CAP_RESTORING_PRAGMAS)
def test_the_next_consumer_write_retries_a_failed_cap_restore_before_it_runs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, failing_pragma: str
) -> None:
    store, operation, flaky = _store_that_cannot_restore_its_cap(
        tmp_path / "retry.db", failing_pragma
    )
    try:
        assert store._consumer_write(operation) == "committed"
        assert store.read_pragma("max_page_count") == _UNCAPPED_PAGES
        caplog.clear()
        flaky.failing = False

        cap_the_next_write_ran_under = store._consumer_write(
            lambda: store.read_pragma("max_page_count")
        )

        assert cap_the_next_write_ran_under < _UNCAPPED_PAGES
        assert store.read_pragma("max_page_count") == cap_the_next_write_ran_under
        assert _shm_errors(caplog) == []
    finally:
        store.close()


@pytest.mark.parametrize("failing_pragma", _CAP_RESTORING_PRAGMAS)
def test_a_consumer_write_still_runs_while_the_cap_restore_keeps_failing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, failing_pragma: str
) -> None:
    store, operation, _ = _store_that_cannot_restore_its_cap(tmp_path / "stuck.db", failing_pragma)
    try:
        assert store._consumer_write(operation) == "committed"
        caplog.clear()

        assert store._consumer_write(lambda: "next") == "next"

        assert len(_shm_errors(caplog)) == 1
        assert store.read_pragma("max_page_count") == _UNCAPPED_PAGES
    finally:
        store.close()


def _outgrow_the_limit_on_a_subscription_write(store: SqliteQueueStore, event: str) -> None:
    store._conn.execute("CREATE TABLE filler(data BLOB)")
    store._conn.execute(
        f"CREATE TRIGGER outgrow AFTER {event} ON shm_subscription "
        "BEGIN INSERT INTO filler VALUES (randomblob(512 * 1024)); END"
    )


def test_a_subscribe_the_cap_cannot_lift_names_the_sqlite_and_disk_remedies(
    tmp_path: Path,
) -> None:
    store = _bounded_store(tmp_path / "stuck-subscribe.db", 256 * 1024)
    try:
        _outgrow_the_limit_on_a_subscription_write(store, "INSERT")
        store._conn = cast(sqlite3.Connection, _CapLiftIgnoringConnection(store._conn))

        with pytest.raises(ConfigurationError) as raised:
            store.subscribe(["events.Created"], "orders")

        message = str(raised.value)
        assert "lower max_store_bytes to what this SQLite build can hold" in message
        assert "or remove a retired group with modulith broker drop-group" in message
        assert "free disk space" in message
        assert "restart every process" in message
    finally:
        store.close()


def test_touch_subscriptions_runs_as_a_consumer_write_in_a_full_store(tmp_path: Path) -> None:
    store = _bounded_store(tmp_path / "touch.db", 256 * 1024)
    try:
        store.subscribe(["events.Created"], "orders")
        with store._conn:
            store._conn.execute("UPDATE shm_subscription SET updated_at=0")
        _outgrow_the_limit_on_a_subscription_write(store, "UPDATE")

        store.touch_subscriptions(["events.Created"], "orders")

        [row] = store._conn.execute("SELECT updated_at FROM shm_subscription")
        assert row["updated_at"] > 0
        assert store.read_pragma("page_count") * store.read_pragma("page_size") > 256 * 1024
    finally:
        store.close()


def test_drop_group_runs_as_a_consumer_write_in_a_full_store(tmp_path: Path) -> None:
    store = _bounded_store(tmp_path / "drop.db", 256 * 1024)
    try:
        store.subscribe(["events.Created"], "orders")
        _outgrow_the_limit_on_a_subscription_write(store, "DELETE")

        assert store.drop_group("orders") == (1, 0)

        assert store.get_subscriptions() == {}
        assert store.read_pragma("page_count") * store.read_pragma("page_size") > 256 * 1024
    finally:
        store.close()


def test_a_store_opened_above_a_lowered_limit_still_drains(tmp_path: Path) -> None:
    path = tmp_path / "lowered.db"
    store = _bounded_store(path, 1024 * 1024, orphan_retention_seconds=1e-6)
    try:
        store.subscribe(["events.Created"], "orders")
        published = _publish_until_refused(store)
    finally:
        store.close()

    store = _bounded_store(path, 256 * 1024, orphan_retention_seconds=1e-6)
    try:
        with pytest.raises(ConfigurationError, match="max_store_bytes"):
            store.publish("events.Created", b"{}", None, None)
        acked = 0
        while rows := store.claim("orders", 100, _LONG_CONSUMER, 30.0):
            for row in rows:
                assert store.ack(row["claim_token"], _LONG_CONSUMER, None)
                acked += 1
        assert acked == published
        store.publish("events.Created", b"{}", None, "after-drain")
    finally:
        store.close()


async def test_sqlite_allocates_one_global_sequence_across_store_instances(
    tmp_path: Path,
) -> None:
    path = tmp_path / "sequence.db"
    first_store = ShmColdStore(str(path))
    second_store = ShmColdStore(str(path))
    try:
        with pytest.raises(TypeError, match="unexpected keyword"):
            await first_store.publish(
                "events.Created",
                b"caller-selected",
                sequence=999,  # type: ignore[call-arg]
            )
        first = await first_store.publish("events.Created", b"one", publication_id="first")
        second = await second_store.publish("events.Created", b"two", publication_id="second")

        assert isinstance(first, PublishResult)
        assert (first.publication_id, first.sequence) == ("first", 1)
        assert (second.publication_id, second.sequence) == ("second", 2)
    finally:
        await first_store.close()
        await second_store.close()


async def test_subscription_matching_does_not_use_prefixes(tmp_path: Path) -> None:
    store = ShmColdStore(str(tmp_path / "exact.db"))
    try:
        await store.subscribe(["events.Created"], "g1")
        await store.publish("events.Created.v2", b"{}", publication_id="other")

        assert await store.claim("g1", consumer_name="worker") == []
    finally:
        await store.close()


async def test_subscribe_reconciles_exact_targets_without_dropping_existing_work(
    tmp_path: Path,
) -> None:
    store = ShmColdStore(str(tmp_path / "reconcile.db"), completion_mode="delete")
    try:
        await store.subscribe(
            ["events.Claimed", "events.Kept", "events.Pending"],
            "g1",
        )
        await store.publish(
            "events.Claimed",
            b"claimed-before-redeploy",
            publication_id="claimed-before-redeploy",
        )
        claimed = (await store.claim("g1", limit=1, consumer_name="worker"))[0]
        await store.publish(
            "events.Pending",
            b"pending-before-redeploy",
            publication_id="pending-before-redeploy",
        )

        await store.subscribe(["events.Kept"], "g1")

        assert await store.get_subscriptions() == {"events.Kept": ["g1"]}
        assert await store.ack(claimed["claim_token"], consumer_name="worker")

        # Removing subscriptions only changes future routing. Work committed
        # before the reconciliation remains an obligation for the group.
        pending = await store.claim("g1", consumer_name="worker")
        assert [row["message_id"] for row in pending] == ["pending-before-redeploy"]
        assert await store.ack(pending[0]["claim_token"], consumer_name="worker")

        await store.publish(
            "events.Claimed",
            b"excluded-after-redeploy",
            publication_id="excluded-after-redeploy",
        )
        assert await store.claim("g1", consumer_name="worker") == []

        await store.subscribe([], "g1")
        assert await store.get_subscriptions() == {}
        await store.publish(
            "events.Kept",
            b"excluded-after-empty-redeploy",
            publication_id="excluded-after-empty-redeploy",
        )
        assert await store.claim("g1", consumer_name="worker") == []
    finally:
        await store.close()


async def test_publication_replays_to_late_groups_within_the_default_hour(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orphan.db"
    store = ShmColdStore(str(path), completion_mode="delete")
    try:
        await store.subscribe(["events.Created"], "g1")
        await store.publish("events.Created", b"{}", publication_id="orphan")
        retained = _rows(
            path,
            "SELECT created_at, retained_until FROM shm_publication",
        )[0]
        assert retained["retained_until"] - retained["created_at"] == pytest.approx(3600.0)

        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.ack(first["claim_token"], consumer_name="worker-1")

        await store.subscribe(["events.Created"], "g2")
        second = await store.claim("g2", consumer_name="worker-2")
        assert second[0]["message_id"] == "orphan"
    finally:
        await store.close()


async def test_delete_ack_tombstone_survives_restart_without_blocking_new_group(
    tmp_path: Path,
) -> None:
    path = tmp_path / "completion-tombstone.db"
    first_store = await _published_store(path, completion_mode="delete")
    row = (await first_store.claim("g1", consumer_name="worker-1"))[0]
    assert await first_store.ack(row["claim_token"], consumer_name="worker-1")
    await first_store.close()

    restarted = ShmColdStore(str(path), completion_mode="delete")
    try:
        await restarted.subscribe(["events.Created"], "g1")
        await restarted.subscribe(["events.Created"], "g1")
        assert await restarted.claim("g1", consumer_name="worker-1") == []

        await restarted.subscribe(["events.Created"], "g2")
        late = await restarted.claim("g2", consumer_name="worker-2")
        assert [item["message_id"] for item in late] == ["publication-1"]
        tombstones = _rows(
            path,
            """
            SELECT publication_id, consumer_group
            FROM shm_completion_tombstone
            """,
        )
        assert [tuple(item) for item in tombstones] == [("publication-1", "g1")]

        assert await restarted.ack(
            late[0]["claim_token"],
            consumer_name="worker-2",
        )
        conn = sqlite3.connect(path)
        conn.execute("UPDATE shm_publication SET retained_until=0 WHERE id='publication-1'")
        conn.commit()
        conn.close()
        # Pruning is cadence-gated (see test_publish_prunes_on_a_bounded_cadence_
        # not_every_call), so reaching the cadence boundary -- not one publish --
        # is what removes the now-expired, delivery-free publication-1 row.
        for index in range(_shm_publications.PRUNE_EVERY_N_PUBLISHES):
            await restarted.publish("events.Other", b"{}", publication_id=f"other-{index}")

        assert (
            _rows(
                path,
                """
            SELECT publication_id FROM shm_completion_tombstone
            WHERE publication_id='publication-1'
            """,
            )
            == []
        )
        assert (
            _rows(
                path,
                "SELECT id FROM shm_publication WHERE id='publication-1'",
            )
            == []
        )
    finally:
        await restarted.close()


async def test_expired_no_subscriber_publication_is_not_replayed(
    tmp_path: Path,
) -> None:
    store = ShmColdStore(
        str(tmp_path / "expired.db"),
        orphan_retention_seconds=0.01,
    )
    try:
        await store.publish("events.Created", b"{}", publication_id="expired")
        await asyncio.sleep(0.02)
        await store.subscribe(["events.Created"], "g1")

        assert await store.claim("g1", consumer_name="worker") == []
    finally:
        await store.close()


async def test_expired_empty_publication_is_pruned_at_the_cadence_boundary(
    tmp_path: Path,
) -> None:
    """Pruning is gated to a bounded publish cadence rather than running as a
    full-table scan on every single publish, so an expired orphan survives
    until the cadence boundary (never earlier) and is gone once it is
    reached (never indefinitely). This supersedes the previous expectation
    that the very next publish always pruned -- that was exactly the
    unconditional per-publish scan the cadence gate replaces."""
    path = tmp_path / "publish-prune.db"
    store = ShmColdStore(str(path), orphan_retention_seconds=0.01)
    try:
        await store.publish("events.Created", b"old", publication_id="expired")
        await asyncio.sleep(0.02)

        cadence = _shm_publications.PRUNE_EVERY_N_PUBLISHES
        for index in range(cadence - 2):
            await store.publish(
                "events.Created",
                b"filler",
                publication_id=f"filler-{index}",
            )
        before_boundary = {row["id"] for row in _rows(path, "SELECT id FROM shm_publication")}
        assert "expired" in before_boundary

        await store.publish("events.Created", b"new", publication_id="retained")

        rows = {row["id"] for row in _rows(path, "SELECT id FROM shm_publication")}
        assert "expired" not in rows
        assert "retained" in rows
    finally:
        await store.close()


def test_publish_prunes_on_a_bounded_cadence_not_every_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A full-table prune scan running on every publish call makes publish
    latency grow with store size. It must instead run at most once per
    cadence window, proven here by counting real invocations rather than
    timing them."""
    calls: list[float] = []
    original_prune = _shm_publications.prune_expired_empty_publications

    def _counting_prune(conn: sqlite3.Connection, now: float, limit: int) -> int:
        calls.append(now)
        return original_prune(conn, now, limit)

    monkeypatch.setattr(
        _shm_publications,
        "prune_expired_empty_publications",
        _counting_prune,
    )

    store = SqliteQueueStore(
        str(tmp_path / "cadence.db"),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )
    try:
        store.subscribe(["events.Created"], "g1")
        windows = 3
        total_publishes = _shm_publications.PRUNE_EVERY_N_PUBLISHES * windows
        for index in range(total_publishes):
            store.publish("events.Created", b"{}", None, f"publication-{index}")

        assert len(calls) == windows
    finally:
        store.close()


async def test_concurrent_claims_partition_rows_without_duplicates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "atomic.db"
    store = ShmColdStore(str(path))
    peer = ShmColdStore(str(path))
    try:
        await store.subscribe(["events.Created"], "g1")
        for index in range(10):
            await store.publish(
                "events.Created",
                str(index).encode(),
                publication_id=f"publication-{index}",
            )

        batches = await asyncio.gather(
            store.claim("g1", limit=5, consumer_name="worker-1"),
            peer.claim("g1", limit=5, consumer_name="worker-2"),
        )
        tokens = [row["claim_token"] for batch in batches for row in batch]

        assert len(tokens) == 10
        assert len({token.delivery_id for token in tokens}) == 10
    finally:
        await store.close()
        await peer.close()


async def test_claim_batches_bound_aggregate_payload_and_leave_remaining_rows_claimable(
    tmp_path: Path,
) -> None:
    max_payload_bytes = 1024
    store = ShmColdStore(
        str(tmp_path / "bounded-claim.db"),
        max_payload_bytes=max_payload_bytes,
    )
    headers = {
        "event_type": "events.Created",
        "padding": "h" * 200,
    }
    try:
        await store.subscribe(["events.Created"], "g1")
        await store.publish(
            "events.Created",
            b"x" * max_payload_bytes,
            headers,
            publication_id="publication-over-budget-with-headers",
        )
        for index in range(8):
            await store.publish(
                "events.Created",
                b"x" * 400,
                headers,
                publication_id=f"publication-{index}",
            )

        claimed_ids: list[str] = []
        for index in range(9):
            batch = await store.claim(
                "g1",
                limit=100,
                consumer_name=f"worker-{index}",
            )
            aggregate_bytes = sum(
                len(row["payload"]) + len(json.dumps(row["headers"]).encode()) for row in batch
            )

            assert len(batch) == 1
            if index == 0:
                assert aggregate_bytes > max_payload_bytes
            else:
                assert aggregate_bytes <= max_payload_bytes
            claimed_ids.append(batch[0]["message_id"])

        assert claimed_ids[0] == "publication-over-budget-with-headers"
        assert len(set(claimed_ids)) == 9
        assert await store.claim("g1", consumer_name="worker-last") == []
    finally:
        await store.close()


def test_claim_sizes_the_batch_before_reading_any_payload(tmp_path: Path) -> None:
    """The byte budget is applied over LENGTH(), then the row query is capped
    to what it affords — so a claim that can only take one of many due
    candidates reads exactly one payload instead of the whole LIMIT window.
    The size check runs once per delivery-status branch (pending and
    stale-claimed) instead of once as a combined OR query, so each branch's
    LIMIT lands directly on its own indexed range scan rather than forcing a
    merge over the whole backlog before a single combined LIMIT can apply."""
    max_payload_bytes = 4096
    store = SqliteQueueStore(
        str(tmp_path / "sized-claim.db"),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
        max_payload_bytes=max_payload_bytes,
        max_store_bytes=1024 * 1024,
    )
    try:
        store.subscribe(["events.Created"], "g1")
        for index in range(6):
            store.publish(
                "events.Created",
                b"x" * max_payload_bytes,
                {"event_type": "events.Created"},
                f"publication-{index}",
            )

        statements: list[str] = []
        store._conn.set_trace_callback(statements.append)
        try:
            batch = store.claim("g1", 100, "worker-1", 60.0)
        finally:
            store._conn.set_trace_callback(None)

        assert [row["message_id"] for row in batch] == ["publication-0"]
        # sqlite3's trace callback expands bound parameters, so the LIMIT each
        # query actually ran with is visible here.
        sized = [text for text in statements if "LENGTH(p.payload)" in text]
        loaded = [text for text in statements if "d.last_error, p.*" in text]
        assert len(sized) == 2
        assert all("LIMIT 100" in text for text in sized)
        assert len(loaded) == 1
        assert "LIMIT 1" in loaded[0]
    finally:
        store.close()


def test_claim_cost_does_not_scale_with_pending_backlog_size(
    tmp_path: Path,
) -> None:
    """claim() must stay bounded by its own batch size as the pending
    backlog grows, because each delivery-status branch is queried with its
    own indexed LIMIT instead of materializing the whole backlog through a
    cross-branch sort before a single combined LIMIT can apply.

    Measured as SQLite VM instruction steps via set_progress_handler rather
    than wall-clock time: a deterministic proxy for work done, immune to
    machine/CI load (this suite's SHM lane also runs on windows-latest and
    macos runners, where a timing-based bound is flaky)."""

    def _claim_vm_steps(backlog: int) -> int:
        store = SqliteQueueStore(
            str(tmp_path / f"scale-{backlog}.db"),
            synchronous="NORMAL",
            completion_mode="delete",
            orphan_retention_seconds=86400.0,
            retry_backoff_base_seconds=0.05,
            retry_backoff_cap_seconds=5.0,
        )
        try:
            store.subscribe(["events.Created"], "g1")
            for index in range(backlog):
                store.publish(
                    "events.Created",
                    b"x",
                    None,
                    f"publication-{index}",
                )
            steps = [0]

            def _count_step() -> int:
                steps[0] += 1
                return 0

            store._conn.set_progress_handler(_count_step, 1)
            try:
                store.claim("g1", 10, "worker-1", 60.0)
            finally:
                store._conn.set_progress_handler(None, 0)
            return steps[0]
        finally:
            store.close()

    small = _claim_vm_steps(300)
    large = _claim_vm_steps(6000)
    # A 20x backlog growth must not translate into anywhere close to 20x
    # VM-step cost; an O(backlog) scan/sort would show roughly linear growth.
    assert large < small * 3 + 50


async def test_stale_reclaim_increments_generation_and_fences_old_owner(
    tmp_path: Path,
) -> None:
    store = await _published_store(tmp_path / "fencing.db")
    try:
        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        await asyncio.sleep(0.01)
        second = (
            await store.claim(
                "g1",
                consumer_name="worker-2",
                reclaim_stale_seconds=0,
            )
        )[0]
        stale_token = first["claim_token"]
        current_token = second["claim_token"]

        assert current_token.delivery_id == stale_token.delivery_id
        assert current_token.generation == stale_token.generation + 1
        assert await store.renew_claims([stale_token], "worker-1") == 0
        assert not await store.ack(stale_token, consumer_name="worker-1")
        assert not await store.fail(stale_token, "late failure", 3, consumer_name="worker-1")
        assert not await store.dead_letter(stale_token, "late poison", consumer_name="worker-1")
        assert await store.renew_claims([current_token], "worker-2") == 1
        assert await store.dead_letter(current_token, "poison", consumer_name="worker-2")
    finally:
        await store.close()


async def test_publication_ids_are_never_accepted_as_claim_tokens(
    tmp_path: Path,
) -> None:
    store = await _published_store(tmp_path / "strict-token.db")
    try:
        row = (await store.claim("g1", consumer_name="worker"))[0]

        with pytest.raises(ValueError, match="claim token"):
            await store.renew_claims(["publication-1"], "worker")
        with pytest.raises(ValueError, match="claim token"):
            await store.ack("publication-1", consumer_name="worker")
        with pytest.raises(ValueError, match="claim token"):
            await store.fail("publication-1", "failure", 2, consumer_name="worker")
        with pytest.raises(ValueError, match="claim token"):
            await store.dead_letter("publication-1", "poison", consumer_name="worker")
        assert (
            await store.renew_claims(
                [str(row["claim_token"])],
                "worker",
            )
            == 1
        )
    finally:
        await store.close()


@pytest.mark.parametrize(
    "value",
    ["cold:not-an-integer:1", "cold:1:0", "cold:0:1", "warm:1:1"],
)
def test_claim_token_decode_rejects_malformed_or_nonpositive_values(value: str) -> None:
    with pytest.raises(ValueError, match="valid claim token"):
        ClaimToken.decode(value)


def _delivery_state(path: Path, columns: str = "*") -> list[dict[str, Any]]:
    return [dict(row) for row in _rows(path, f"SELECT {columns} FROM shm_delivery ORDER BY id")]


async def test_release_claims_makes_an_unstarted_claim_claimable_at_once_without_a_charge(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release.db"
    store = await _published_store(
        path,
        retry_backoff_base_seconds=0.03,
        retry_backoff_cap_seconds=0.03,
    )
    columns = (
        "status, attempts, available_at, last_error, claimed_at, claimed_by, "
        "dispatch_started, completed_at"
    )
    try:
        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.fail(first["claim_token"], "temporary", 3, consumer_name="worker-1")
        await asyncio.sleep(0.04)
        retried = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert retried["attempts"] == 1
        assert await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=3600) == []
        claimed = _delivery_state(path, columns)

        assert await store.release_claims([retried["claim_token"]], "worker-1") == 1

        assert _delivery_state(path, columns) == [
            {**claimed[0], "status": "pending", "claimed_at": None, "claimed_by": None}
        ]
        (peer,) = await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=3600)
        assert (peer["message_id"], peer["attempts"], peer["last_error"]) == (
            "publication-1",
            1,
            "temporary",
        )
    finally:
        await store.close()


async def test_release_claims_leaves_a_claim_whose_dispatch_started(tmp_path: Path) -> None:
    path = tmp_path / "release-started.db"
    store = await _published_store(path)
    try:
        row = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.renew_claims([row["claim_token"]], "worker-1", start_dispatch=True) == 1
        before = _delivery_state(path)
        assert (before[0]["status"], before[0]["claimed_by"], before[0]["dispatch_started"]) == (
            "claimed",
            "worker-1",
            1,
        )

        assert await store.release_claims([row["claim_token"]], "worker-1") == 0

        assert _delivery_state(path) == before
    finally:
        await store.close()


async def test_release_claims_leaves_a_claim_another_consumer_owns(tmp_path: Path) -> None:
    path = tmp_path / "release-owner.db"
    store = await _published_store(path)
    try:
        row = (await store.claim("g1", consumer_name="worker-1"))[0]
        before = _delivery_state(path)
        assert (before[0]["status"], before[0]["claimed_by"]) == ("claimed", "worker-1")

        assert await store.release_claims([row["claim_token"]], "worker-2") == 0

        assert _delivery_state(path) == before
    finally:
        await store.close()


async def test_release_claims_leaves_a_claim_a_stale_reclaim_replaced(tmp_path: Path) -> None:
    path = tmp_path / "release-stale.db"
    store = await _published_store(path)
    try:
        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        await asyncio.sleep(0.01)
        second = (await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=0))[0]
        before = _delivery_state(path)
        assert (before[0]["status"], before[0]["claimed_by"]) == ("claimed", "worker-2")

        assert await store.release_claims([first["claim_token"]], "worker-1") == 0
        # The current owner presenting the replaced token differs only by generation.
        assert await store.release_claims([first["claim_token"]], "worker-2") == 0

        assert _delivery_state(path) == before
        assert await store.release_claims([second["claim_token"]], "worker-2") == 1
    finally:
        await store.close()


@pytest.mark.parametrize("peer", ["worker-2", "worker-1"], ids=["other-consumer", "same-consumer"])
async def test_release_claims_fences_the_old_token_once_a_peer_claims(
    tmp_path: Path,
    peer: str,
) -> None:
    store = await _published_store(tmp_path / "release-fence.db")
    try:
        old = (await store.claim("g1", consumer_name="worker-1"))[0]["claim_token"]
        assert await store.release_claims([old], "worker-1") == 1
        claimed = await store.claim("g1", consumer_name=peer, reclaim_stale_seconds=3600)
        new = claimed[0]["claim_token"]

        assert (new.delivery_id, new.generation) == (old.delivery_id, old.generation + 1)
        assert not await store.ack(old, consumer_name="worker-1")
        assert not await store.fail(old, "late failure", 3, consumer_name="worker-1")
        assert await store.renew_claims([old], "worker-1") == 0
        assert await store.release_claims([old], "worker-1") == 0
        assert await store.ack(new, consumer_name=peer)
    finally:
        await store.close()


async def test_release_claims_counts_only_the_deliveries_it_released(tmp_path: Path) -> None:
    store = ShmColdStore(str(tmp_path / "release-count.db"))
    try:
        await store.subscribe(["events.Created"], "g1")
        for index in range(3):
            await store.publish("events.Created", b"x", publication_id=f"publication-{index}")
        tokens = [row["claim_token"] for row in await store.claim("g1", consumer_name="worker-1")]
        assert await store.renew_claims([tokens[1]], "worker-1", start_dispatch=True) == 1

        assert await store.release_claims(tokens, "worker-1") == 2

        reclaimed = await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=3600)
        assert [row["message_id"] for row in reclaimed] == ["publication-0", "publication-2"]
    finally:
        await store.close()


def test_release_claims_of_nothing_opens_no_write_transaction(tmp_path: Path) -> None:
    store = SqliteQueueStore(
        str(tmp_path / "release-empty.db"),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )
    statements: list[str] = []
    try:
        store._conn.set_trace_callback(statements.append)
        try:
            assert store.release_claims([], "worker-1") == 0
        finally:
            store._conn.set_trace_callback(None)
    finally:
        store.close()

    assert [statement for statement in statements if statement.startswith("BEGIN")] == []


async def test_release_claims_requires_a_nonempty_string_owner(tmp_path: Path) -> None:
    store = ShmColdStore(str(tmp_path / "release-owners.db"))
    try:
        with pytest.raises(TypeError, match="consumer_name"):
            await store.release_claims([], 7)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="consumer_name"):
            await store.release_claims([], "")
    finally:
        await store.close()


def test_synchronous_release_claims_validates_owner_before_opening_transaction(
    tmp_path: Path,
) -> None:
    store = SqliteQueueStore(
        str(tmp_path / "sync-release-owner.db"),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )
    store.close()

    with pytest.raises(ValueError, match="consumer_name"):
        store.release_claims([], "")


async def test_release_interrupted_claims_hands_a_started_claim_back_without_a_charge(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-interrupted.db"
    store = await _published_store(
        path,
        retry_backoff_base_seconds=0.03,
        retry_backoff_cap_seconds=0.03,
    )
    columns = (
        "status, attempts, available_at, last_error, claimed_at, claimed_by, "
        "dispatch_started, completed_at"
    )
    try:
        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.fail(first["claim_token"], "temporary", 3, consumer_name="worker-1")
        await asyncio.sleep(0.04)
        retried = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert retried["attempts"] == 1
        assert await store.renew_claims([retried["claim_token"]], "worker-1", start_dispatch=True)
        claimed = _delivery_state(path, columns)
        assert claimed[0]["dispatch_started"] == 1

        assert await store.release_interrupted_claims([retried["claim_token"]], "worker-1") == 1

        assert _delivery_state(path, columns) == [
            {
                **claimed[0],
                "status": "pending",
                "claimed_at": None,
                "claimed_by": None,
                "dispatch_started": 0,
            }
        ]
        (peer,) = await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=3600)
        assert (peer["message_id"], peer["attempts"], peer["last_error"]) == (
            "publication-1",
            1,
            "temporary",
        )
    finally:
        await store.close()


async def test_release_interrupted_claims_leaves_a_claim_another_consumer_owns(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-interrupted-owner.db"
    store = await _published_store(path)
    try:
        row = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.renew_claims([row["claim_token"]], "worker-1", start_dispatch=True)
        before = _delivery_state(path)
        assert (before[0]["status"], before[0]["claimed_by"], before[0]["dispatch_started"]) == (
            "claimed",
            "worker-1",
            1,
        )

        assert await store.release_interrupted_claims([row["claim_token"]], "worker-2") == 0

        assert _delivery_state(path) == before
    finally:
        await store.close()


async def test_release_interrupted_claims_leaves_a_claim_a_stale_reclaim_replaced(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-interrupted-stale.db"
    store = await _published_store(path)
    try:
        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.renew_claims([first["claim_token"]], "worker-1", start_dispatch=True)
        await asyncio.sleep(0.01)
        second = (await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=0))[0]
        assert await store.renew_claims([second["claim_token"]], "worker-2", start_dispatch=True)
        before = _delivery_state(path)
        assert (before[0]["status"], before[0]["claimed_by"], before[0]["dispatch_started"]) == (
            "claimed",
            "worker-2",
            1,
        )

        assert await store.release_interrupted_claims([first["claim_token"]], "worker-1") == 0
        # The current owner presenting the replaced token differs only by generation.
        assert await store.release_interrupted_claims([first["claim_token"]], "worker-2") == 0

        assert _delivery_state(path) == before
        assert await store.release_interrupted_claims([second["claim_token"]], "worker-2") == 1
    finally:
        await store.close()


async def test_release_interrupted_claims_counts_only_the_deliveries_it_released(
    tmp_path: Path,
) -> None:
    store = ShmColdStore(str(tmp_path / "release-interrupted-count.db"))
    try:
        await store.subscribe(["events.Created"], "g1")
        for index in range(3):
            await store.publish("events.Created", b"x", publication_id=f"publication-{index}")
        tokens = [row["claim_token"] for row in await store.claim("g1", consumer_name="worker-1")]
        assert await store.renew_claims([tokens[1]], "worker-1", start_dispatch=True) == 1
        assert await store.release_claims([tokens[0]], "worker-1") == 1

        assert await store.release_interrupted_claims(tokens, "worker-1") == 2

        reclaimed = await store.claim("g1", consumer_name="worker-2", reclaim_stale_seconds=3600)
        assert [(row["message_id"], row["attempts"]) for row in reclaimed] == [
            ("publication-0", 0),
            ("publication-1", 0),
            ("publication-2", 0),
        ]
    finally:
        await store.close()


async def test_failure_applies_backoff_then_dead_letters_at_attempt_cap(
    tmp_path: Path,
) -> None:
    path = tmp_path / "retry.db"
    store = await _published_store(
        path,
        retry_backoff_base_seconds=0.03,
        retry_backoff_cap_seconds=0.03,
    )
    try:
        first = (await store.claim("g1", consumer_name="worker-1"))[0]
        assert await store.fail(
            first["claim_token"],
            "temporary",
            2,
            consumer_name="worker-1",
        )
        assert await store.claim("g1", consumer_name="worker-2") == []

        await asyncio.sleep(0.04)
        second = (await store.claim("g1", consumer_name="worker-2"))[0]
        assert second["attempts"] == 1
        assert await store.fail(
            second["claim_token"],
            "permanent",
            2,
            consumer_name="worker-2",
        )
        state = _rows(path, "SELECT status, attempts FROM shm_delivery")[0]
        assert dict(state) == {"status": "dead", "attempts": 2}
    finally:
        await store.close()


async def test_failure_caps_retry_exponent_before_exponentiation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "large-attempts.db"
    store = await _published_store(path)
    try:
        row = (await store.claim("g1", consumer_name="worker"))[0]
        conn = sqlite3.connect(path)
        conn.execute(
            "UPDATE shm_delivery SET attempts=9999 WHERE id=?",
            (row["claim_token"].delivery_id,),
        )
        conn.commit()
        conn.close()

        assert await store.fail(
            row["claim_token"],
            "still retryable",
            10001,
            consumer_name="worker",
        )
        state = _rows(path, "SELECT status, attempts FROM shm_delivery")[0]
        assert dict(state) == {"status": "pending", "attempts": 10000}
    finally:
        await store.close()


async def test_completion_operations_require_nonempty_string_owner(
    tmp_path: Path,
) -> None:
    store = ShmColdStore(str(tmp_path / "owners.db"))
    token = ClaimToken(1, 1)
    try:
        with pytest.raises(TypeError, match="consumer_name"):
            await store.ack(token)  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="consumer_name"):
            await store.fail(token, "failure", 2)  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="consumer_name"):
            await store.dead_letter(token, "poison")  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="consumer_name"):
            await store.renew_claims([], 7)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="consumer_name"):
            await store.renew_claims([], "")
        with pytest.raises(ValueError, match="consumer_name"):
            await store.ack(token, consumer_name="")
        with pytest.raises(ValueError, match="consumer_name"):
            await store.fail(token, "failure", 2, consumer_name="")
        with pytest.raises(ValueError, match="consumer_name"):
            await store.dead_letter(token, "poison", consumer_name="")
    finally:
        await store.close()


def test_synchronous_completion_operations_require_nonempty_string_owner(
    tmp_path: Path,
) -> None:
    store = SqliteQueueStore(
        str(tmp_path / "sync-owners.db"),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )
    token = ClaimToken(1, 1)
    try:
        with pytest.raises(ValueError, match="consumer_name"):
            store.renew_claims([], "")
        with pytest.raises(TypeError, match="consumer_name"):
            store.ack(token, None, None)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="consumer_name"):
            store.fail(token, "failure", 2, None)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="consumer_name"):
            store.dead_letter(token, "poison", None)  # type: ignore[arg-type]
    finally:
        store.close()


def test_synchronous_claim_validates_owner_before_opening_transaction(
    tmp_path: Path,
) -> None:
    store = SqliteQueueStore(
        str(tmp_path / "sync-claim-owner.db"),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )
    store.close()

    with pytest.raises(ValueError, match="consumer_name"):
        store.claim("g1", 1, "", 60.0)


@pytest.mark.parametrize(
    ("completion_mode", "expected_status"),
    [("delete", None), ("mark", "done")],
)
async def test_ack_supports_true_delete_and_mark_modes(
    tmp_path: Path,
    completion_mode: str,
    expected_status: str | None,
) -> None:
    path = tmp_path / f"{completion_mode}.db"
    store = await _published_store(path, completion_mode=completion_mode)
    try:
        row = (await store.claim("g1", consumer_name="worker"))[0]
        assert await store.ack(row["claim_token"], consumer_name="worker")

        states = _rows(path, "SELECT status FROM shm_delivery")
        assert (states[0]["status"] if states else None) == expected_status
    finally:
        await store.close()


async def test_prune_is_bounded_and_never_removes_nonterminal_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "prune.db"
    store = ShmColdStore(str(path), completion_mode="mark")
    try:
        await store.subscribe(["events.Created"], "g1")
        for index in range(4):
            await store.publish(
                "events.Created",
                b"{}",
                publication_id=f"publication-{index}",
            )
        claimed = await store.claim("g1", limit=3, consumer_name="worker")
        assert await store.ack(claimed[0]["claim_token"], consumer_name="worker")
        assert await store.dead_letter(claimed[1]["claim_token"], "poison", consumer_name="worker")

        assert await store.prune(retention_age_seconds=0, limit=1) == 1
        states = [row["status"] for row in _rows(path, "SELECT status FROM shm_delivery")]
        assert len(states) == 3
        assert "claimed" in states
        assert "pending" in states
        assert sum(status in {"done", "dead"} for status in states) == 1
    finally:
        await store.close()


@pytest.mark.parametrize("terminal_state", ["done", "dead"])
async def test_prune_tombstones_terminal_work_before_resubscribe_replay(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    path = tmp_path / f"prune-{terminal_state}.db"
    store = ShmColdStore(str(path), completion_mode="mark")
    try:
        await store.subscribe(["events.Created"], "g1")
        await store.publish(
            "events.Created",
            b"retained",
            publication_id="publication-1",
        )
        row = (await store.claim("g1", consumer_name="worker"))[0]
        if terminal_state == "done":
            assert await store.ack(row["claim_token"], consumer_name="worker")
        else:
            assert await store.dead_letter(
                row["claim_token"],
                "poison",
                consumer_name="worker",
            )

        await store.subscribe([], "g1")
        assert await store.prune(retention_age_seconds=0) == 1
        tombstones = _rows(
            path,
            """
            SELECT publication_id, consumer_group
            FROM shm_completion_tombstone
            """,
        )
        assert [tuple(item) for item in tombstones] == [("publication-1", "g1")]

        await store.subscribe(["events.Created"], "g1")
        assert await store.claim("g1", consumer_name="worker") == []
    finally:
        await store.close()


async def test_prune_uses_completion_time_instead_of_publication_age(
    tmp_path: Path,
) -> None:
    path = tmp_path / "completion-age.db"
    store = await _published_store(path, completion_mode="mark")
    try:
        row = (await store.claim("g1", consumer_name="worker"))[0]
        conn = sqlite3.connect(path)
        conn.execute("UPDATE shm_publication SET created_at=0 WHERE id='publication-1'")
        conn.execute("UPDATE shm_delivery SET created_at=0 WHERE publication_id='publication-1'")
        conn.commit()
        conn.close()

        assert await store.ack(row["claim_token"], consumer_name="worker")
        assert await store.prune(retention_age_seconds=60) == 0
        assert _rows(path, "SELECT status FROM shm_delivery")[0]["status"] == "done"

        conn = sqlite3.connect(path)
        conn.execute("UPDATE shm_delivery SET completed_at=0 WHERE publication_id='publication-1'")
        conn.commit()
        conn.close()
        assert await store.prune(retention_age_seconds=60) == 1
    finally:
        await store.close()


async def _dead_letter_at_claim(path: Path, **options: Any) -> ShmColdStore:
    """Dead-letter the only delivery through a charged stale reclaim."""
    store = await _published_store(path, **options)
    first = (await store.claim("g1", consumer_name="worker-1", max_attempts=1))[0]
    assert await store.renew_claims([first["claim_token"]], "worker-1", start_dispatch=True) == 1
    await asyncio.sleep(0.01)
    reclaimed = await store.claim(
        "g1", consumer_name="worker-2", reclaim_stale_seconds=0, max_attempts=1
    )
    assert reclaimed == []
    assert _delivery_state(path, "status")[0]["status"] == "dead"
    return store


async def test_a_row_dead_lettered_at_claim_time_is_pruned_once_its_retention_passes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "claim-dead-letter-prune.db"
    store = await _dead_letter_at_claim(path, orphan_retention_seconds=1e-6)
    try:
        before = time.time()
        [completed_at] = [row["completed_at"] for row in _delivery_state(path, "completed_at")]
        assert completed_at is not None
        assert completed_at <= before

        assert await store.prune(retention_age_seconds=3600) == 0
        assert len(_delivery_state(path)) == 1

        # The delivery goes first, then its now-orphaned, expired publication.
        assert await store.prune(retention_age_seconds=0) == 2
        assert _delivery_state(path) == []
        assert _rows(path, "SELECT id FROM shm_publication") == []
    finally:
        await store.close()


async def test_a_claim_time_dead_letter_retried_is_not_pruned(tmp_path: Path) -> None:
    path = tmp_path / "claim-dead-letter-retry.db"
    store = await _dead_letter_at_claim(path)
    try:
        assert await store.retry_dead_letters() == 1

        [row] = _delivery_state(path, "status, completed_at")
        assert (row["status"], row["completed_at"]) == ("pending", None)
        assert await store.prune(retention_age_seconds=0) == 0
        assert len(_delivery_state(path)) == 1
    finally:
        await store.close()


def test_prune_removes_a_row_completed_on_the_same_clock_tick_as_the_prune_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """retention_age_seconds=0 means "prunable once terminal for at least zero
    seconds". On a coarse wall clock (observed on Windows CI), the completion
    write and the prune read can land on the identical time.time() tick, so
    the row must still be pruned even though the clock never advanced between
    them."""
    path = tmp_path / "frozen-clock.db"
    store = SqliteQueueStore(
        str(path),
        synchronous="NORMAL",
        completion_mode="mark",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )
    try:
        monkeypatch.setattr(time, "time", lambda: 1_000_000.0)
        store.subscribe(["events.Created"], "g1")
        store.publish("events.Created", b"{}", None, "publication-1")
        claimed = store.claim("g1", 1, "worker", 60.0)
        assert store.ack(claimed[0]["claim_token"], "worker", None)

        assert store.prune(retention_age_seconds=0, limit=10) == 1

        assert _rows(path, "SELECT status FROM shm_delivery") == []
        tombstones = _rows(
            path,
            "SELECT publication_id, consumer_group FROM shm_completion_tombstone",
        )
        assert [tuple(item) for item in tombstones] == [("publication-1", "g1")]
    finally:
        store.close()


def _index_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
    }


def _new_store(path: Path) -> SqliteQueueStore:
    return SqliteQueueStore(
        str(path),
        synchronous="NORMAL",
        completion_mode="delete",
        orphan_retention_seconds=86400.0,
        retry_backoff_base_seconds=0.05,
        retry_backoff_cap_seconds=5.0,
    )


def test_fresh_shm_store_has_publication_expiry_index(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "fresh.db")
    try:
        assert "idx_shm_publication_expiry" in _index_names(store._conn)
    finally:
        store.close()


def test_shm_store_missing_expiry_index_is_backfilled_on_next_open(tmp_path: Path) -> None:
    path = tmp_path / "backfill.db"
    store = _new_store(path)
    store.close()

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("DROP INDEX idx_shm_publication_expiry")
    conn.commit()
    assert "idx_shm_publication_expiry" not in _index_names(conn)
    conn.close()

    reopened = _new_store(path)
    try:
        assert "idx_shm_publication_expiry" in _index_names(reopened._conn)
    finally:
        reopened.close()


def test_prune_expired_publications_query_uses_expiry_index(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "prune-plan.db")
    try:
        bounds = (time.time(), *_shm_publications._BEFORE_ALL, *_shm_publications._AFTER_ALL)
        plan = " | ".join(
            str(row["detail"])
            for row in store._conn.execute(
                f"EXPLAIN QUERY PLAN {_shm_publications._EXPIRED_BETWEEN_SQL}", bounds
            )
        )
        assert "idx_shm_publication_expiry" in plan
        assert "TEMP B-TREE" not in plan
    finally:
        store.close()


def test_group_backlog_with_targets_counts_only_that_groups_undelivered_rows_on_them(
    tmp_path: Path,
) -> None:
    store = _bounded_store(tmp_path / "backlog.db", 4 * 1024 * 1024, completion_mode="mark")
    try:
        store.subscribe(["events.A", "events.B"], "g")
        store.subscribe(["events.A"], "other")
        for index in range(3):
            store.publish("events.A", b'{"a":%d}' % index, None, None)
        for index in range(2):
            store.publish("events.B", b'{"b":%d}' % index, None, None)
        done = store.claim("g", 1, _LONG_CONSUMER, 30.0)
        assert store.ack(done[0]["claim_token"], _LONG_CONSUMER, None)
        assert store.claim("g", 1, _LONG_CONSUMER, 30.0), "a claimed row still counts"

        assert store.group_backlog() == {"g": 4, "other": 3}
        assert store.group_backlog(["events.A"]) == {"g": 2, "other": 3}
        assert store.group_backlog(["events.B"]) == {"g": 2}
        assert store.group_backlog(["events.A", "events.B"]) == {"g": 4, "other": 3}
        assert store.group_backlog(["events.never"]) == {}
        assert store.group_backlog([]) == {}
    finally:
        store.close()


def test_group_backlog_with_targets_counts_the_rows_drop_group_deletes(tmp_path: Path) -> None:
    store = _bounded_store(tmp_path / "drop-count.db", 4 * 1024 * 1024)
    try:
        store.subscribe(["events.A", "events.B"], "g")
        for target in ("events.A", "events.A", "events.B"):
            store.publish(target, b"{}", None, None)
        store.claim("g", 1, _LONG_CONSUMER, 30.0)

        counted = store.group_backlog(["events.A"])["g"]

        assert counted == 2
        assert store.drop_group("g", ["events.A"])[1] == counted
    finally:
        store.close()


def _publication_count(path: Path) -> int:
    return int(_rows(path, "SELECT count(*) AS n FROM shm_publication")[0]["n"])


def _expired_full_store(path: Path) -> tuple[SqliteQueueStore, int]:
    """A store that refuses publishes, holding only expired orphan publications."""
    store = _bounded_store(path, 1024 * 1024)
    published = 0
    with pytest.raises(ConfigurationError, match="max_store_bytes"):
        while True:
            store.publish("events.Created", b"x" * 1000, None, None)
            published += 1
    assert published >= 100
    conn = sqlite3.connect(path)
    conn.execute("UPDATE shm_publication SET retained_until=0")
    conn.commit()
    conn.close()
    store._publishes_since_prune = _shm_publications.PRUNE_EVERY_N_PUBLISHES - 1
    return store, published


def _headroom_pages(path: Path, max_store_bytes: int) -> int:
    conn = sqlite3.connect(path)
    try:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        used = int(conn.execute("PRAGMA page_count").fetchone()[0]) - int(
            conn.execute("PRAGMA freelist_count").fetchone()[0]
        )
    finally:
        conn.close()
    budget = max_store_bytes // page_size
    return budget - min(_shm_publications.CONSUMER_RESERVE_PAGES, budget // 8) - used


def test_a_publish_refused_for_a_full_store_keeps_the_prune_it_ran(tmp_path: Path) -> None:
    path = tmp_path / "refused.db"
    store, published = _expired_full_store(path)
    try:
        too_big = b"x" * (200 * 4096)

        with pytest.raises(ConfigurationError, match="max_store_bytes"):
            store.publish("events.Created", too_big, None, None)

        assert _publication_count(path) == published - _shm_publications._PUBLISH_PRUNE_LIMIT
        assert not store._conn.in_transaction
    finally:
        store.close()


def test_a_publish_that_fits_once_its_due_prune_frees_pages_succeeds(tmp_path: Path) -> None:
    path = tmp_path / "fits.db"
    store, published = _expired_full_store(path)
    try:
        page_size = 4096
        payload = b"x" * ((_headroom_pages(path, 1024 * 1024) + 10) * page_size)

        result = store.publish("events.Created", payload, None, "fits-after-prune")

        assert result.publication_id == "fits-after-prune"
        assert _publication_count(path) == (published - _shm_publications._PUBLISH_PRUNE_LIMIT + 1)
    finally:
        store.close()


def _insert_pinned_expired_publications(store: SqliteQueueStore, count: int) -> None:
    """Expired publications that each still hold a pending delivery."""
    conn = store._conn
    conn.execute("BEGIN")
    conn.executemany(
        """
        INSERT INTO shm_publication (id, target, event_type, payload, created_at, retained_until)
        VALUES (?, 'events.Pinned', 'events.Pinned', x'00', 1.0, 1.0)
        """,
        ((f"pinned-{index}",) for index in range(count)),
    )
    conn.execute(
        """
        INSERT INTO shm_delivery (
            publication_id, consumer_group, status, attempts,
            available_at, claim_generation, created_at
        )
        SELECT id, 'g', 'pending', 0, 0, 0, 0 FROM shm_publication
        """
    )
    conn.execute("COMMIT")


def _insert_expired_orphans(
    store: SqliteQueueStore, count: int, retained_until: float = 2.0
) -> None:
    conn = store._conn
    conn.execute("BEGIN")
    conn.executemany(
        """
        INSERT INTO shm_publication (id, target, event_type, payload, created_at, retained_until)
        VALUES (?, 'events.Orphan', 'events.Orphan', x'00', 2.0, ?)
        """,
        ((f"orphan-{index}-{retained_until}", retained_until) for index in range(count)),
    )
    conn.execute("COMMIT")


def _vm_steps(conn: sqlite3.Connection, operation: Callable[[], object]) -> int:
    ticks = 0

    def count() -> int:
        nonlocal ticks
        ticks += 1
        return 0

    conn.set_progress_handler(count, 10)
    try:
        operation()
    finally:
        conn.set_progress_handler(None, 0)
    return ticks


def test_a_publish_side_prune_walks_a_bounded_window_of_pinned_expired_publications(
    tmp_path: Path,
) -> None:
    def due_publish_steps(pinned: int) -> int:
        store = _new_store(tmp_path / f"pinned-{pinned}.db")
        try:
            _insert_pinned_expired_publications(store, pinned)
            for _ in range(_shm_publications.PRUNE_EVERY_N_PUBLISHES - 1):
                store.publish("events.Created", b"{}", None, None)
            return _vm_steps(
                store._conn, lambda: store.publish("events.Created", b"{}", None, None)
            )
        finally:
            store.close()

    assert due_publish_steps(8000) < 2 * due_publish_steps(1000)


def test_a_prune_that_runs_out_of_rows_wraps_to_the_oldest_in_the_same_call(
    tmp_path: Path,
) -> None:
    store = _new_store(tmp_path / "wrap.db")
    try:
        _insert_pinned_expired_publications(store, 1500)
        assert store.prune(1e9, 1000) == 0, "the first window holds only pinned rows"
        _insert_expired_orphans(store, 3, retained_until=0.5)

        assert store.prune(1e9, 1000) == 3
    finally:
        store.close()


@pytest.mark.parametrize("side", ["publish", "consumer"])
def test_repeated_prunes_reach_every_prunable_publication_behind_pinned_ones(
    tmp_path: Path, side: str
) -> None:
    path = tmp_path / f"{side}.db"
    store = _new_store(path)
    try:
        _insert_pinned_expired_publications(store, 3500)
        _insert_expired_orphans(store, 7)

        def prune_once() -> None:
            if side == "consumer":
                store.prune(1e9, 1000)
                return
            for _ in range(_shm_publications.PRUNE_EVERY_N_PUBLISHES):
                store.publish("events.Created", b"{}", None, None)

        orphans = "SELECT count(*) AS n FROM shm_publication WHERE target='events.Orphan'"
        calls = 0
        while _rows(path, orphans)[0]["n"]:
            calls += 1
            assert calls <= 6, "a prune never reached the orphans behind the pinned rows"
            prune_once()

        assert calls > 1, "one prune call walked all 3500 pinned rows"
        assert (
            _rows(path, "SELECT count(*) AS n FROM shm_publication WHERE id LIKE 'pinned-%'")[0][
                "n"
            ]
            == 3500
        )
    finally:
        store.close()
