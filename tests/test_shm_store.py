"""State-machine tests for the SQLite-authoritative SHM queue."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path

import pytest

from modulith import ConfigurationError
from modulith.adapters._shm_coldstore import ShmColdStore
from modulith.adapters._shm_store import ClaimToken, PublishResult, SqliteQueueStore


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
                assert "max_store_bytes" in str(error)
                assert "disk space" in str(error)
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


async def test_publication_replays_to_late_groups_within_24h(
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
        assert retained["retained_until"] - retained["created_at"] == pytest.approx(86400.0)

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
        await restarted.publish("events.Other", b"{}", publication_id="other")

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


async def test_later_publish_prunes_expired_delivery_free_publication(
    tmp_path: Path,
) -> None:
    path = tmp_path / "publish-prune.db"
    store = ShmColdStore(str(path), orphan_retention_seconds=0.01)
    try:
        await store.publish("events.Created", b"old", publication_id="expired")
        await asyncio.sleep(0.02)

        await store.publish("events.Created", b"new", publication_id="retained")

        rows = _rows(path, "SELECT id FROM shm_publication ORDER BY sequence")
        assert [row["id"] for row in rows] == ["retained"]
    finally:
        await store.close()


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
    Reading first and discarding afterwards made every poll of a large-payload
    backlog copy the entire window through SQLite's ORDER BY sorter."""
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
        # sqlite3's trace callback expands bound parameters, so the LIMIT the
        # row query actually ran with is visible here.
        sized = [text for text in statements if "LENGTH(p.payload)" in text]
        loaded = [text for text in statements if "d.last_error, p.*" in text]
        assert len(sized) == 1
        assert len(loaded) == 1
        assert "LIMIT 100" in sized[0]
        assert "LIMIT 1" in loaded[0]
    finally:
        store.close()


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
