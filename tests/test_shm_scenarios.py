"""End-to-end durability and process-isolation scenarios for the SHM broker."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Protocol

from modulith.adapters.shm_broker import ShmBroker

_EXIT_DURING_NOTIFY = 71
_EXIT_AFTER_CLAIM = 72
_EXIT_PHASE_TIMEOUT = 79
_PROCESS_TIMEOUT_SECONDS = 10.0


class _SpawnProcess(Protocol):
    @property
    def exitcode(self) -> int | None: ...

    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


def _spawn_publish(
    shm_name: str,
    db_path: str,
    producer_number: int,
    message_count: int,
) -> int:
    """Publish from a fresh interpreter without creating a local subscription."""

    async def run() -> int:
        broker = ShmBroker(
            shm_name=shm_name,
            capacity=32,
            slot_size=128,
            db_path=db_path,
            create=False,
        )
        try:
            for message_number in range(message_count):
                payload = f"producer-{producer_number}-message-{message_number}".encode()
                await broker.publish("events", payload, {"event_type": "contracts.Event"})
            return message_count
        finally:
            await broker.close()

    return asyncio.run(run())


def _spawn_publish_then_exit_in_notifier(
    shm_name: str,
    db_path: str,
    notifier_entered: Any,
    exit_now: Any,
) -> None:
    """Exit after SQLite commit but before the advisory notifier can write."""

    async def run() -> None:
        broker = ShmBroker(
            shm_name=shm_name,
            capacity=32,
            slot_size=128,
            db_path=db_path,
            create=False,
        )

        def exit_during_notify(_sequence: int) -> bool:
            # ShmBroker calls notify only after the authoritative commit returns.
            notifier_entered.set()
            if not exit_now.wait(_PROCESS_TIMEOUT_SECONDS):
                os._exit(_EXIT_PHASE_TIMEOUT)
            os._exit(_EXIT_DURING_NOTIFY)

        # Fault injection at the notifier boundary leaves the real durable store intact.
        broker._ring.notify = exit_during_notify  # type: ignore[assignment]
        await broker.publish("events", b"committed-before-notify")

    asyncio.run(run())


def _spawn_claim_then_exit(
    shm_name: str,
    db_path: str,
    claimed_rows: Any,
    claim_ready: Any,
    exit_now: Any,
) -> None:
    """Claim one real delivery, report its token, then exit without completion."""

    async def run() -> None:
        broker = ShmBroker(
            shm_name=shm_name,
            capacity=32,
            slot_size=128,
            db_path=db_path,
            create=False,
        )
        rows = await broker.claim_batch(
            "workers",
            batch_size=1,
            consumer_name="worker",
            reclaim_stale_seconds=1.0,
        )
        if len(rows) != 1:
            raise AssertionError(f"expected one claim, got {len(rows)}")
        row = rows[0]
        claimed_rows.put(
            {
                "id": row["id"],
                "delivery_id": row["claim_token"].delivery_id,
                "message_id": row["message_id"],
                "claim_generation": row["claim_generation"],
                "attempts": row["attempts"],
            }
        )
        # Flush the queue before signaling so the parent never races its feeder thread.
        claimed_rows.close()
        claimed_rows.join_thread()
        claim_ready.set()
        if not exit_now.wait(_PROCESS_TIMEOUT_SECONDS):
            os._exit(_EXIT_PHASE_TIMEOUT)
        os._exit(_EXIT_AFTER_CLAIM)

    asyncio.run(run())


def _stop_process(process: _SpawnProcess) -> None:
    """Reap a live child with terminate, then kill as a bounded fallback."""
    try:
        process.join(_PROCESS_TIMEOUT_SECONDS)
    except AssertionError:
        # Process.start() itself can fail; an unstarted child needs no cleanup.
        return
    if process.is_alive():
        process.terminate()
        process.join(_PROCESS_TIMEOUT_SECONDS)
    if process.is_alive():
        process.kill()
        process.join(_PROCESS_TIMEOUT_SECONDS)


def _assert_process_exit(process: _SpawnProcess, expected_exitcode: int) -> None:
    """Bound process cleanup before asserting the intended crash boundary."""

    _stop_process(process)
    assert not process.is_alive(), "spawned process did not exit after terminate/kill"
    assert process.exitcode == expected_exitcode


def _delivery_states(path: Path) -> list[tuple[str, int]]:
    connection = sqlite3.connect(path)
    try:
        return list(connection.execute("SELECT status, attempts FROM shm_delivery ORDER BY id"))
    finally:
        connection.close()


async def test_restart_recovers_every_published_message_exactly_once_per_claim(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "restart.db")
    first = ShmBroker(shm_name="restart-hints", capacity=2, db_path=db_path)
    expected = [f"message-{index}".encode() for index in range(12)]
    try:
        await first.subscribe(["events"], "workers")
        for payload in expected:
            await first.publish(
                "events",
                payload,
                {"event_type": "contracts.RestartedEvent"},
            )
    finally:
        await first.close()

    second = ShmBroker(
        shm_name="restart-hints",
        capacity=2,
        db_path=db_path,
        create=False,
    )
    try:
        # Re-subscribing is idempotent and must not duplicate retained work.
        await second.subscribe(["events"], "workers")
        rows = await second.claim_batch("workers", batch_size=100, consumer_name="worker")
        assert [row["payload"] for row in rows] == expected
        assert {row["event_type"] for row in rows} == {"contracts.RestartedEvent"}
        for row in rows:
            await second.ack(row["id"], consumer_name="worker")
        assert await second.claim_batch("workers", batch_size=100, consumer_name="worker") == []
    finally:
        await second.close()
        first._ring.unlink()


async def test_retry_and_dead_letter_applies_to_every_message(tmp_path: Path) -> None:
    db_path = tmp_path / "retries.db"
    broker = ShmBroker(shm_name="retry-hints", db_path=str(db_path))
    try:
        await broker.subscribe(["events"], "workers")
        for index in range(5):
            await broker.publish("events", f"message-{index}".encode())

        first_attempt = await broker.claim_batch("workers", batch_size=10, consumer_name="worker-1")
        assert len(first_attempt) == 5
        for row in first_attempt:
            await broker.fail(
                row["id"],
                "temporary",
                consumer_name="worker-1",
                max_attempts=2,
            )

        # The durable store's first retry delay is 50 ms.
        await asyncio.sleep(0.08)
        second_attempt = await broker.claim_batch(
            "workers", batch_size=10, consumer_name="worker-2"
        )
        assert len(second_attempt) == 5
        assert {row["attempts"] for row in second_attempt} == {1}
        for row in second_attempt:
            await broker.fail(
                row["id"],
                "permanent",
                consumer_name="worker-2",
                max_attempts=2,
            )

        assert _delivery_states(db_path) == [("dead", 2)] * 5
    finally:
        await broker.close()
        broker._ring.unlink()


async def test_long_consumer_groups_remain_isolated(tmp_path: Path) -> None:
    broker = ShmBroker(
        shm_name="long-group-hints",
        db_path=str(tmp_path / "long-groups.db"),
    )
    first_group = f"group:{'a' * 400}:one"
    second_group = f"group:{'a' * 400}:two"
    try:
        await broker.subscribe(["events"], first_group)
        await broker.subscribe(["events"], second_group)
        await broker.publish("events", b"shared")

        first_rows = await broker.claim_batch(first_group, batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in first_rows] == [b"shared"]
        await broker.ack(first_rows[0]["id"], consumer_name="worker")

        # Completing one long-named group cannot consume the peer's delivery.
        second_rows = await broker.claim_batch(second_group, batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in second_rows] == [b"shared"]
    finally:
        await broker.close()
        broker._ring.unlink()


async def test_missing_notifier_still_delivers_from_sqlite(tmp_path: Path) -> None:
    broker = ShmBroker(
        shm_name="missing-hints",
        db_path=str(tmp_path / "missing.db"),
        create=False,
    )
    try:
        assert not broker._ring.available
        await broker.subscribe(["events"], "workers")
        await broker.publish("events", b"durable")
        rows = await broker.claim_batch("workers", batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in rows] == [b"durable"]
    finally:
        await broker.close()


async def test_corrupt_notifier_still_delivers_from_sqlite(tmp_path: Path) -> None:
    notifier_path = tmp_path / "corrupt-hints"
    notifier_path.write_bytes(b"not a notifier")
    broker = ShmBroker(
        shm_name="corrupt-hints",
        db_path=str(tmp_path / "corrupt.db"),
    )
    try:
        assert not broker._ring.available
        await broker.subscribe(["events"], "workers")
        await broker.publish("events", b"durable")
        rows = await broker.claim_batch("workers", batch_size=10, consumer_name="worker")
        assert [row["payload"] for row in rows] == [b"durable"]
        assert notifier_path.read_bytes() == b"not a notifier"
    finally:
        await broker.close()


async def test_spawned_producers_publish_exactly_150_unique_messages(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "spawn.db")
    broker = ShmBroker(
        shm_name="spawn-hints",
        capacity=32,
        db_path=db_path,
    )
    await broker.subscribe(["events"], "workers")
    context = multiprocessing.get_context("spawn")

    try:
        pool = context.Pool(3)
        try:
            result = pool.starmap_async(
                _spawn_publish,
                [("spawn-hints", db_path, producer_number, 50) for producer_number in range(3)],
            )
            published = result.get(timeout=_PROCESS_TIMEOUT_SECONDS)
        finally:
            pool.terminate()
            pool.join()
        assert published == [50, 50, 50]

        rows = await broker.claim_batch("workers", batch_size=200, consumer_name="worker")
        payloads = [row["payload"] for row in rows]
        assert len(payloads) == 150
        assert len(set(payloads)) == 150
        assert set(payloads) == {
            f"producer-{producer}-message-{message}".encode()
            for producer in range(3)
            for message in range(50)
        }
    finally:
        await broker.close()
        broker._ring.unlink()


async def test_spawned_publisher_crash_inside_notifier_keeps_committed_payload(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    db_path = str(tmp_path / "publish-crash.db")
    notifier_path = str(tmp_path / "publish-crash.hints")
    broker = ShmBroker(
        shm_name=notifier_path,
        capacity=32,
        slot_size=128,
        db_path=db_path,
    )
    notifier_entered = context.Event()
    exit_now = context.Event()
    process = context.Process(
        target=_spawn_publish_then_exit_in_notifier,
        args=(notifier_path, db_path, notifier_entered, exit_now),
    )
    try:
        await broker.subscribe(["events"], "workers")
        process.start()
        assert notifier_entered.wait(_PROCESS_TIMEOUT_SECONDS)
        exit_now.set()
        _assert_process_exit(process, _EXIT_DURING_NOTIFY)

        rows = await broker.claim_batch(
            "workers",
            batch_size=1,
            consumer_name="parent",
        )
        assert [row["payload"] for row in rows] == [b"committed-before-notify"]
    finally:
        exit_now.set()
        _stop_process(process)
        await broker.close()
        broker._ring.unlink()


async def test_spawned_claimant_crash_preserves_lease_and_fences_stale_token(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    db_file = tmp_path / "claim-crash.db"
    db_path = str(db_file)
    notifier_path = str(tmp_path / "claim-crash.hints")
    broker = ShmBroker(
        shm_name=notifier_path,
        capacity=32,
        slot_size=128,
        db_path=db_path,
    )
    claimed_rows = context.Queue()
    claim_ready = context.Event()
    exit_now = context.Event()
    process = context.Process(
        target=_spawn_claim_then_exit,
        args=(notifier_path, db_path, claimed_rows, claim_ready, exit_now),
    )
    reclaim_after_seconds = 1.0
    try:
        await broker.subscribe(["events"], "workers")
        await broker.publish("events", b"abandoned-claim")
        process.start()
        assert claim_ready.wait(_PROCESS_TIMEOUT_SECONDS)
        stale = claimed_rows.get(timeout=_PROCESS_TIMEOUT_SECONDS)
        exit_now.set()
        _assert_process_exit(process, _EXIT_AFTER_CLAIM)

        # A crashed owner does not make its claim immediately available.
        assert (
            await broker.claim_batch(
                "workers",
                batch_size=1,
                consumer_name="worker",
                reclaim_stale_seconds=reclaim_after_seconds,
            )
            == []
        )

        connection = sqlite3.connect(db_file)
        try:
            claimed_at = float(
                connection.execute(
                    "SELECT claimed_at FROM shm_delivery WHERE id=?",
                    (stale["delivery_id"],),
                ).fetchone()[0]
            )
        finally:
            connection.close()
        # Wait only for the unexpired remainder of the real persisted lease.
        stale_wait = max(0.0, claimed_at + reclaim_after_seconds - time.time()) + 0.02
        assert stale_wait <= reclaim_after_seconds + 0.02
        await asyncio.sleep(stale_wait)

        reclaimed = await broker.claim_batch(
            "workers",
            batch_size=1,
            consumer_name="worker",
            reclaim_stale_seconds=reclaim_after_seconds,
        )
        assert len(reclaimed) == 1
        current = reclaimed[0]
        assert current["message_id"] == stale["message_id"]
        assert current["claim_generation"] == stale["claim_generation"] + 1
        assert current["attempts"] == stale["attempts"]

        # The owner name is intentionally unchanged so generation fencing is decisive.
        assert await broker.renew_claims([stale["id"]], consumer_name="worker") == 0
        await broker.ack(stale["id"], consumer_name="worker")
        await broker.fail(
            stale["id"],
            "stale failure",
            consumer_name="worker",
            max_attempts=1,
        )
        await broker.dead_letter(
            stale["id"],
            "stale poison",
            consumer_name="worker",
        )
        assert _delivery_states(db_file) == [("claimed", current["attempts"])]
        assert await broker.renew_claims([current["id"]], consumer_name="worker") == 1
        await broker.ack(current["id"], consumer_name="worker")
        assert (
            await broker.claim_batch(
                "workers",
                batch_size=1,
                consumer_name="worker",
                reclaim_stale_seconds=0.0,
            )
            == []
        )
    finally:
        exit_now.set()
        _stop_process(process)
        claimed_rows.close()
        claimed_rows.join_thread()
        await broker.close()
        broker._ring.unlink()


async def test_notifier_deleted_after_spawned_commit_cannot_hide_payload(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    db_path = str(tmp_path / "deleted-notifier.db")
    notifier_path = tmp_path / "deleted-notifier.hints"
    setup = ShmBroker(
        shm_name=str(notifier_path),
        capacity=32,
        slot_size=128,
        db_path=db_path,
    )
    reopened: ShmBroker | None = None
    process = context.Process(
        target=_spawn_publish,
        args=(str(notifier_path), db_path, 0, 1),
    )
    try:
        await setup.subscribe(["events"], "workers")
        await setup.close()
        process.start()
        _assert_process_exit(process, 0)
        assert notifier_path.is_file()
        notifier_path.unlink()

        reopened = ShmBroker(
            shm_name=str(notifier_path),
            capacity=32,
            slot_size=128,
            db_path=db_path,
            create=False,
        )
        assert not reopened._ring.available
        rows = await reopened.claim_batch(
            "workers",
            batch_size=1,
            consumer_name="parent",
        )
        assert [row["payload"] for row in rows] == [b"producer-0-message-0"]
    finally:
        _stop_process(process)
        if reopened is not None:
            await reopened.close()
        await setup.close()
        setup._ring.unlink()
