#!/usr/bin/env python3
"""Same-run durable-broker benchmark, excluded by pytest's ``test_*`` pattern."""

from __future__ import annotations

import argparse
import asyncio
import math
import platform
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Protocol

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from modulith.adapters.shm_broker import ShmBroker  # noqa: E402

TARGET = "bench.orders.placed"
GROUP = "bench-workers"
CONSUMER = "bench-consumer"
OPERATIONS = ("publish", "claim", "ack", "e2e")


class BenchBroker(Protocol):
    async def subscribe(self, targets: list[str], group: str) -> None: ...

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None: ...

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
    ) -> list[dict[str, Any]]: ...

    async def ack(self, row_id: str, *, consumer_name: str) -> None: ...

    async def close(self) -> None: ...


def _elapsed_ms(start_ns: int) -> float:
    return (time.perf_counter_ns() - start_ns) / 1_000_000


async def _measure(
    broker: BenchBroker,
    *,
    messages: int,
    warmup: int,
    rounds: int,
) -> dict[str, list[float]]:
    await broker.subscribe([TARGET], GROUP)
    samples: dict[str, list[float]] = {operation: [] for operation in OPERATIONS}
    sequence = 0

    for round_index in range(rounds + 1):
        measured = round_index > 0
        count = messages if measured else warmup
        for _ in range(count):
            payload = sequence.to_bytes(8, "big")
            sequence += 1
            e2e_start = time.perf_counter_ns()

            start = time.perf_counter_ns()
            await broker.publish(TARGET, payload, {"event_type": TARGET})
            publish_ms = _elapsed_ms(start)

            start = time.perf_counter_ns()
            rows = await broker.claim_batch(GROUP, batch_size=1, consumer_name=CONSUMER)
            claim_ms = _elapsed_ms(start)
            if len(rows) != 1 or rows[0]["payload"] != payload:
                actual = [row.get("payload") for row in rows]
                raise RuntimeError(
                    "one-delivery-per-operation validation failed in the no-fault "
                    f"serial benchmark for sequence {sequence - 1}: {actual!r}"
                )

            start = time.perf_counter_ns()
            await broker.ack(rows[0]["id"], consumer_name=CONSUMER)
            ack_ms = _elapsed_ms(start)

            if measured:
                samples["publish"].append(publish_ms)
                samples["claim"].append(claim_ms)
                samples["ack"].append(ack_ms)
                samples["e2e"].append(_elapsed_ms(e2e_start))

    leftovers = await broker.claim_batch(GROUP, batch_size=1, consumer_name=CONSUMER)
    if leftovers:
        raise RuntimeError(
            "one-delivery-per-operation validation found unexpected leftover "
            "deliveries in the no-fault serial benchmark"
        )
    expected = messages * rounds
    if any(len(values) != expected for values in samples.values()):
        raise RuntimeError(f"expected {expected} measured deliveries")
    return samples


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _print_results(results: dict[str, dict[str, list[float]]]) -> None:
    print(
        "\nValidated one delivery per operation in this no-fault serial benchmark; "
        "this is not an exactly-once broker guarantee. Latencies are milliseconds."
    )
    print(f"{'broker':<16} {'operation':<10} {'p50':>10} {'p95':>10} {'p99':>10} {'mean':>10}")
    print("-" * 72)
    for broker_name, operations in results.items():
        for operation in OPERATIONS:
            values = operations[operation]
            print(
                f"{broker_name:<16} {operation:<10} "
                f"{_percentile(values, 0.50):>10.3f} "
                f"{_percentile(values, 0.95):>10.3f} "
                f"{_percentile(values, 0.99):>10.3f} "
                f"{statistics.fmean(values):>10.3f}"
            )


async def _database_broker(path: Path) -> BenchBroker | None:
    try:
        from sqlalchemy.engine import URL

        from modulith.adapters.db_broker import DatabaseBroker
    except ImportError:
        return None
    url = URL.create("sqlite+aiosqlite", database=str(path))
    return DatabaseBroker(
        url=url,
        engine_options={"sqlite_synchronous": "NORMAL"},
    )


async def main(messages: int, warmup: int, rounds: int) -> None:
    results: dict[str, dict[str, list[float]]] = {}
    print(
        f"Python {platform.python_version()} on {platform.platform()} | "
        f"{messages} messages x {rounds} rounds, {warmup} warmup"
    )

    with tempfile.TemporaryDirectory(prefix="modulith-bench-") as temporary:
        root = Path(temporary)
        shm = ShmBroker(
            shm_name=str(root / "shm.hints"),
            db_path=str(root / "shm.db"),
            synchronous="NORMAL",
        )
        try:
            results["ShmBroker"] = await _measure(
                shm,
                messages=messages,
                warmup=warmup,
                rounds=rounds,
            )
        finally:
            await shm.close()

        database = await _database_broker(root / "database.db")
        if database is not None:
            try:
                results["DatabaseBroker"] = await _measure(
                    database,
                    messages=messages,
                    warmup=warmup,
                    rounds=rounds,
                )
            finally:
                await database.close()
        else:
            print("DatabaseBroker skipped: install modupy[database] for same-run comparison.")

    _print_results(results)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=500)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if args.messages < 1 or args.warmup < 0 or args.rounds < 1:
        parser.error("messages and rounds must be positive; warmup must be non-negative")
    return args


if __name__ == "__main__":
    options = _parse_args()
    asyncio.run(main(options.messages, options.warmup, options.rounds))
