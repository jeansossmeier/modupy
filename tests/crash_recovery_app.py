"""Subprocess helper for the outbox crash-recovery test.

Run in two modes against the SAME file-based SQLite database:

    python crash_recovery_app.py publish <db_path> <out_path>
    python crash_recovery_app.py recover <db_path> <out_path>

* ``publish`` opens a transaction, publishes N events (each persisted to the
  outbox in the same transaction), commits, then HARD-EXITS via ``os._exit``
  before the after-commit dispatch tasks can run — simulating a crash between
  commit and delivery.
* ``recover`` reconfigures the outbox; its retry loop's crash sweep finds the
  committed-but-undelivered publications and delivers them. The listener
  appends each delivered value to ``out_path``.

The event type is module-level so its fully-qualified name round-trips through
the serializer in both processes (both run this file as ``__main__``).

Not collected by pytest (no ``test_`` prefix); invoked as a subprocess by
tests/test_outbox_crash_recovery.py.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from modulith import event, publish
from modulith.adapters.postgres_outbox import (
    Base,
    PostgresPublicationStore,
    bind_session,
)
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

N_EVENTS = 25


@event
@dataclass(frozen=True)
class CrashEvent:
    n: int


# Set in main() from argv — kept module-level so importing this file (e.g. to
# read N_EVENTS) does not require command-line arguments.
_OUT_PATH = ""


async def deliver(evt: CrashEvent) -> None:
    with open(_OUT_PATH, "a") as fh:
        fh.write(f"{evt.n}\n")


async def _setup(db_path: str):
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    _runtime.configure(package="crashapp", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(CrashEvent, deliver)
    return engine


async def _publish(db_path: str) -> None:
    engine = await _setup(db_path)
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    session = async_sessionmaker(engine)()
    bind_session(session)  # not reset — this process crashes before teardown
    for i in range(N_EVENTS):
        await publish(CrashEvent(n=i))
    await session.commit()  # rows are durable; dispatch tasks just scheduled
    # Hard exit before the scheduled after-commit dispatch tasks run — this is
    # the crash. No further await, so the event loop never runs them.
    os._exit(0)


async def _recover(db_path: str) -> None:
    engine = await _setup(db_path)
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), retry_interval_seconds=0.05)

    # The crash sweep (older_than=0) runs at loop entry and delivers the
    # committed-but-undelivered publications. Poll the output for completion.
    for _ in range(200):
        if _count_lines(_OUT_PATH) >= N_EVENTS:
            break
        await asyncio.sleep(0.05)
    await outbox.shutdown()
    await store.dispose()
    await engine.dispose()


def _count_lines(path: str) -> int:
    try:
        with open(path) as fh:
            return sum(1 for _ in fh)
    except FileNotFoundError:
        return 0


def main() -> None:
    global _OUT_PATH
    mode, db_path, _OUT_PATH = sys.argv[1], sys.argv[2], sys.argv[3]
    if mode == "publish":
        asyncio.run(_publish(db_path))
    elif mode == "recover":
        asyncio.run(_recover(db_path))
    else:  # pragma: no cover - misuse
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
