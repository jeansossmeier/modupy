import asyncio
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import aiosqlite  # noqa: F401
import pytest
import sqlalchemy.dialects.sqlite.aiosqlite
import sqlalchemy.ext.asyncio  # noqa: F401
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.testing import ModulithTestApp

# modulith_app drops every module first imported during a test, so the libraries
# the marketplace uses are imported here, at collection time.
Delivered = Callable[[int], Awaitable[dict[str, int]]]
PROJECT = Path(__file__).resolve().parents[1]


def run_python(*args: str, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=PROJECT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path}/marketplace.db"
    monkeypatch.setenv("MODULITH_OUTBOX_URL", url)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    for command in (("-m", "modulith", "migrate"), ("-m", "marketplace.schema")):
        result = run_python(*command)
        assert result.returncode == 0, result.stderr
    return url


@pytest.fixture
async def marketplace(
    database: str, modulith_app: ModulithTestApp
) -> AsyncIterator[ModulithTestApp]:
    yield modulith_app
    await _runtime.shutdown()
    from marketplace.db import engine

    await engine().dispose()


@pytest.fixture
def delivered() -> Delivered:
    async def wait_for(completed: int) -> dict[str, int]:
        deadline = time.monotonic() + 5
        while True:
            counts = await outbox.status()
            if counts["incomplete"] == 0 and counts["completed"] == completed:
                return counts
            if time.monotonic() > deadline:
                return counts
            await asyncio.sleep(0.05)

    return wait_for
