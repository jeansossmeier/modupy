"""Shared pytest fixtures for the modulith test suite.

This file provides the ``make_fake_app`` factory used across integration
tests (zero-config, manifest, outbox, verifier, etc.) plus the legacy
``fake_app`` fixture that test_zero_config.py was originally written
against.

Why a factory: each subsystem test wants a different module shape
(manifest tests need ``_manifest.py`` files, verifier tests need
``_internal/`` packages, outbox tests need handlers that touch a session,
etc.). Hardcoding one shape would force every test to either reuse it
even when wrong or build its own from scratch — both are bad. The factory
takes a dict of ``module_name -> source_code`` so each test declares
exactly what it needs.

Cleanup is automatic: at fixture teardown, all modules under the fake app
are removed from ``sys.modules`` and the modulith runtime singleton is
reset, so test order can never matter.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import random
import re
import socket
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from textwrap import dedent
from typing import Any, NamedTuple
from uuid import uuid4

import pytest

from modulith.config import _announced_broker_defaults

FENCE = re.compile(r"^```(\S*)\s*$")

_installed_loop: asyncio.AbstractEventLoop | None = None


def replace_current_event_loop() -> None:
    """Install a fresh current event loop, closing the one installed here last.

    A test whose code calls ``asyncio.run()`` leaves the thread with no current
    loop, and later sync tests call ``asyncio.get_event_loop()``. A loop that is
    installed and then replaced without ``close()`` is finalised by the garbage
    collector, raising an "unclosed event loop" ResourceWarning in whichever
    test happens to be running then.
    """
    global _installed_loop
    if _installed_loop is not None and not _installed_loop.is_running():
        _installed_loop.close()
    _installed_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_installed_loop)


def pytest_sessionfinish() -> None:
    if _installed_loop is not None and not _installed_loop.is_running():
        _installed_loop.close()


class Block(NamedTuple):
    """One fenced code block: its info string, first content line, and body."""

    lang: str
    line: int
    body: str


def fenced_blocks(path: Path) -> list[Block]:
    """Every fenced block in a Markdown file, with 1-based line numbers."""
    lines = path.read_text(encoding="utf-8").splitlines()
    blocks: list[Block] = []
    opened: tuple[str, int] | None = None
    for number, text in enumerate(lines, start=1):
        fence = FENCE.match(text)
        if fence is None:
            continue
        if opened is None:
            opened = (fence.group(1), number + 1)
        else:
            lang, start = opened
            blocks.append(Block(lang, start, "\n".join(lines[start - 1 : number - 1])))
            opened = None
    if opened is not None:
        raise AssertionError(f"{path}:{opened[1] - 1} opens a code fence that is never closed")
    return blocks


@pytest.fixture(autouse=True)
def _fresh_broker_announcements() -> Iterator[None]:
    """Give each test the clean slate a freshly-started process would have.

    ``load_configuration`` announces an auto-selected broker once per process
    and remembers the decision in module-global state. Without this, the first
    test anywhere in the suite that defaults a broker silences the warning for
    every test that runs after it, so whether a test asserting on the
    announcement passes depends on which files ran before it.
    """
    _announced_broker_defaults.clear()
    yield
    _announced_broker_defaults.clear()


@pytest.fixture(autouse=True)
def _private_state_home(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the broker state a test or its workers create out of the real home.

    The default state directory is named after the package's path, so every
    test's fresh ``tmp_path`` app would otherwise leave a new directory under
    ``~/.local/state/modulith`` for good. macOS resolves the state home from
    HOME alone (``modulith.adapters._state_path._default_state_home``), which
    cannot be redirected here without breaking git in tests.
    """
    state_home = str(tmp_path_factory.mktemp("state-home"))
    monkeypatch.setenv("XDG_STATE_HOME", state_home)
    monkeypatch.setenv("LOCALAPPDATA", state_home)


def _free_port() -> int:
    """Find a free TCP port by binding to port 0, then releasing it.

    TOCTOU-style: the port is released before return, so another process
    could claim it. Callers should bind immediately after calling this.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


def _free_port_block(size: int) -> int:
    """The first of ``size`` consecutive free TCP ports, all below 32768.

    A proxy plus its workers need adjacent ports, and the server binds them
    seconds after this probe releases them. ``_free_port`` would land in the
    ephemeral range (Linux 32768+, macOS and Windows 49152+), where any
    outgoing connection in that window can take a worker's port, so the block
    is drawn from below it.
    """
    while True:
        base = random.randrange(20000, 32768 - size)
        held: list[socket.socket] = []
        try:
            for port in range(base, base + size):
                held.append(socket.socket())
                held[-1].bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            for sock in held:
                sock.close()
        return base


async def _serve(app: Any, port: int, http: Any) -> tuple[Any, asyncio.Task[None]]:
    """Serve ``app`` with uvicorn on ``port`` in this loop; return once it listens."""
    import uvicorn

    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, http=http, log_level="warning")
    )
    task = asyncio.create_task(server.serve())
    deadline = time.monotonic() + 15.0
    while not server.started:
        assert not task.done(), f"server on port {port} died: {task.exception()!r}"
        assert time.monotonic() < deadline, f"server on port {port} never came up"
        await asyncio.sleep(0.02)
    return server, task


def _health_answer(request: Any, token: str, module: str = "orders") -> dict[str, str]:
    """What a worker of the deployment holding ``token`` answers ``/health`` with.

    Built by the worker's own ``health_identity``, so a fake backend and the real
    worker prove identity the same way. A Starlette ``Request`` carries the nonce
    header and the ASGI ``server`` entry; ``httpx.ASGITransport`` leaves the port
    ``None`` for a default-port URL, which the proxy reads as 80.
    """
    from modulith._worker import NONCE_HEADER, health_identity

    server = request.scope["server"]
    server = (server[0], server[1] or 80)
    return {
        "status": "ok",
        **health_identity(module, token, request.headers.get(NONCE_HEADER), server),
    }


def _held_backend(release: asyncio.Event, in_flight: list[int], deployment: str = "") -> Any:
    """Worker app whose ``/orders/slow`` holds its connection until ``release``."""
    from fastapi import FastAPI
    from starlette.responses import JSONResponse

    up = FastAPI()

    @up.get("/orders/slow")
    async def slow() -> dict[str, str]:
        in_flight[0] += 1
        await release.wait()
        return {"ok": "slow"}

    @up.get("/orders/fast")
    async def fast() -> dict[str, str]:
        return {"ok": "fast"}

    async def health(request: Any) -> Any:
        return JSONResponse(_health_answer(request, deployment))

    up.add_route("/health", health)
    return up


async def _wait_for(predicate: Callable[[], bool], seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not predicate() and time.monotonic() < deadline:
        await asyncio.sleep(0.02)


@pytest.fixture
def make_fake_app(tmp_path: Path, monkeypatch) -> Callable[..., str]:
    """Factory fixture: build an importable fake app on disk.

    Usage::

        def test_thing(make_fake_app):
            pkg = make_fake_app({
                "orders": "from modulith import event\\n@event\\nclass X: ...",
                "inventory": "from modulith import listener\\n...",
            }, package_name="myapp")  # default: "fakeapp"
            # `pkg` is the importable package name.

    Module sources are dedent'd before being written so callers can use
    triple-quoted strings with leading indentation. Each named module
    becomes a subpackage with an ``__init__.py`` containing the source.

    Files beyond the module's ``__init__.py`` (e.g. ``_manifest.py``,
    ``_internal/persistence.py``) can be created via the optional
    ``extra_files`` argument: a dict of relative path -> source code.
    """
    created_packages: list[str] = []

    def _make(
        modules: dict[str, str],
        *,
        package_name: str = "fakeapp",
        extra_files: dict[str, str] | None = None,
    ) -> str:
        app_dir = tmp_path / package_name
        if not app_dir.exists():
            app_dir.mkdir()
            (app_dir / "__init__.py").write_text("")
            created_packages.append(package_name)

        for module_name, source in modules.items():
            mod_dir = app_dir / module_name
            mod_dir.mkdir(exist_ok=True)
            (mod_dir / "__init__.py").write_text(dedent(source))

        if extra_files:
            for rel_path, source in extra_files.items():
                target = app_dir / rel_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(dedent(source))

        # Make the fake app importable and set cwd so pyproject auto-detect
        # (when relevant) lands in the test's tmp_path rather than walking
        # up to the modulith repo's own pyproject.toml.
        monkeypatch.syspath_prepend(str(tmp_path))
        monkeypatch.chdir(tmp_path)

        return package_name

    yield _make

    # Cleanup: drop the fake-app modules from sys.modules and reset the
    # modulith runtime so the next test starts from a clean slate.
    for pkg in created_packages:
        for mod_name in list(sys.modules):
            if mod_name == pkg or mod_name.startswith(f"{pkg}."):
                del sys.modules[mod_name]

    # Imported here so test files that don't use modulith state don't pay
    # the import cost just by depending on this fixture.
    from modulith.runtime import _runtime

    _runtime._reset_for_testing()


@pytest.fixture
def fake_app(make_fake_app: Callable[..., str]) -> str:
    """Two-module fake app (orders + inventory) for zero-config tests.

    Provides a stable shape used by ``tests/test_zero_config.py``: orders
    publishes ``OrderCreated``; inventory listens and records receipts.
    New test files should prefer ``make_fake_app`` directly so they can
    declare exactly the modules they need.
    """
    return make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderCreated:
                    order_id: str

                # Module-level state for tests to inspect.
                published: list[OrderCreated] = []

                async def create_order(order_id: str) -> None:
                    evt = OrderCreated(order_id=order_id)
                    published.append(evt)
                    await publish(evt)
            """,
            "inventory": """
                from modulith import listener
                from fakeapp.orders import OrderCreated

                # Module-level state for tests to inspect.
                received: list[OrderCreated] = []

                @listener
                async def reserve_stock(event: OrderCreated) -> None:
                    received.append(event)
            """,
        }
    )


# Integration fixtures prefer explicit service URLs, fall back to pinned
# testcontainers images, and skip when neither is available. Database tests
# create disposable databases; Redis tests remove only their namespaced keys.

_PG_IMAGE = os.environ.get("MODULITH_TEST_POSTGRES_IMAGE", "postgres:16-alpine")
_REDIS_IMAGE = os.environ.get("MODULITH_TEST_REDIS_IMAGE", "redis:7-alpine")
_MYSQL_IMAGE = os.environ.get("MODULITH_TEST_MYSQL_IMAGE", "mysql:8.0")

# Deterministic teardown happens at session end via the context managers below,
# so the Ryuk resource-reaper (an extra image pull) is unnecessary. Opt back in
# by exporting TESTCONTAINERS_RYUK_DISABLED=false before running.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


def _docker_available() -> bool:
    """True when a Docker daemon is reachable from this process."""
    try:
        import docker
    except ImportError:
        return False
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


def _testcontainer_class(module: str, name: str) -> Any:
    """Return testcontainers' ``name`` class from ``module`` (postgres, mysql or redis).

    testcontainers 4.15 moved these modules under ``testcontainers.community`` and
    warns on the old import paths; the earlier 4.x releases, which the
    ``integration`` extra still allows, have only the old ones. Raises ImportError
    when testcontainers is not installed.
    """
    try:
        found = importlib.import_module(f"testcontainers.community.{module}")
    except ImportError:
        found = importlib.import_module(f"testcontainers.{module}")
    return getattr(found, name)


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """Yield an isolated disposable PostgreSQL URL using the asyncpg driver."""
    env_url = os.environ.get("MODULITH_TEST_POSTGRES_URL")
    if env_url:
        yield from _disposable_postgres_database(env_url)
        return
    try:
        PostgresContainer = _testcontainer_class("postgres", "PostgresContainer")
    except ImportError:
        pytest.skip("testcontainers not installed and MODULITH_TEST_POSTGRES_URL unset")
    if not _docker_available():
        pytest.skip("Docker unavailable and MODULITH_TEST_POSTGRES_URL unset")
    with PostgresContainer(_PG_IMAGE, driver="asyncpg") as pg:
        yield from _disposable_postgres_database(pg.get_connection_url())


def _disposable_postgres_database(base_url: str) -> Iterator[str]:
    """Yield a disposable database without altering the supplied database."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import SQLAlchemyError

    source_url = make_url(base_url)
    admin_url = source_url.set(drivername="postgresql+psycopg")
    async_url = source_url.set(drivername="postgresql+asyncpg")
    database_name = f"modupy_test_{uuid4().hex}"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    created = False
    try:
        try:
            with admin_engine.connect() as conn:
                conn.execute(text(f'CREATE DATABASE "{database_name}"'))
            created = True
        except SQLAlchemyError as exc:
            pytest.skip(
                "PostgreSQL integration tests require CREATEDB privilege to protect "
                f"the supplied database ({type(exc).__name__})"
            )

        yield async_url.set(database=database_name).render_as_string(hide_password=False)
    finally:
        try:
            if created:
                with admin_engine.connect() as conn:
                    conn.execute(
                        text(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                        ),
                        {"database_name": database_name},
                    )
                    conn.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        finally:
            admin_engine.dispose()


@pytest.fixture(scope="session")
def mysql_url() -> Iterator[str]:
    """Yield an isolated disposable MySQL URL using the aiomysql driver."""
    env_url = os.environ.get("MODULITH_TEST_MYSQL_URL")
    if env_url:
        yield from _disposable_mysql_database(env_url)
        return
    try:
        MySqlContainer = _testcontainer_class("mysql", "MySqlContainer")
    except ImportError:
        pytest.skip("testcontainers not installed and MODULITH_TEST_MYSQL_URL unset")
    if not _docker_available():
        pytest.skip("Docker unavailable and MODULITH_TEST_MYSQL_URL unset")
    with MySqlContainer(_MYSQL_IMAGE, dialect="aiomysql") as mysql:
        from sqlalchemy.engine import make_url

        admin_url = make_url(mysql.get_connection_url()).set(
            username="root",
            password=mysql.root_password,
        )
        yield from _disposable_mysql_database(admin_url.render_as_string(hide_password=False))


def _disposable_mysql_database(base_url: str) -> Iterator[str]:
    """Yield a disposable database without altering the supplied database."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import SQLAlchemyError

    async_url = make_url(base_url)
    sync_driver = f"{async_url.get_backend_name()}+pymysql"
    admin_url = async_url.set(drivername=sync_driver)
    database_name = f"modupy_test_{uuid4().hex}"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    created = False
    try:
        try:
            with admin_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE `{database_name}`"))
            created = True
        except SQLAlchemyError as exc:
            pytest.skip(
                "MySQL integration tests require CREATE DATABASE privilege to protect "
                f"the supplied database ({type(exc).__name__})"
            )

        yield async_url.set(database=database_name).render_as_string(hide_password=False)
    finally:
        try:
            if created:
                with admin_engine.connect() as conn:
                    conn.execute(text(f"DROP DATABASE IF EXISTS `{database_name}`"))
        finally:
            admin_engine.dispose()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A reachable Redis URL for integration tests.

    Yields ``MODULITH_TEST_REDIS_URL`` when set, else a throwaway
    testcontainers Redis, else skips. Session-scoped.
    """
    env_url = os.environ.get("MODULITH_TEST_REDIS_URL")
    if env_url:
        yield env_url
        return
    try:
        RedisContainer = _testcontainer_class("redis", "RedisContainer")
    except ImportError:
        pytest.skip("testcontainers not installed and MODULITH_TEST_REDIS_URL unset")
    if not _docker_available():
        pytest.skip("Docker unavailable and MODULITH_TEST_REDIS_URL unset")
    with RedisContainer(_REDIS_IMAGE) as rc:
        host = rc.get_container_host_ip()
        port = rc.get_exposed_port(6379)
        yield f"redis://{host}:{port}"


@pytest.fixture
async def pg_engine(postgres_url: str):
    """Yield a real PostgreSQL engine isolated to a disposable schema."""
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.schema import CreateSchema, DropSchema

    from modulith.adapters.postgres_outbox import Base

    schema = f"modupy_outbox_{uuid4().hex}"
    admin_engine = create_async_engine(postgres_url)
    engine = admin_engine.execution_options(schema_translate_map={None: schema})
    try:
        async with admin_engine.begin() as conn:
            await conn.execute(CreateSchema(schema))
    except SQLAlchemyError as exc:
        await admin_engine.dispose()
        pytest.skip(
            "PostgreSQL integration tests require CREATE SCHEMA privilege to protect "
            f"the supplied database ({type(exc).__name__})"
        )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        try:
            async with admin_engine.begin() as conn:
                await conn.execute(DropSchema(schema, if_exists=True, cascade=True))
        finally:
            await engine.dispose()
            await admin_engine.dispose()


@pytest.fixture
def redis_key_prefix() -> str:
    """Namespace keys for one real-Redis integration test."""
    return f"modupy.test.{uuid4().hex}"


@pytest.fixture
async def redis_client(redis_url: str, redis_key_prefix: str):
    """Yield a real Redis client and remove only this test's namespaced keys."""
    import redis.asyncio as redis

    client = redis.Redis.from_url(redis_url)
    try:
        yield client
    finally:
        try:
            keys = [key async for key in client.scan_iter(match=f"{redis_key_prefix}*")]
            if keys:
                await client.delete(*keys)
        finally:
            await client.aclose()
