"""MySQL 8 ``caching_sha2_password`` full authentication through the database broker.

Without TLS and with a cold server-side cache, the server accepts only a password
RSA-OAEP-encrypted with its public key. PyMySQL does that in
``pymysql._auth.sha2_rsa_encrypt`` (aiomysql calls it too), and it is the one login
step that needs the ``cryptography`` package the ``database`` extra installs for
MySQL 8.

The shared ``mysql_url`` fixture logs in as root, whose server-side cache entry is
already warm, so no other MySQL test reaches that step. A freshly created account has
no cache entry, so its first non-TLS login is the only way to exercise it.
"""

from __future__ import annotations

import importlib
from unittest import mock
from uuid import uuid4

import pytest

from modulith.adapters.db_broker import DatabaseBroker

pytestmark = [pytest.mark.integration]


async def test_mysql_cold_cache_login_without_tls_runs_the_rsa_password_exchange(
    mysql_url: str,
) -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    # pymysql ships no type stubs, so a plain import fails ``mypy --strict tests/``.
    _auth = importlib.import_module("pymysql._auth")
    root_url = make_url(mysql_url)
    user = f"modupy_auth_{uuid4().hex[:16]}"
    password = f"pw-{uuid4().hex}"
    admin = create_engine(
        root_url.set(drivername=f"{root_url.get_backend_name()}+pymysql"),
        isolation_level="AUTOCOMMIT",
    )
    broker: DatabaseBroker | None = None
    try:
        with admin.connect() as conn:
            conn.execute(
                text(
                    f"CREATE USER '{user}'@'%' IDENTIFIED WITH caching_sha2_password BY '{password}'"
                )
            )
            conn.execute(text(f"GRANT ALL PRIVILEGES ON `{root_url.database}`.* TO '{user}'@'%'"))

        broker = DatabaseBroker(root_url.set(username=user, password=password))
        with mock.patch.object(_auth, "sha2_rsa_encrypt", wraps=_auth.sha2_rsa_encrypt) as rsa:
            async with broker.engine.connect() as conn:
                logged_in_as = (await conn.exec_driver_sql("SELECT CURRENT_USER()")).scalar_one()

        assert logged_in_as == f"{user}@%"
        assert rsa.call_count >= 1
    finally:
        if broker is not None:
            await broker.close()
        with admin.connect() as conn:
            conn.execute(text(f"DROP USER IF EXISTS '{user}'@'%'"))
        admin.dispose()
