"""Where modulith's packaged migrations record their revision.

The chain tracks itself in ``modulith_alembic_version`` so a database with its
own Alembic history in the default ``alembic_version`` table can run it.
A modulith revision still held in ``alembic_version`` is moved over on the next
migration run by ``move_legacy_revision``, which never touches a revision
outside the packaged chain.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Column, MetaData, String, Table, func, inspect, select
from sqlalchemy.engine import Connection

VERSION_TABLE = "modulith_alembic_version"
LEGACY_VERSION_TABLE = "alembic_version"


@cache
def _script_directory() -> ScriptDirectory:
    return ScriptDirectory(str(Path(__file__).parent))


def chain_revisions() -> frozenset[str]:
    """Every revision id in the packaged migration chain."""
    return frozenset(script.revision for script in _script_directory().walk_revisions())


def _version_table(name: str, schema: str | None) -> Table:
    return Table(name, MetaData(), Column("version_num", String(32)), schema=schema)


def read_revision_table(
    connection: Connection, schema: str | None
) -> tuple[str, frozenset[str]] | None:
    """The table that tracks modulith's revision, with the chain revisions it holds.

    The first of ``modulith_alembic_version`` and ``alembic_version`` holding a
    chain revision wins, so an install not yet moved is found even beside an
    empty ``modulith_alembic_version`` left by an interrupted move. With no
    chain revision anywhere, the first existing table is returned with an
    empty set; ``None`` means neither table exists.
    """
    inspector = inspect(connection)
    empty: tuple[str, frozenset[str]] | None = None
    for name in (VERSION_TABLE, LEGACY_VERSION_TABLE):
        if inspector.has_table(name, schema=schema):
            table = _version_table(name, schema)
            stored: list[str] = list(connection.execute(select(table.c.version_num)).scalars())
            revisions = frozenset(stored) & chain_revisions()
            if revisions:
                return name, revisions
            empty = empty or (name, revisions)
    return empty


def move_legacy_revision(migration_context: MigrationContext, schema: str | None) -> None:
    """Move modulith's revision out of ``alembic_version`` into its own table.

    Runs inside the migration transaction when the dialect has transactional
    DDL; otherwise it commits its own transaction before the upgrade starts.
    """
    connection = migration_context.connection
    assert connection is not None
    inside_migration_transaction = connection.in_transaction()
    found = read_revision_table(connection, schema)
    if found is not None and found[0] == LEGACY_VERSION_TABLE and found[1]:
        for revision in sorted(found[1]):
            migration_context.stamp(_script_directory(), revision)
        legacy = _version_table(LEGACY_VERSION_TABLE, schema)
        connection.execute(legacy.delete().where(legacy.c.version_num.in_(found[1])))
        remaining = connection.execute(select(func.count()).select_from(legacy)).scalar_one()
        if remaining == 0:
            legacy.drop(connection)
    if not inside_migration_transaction and connection.in_transaction():
        connection.commit()
