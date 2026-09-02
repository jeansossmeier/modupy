# Contributing to modulith

## Development Setup

The project uses `uv` for dependency management (see `uv.lock`). Create a
virtualenv and install the development environment into it:

```bash
uv venv                      # picks an interpreter matching requires-python (>=3.11)
source .venv/bin/activate    # Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"
```

Do not add `--system`: it tells uv to ignore the virtualenv and target the
interpreter outside it, which on most machines is an older system Python than
the `>=3.11` this project requires (the install then fails to resolve).

This installs the framework, all feature extras (postgres, redis, database, otel, fastapi, cli), and dev tools (ruff, mypy, pytest, pytest-cov, integration test dependencies).

## Running Tests

There are two test suites. Both require the dev setup above.

**Default suite** (fast, no external services):
```bash
pytest -m "not integration" -v --tb=short
```

This runs the full default suite (~1,770 tests) against in-memory SQLite,
temporary files, and real in-process brokers.

**Integration suite** (requires Docker):
```bash
pytest -m integration -v
```

This exercises real Postgres, MySQL, Redis, and cross-process delivery via testcontainers. Tests auto-skip if Docker is unreachable.

To run against existing services instead of letting testcontainers manage them:
```bash
export MODULITH_TEST_POSTGRES_URL='postgresql+asyncpg://user:pass@localhost:5432/test'
export MODULITH_TEST_MYSQL_URL='mysql+aiomysql://user:pass@localhost:3306/test'
export MODULITH_TEST_REDIS_URL='redis://localhost:6379'
pytest -m integration -v
```

## Lint and Type Checking

Both commands run the CI checks exactly as they appear in the workflow:

```bash
# Lint and format check
ruff check modulith tests scripts examples
ruff format --check modulith tests scripts examples

# Type checking (--strict mode)
mypy --strict modulith/ tests/ scripts/ examples/
```

## Database Migrations

Schema revisions live in `modulith/adapters/migrations/versions/` and ship
inside the package; operators apply them with the packaged `alembic.ini` (see
[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)).

**A revision that has shipped in a release is immutable.** Alembic records every
applied revision in `alembic_version` and never re-runs it, so editing a shipped
revision's DDL changes what *fresh* installs get and nothing else. An existing
database keeps the old shape permanently, the two schemas diverge silently, and
the damage surfaces much later as a runtime error against a column that should
have been migrated. To change a shipped column, add a new revision carrying the
`ALTER`.

Revisions `0001`–`0005` predate the first release and were edited in place while
nothing had shipped — which is why `0003` widens `payload` on tables that `0001`
already creates. That latitude ends at 0.10.0.

New revisions need a MySQL check, not just Postgres and SQLite: plain
`sa.LargeBinary()` compiles to MySQL `BLOB`, capped at 65,535 bytes, and an
oversized insert fails with error 1406 rather than truncating quietly. Use the
`_PAYLOAD` variant the existing revisions define. `tests/test_migrations.py`
compiles each revision's DDL for every dialect without needing a server, and
`tests/test_migration_mysql.py` / `test_migration_postgres.py` run
`alembic upgrade head` against real ones.

## Before Opening a Pull Request

All three must pass:
- `pytest -m "not integration" -v --tb=short` (or integration tests if adding adapters/brokers)
- `ruff check` and `ruff format --check` (linting and formatting)
- `mypy --strict` (strict type checking)

## Design & Scope

The project is currently single-author development targeting v1.0. Contributions are welcome but the design is opinionated — please read [SPEC.md](SPEC.md) before opening large PRs.

The plugin contract (13 hookspecs, 5 protocols) is the most stable surface. Additions are easier to review than signature changes.

## Response Times

This is a single-maintainer project. Response times on issues and PRs may vary. Thank you for your patience.
