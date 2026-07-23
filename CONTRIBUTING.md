# Contributing to modulith

## Development Setup

The project uses `uv` for dependency management (see `uv.lock`). Install the development environment:

```bash
uv pip install --system -e ".[dev]"
```

This installs the framework, all feature extras (postgres, redis, database, otel, fastapi, cli), and dev tools (ruff, mypy, pytest, pytest-cov, integration test dependencies).

## Running Tests

There are two test suites. Both require the dev setup above.

**Default suite** (fast, no external services):
```bash
pytest -m "not integration" -v --tb=short
```

This runs ~400 tests against in-memory SQLite and fake brokers.

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

## Before Opening a Pull Request

All three must pass:
- `pytest -m "not integration" -v --tb=short` (or integration tests if adding adapters/brokers)
- `ruff check` and `ruff format --check` (linting and formatting)
- `mypy --strict` (strict type checking)

## Design & Scope

The project is currently single-author development targeting v1.0. Contributions are welcome but the design is opinionated — please read [SPEC.md](SPEC.md) before opening large PRs.

The plugin contract (12 hookspecs, 4 protocols) is the most stable surface. Additions are easier to review than signature changes.

## Response Times

This is a single-maintainer project. Response times on issues and PRs may vary. Thank you for your patience.
