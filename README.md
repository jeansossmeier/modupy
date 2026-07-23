# modulith

[![CI Status](https://github.com/jeansossmeier/modupy/actions/workflows/ci.yml/badge.svg)](https://github.com/jeansossmeier/modupy/actions?query=workflow%3ACI)
[![PyPI Version](https://img.shields.io/pypi/v/modulith)](https://pypi.org/project/modulith/)
[![Python Versions](https://img.shields.io/pypi/pyversions/modulith)](https://pypi.org/project/modulith/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> A Python framework for the modular monolith pattern. Module structure
> with enforced boundaries, event-driven communication between modules,
> transactional outbox for crash-safe delivery, and an optional
> process-per-module runtime when you outgrow a single process.
>
> Inspired by [Spring Modulith](https://spring.io/projects/spring-modulith).

---

## Status

**Pre-1.0, alpha.** All three implementation phases are code-complete and
covered by a passing test suite (`pytest`, `mypy --strict`, and `ruff` all
green):

- **Phase 1 — v1 essentials:** sync entrypoint, manifests, boundary verifier
  (with ratcheting baselines), transactional outbox (Postgres adapter +
  alembic migrations), the CLI, and the documentation generator.
- **Phase 2 — polish:** the pytest plugin, codebase audit + `doctor`
  diagnostics, OpenTelemetry auto-instrumentation, and cross-process brokers:
  durable local SHM, Redis Streams, and a database-backed broker
  (Postgres / MySQL / SQLite).
- **Phase 3 — process-per-module:** the worker factory, process supervisor
  with crash recovery, reverse proxy, and cross-process event delivery through
  the broker (publishing *and* consuming — each worker subscribes to the
  streams for the events it consumes).

A runnable example lives in [`examples/demo_app`](examples/demo_app) — three
modules wired together purely through events. What remains before 1.0 is
ecosystem breadth (more broker/store adapters — Phase 4). See
[ROADMAP.md](ROADMAP.md).

The full design is documented in [SPEC.md](SPEC.md) — start there if
you want to understand the project completely or contribute.

---

## The 30-second pitch

```python
# myapp/orders/__init__.py
from modulith import event, listener, publish
from myapp.contracts.events import OrderCreated, PaymentReceived

@listener
async def on_payment(event: PaymentReceived) -> None:
    """Cross-module communication via events, not direct calls."""
    await fulfill_order(event.order_id)

async def create_order(customer_id: str) -> str:
    order_id = await persist_order(customer_id)
    await publish(OrderCreated(order_id=order_id))
    return order_id
```

```python
# myapp/main.py
from fastapi import FastAPI
from myapp.orders.api import router as orders_router

app = FastAPI()
app.include_router(orders_router)

# That's it. Modules auto-discovered. Listeners auto-registered.
# Transactional outbox available with one config line.
```

```bash
$ uvicorn myapp.main:app
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:modulith:detected application package 'myapp'
INFO:modulith:discovered 4 module(s): contracts, inventory, orders, payments
INFO:modulith:outbox=memory, broker=memory, topology=single
INFO:modulith:outbox disabled — set [tool.modulith].outbox = 'postgres' for durable event delivery
INFO:modulith:ready
```

The banner is emitted through the standard `modulith` logger at INFO
level — it inherits your app's logging configuration rather than
printing directly. Python surfaces only WARNING+ by default (and
uvicorn configures only its own loggers), so enable INFO logging to
see it, e.g. `logging.basicConfig(level=logging.INFO)` in `main.py`.
modulith bootstraps lazily, so the banner follows uvicorn's own startup
lines: it appears on first use — the first request that `publish()`es
an event — not at process start.

The framework starts with zero external dependencies: in-memory broker and
outbox for development, then scales to durable SHM/SQLite (single-host),
Redis Streams, or Postgres/MySQL/SQLite (distributed) with one config line.

---

## What modulith provides

**Module structure with enforced boundaries.** Subpackages of your
application are modules. Underscore-prefixed names are private. Static
AST analysis verifies module boundaries at load time. `modulith verify`
catches and fails CI on boundary violations; `modulith dev` reports them
as startup warnings (non-fatal by design).

**Event-driven inter-module communication.** Modules talk through
events, not direct function calls. The coupling stays low; refactoring
stays cheap.

**Transactional outbox.** The outbox is always wired into your
application; durability is opt-in. Default is in-memory (no persistence);
set `outbox = "postgres"` (or MySQL/SQLite) in `[tool.modulith]` for
durable, transactional event storage with at-least-once delivery after
commit. Process crashes and transaction rollbacks stay consistent.

**Optional process-per-module runtime.** When one module needs its own
CPU/memory budget, run it in its own process via the supervisor. Same
code, no rewrite for the messaging layer. Cross-module events route
through the configured broker automatically; mark an event
`@externalized` when remote workers must consume it *in addition to*
local listeners, or `@externalized(target="scheme:destination")` to pin
its destination.

**Auto-discovery and zero-config.** Install, define modules as
subpackages, run with uvicorn as you always have. The framework
configures itself.

---

## Quickstart

### Install

```bash
pip install modulith                 # just the framework
pip install 'modulith[postgres]'     # adds Postgres outbox
pip install 'modulith[database]'     # adds the database broker (Postgres/MySQL/SQLite)
pip install 'modulith[all]'          # everything
```

### Define modules as subpackages

```
myapp/
├── __init__.py
├── contracts/
│   └── events.py              # shared event definitions
├── orders/
│   ├── __init__.py            # public API
│   ├── _internal/             # private — verifier blocks cross-module access
│   ├── api.py                 # FastAPI router
│   └── handlers.py            # @listener functions (import from __init__.py!)
├── inventory/
└── main.py                    # FastAPI app
```

Discovery imports each module *package* — keep `@listener` functions
reachable from the module's `__init__.py` (e.g. `from . import handlers`)
so they register at startup.

### Run normally

```bash
uvicorn myapp.main:app --reload
```

The framework auto-detects your package, discovers modules, registers
listeners, and configures itself. No `modulith.bootstrap()` call needed.

---

## Configuration

Everything is in `pyproject.toml`. Defaults are good enough that most
users never set anything beyond `outbox`:

```toml
[tool.modulith]
package = "myapp"               # auto-detected if not set
outbox = "postgres"             # default "memory" — switch for production
broker = "redis-streams"        # default "memory" (single) / "shm" (processes)
topology = "single"             # "single" | "processes"

[tool.modulith.workers]
default = 1
reports = 4                     # this module gets 4 workers
```

Outbox tuning has a reserved home: `[tool.modulith.outbox_options]`. Keys
are validated at config load (`claim_strategy`, `claim_lease_seconds`,
`claim_batch_size`, …). Pass them through to
`outbox.configure(claim_strategy=..., completion_mode=...)` when wiring the
store — see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md), Step 5. Defaults are
`claim_strategy="lease"` (atomic claim + token fencing for concurrent
sweepers), with `"advisory_lock"` (Postgres) and `"none"` as alternatives.

Broker destinations for process-per-module topology are declared via
`subscription_source` (`manifest` by default, or `config` /
`listener`) plus static `@externalized(target=...)` inference. The SHM broker
retains publications for 24 hours so a subscription that registers after a
publish can replay them. The database broker's `no_subscriber_policy` defaults
to `"error"` (also `"wait"` / `"store"` with configurable orphan replay).
Actuator protection is
`actuator_mode="auto"` (token required in production / non-loopback).

The local SHM broker reads canonical `state_dir`, `sqlite_path`, and `hint_path`
options. Defaults are absolute, package-namespaced paths in the platform's
private per-user state directory (`0700` directories and `0600` files on
POSIX). `sqlite_synchronous` defaults to `"NORMAL"`; use `"FULL"` when the last
commits must survive OS failure or power loss. Explicit `broker = "shm"`
accepts filesystem paths only and rejects DSNs and SQLAlchemy/network URLs.
`max_payload_bytes` defaults to 16 MiB (maximum 1 GiB), and
`max_store_bytes` defaults to 1 GiB (maximum 1 TiB). Oversized payloads fail
before a SQLite transaction starts; a full store applies publish backpressure
through SQLite `max_page_count`. Legacy `shm_slot_size` is deprecated and
ignored because hint slots contain fixed-size sequences.

The database broker reads `[tool.modulith.broker_options]` too: `url`/`dsn`
(the SQLAlchemy URL — its dialect selects Postgres, MySQL, or SQLite),
`completion_mode` (`delete`/`mark`), `pool_size`/`max_overflow` (server
pooling), `busy_timeout_ms` (SQLite), `poll_interval_ms`/`batch_size` (consumer
cadence), `reclaim_stale_seconds` (crash-reclaim window) and
`max_delivery_attempts` (dead-letter cap), and
`retention_age_seconds`/`retention_count`/`prune_interval_seconds` (the
background prune). Every one is env-overridable via `MODULITH_BROKER_<KEY>`.
See [ARCHITECTURE.md](docs/ARCHITECTURE.md) §8.4.

Any *scalar* key has a `MODULITH_*` env var equivalent for production
overrides (e.g. `MODULITH_OUTBOX`, `MODULITH_BROKER`, `MODULITH_PRODUCTION`).
The table-valued keys (`outbox_options`, `broker_options`, `workers`) are
pyproject-only — but a couple of adapters lift their own subtable from the
environment on top. The SHM broker honors `MODULITH_BROKER_STATE_DIR`,
`MODULITH_BROKER_SQLITE_PATH`, `MODULITH_BROKER_HINT_PATH`,
`MODULITH_BROKER_MAX_PAYLOAD_BYTES`, and
`MODULITH_BROKER_MAX_STORE_BYTES`. The Redis Streams broker honors `REDIS_URL` (plus
`MODULITH_CONSUMER_GROUP` / `MODULITH_STREAM_PREFIX` /
`MODULITH_STREAM_MAXLEN`); the database broker reads every `broker_options`
key from `MODULITH_BROKER_<KEY>` (e.g. `MODULITH_BROKER_URL`,
`MODULITH_BROKER_POLL_INTERVAL_MS`) — this is how a process-per-module worker
receives its connection URL; and the packaged alembic migration runner reads
`MODULITH_DB_URL` (see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md), Step 5).

For `topology = "processes"`, an omitted broker defaults to the stdlib-only
`shm` adapter. If `broker_options.url`/`dsn` or its environment equivalent is
present, modulith instead infers `database`. An explicit `broker = "memory"` is
a loud `ConfigurationError`; explicit SHM rejects connection URLs.

Despite its name, SHM is a same-host durable SQLite queue. Every successful
publish has committed to SQLite before the file-backed mmap ring receives an
advisory sequence hint. Missing, torn, stale, or wrapped hints only delay the
next safety poll; they never own payloads or delivery state. `NORMAL` survives
application, worker, supervisor, and process restart on the same disk.
Delivery is at-least-once: a crash after the listener returns but before its
ack commits can deliver the event again, so listeners must be idempotent.
Use a networked `database` or Redis broker for cross-host delivery.

---

## CLI

```bash
modulith dev myapp.main:app       # like uvicorn --reload, with banner +
                                  # boundary warnings at startup (non-fatal)
modulith run myapp.main:app --topology=processes # production, process-per-module
modulith verify --mode=ratchet    # boundary checks for CI
                                  # (ERROR-severity failures always fatal)
modulith docs                     # generate Mermaid diagrams + canvas
modulith audit                    # analyze existing codebase for migration
                                  # (writes MIGRATION.md; --output to change)
modulith doctor                   # operational + architectural health
modulith outbox status            # outbox metrics
modulith info                     # show detected config
```

The CLI requires the `cli` extra (`pip install 'modulith[cli]'`) and is a
progressive enhancement, not a requirement. Plain `uvicorn myapp.main:app`
works the same way.

Exit codes are uniform: **0** success (warnings may still be reported —
`modulith dev` echoes verifier violations as non-fatal startup warnings,
and `verify` fails only on ERROR-severity findings), **1** violations or
user error (bad flags, config errors), **2** unexpected internal error.

Set `strict_boundaries = true` in `[tool.modulith]` to fail fast on any
boundary violation (ERROR or WARNING) in `modulith verify`, `modulith run`,
and `modulith dev --topology=processes`. Note: single-process `modulith dev`
remains warn-only regardless of `strict_boundaries` (its interactive
development contract is inviolable).

---

## Documentation

- **[SPEC.md](SPEC.md)** — complete project specification, every design decision (this is the canonical reference)
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — how modulith works internally: runtime, plugin contract, outbox, cross-process delivery, verifier
- **[docs/COOKBOOK.md](docs/COOKBOOK.md)** — task-oriented recipes for common jobs
- **[docs/API_REFERENCE.md](docs/API_REFERENCE.md)** — the public API surface (generated from docstrings via `scripts/gen_api_reference.py`)
- **[ROADMAP.md](ROADMAP.md)** — phase plan with checkboxes and kill criteria
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** — adopting on existing codebases
- **[examples/demo_app](examples/demo_app)** — a runnable three-module shop; the fastest way to see modulith end-to-end

For single-file plugin examples, see `examples/`. For tests demonstrating
the public API, see `tests/`.

---

## Comparison to alternatives

| | modulith | Spring Modulith | bare FastAPI + folders | FastAPI + Celery + import-linter | microservices |
|---|---|---|---|---|---|
| Module boundaries | ✓ enforced | ✓ enforced | ✗ convention only | ✓ import-linter enforces | ✓ network-enforced |
| Event-driven IPC | ✓ in-process or broker | ✓ in-process or broker | ✗ DIY | ✓ Celery | ✓ broker-only |
| Transactional outbox | ✓ built-in | ✓ built-in | ✗ DIY | ✗ hand-rolled | ✓ DIY per service |
| Migration path | ✓ ratchet from existing | ✓ ratchet | n/a | ✓ from existing | ✗ rewrite |
| Process-per-module | ✓ optional | ✗ | ✗ | ✗ broker-based only | n/a — already separate |
| Operational complexity | low | low | lowest | medium | highest |
| Python | ✓ | ✗ Java | ✓ | ✓ | ✓ |

The modulith pattern fits teams of 3-15 engineers building B2B SaaS in
Python who want to delay microservices for as long as possible. If
that's not you, modulith may not be the right fit. See SPEC.md Part II
for the audience analysis.

---

## Contributing

The project is currently in single-author development with the goal of
shipping v1 in 3 months. Contributions are welcome but the design is
opinionated; please read [SPEC.md](SPEC.md) before opening large PRs.

The plugin contract (12 hookspecs, 4 protocols) is the most stable
part of the project — additions are easy, signature changes require
strong justification. For a complete stability policy and what's guaranteed
across 0.x minor releases, see [STABILITY.md](docs/STABILITY.md).

### Running the tests

There are two suites. The **default suite** is fast, hermetic, and needs no
Docker — the Postgres outbox runs against in-memory SQLite and the broker runs
against fakes:

```bash
pip install -e '.[test]'
pytest                     # ~400 tests, no external services
```

The **integration suite** exercises the real adapters end-to-end — a real
Postgres (the outbox, its Alembic migration, `FOR UPDATE SKIP LOCKED`), a real
Redis (Streams broker, consumer groups, `XAUTOCLAIM` reclaim, dead-lettering),
the database broker against real Postgres **and** MySQL (fan-out subscriptions,
SKIP LOCKED claims, prune, dialect-native upsert — plus a full two-worker
cross-process delivery over both, and an embedded-SQLite-file variant that needs
no container), real cross-process event delivery, and real `uvicorn` worker
subprocesses behind the reverse proxy. It uses
[testcontainers](https://testcontainers.com) to spin up disposable `postgres:16`,
`mysql:8.0`, and `redis:7` containers, so it needs a running Docker daemon:

```bash
pip install -e '.[integration]'
pytest -m integration      # spins up Postgres + Redis containers
```

These tests are marked `@pytest.mark.integration` and **auto-skip when Docker is
unreachable**, so a plain `pytest` on a machine without Docker stays green. To
run against services you already have (e.g. in CI) instead of letting
testcontainers manage them, point the suite at them:

```bash
export MODULITH_TEST_POSTGRES_URL='postgresql+asyncpg://user:pass@localhost:5432/test'
export MODULITH_TEST_MYSQL_URL='mysql+aiomysql://user:pass@localhost:3306/test'
export MODULITH_TEST_REDIS_URL='redis://localhost:6379'
pytest -m integration
```

---

## License

Apache-2.0. See [LICENSE](LICENSE).
