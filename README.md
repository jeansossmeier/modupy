# modulith

[![CI Status](https://github.com/jeansossmeier/modupy/actions/workflows/ci.yml/badge.svg)](https://github.com/jeansossmeier/modupy/actions?query=workflow%3ACI)
[![PyPI Version](https://img.shields.io/pypi/v/modupy)](https://pypi.org/project/modupy/)
[![Python Versions](https://img.shields.io/pypi/pyversions/modupy)](https://pypi.org/project/modupy/)
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
# myapp/contracts/events.py
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str


@event
@dataclass(frozen=True)
class PaymentReceived:
    order_id: str
```

An event is any class marked `@event`; a frozen dataclass is the recommended
shape because its value semantics (equality, hashing) give the outbox reliable
round-trip fidelity. Cookbook recipes
[1](docs/COOKBOOK.md#1-define-an-event-and-a-listener) and
[2](docs/COOKBOOK.md#2-share-event-types-through-a-contracts-module) go deeper.

```python
# myapp/orders/__init__.py
from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived

_orders: dict[str, str] = {}  # order_id -> customer_id
_fulfilled: set[str] = set()


async def create_order(customer_id: str) -> str:
    order_id = f"ord-{len(_orders) + 1}"
    _orders[order_id] = customer_id  # your real persistence goes here
    await publish(OrderCreated(order_id=order_id))
    return order_id


@listener
async def on_payment(event: PaymentReceived) -> None:
    """Cross-module communication via events, not direct calls."""
    _fulfilled.add(event.order_id)  # your real fulfilment goes here


# Re-export the router onto the module package. Process-per-module mode serves
# HTTP by mounting each module package's `router` attribute under /<module>.
from myapp.orders.api import router as router  # noqa: E402
```

```python
# myapp/orders/api.py
from fastapi import APIRouter
from pydantic import BaseModel

from myapp.orders import create_order

router = APIRouter()


class NewOrder(BaseModel):
    customer_id: str


@router.post("")
async def post_order(body: NewOrder) -> dict[str, str]:
    return {"order_id": await create_order(body.customer_id)}
```

The route sits at the router root (`""`) and `main.py` mounts the router under
`/orders` — the same prefix a process-per-module worker uses. Both topologies
therefore serve the identical URL, `POST /orders`.

```python
# myapp/main.py
import logging

from fastapi import FastAPI

from myapp.orders import router as orders_router

logging.basicConfig(level=logging.INFO)  # so the banner below is visible

app = FastAPI()
app.include_router(orders_router, prefix="/orders")

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

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' -d '{"customer_id": "alice"}'
{"order_id":"ord-1"}
```

The banner is emitted through the standard `modulith` logger at INFO
level — it inherits your app's logging configuration rather than
printing directly. Python surfaces only WARNING+ by default (and
uvicorn configures only its own loggers), which is why `main.py` above
calls `logging.basicConfig(level=logging.INFO)`. That line is for plain
`uvicorn`; the CLI sets the level itself from `--log-level` (default
`info`), so `modulith dev` and `modulith run` show the banner with no
logging setup of your own.
modulith bootstraps lazily, so the banner follows uvicorn's own startup
lines: it appears on first use — the first request that `publish()`es
an event, i.e. the `curl` above — not at process start.

The framework starts with no external services at all: in-memory broker and
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
pip install 'modupy[fastapi,cli]'  # what the quickstart needs: FastAPI, uvicorn, the CLI
pip install modupy                 # framework only — pluggy is its single dependency
pip install 'modupy[postgres]'     # adds Postgres outbox
pip install 'modupy[database]'     # adds the database broker (Postgres/MySQL/SQLite)
pip install 'modupy[all]'          # everything
```

Plain `pip install modupy` deliberately resolves to two packages, `modupy` and
`pluggy`: the framework does not pick your web layer for you. The `fastapi`
extra adds `fastapi`, `uvicorn` and `httpx` — needed by the quickstart below,
by `uvicorn myapp.main:app`, and by the reverse proxy in process-per-module
mode. The `cli` extra adds the `modulith` command (`typer` + `rich`).

The distribution is `modupy` on PyPI, but the import name and the CLI are both
`modulith` — `import modulith`, `modulith --help`. The two differ because the
`modulith` name on PyPI belongs to an unrelated project.

### Define modules as subpackages

```
myproject/                         # project root
├── pyproject.toml                 # [project].name or [tool.modulith].package
│                                  # — required by the CLI, see below
└── myapp/
    ├── __init__.py
    ├── contracts/
    │   ├── __init__.py            # shared event definitions
    │   └── events.py
    ├── inventory/
    │   └── __init__.py
    ├── orders/
    │   ├── __init__.py            # public API + `router` re-export
    │   ├── _internal/             # private — verifier blocks cross-module access
    │   ├── api.py                 # FastAPI router — re-exported by __init__.py
    │   └── handlers.py            # @listener functions (import from __init__.py!)
    ├── payments/
    │   └── __init__.py
    └── main.py                    # FastAPI app
```

Discovery imports each module *package*, so every module directory needs an
`__init__.py` — a directory without one is a namespace package and is skipped
silently. Keep `@listener` functions reachable from that `__init__.py`
(e.g. `from . import handlers`) so they register at startup.

**Re-export each module's router from its `__init__.py`.** Under `uvicorn`,
`main.py` decides what is mounted where. Under `--topology=processes` there is
no `main.py` in the worker at all: each worker process imports only its own
module package and mounts whatever that package's `router` attribute holds
under `/<module>`. A module whose router lives only in `api.py`, wired only
through `main.py`, therefore serves nothing in that topology — every route
answers 404 while the supervisor correctly reports the worker healthy, because
the worker did boot and simply found no router to mount. The worker does say
so — it logs a warning naming the module and the missing `router`, which the
supervisor re-emits under a `[<module>]` prefix. Read that line before hunting
an unexplained 404 anywhere else; if you have turned the logs down, the default
`modulith run --log-level info` brings it back. The
`from myapp.orders.api import router` line in `orders/__init__.py` above is what
lets the same codebase serve both topologies; Cookbook recipe
[8](docs/COOKBOOK.md#8-go-process-per-module-and-externalize-an-event) shows the
same re-export in context, including where in `__init__.py` to put it when
`api.py` imports back from the package. A module with no HTTP surface needs no
`router`; it still gets a worker, and still consumes events.

`pyproject.toml` is optional under `uvicorn` — the runtime infers the package
from the calling module — but every CLI command except `audit` needs it (see
the [CLI](#cli) section). The minimum that satisfies it:

```toml
# myproject/pyproject.toml
[project]
name = "myapp"
version = "0.1.0"
```

### Run normally

```bash
uvicorn myapp.main:app --reload
```

The framework auto-detects your package, discovers modules, registers
listeners, and configures itself. No `modulith.bootstrap()` call needed.

### Run the same code process-per-module

```bash
$ modulith run myapp.main:app --topology=processes
modulith → process-per-module: 3 worker(s) [inventory:9001, orders:9002, payments:9003], reverse proxy on http://0.0.0.0:8000
```

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' -d '{"customer_id": "alice"}'
{"order_id":"ord-1"}
```

The reverse proxy is the only public port; it routes `/<module>/...` to that
module's worker, so the URL is unchanged from the single-process run above.
The shared `contracts` module gets no worker of its own — it holds event
definitions, not behaviour, and every worker imports it directly.

Each worker's own output is re-emitted by the supervisor under a `[<module>]`
prefix. The default `--log-level info` shows all of it; raise the level and a
failing worker can shrink to `worker <name> exited with code N` with the
traceback that explains it filtered out.

---

## Configuration

Everything is in `pyproject.toml`. Defaults are good enough that most
users never set anything beyond `outbox`:

```toml
[tool.modulith]
package = "myapp"               # falls back to [project].name
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

Two env vars have no `[tool.modulith]` counterpart at all.
`MODULITH_PROXY_MAX_BODY_BYTES` raises the reverse proxy's request-body cap
under `--topology processes` (10 MiB by default; the proxy buffers each body
in memory, and a non-positive-integer value is a configuration error).
`MODULITH_DEV_WARN_ONLY=1` downgrades `strict_boundaries` to warnings outside
production — see the `strict_boundaries` note in the CLI section below.

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
modulith outbox status            # outbox metrics (needs a durable outbox —
                                  # the default 'memory' store has nothing to
                                  # report and exits 1)
modulith info                     # show detected config
```

The CLI requires the `cli` extra (`pip install 'modupy[cli]'`) and is a
progressive enhancement, not a requirement. Plain `uvicorn myapp.main:app`
works the same way.

**Turning the logs up or down.** `modulith dev` and `modulith run` both take
`--log-level` — `debug`, `info`, `warning`, `error` or `critical`,
case-insensitive — and propagate it to every worker subprocess under
`--topology=processes`. The default is `info`, which is what makes the startup
banner, the boundary warnings, and each worker's re-emitted `[<module>]` output
visible without any `logging.basicConfig()` of your own; raise it and they go
quiet. It is a flag only: there is no `[tool.modulith]` key and no environment
variable for it.

**The CLI needs a `pyproject.toml`.** Under uvicorn the runtime infers the
application package by walking the call stack to the module that called into
modulith; a CLI process has no such frame, so it reads
`[tool.modulith].package` — falling back to `[project].name` — from the
`pyproject.toml` in the current directory or any parent. Without one, every
command except `audit` (which takes a path and needs no config) exits **1**
with `could not determine the application package`. `MODULITH_PACKAGE` is the
escape hatch when there is genuinely no `pyproject.toml`.

Exit codes are uniform: **0** success (warnings may still be reported —
`modulith dev` echoes verifier violations as non-fatal startup warnings,
and `verify` fails only on ERROR-severity findings), **1** violations or
user error within a recognized command line (bad flag *values*, config
errors), **2** unexpected internal error *and* CLI usage errors — an
unknown option or a missing required argument exits 2, click's convention,
which the CLI follows rather than fighting the framework.

Set `strict_boundaries = true` in `[tool.modulith]` to fail fast on any
boundary violation (ERROR or WARNING) in `modulith verify`, `modulith run`,
and `modulith dev --topology=processes`. Note: single-process `modulith dev`
remains warn-only regardless of `strict_boundaries` (its interactive
development contract is inviolable). It signals that to the runtime by
setting `MODULITH_DEV_WARN_ONLY=1`, which survives uvicorn's `--reload` fork;
exporting it yourself makes any non-production run warn-only, and
`production = true` ignores it.

---

## Documentation

- **[SPEC.md](SPEC.md)** — complete project specification, every design decision (this is the canonical reference)
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — how modulith works internally: runtime, plugin contract, outbox, cross-process delivery, verifier
- **[docs/COOKBOOK.md](docs/COOKBOOK.md)** — task-oriented recipes for common jobs
- **[docs/API_REFERENCE.md](docs/API_REFERENCE.md)** — the public API surface (generated from docstrings via `scripts/gen_api_reference.py`)
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** — Docker/Kubernetes topologies, scaling strategies, health probes, operational playbooks
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

The plugin contract (13 hookspecs, 5 protocols) is the most stable
part of the project — additions are easy, signature changes require
strong justification. For a complete stability policy and what's guaranteed
across 0.x minor releases, see [STABILITY.md](docs/STABILITY.md).

### Running the tests

There are two suites. The **default suite** is fast, hermetic, and needs no
Docker — the Postgres outbox runs against in-memory SQLite and the broker runs
against fakes:

```bash
pip install -e '.[test]'
pytest                     # ~1,400 tests, no external services
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

Copyright 2026 Jean Sossmeier. Apache-2.0 — see [LICENSE](LICENSE).
