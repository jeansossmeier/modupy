# modulith

[![CI Status](https://github.com/jeansossmeier/modupy/actions/workflows/ci.yml/badge.svg)](https://github.com/jeansossmeier/modupy/actions?query=workflow%3ACI)
[![PyPI Version](https://img.shields.io/pypi/v/modupy)](https://pypi.org/project/modupy/)
[![Python Versions](https://img.shields.io/pypi/pyversions/modupy)](https://pypi.org/project/modupy/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **The modular monolith for Python: start as one process, grow into
> process-per-module, extract a service only when it pays.** Modules with
> enforced boundaries, events instead of cross-module calls, a transactional
> outbox for crash-safe delivery, and a supervisor that runs the very same
> code one-process-per-module when a module outgrows the rest.
>
> Inspired by [Spring Modulith](https://spring.io/projects/spring-modulith).

---

## Start small. Grow without the rewrite.

Every stage below runs the same application code. What changes between them
is a line of `pyproject.toml` or a CLI flag: listeners stay `@listener`, URLs
stay identical, and nothing is rewritten for the messaging layer.

**Day one: one process, zero infrastructure.** `pip install modupy` brings in
exactly one dependency, `pluggy`. Modules are subpackages of your app; they
talk through `@event`, `@listener` and `publish()`; you run
`uvicorn myapp.main:app` as you always have. Boundaries are checked by static
AST analysis at load time, and `modulith verify` fails CI on boundary
violations. Broker and outbox are in-memory, so there is nothing to provision.

**Durable: two config lines.** `outbox = "postgres"` plus `outbox_url`, the
async SQLAlchemy URL of your business database (Postgres, MySQL or SQLite),
makes the outbox transactional in every process while module discovery
(`auto_discover`) stays on, its default: events are stored durably
and delivered at-least-once after commit, and process crashes and
transaction rollbacks stay consistent. The Alembic migrations ship inside the
package. A single-process app calls `modulith.bootstrap()` and then
`outbox.start()` in its lifespan, so rows a crashed process left undelivered
are retried at startup rather than after the first publish
([DEPLOYMENT.md](docs/DEPLOYMENT.md#durable-single-process-outbox-pattern)).

**Process-per-module: one flag, still one host.**
`modulith run myapp.main:app --topology=processes` gives each module its own
process behind a single reverse-proxy port. The supervisor restarts crashed
workers with exponential backoff and a crash-loop breaker, and the default
SHM broker is a stdlib-only SQLite queue, so still no extra service to run.
One hot module? `[tool.modulith.workers] reports = 4`.

**Multi-host: swap the broker.** `broker = "redis-streams"` or
`broker = "database"` (Postgres / MySQL) carries events across machines.
`modulith k8s-manifest` emits a Deployment and Service per module plus one
Ingress, `modulith openapi` merges every module's spec into one document,
`modulith doctor` runs nine operational and architectural checks, and the
`otel` extra adds an OpenTelemetry span per publication and per listener
dispatch.

**Microservice: only when it pays.** `modulith extract <module>` scaffolds a
standalone service from one module: its package tree plus `pyproject.toml`,
`Dockerfile`, `README.md` and `.env.example`. The rest of the monolith keeps
publishing through the broker and the extracted service subscribes; you wire
its outbox yourself. Extraction copies the package-level helpers the module
and its contracts import, transitively. It refuses a module with outbound
boundary violations, tables shared with another module, or imports of another
declared module unless you pass `--force`, and the generated README records
what you overrode. It then imports the extracted module in a subprocess and
fails, naming the missing import, if that import fails or loads first-party
code from outside the extracted tree, so the service's third-party
dependencies must be installed. `--force` never overrides that import check:
a module-level import of another declared module still fails it, so only a
deferred one (inside a function) can be forced through.

Adopting on an existing codebase? `modulith audit` writes a `MIGRATION.md`
for it, and `modulith verify --mode=ratchet` baselines today's violations and
forbids new ones.

Where this stands, honestly: **pre-1.0 alpha**. Breaking changes may land in
0.x minor releases and are always listed in [CHANGELOG.md](CHANGELOG.md).
Every push runs ~1,770 hermetic tests on Python 3.11, 3.12 and 3.13 (Linux,
with the SHM broker additionally on macOS and Windows) plus 91 integration
tests against real Postgres, MySQL and Redis containers.

---

## Status

The core, the transactional outbox, the tooling and the process-per-module
runtime are code-complete and green under `pytest`, `mypy --strict` and
`ruff`; what remains before 1.0 is adapter breadth. [ROADMAP.md](ROADMAP.md)
has the plan, [SPEC.md](SPEC.md) every design decision, and
[`examples/demo_app`](examples/demo_app) is a runnable three-module shop wired
purely through events.

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
shape because its value semantics give the outbox reliable round-trip
fidelity (Cookbook recipes
[1](docs/COOKBOOK.md#1-define-an-event-and-a-listener) and
[2](docs/COOKBOOK.md#2-share-event-types-through-a-contracts-module)).

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
`/orders` — the same prefix a process-per-module worker uses, so both
topologies serve the identical URL, `POST /orders`.

```python
# myapp/main.py
import logging

from fastapi import FastAPI

from myapp.orders import router as orders_router

logging.basicConfig(level=logging.INFO)  # so the banner below is visible

app = FastAPI()
app.include_router(orders_router, prefix="/orders")

# That's it. Modules auto-discovered. Listeners auto-registered.
# Transactional outbox available with two config lines (outbox, outbox_url).
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

The banner goes through the standard `modulith` logger at INFO level, which
is why `main.py` calls `logging.basicConfig(level=logging.INFO)` under plain
`uvicorn`; `modulith dev` and `modulith run` set the level from `--log-level`
themselves. Bootstrap is lazy, so the banner appears on the first `publish()`
— the `curl` above — not at process start.

---

## Quickstart

### Install

```bash
pip install 'modupy[fastapi,cli]'
```

- `pip install modupy` — framework only; `pluggy` is its single dependency
- `pip install 'modupy[postgres]'` — adds the Postgres outbox
- `pip install 'modupy[database]'` — adds the database broker
- `pip install 'modupy[all]'` — installs everything

The `fastapi` extra (`fastapi`, `uvicorn`, `httpx`) is what the quickstart
and the reverse proxy need; the `cli` extra adds the `modulith` command. The
distribution is `modupy` on PyPI because the `modulith` name there belongs to
an unrelated project — the import name and the CLI are both `modulith`.

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

Every module directory needs an `__init__.py` — a namespace package is
skipped silently — and `@listener` functions must be reachable from it
(e.g. `from . import handlers`) to register at startup.

**Re-export each module's router from its `__init__.py`.** Under
`--topology=processes` there is no `main.py` in the worker: each worker
imports its own module package and mounts its `router` attribute under
`/<module>`. Sibling modules that package imports are loaded too, but their
listeners run only in their owner's worker. A router wired only through
`main.py` serves nothing there —
every route answers 404 while the worker reports healthy, and the worker log
carries a warning naming the missing `router`. Cookbook recipe
[8](docs/COOKBOOK.md#8-go-process-per-module-and-externalize-an-event) shows
the re-export in context. A module with no HTTP surface needs no `router`; it
still gets a worker and still consumes events.

`pyproject.toml` is optional under `uvicorn` but required by every CLI command
except `audit`. The minimum:

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

Package detection, module discovery and listener registration are automatic
and run at the first `publish()`. A durable outbox needs
`modulith.bootstrap()` at startup, as described under **Durable** above. Call
it at startup too when a manifest or boundary violation should stop the server
from starting rather than fail its first `publish()`.

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

The reverse proxy is the only public port and routes `/<module>/...` to that
module's worker, so the URL is unchanged. `contracts` holds event
definitions, not behaviour, so it gets no worker; every worker imports it
directly. Cross-module events route through the configured broker
automatically; mark an event `@externalized` when remote workers must consume
it *in addition to* local listeners, or
`@externalized(target="scheme:destination")` to pin its destination.

---

## Configuration

Everything is in `pyproject.toml`. Defaults are good enough that most
users never set anything beyond `outbox` and `outbox_url`:

```toml
[tool.modulith]
package = "myapp"               # falls back to [project].name
outbox = "postgres"             # default "memory" — switch for production
outbox_url = "postgresql+asyncpg://app@db/app"  # the business database; binds the store everywhere while auto_discover is on
broker = "redis-streams"        # default "memory" (single) / "shm" (processes)
topology = "single"             # "single" | "processes"

[tool.modulith.workers]
default = 1
reports = 4                     # this module gets 4 workers
```

Any *scalar* key has a `MODULITH_*` environment equivalent
(`MODULITH_OUTBOX`, `MODULITH_BROKER`, `MODULITH_PRODUCTION`, …). The
table-valued keys — `outbox_options`, `broker_options`, `workers` — are
pyproject-only, though the SHM and database brokers lift their own
`MODULITH_BROKER_<KEY>` variables on top and the Redis Streams broker reads
`REDIS_URL`.

Under `topology = "processes"` an omitted broker defaults to `shm`: despite
the name, a same-host durable SQLite queue with at-least-once delivery, so
listeners must be idempotent. Its default store location is keyed on the
package's install path, so production deploys must set `state_dir` (see
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)). Use `redis-streams` or `database` (Postgres /
MySQL) for cross-host delivery. Every option — outbox claim strategies, SHM
sizing and payload caps, database-broker polling, retention and
dead-lettering, actuator protection — is documented in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §8 and
[docs/COOKBOOK.md](docs/COOKBOOK.md).

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
modulith extract <module>         # scaffold a standalone service from one
                                  # module (pyproject, Dockerfile, README)
modulith k8s-manifest             # generate per-module Deployment/Service +
                                  # one Ingress for --topology=processes
modulith openapi                  # merge every module's OpenAPI doc into one
                                  # build-time spec (schemas prefixed per module)
modulith doctor                   # nine operational + architectural health checks
modulith outbox status            # outbox metrics (needs a durable outbox —
                                  # the default 'memory' store has nothing to
                                  # report and exits 1)
modulith broker drop-group <group> [--target <target>]
                                  # remove a retired module's consumer group, or
                                  # only its stale targets (shm/database brokers;
                                  # asks to confirm)
modulith info                     # show detected config
```

`modulith dev` and `modulith run` take `--log-level` (default `info`,
propagated to every worker); that default is what makes the banner, the
boundary warnings and each worker's `[<module>]` output visible. The CLI
needs a `pyproject.toml` — `[tool.modulith].package`, falling back to
`[project].name` — in the current directory or a parent; `MODULITH_PACKAGE`
is the escape hatch when there is none. Exit codes: **0** success, **1**
violations or user error, **2** internal error or CLI usage error (click's
convention). Set `strict_boundaries = true` in `[tool.modulith]` to make
WARNING-severity findings fatal too in `verify`, `run` and
`dev --topology=processes`; single-process `modulith dev` stays warn-only by
design. `extract`, `k8s-manifest` and `openapi` import your application to
build their artifacts — run them only against trusted source.

---

## Documentation

- **[SPEC.md](SPEC.md)** — complete project specification, every design decision (this is the canonical reference)
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — how modulith works internally: runtime, plugin contract, outbox, cross-process delivery, verifier
- **[docs/COOKBOOK.md](docs/COOKBOOK.md)** — task-oriented recipes for common jobs
- **[docs/API_REFERENCE.md](docs/API_REFERENCE.md)** — the public API surface (generated from docstrings via `scripts/gen_api_reference.py`)
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** — Docker/Kubernetes topologies, scaling strategies, health probes, operational playbooks
- **[docs/STABILITY.md](docs/STABILITY.md)** — what is guaranteed across 0.x releases and on the way to 1.0
- **[ROADMAP.md](ROADMAP.md)** — phase plan with checkboxes and kill criteria
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** — adopting on existing codebases
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — development setup, both test suites, lint and type checks
- **[examples/demo_app](examples/demo_app)** — a runnable three-module shop; the fastest way to see modulith end-to-end

Single-file plugin examples (a Redis Streams broker, a naming-convention
verifier) live in `examples/`.

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

The design is opinionated; please read [SPEC.md](SPEC.md) before opening
large PRs. The plugin contract (13 hookspecs, 5 protocols) is the most stable
part of the project — additions are easy, signature changes require strong
justification — and [STABILITY.md](docs/STABILITY.md) states what is
guaranteed across 0.x releases. Development setup and both test suites (the
hermetic default and the Docker-backed integration suite) are in
[CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Copyright 2026 Jean Sossmeier. Apache-2.0 — see [LICENSE](LICENSE).
