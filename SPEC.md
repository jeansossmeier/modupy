# modulith — Complete Project Specification

> A Spring Modulith-inspired modular monolith framework for Python. This document is the single source of truth for the project: every design decision, every interface, every file, every gap, every mitigation, every phase. An LLM reading this document plus the accompanying source tree should be able to execute the project to v1 without external context.

---

## Table of Contents

- [Part I — Vision and Scope](#part-i--vision-and-scope)
- [Part II — Audience and Non-Goals](#part-ii--audience-and-non-goals)
- [Part III — Design Philosophy](#part-iii--design-philosophy)
- [Part IV — The Plugin Contract](#part-iv--the-plugin-contract)
- [Part V — User-Facing API](#part-v--user-facing-api)
- [Part VI — Runtime Architecture](#part-vi--runtime-architecture)
- [Part VII — The Transactional Outbox](#part-vii--the-transactional-outbox)
- [Part VIII — Boundary Verification](#part-viii--boundary-verification)
- [Part IX — Process-Per-Module Runtime](#part-ix--process-per-module-runtime)
- [Part X — Built-in Adapters](#part-x--built-in-adapters)
- [Part XI — Testing](#part-xi--testing)
- [Part XII — The CLI](#part-xii--the-cli)
- [Part XIII — Migration Strategy](#part-xiii--migration-strategy)
- [Part XIV — The Seven Gap Mitigations](#part-xiv--the-seven-gap-mitigations)
- [Part XV — Implementation Roadmap](#part-xv--implementation-roadmap)
- [Part XVI — File Inventory](#part-xvi--file-inventory)
- [Part XVII — Brutal Truths and Decision Points](#part-xvii--brutal-truths-and-decision-points)
- [Appendices](#appendices)

---

## Part I — Vision and Scope

### 1.1 What This Is

`modulith` is a Python library that supports the modular monolith architectural pattern. It provides:

- **Module structure with enforced boundaries** — packages within an application that have public APIs and private internals, with violations caught at verification time.
- **Event-driven inter-module communication** — modules talk to each other through events, not direct calls, keeping coupling low.
- **Transactional outbox** — events published inside a database transaction are durably stored and delivered at-least-once, surviving crashes.
- **Optional process-per-module runtime** — when a module needs its own CPU/memory budget, promote it to its own process without rewriting code.
- **Auto-discovery and zero-config setup** — install, define modules as subpackages, run with uvicorn as you always have. The framework configures itself.

It is inspired by Spring Modulith, which provides similar capabilities for Spring Boot. Where Java idioms don't translate cleanly to Python, we choose the most pythonic equivalent rather than porting verbatim.

### 1.2 The Three-Tier Promise

The project's value proposition is a single sentence with three escape valves:

> **`modulith dev` for development, `modulith dev --topology=processes` when one module needs its own CPU, `modulith deploy` when one module needs its own container — same code, no rewrites for the messaging layer.**

The phrase "no rewrites for the messaging layer" is doing important work. We promise zero-rewrite for events, listeners, and module structure. We do not promise zero-rewrite for shared databases, shared transactions, or shared in-memory state. Those are separate decisions a user makes consciously, ideally early.

### 1.3 The One-Sentence Pitch

> A modular monolith for Python that grows with you: in-process today, multi-process tomorrow, microservices when you actually need them.

### 1.4 What Success Looks Like

A team installs modulith, restructures their FastAPI app into subpackages, sprinkles `@event` and `@listener` decorators, and ships to production. Six months later, one module needs more CPU; they flip a flag and that module runs in its own process. Two years later, one module needs to be its own service for organizational reasons; they extract it with the contracts module already documenting the API surface. At every step, the code that didn't need to change didn't change.

### 1.5 What Failure Looks Like

A solo developer's portfolio piece that demonstrates ambition without execution. Eight months of design, no v1, no users, repo archived in 2027. This is the dominant outcome for projects of this scope. **The roadmap in [Part XV](#part-xv--implementation-roadmap) is calibrated to avoid this outcome by ruthlessly time-boxing each phase.**

---

## Part II — Audience and Non-Goals

### 2.1 Who This Is For

The realistic addressable market: **teams of 3-15 engineers building B2B SaaS in Python who are scaling a single application past one team's worth of code and want to delay microservices for as long as possible.** That's the segment where modulith is materially better than "FastAPI plus folders."

Specifically:
- Teams using FastAPI, Starlette, or Flask, on Python 3.11+
- Teams with a Postgres or MongoDB database, async-friendly stack
- Teams that have started feeling pain from cross-cutting changes touching multiple folders
- Teams that have considered microservices and decided the operational cost isn't worth it yet
- Teams in DDD-adjacent territory who already think in bounded contexts

### 2.2 Who This Is Not For

Equally important to be honest about:
- **Solo developers on small apps** — you don't have the structural pain modulith solves
- **Teams already on microservices** — you've paid the cost; coming back to a monolith is rare
- **Teams using Django** — different conventions, different ORM, different ecosystem; we don't fight Django
- **Sync-only codebases that won't go async** — the outbox requires async DB integration to be production-grade
- **Teams that need true GIL isolation today** — Python 3.13's free-threaded mode is the answer there, not us

### 2.3 The Adoption Calculus

For modulith to succeed:
- The benefit must clear the bar of "what FastAPI plus folders gives you for free"
- The migration cost on existing codebases must be small enough to try on a Friday
- The transactional outbox specifically must be production-grade — that's the one feature competitors don't have

If those three things hold, modulith is genuinely useful. If any one fails, modulith is architecture for its own sake.

---

## Part III — Design Philosophy

### 3.1 Convention Over Configuration

Spring needs `@ApplicationModule` because Java packages are flat. Python already has the convention: top-level subpackages are modules, underscore-prefixed names are private. We use what's already there.

Every default decision has a corresponding escape hatch, **and the escape hatch is discoverable from the default behavior.** The startup banner shows active config; clicking through any value to override takes you to pyproject.toml. Error messages name the config keys that would have prevented the error.

### 3.2 Defaults So Good You Don't Change Them

The defaults stack:
- **Discovery**: subpackages of the root package, ignoring underscore-prefixed
- **Event bus**: in-memory async bus, no broker required
- **Outbox**: disabled by default; enable with `outbox = "postgres"` in pyproject
- **Topology**: single-process; flip to processes with one flag
- **Observability**: auto-enabled if OpenTelemetry is installed, silent no-op if not
- **Verification**: warnings in dev, hard checks via `modulith verify` in CI
- **Logging**: standard library `logging`, inherits app's config

A first-time user with no `[tool.modulith]` section at all has a fully-working app.

### 3.3 Three Plugin Shapes, Three Mechanisms

Plugins fall into three shapes; forcing all three into one mechanism makes the wrong one awkward:

| Shape | Examples | Mechanism |
|---|---|---|
| **Driver** (one wins) | `PublicationStore`, `EventSerializer` | `typing.Protocol` + entry points |
| **Dispatch** (route by key) | Brokers (Kafka, SQS, RabbitMQ, …) | `BrokerRegistry` + scheme prefix |
| **Hook** (everyone runs) | Lifecycle, verification, observability | `pluggy` hookspecs |

### 3.4 The Architectural Test

> Built-in plugins are not privileged, just first-party.

Users must be able to disable any built-in feature and replace it with their own implementation using the same hookspec contract. If this isn't true, we've built two-tier architecture (privileged core + second-class plugins) and people will route around it.

### 3.5 Async-Native, Sync-Compatible

Modern Python is async; SQLAlchemy 2.0 is async-first. The framework assumes `asyncio` internally. But we ship `publish_sync()` and accept sync `@listener` functions because most real Python apps are mixed sync/async, and forcing async-only refactors is hostile to adoption.

### 3.6 Honest About What We Don't Do

The single most important framing principle: **we don't lie about our limitations.** The "modulith now, microservices later" pitch has a hidden cliff (shared databases, shared transactions). We document the cliff, give users tools to measure their proximity to it, and let them make informed decisions. The first time we oversell and a user hits the cliff in production, we lose them and they tell their network. Honest framing is risk management.

---

## Part IV — The Plugin Contract

The plugin contract is the most stable part of modulith. Once published, every plugin ever written depends on it. Additions are fine; signature changes are major-version events.

### 4.1 The Ten Hookspecs

Defined in `modulith/hooks.py`. Each is a stable, versioned contract.

**Module lifecycle (2):**

1. `modulith_discover_modules(app_package: str) -> list[ModuleInfo] | None` — `firstresult=True`. Override module discovery; the default walks subpackages.

2. `modulith_after_module_load(module: ModuleInfo) -> None` — react to module load completion. Plugins use this for startup metrics, registration, etc.

**Verification (1):**

3. `modulith_verify_module(module: ModuleInfo, all_modules: list[ModuleInfo]) -> list[Violation]` — aggregate. Plugins return violations; results combine into the full report.

**Event lifecycle (4):**

4. `modulith_before_event_published(event: Any) -> None` — pre-publish validation/enrichment. Raising aborts publication.

5. `modulith_after_event_published(event: Any, publication: EventPublication) -> None` — post-publish observability.

6. `modulith_on_listener_dispatch(event: Any, listener_name: str, publication: EventPublication) -> None` — per-listener tracing/correlation.

7. `modulith_on_listener_error(event: Any, listener_name: str, publication: EventPublication, exception: BaseException) -> None` — listener failure handling.

**Externalization (2):**

8. `modulith_resolve_event_target(event: Any) -> str | None` — `firstresult=True`. Dynamic routing override; first non-None wins.

9. `modulith_register_brokers(registry: BrokerRegistry) -> None` — broker adapters register at startup.

**Documentation (1):**

10. `modulith_render_documentation(modules: list[ModuleInfo], output_dir: str) -> list[str]` — aggregate. Plugins write artifacts and return paths.

### 4.2 The Three Driver Protocols

Defined in `modulith/protocols.py`. Marked `runtime_checkable` for diagnostics; adapters use duck typing, no inheritance required.

**`PublicationStore`** — outbox storage:
```
async save(publication: EventPublication) -> None
async mark_complete(publication_id: UUID) -> None
async find_incomplete(older_than: timedelta) -> list[EventPublication]
async archive(publication_id: UUID) -> None
async delete(publication_id: UUID) -> None
```

**`EventSerializer`** — event encoding:
```
serialize(event: Any) -> bytes
deserialize(data: bytes, event_type: str) -> Any
```

**`Broker`** — external message broker:
```
async publish(target: str, payload: bytes, headers: dict[str, str] | None) -> None
async close() -> None
```

### 4.3 The Broker Dispatch Registry

`BrokerRegistry` in `modulith/brokers.py`. URI-scheme dispatch:

- `register(scheme: str, broker: Broker)` — fails loudly on duplicate
- `unregister(scheme: str)` — no-op if absent
- `get(scheme: str) -> Broker` — raises `UnknownBrokerError` with helpful message
- `publish(target: str, payload: bytes, headers)` — splits on first colon, routes by scheme
- `close_all()` — shutdown cleanup, swallows individual failures
- `schemes()` — diagnostics

Targets follow `scheme:destination` format. Destination may contain colons (e.g. AMQP `exchange:routing.key` works because we split on the first colon only).

### 4.4 The Plugin Manager

`create_plugin_manager()` in `modulith/manager.py`. Built around pluggy:

- Registers hookspecs from `modulith.hooks`
- Loads built-in plugins from `BUILTIN_PLUGINS` tuple
- Discovers third-party plugins via `modulith` entry point group
- Accepts `extra_plugins`, `disable`, `load_entrypoints`, `load_builtins` for test control

Built-ins load through the same path as third-party plugins. Disabling and replacing a built-in is one line:

```python
pm = create_plugin_manager(
    disable=["modulith.builtin.verifier"],
    extra_plugins=[my_custom_verifier],
)
```

---

## Part V — User-Facing API

### 5.1 The Four Names That Matter

```python
from modulith import event, listener, publish, configure
```

**`@event`** — marks a class as a domain event. Sets `__modulith_event__ = True`. Beyond that, an event is a regular class (typically a frozen dataclass).

**`@listener`** — registers an async function as an event listener. Event type inferred from the first argument's annotation. Sync functions accepted with a wrapper (see [§5.2](#52-sync-vs-async)).

**`publish(event)`** — async. Dispatches to all registered listeners.

**`configure(**kwargs)`** — overrides defaults before bootstrap. Most users never call it.

### 5.2 Sync vs Async

The `decorators.py` module exposes both:

```python
# Async (default, preferred for new code)
async def create_order():
    await publish(OrderCreated(...))

# Sync (for existing FastAPI sync views, scripts, sync DB code)
def create_order():
    publish_sync(OrderCreated(...))
```

`publish_sync()` detects context:
- **No event loop running** → uses a persistent thread-pool-backed loop, blocks until done
- **Event loop running, called from a sync function** (e.g. FastAPI sync view in Starlette's threadpool) → schedules on the running loop, waits via `concurrent.futures.Future`

Sync listeners are accepted:
```python
@listener
def reserve_stock(event: OrderCreated) -> None:  # sync def, not async def
    ...
```
Sync listeners run in the event loop's executor; async listeners run directly.

For SQLAlchemy: `publish_sync()` inside a `with session.begin()` block records to the outbox using the same session. Dispatch happens after commit via SQLAlchemy's `after_commit` event.

### 5.3 The Contracts Module Pattern

Cross-module event imports create implicit dependencies. The convention solves this:

```
myapp/
├── contracts/                  # ← shared event definitions
│   ├── __init__.py
│   └── events.py              # OrderCreated, InventoryReserved, etc.
├── orders/
│   └── __init__.py            # imports from myapp.contracts
└── inventory/
    └── __init__.py            # imports from myapp.contracts
```

The verifier treats `myapp.contracts` (or whatever name `[tool.modulith].contracts_module` points at) as a sink: everyone may import from it; it may not import from any module. Schema changes there are intentional and visible.

For the distributed case, `contracts` becomes versioned. The broker carries a `schema_version` header; consumers check it. `modulith` provides this header automatically.

### 5.4 The Manifest File

Optional but recommended for production:

```python
# myapp/orders/_manifest.py
from modulith import declare_module
from . import handlers

declare_module(
    publishes=["OrderCreated", "OrderShipped"],
    consumes=["PaymentReceived"],
    listeners=[handlers.on_payment_received],
    owns_tables=["orders", "order_items"],
    declared_dependencies=["payments"],
)
```

The framework reads manifests at startup. If a declared listener wasn't registered (because the module failed to import, was renamed, etc.), startup fails with a clear error and file:line.

The manifest also drives:
- The documentation generator (canvas content)
- The verifier (declared vs observed dependencies)
- The audit tool (manifest coverage as a readiness signal)

### 5.5 Module Conventions

Default module structure:

```
myapp/
├── __init__.py           # the application package
├── contracts/            # shared event definitions (special)
├── orders/               # a module
│   ├── __init__.py       # public API surface
│   ├── _manifest.py      # optional declarative manifest
│   ├── _internal/        # private — not importable by other modules
│   │   ├── persistence.py
│   │   └── domain.py
│   ├── handlers.py       # @listener functions
│   └── api.py            # FastAPI router (auto-mounted at /orders)
└── main.py               # FastAPI app; imports nothing from modules
```

Conventions enforced by the verifier:
- Only `myapp.orders.__init__` and explicitly-named submodules are importable from outside
- `myapp.orders._internal.*` is never importable from outside `myapp.orders.*`
- `myapp.orders` may not import from `myapp.inventory._internal.*`
- Cross-module imports of events go through `myapp.contracts.*`

---

## Part VI — Runtime Architecture

### 6.1 Lazy Bootstrap

The runtime does no work on `import modulith`. The first call to `publish()`, `@listener` registration, or `configure()` triggers bootstrap, which runs once.

Bootstrap sequence (in `Runtime._bootstrap()`):
1. Load configuration (pyproject + env + overrides)
2. Auto-detect application package if not configured
3. Create plugin manager, load built-ins and entry points
4. Create event bus (in-memory by default)
5. Flush listeners that were registered before bootstrap
6. Run module discovery hook (default: walk subpackages)
7. Log the friendly startup banner
8. Mark complete; subsequent calls take the fast path

### 6.2 The Singleton Pattern

A module-level `_runtime: Runtime` instance lives in `modulith/runtime.py`. Decorators and `publish()` reach through it. Double-checked locking guards bootstrap against concurrent first-uses. After bootstrap, the bootstrapped flag is read without locking.

**Critical correctness rule:** `register_listener()` gates on whether the **event bus exists**, not on whether bootstrap is **complete**. During discovery, modules are imported, which fires their `@listener` decorators. The bus exists at that point but bootstrap isn't done. If we gated on the bootstrap flag, those listeners would queue into a list that was already flushed.

### 6.3 Configuration Resolution

In `modulith/config.py`. Resolution order (highest priority first):

1. Explicit kwargs to `load_configuration()` / `configure()`
2. `MODULITH_*` environment variables
3. `[tool.modulith]` section in pyproject.toml
4. Hardcoded defaults

Every `Configuration` field has an `MODULITH_<KEY>` env var equivalent. Booleans accept `1`, `true`, `yes` (case-insensitive).

Validation happens before construction. Unknown keys raise `ConfigurationError` with the list of valid keys (catches typos). Production mode + default memory outbox raises (forces explicit opt-in for unsafe defaults).

The `explicit_keys: frozenset[str]` field tracks which values were set vs defaulted. Used by the production safety check.

### 6.4 Auto-Discovery

In `modulith/discovery.py`. Two strategies:

1. **Call-stack walking** (`_detect_from_caller_stack`): use `sys._getframe()` to walk back from the modulith bootstrap call. Skip frames inside modulith itself, stdlib, and site-packages. Return the top-level package of the first user-code frame.

2. **pyproject.toml** (`_detect_from_pyproject_name`): walk up from cwd looking for `pyproject.toml`, read `[project].name`, normalize hyphens to underscores per PEP 503.

If both fail, raise `ConfigurationError` with all three escape hatches in the message.

### 6.5 The Friendly Banner

Logged at INFO via the `modulith` logger:

```
modulith: detected application package 'myapp'
modulith: discovered 3 modules: orders, inventory, reports
modulith: outbox=memory, broker=memory, topology=single
modulith: outbox disabled — set [tool.modulith].outbox = 'postgres' for durable event delivery
modulith: ready
```

Five log lines that tell the user exactly what's active and how to change it. If they don't want any of it, the message tells them where to turn it off.

---

## Part VII — The Transactional Outbox

This is the technically hardest piece and the one feature users can't easily build themselves. Spring Modulith's Event Publication Registry is the reference implementation; we provide its Python equivalent.

### 7.1 The Problem It Solves

Without an outbox: publish an event inside a DB transaction. Transaction commits. Process crashes before the event reaches its listener. Event is lost. Or: transaction rolls back, but the event was already dispatched. Inconsistent state.

With an outbox: publish writes to a database table in the same transaction as the business work. After commit, a dispatcher picks up the row and delivers to the listener. If the dispatcher crashes mid-delivery, the row stays incomplete and gets retried on restart.

**Guarantee: at-least-once delivery, transaction-aligned.** Listeners must be idempotent.

### 7.2 SQLAlchemy Integration

The Postgres adapter (`modulith/adapters/postgres_outbox.py`) integrates with SQLAlchemy via session events:

```python
# Pseudocode for the integration
@event.listens_for(AsyncSession.sync_session_class, "after_commit")
def _after_commit(session: Session) -> None:
    pending = session.info.pop("_modulith_pending", [])
    for record in pending:
        asyncio.create_task(_dispatch_and_complete(record))
```

When `publish()` is called:
- If a session is bound (via `_current_session: ContextVar`), the event is serialized and added as a record in the same session, queued in `session.info["_modulith_pending"]`.
- After commit, the queued records are dispatched.
- If commit fails (rollback), the session pops the pending list and nothing is dispatched.

When `publish()` is called outside a transaction context, the bus dispatches directly without persistence (the outbox doesn't apply).

### 7.3 Completion Modes

Three modes, configurable via `[tool.modulith.outbox].completion_mode`:

- **`update`** (default) — set `completed_at`. Old records remain for inspection until a maintenance job purges them.
- **`delete`** — remove the row on success. Lower overhead, no historical visibility.
- **`archive`** — copy to `event_publications_archive` table, delete from primary. Best for high-volume systems where you want history without performance impact.

### 7.4 The Retry Loop

A background task started by the outbox plugin polls `find_incomplete(older_than=...)` periodically:

- On startup: `older_than=timedelta(0)` to catch crash recovery
- During normal operation: `older_than=timedelta(seconds=30)` to avoid thrashing fresh events

Failed dispatches stay incomplete with `attempt_count` incremented and `last_error` set. Retries use exponential backoff capped at the configured max (default 5 minutes). After `max_attempts` (default 10), the record is moved to a dead-letter status (column flag) and surfaced via the actuator.

### 7.5 Maintenance Operations

Exposed via the CLI and as plugin-callable APIs:

- `modulith outbox status` — counts of incomplete, completed, dead-lettered
- `modulith outbox retry <id>` — force retry of a specific publication
- `modulith outbox purge --older-than=30d` — clean up completed records
- `modulith outbox dead-letter` — list dead-lettered events for manual inspection

---

## Part VIII — Boundary Verification

### 8.1 The Default Rules

The built-in verifier in `modulith/builtin/verifier.py` ships these rules:

1. **No cross-module internal imports** — `myapp.orders` cannot import from `myapp.inventory._internal.*`
2. **No cyclic dependencies** — the module dependency graph must be a DAG
3. **Declared dependencies match observed** — if a manifest declares `dependencies=["payments"]`, only those modules may be imported (when manifest is present)
4. **Events flow through contracts module** — cross-module type imports must come from `myapp.contracts.*`, not from another module's package
5. **Module data ownership** — when manifests declare `owns_tables=[...]`, queries against another module's tables are violations

### 8.2 The AST-Based Verifier

Implementation: walk every `.py` file under the application package with `ast.parse()`. Collect all `Import` and `ImportFrom` nodes. For each, check:

- Is the source inside an application module?
- Is the target inside a different module's `_internal` package?
- Is the target a different module's package (not contracts)?

Emit `Violation` for each rule failure. The verifier hookspec is aggregating, so multiple plugins (built-in + custom) all contribute.

For data ownership rules, parse SQLAlchemy queries via static analysis of `.select()`, `.from_()`, `.execute()` calls. This is approximate — full coverage requires runtime checks via SQLAlchemy events, which is the v1.1 enhancement.

### 8.3 Ratcheting Mode

For brownfield adoption. Configure:

```toml
[tool.modulith.verify]
mode = "ratchet"  # or "strict" for new projects
baseline = ".modulith-baseline.json"
```

The baseline file records existing violations. The verifier:
- Passes any violation listed in the baseline (grandfathered)
- Fails any new violation
- `modulith verify --update-baseline` regenerates the file after refactoring

Same pattern as `mypy --strict` rolling out gradually. The baseline diff in git review shows what got fixed and what got worse. This is the single biggest adoption lever — without it, modulith is "for new projects only."

### 8.4 The Audit Tool

`modulith audit` analyzes an existing codebase non-destructively:

- Proposed module structure based on observed imports and folder layout
- List of cross-module imports that would become violations
- List of shared database tables that need ownership decisions
- Modulith-readiness score (0-100): percentage of cross-module interactions that go through events vs direct calls

Output is Markdown. Teams can run it on Friday afternoon, generate a baseline, have green CI on Monday, then tighten over weeks.

### 8.5 The Doctor Command

`modulith doctor` reports operational and architectural health:

- **Boundary health**: violation count, baseline drift over the last N commits
- **Process-split readiness**: percentage of cross-module interactions that are events vs direct calls (the "are you ready to split this module?" metric)
- **Schema drift**: events whose definitions changed without a `schema_version` bump
- **Outbox health**: dead-lettered count, oldest incomplete event age
- **Listener registration coverage**: declared listeners vs actually-registered listeners

Same machinery as `modulith doctor` in Spring Modulith's spirit but expanded to operational concerns.

---

## Part IX — Process-Per-Module Runtime

The v2 wedge. The feature that makes "modulith now, microservices later" credible.

### 9.1 The Topology Decision

Three options, ranked by setup ease:

- **A. One process per module, local broker for IPC** ✅ **chosen.** Each module runs as its own uvicorn worker. Communication via Redis Streams (default) or any registered broker. Reuses outbox + externalization machinery. Latency: ~1-5ms per inter-module call.
- **B. Unix domain sockets** — lower latency, no broker dependency, but you lose durability without keeping Postgres in the loop. Net complexity gain is small.
- **C. Subinterpreters (PEP 734, 3.13+)** — true per-module GIL, no IPC. Ecosystem support too thin in 2026. Worth designing toward; not worth shipping on.

### 9.2 The Worker Pattern

`modulith/_worker.py` is invoked by uvicorn:

```bash
uvicorn modulith._worker:create_app --factory \
    --host 127.0.0.1 --port 9001 \
    --env MODULITH_MODULE=orders
```

`create_app()`:
- Reads `MODULITH_MODULE` from env
- Imports only that module's package
- Configures the event bus to route cross-module events through the broker
- Returns a FastAPI app exposing only that module's router

Standard uvicorn machinery from there — workers, reload, signals, graceful shutdown.

### 9.3 The Supervisor

`modulith/supervisor.py` orchestrates workers in process-per-module mode:

- Spawns one subprocess per module via `asyncio.create_subprocess_exec`
- Each is a uvicorn invocation pointing at `modulith._worker:create_app`
- Stdout/stderr stream back with module-name prefixes
- SIGTERM cascades; crashed workers restart with exponential backoff
- Health checks against each worker's `/health` endpoint
- Topology changes (worker count, module isolation) without app code changes

Usage:
```bash
modulith run --topology=processes
modulith run --workers='{"reports": 4, "default": 1}'
modulith dev --isolate=reports  # only reports gets its own process
```

### 9.4 The Reverse Proxy

`modulith/proxy.py` is a Starlette ASGI app that routes requests to workers by URL prefix:

```
/orders/*    → http://127.0.0.1:9001
/inventory/* → http://127.0.0.1:9002
/reports/*   → http://127.0.0.1:9003
/_modulith/* → supervisor's own actuator
```

Implementation: `httpx.AsyncClient` for streaming proxy. WebSocket support is v2.1 enhancement.

### 9.5 Topology Configuration

```toml
[tool.modulith]
topology = "processes"  # or "single" or "subinterpreters"

[tool.modulith.workers]
default = 1
reports = 4

[tool.modulith.supervisor]
restart_backoff_initial = "1s"
restart_backoff_max = "60s"
health_check_interval = "10s"
```

---

## Part X — Built-in Adapters

Each ships as a separate package so dependencies stay optional. `pip install modulith[postgres]` pulls in the SQLAlchemy adapter; without it, the outbox can't use Postgres but everything else works.

### 10.1 Postgres Outbox Store

Package: `modulith-postgres`. Implements `PublicationStore` against a SQLAlchemy async engine. Ships:

- Schema migrations (alembic) for `event_publications` and `event_publications_archive`
- The session-event integration described in [§7.2](#72-sqlalchemy-integration)
- Health check endpoint integration

Schema:
```sql
CREATE TABLE event_publications (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    listener TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    attempt_count INT DEFAULT 0,
    last_error TEXT,
    is_dead_lettered BOOLEAN DEFAULT FALSE
);
CREATE INDEX idx_pending ON event_publications (published_at)
    WHERE completed_at IS NULL AND is_dead_lettered = FALSE;
```

### 10.2 Redis Streams Broker

Package: `modulith-redis`. Implements `Broker` against `redis.asyncio`. Default broker for process-per-module mode because of microsecond latencies and ubiquity.

```toml
[tool.modulith]
broker = "redis-streams"

[tool.modulith.broker.options]
url = "${REDIS_URL}"
consumer_group = "modulith-${MODULITH_MODULE}"
```

### 10.3 Kafka Broker

Package: `modulith-kafka`. Implements `Broker` against `aiokafka`. For teams already running Kafka.

### 10.4 OpenTelemetry Observability

Package: `modulith-otel` (or built-in if OTel is detected as installed). Auto-instrumentation:

- Every cross-module bean invocation gets a span tagged `modulith.module=<name>` and `modulith.api=<function>`
- Cross-module calls show as parent-child spans
- Event dispatches: span links from publisher to listener
- Routing distribution metrics (which tools called how often)
- Per-tool error rates, p95 latency
- Cost per query (when `modulith.cost.usd` attribute is set by adapters)
- Eval scores over time (when test infrastructure exposes them)

### 10.5 Documentation Generator

`modulith/builtin/docs.py`. Generates:

- `docs/architecture.mmd` — Mermaid C4 component diagram
- `docs/modules/<name>.md` — Application Module Canvas (public API, events published, events consumed, dependencies, internals)
- `docs/events.mmd` — Sequence diagram of event flows

Mermaid over PlantUML because it renders natively on GitHub/GitLab. Canvas is markdown so it diffs cleanly in PRs.

---

## Part XI — Testing

### 11.1 The pytest-modulith Plugin

Separate package: `pytest-modulith`. Provides:

```python
# Automatic per-test isolation (autouse fixture)
def test_orders_publishes_correctly(modulith_app):
    # Fresh runtime, fresh event bus, fresh module state
    from myapp.orders import create_order
    asyncio.run(create_order("123"))
    assert modulith_app.published_events_of_type(OrderCreated) == [...]

# Module-isolated tests
def test_orders_in_isolation(modulith_module):
    with modulith_module("orders", mock_modules=["inventory", "payments"]):
        ...

# Scenario API for event-driven flows
def test_order_lifecycle(scenario):
    scenario.publish(OrderPlaced(...)) \
            .expect_event(OrderConfirmed) \
            .within(seconds=2)
```

### 11.2 The Scenario API

Spring's Scenario abstraction. Builder pattern:

```python
class Scenario:
    def publish(event) -> Self
    def call(callable, *args, **kwargs) -> Self
    def expect_event(type) -> Self
    def matching(predicate) -> Self
    def within(seconds: float) -> None  # raises on timeout
    def with_state_change(callable, predicate) -> Self
```

Handles the eventual-consistency dance — async listeners need awaiting in tests. Internally polls the published events list with timeout, no `asyncio.sleep` in user code.

### 11.3 Module-Isolated Tests

Implementation strategy: subprocess-per-test for the strictest isolation mode. Fork overhead is fine for integration tests in CI; not for unit tests on save.

For non-isolated tests, the plugin handles state reset via session fixtures: clears event bus, drops listeners, restores `sys.modules` to a snapshot. The user never thinks about it.

### 11.4 Subprocess-Per-Test Mode

Activated via `@pytest.mark.modulith_isolated`. Each test runs in its own Python subprocess via pytest-xdist's worker mechanism. State leaks are impossible. Cost: ~100ms per test for the subprocess fork.

---

## Part XII — The CLI

`modulith` is a typer-based CLI. Distributed via `[project.scripts]` in pyproject.toml.

### Commands

```
modulith dev [--topology=single|processes] [--isolate=MODULE] [--reload]
modulith run [--topology=single|processes] [--workers=JSON]
modulith verify [--mode=strict|ratchet] [--baseline=PATH] [--update-baseline]
modulith docs [--output-dir=DIR] [--format=mermaid|plantuml]
modulith audit [--output=FILE]
modulith doctor
modulith outbox status
modulith outbox retry <id>
modulith outbox purge --older-than=DURATION
modulith outbox dead-letter [--list|--retry-all]
modulith info  # show detected config, modules, plugins
```

### `modulith dev` semantics

`modulith dev` is *almost* `uvicorn --reload` with quality-of-life additions:
- Prints the discovered module list at startup
- Shows a friendly banner with topology and detected adapters
- Pretty-prints events when `--trace` is set

**It is not a different way to run the app; it's a nicer way.** Users with muscle memory for `uvicorn` keep using `uvicorn`. The CLI is a progressive enhancement.

### `modulith run` semantics

`modulith run` is `modulith dev` minus reload, plus production-mode toggles. In `--topology=processes`, it spawns the supervisor. Designed to be the actual production entrypoint for users who want modulith to manage their topology, but optional — running each worker as a vanilla uvicorn process is also supported.

---

## Part XIII — Migration Strategy

### 13.1 Greenfield Adoption

```bash
# 1. Install
uv add modulith

# 2. Define modules as subpackages (zero config)
mkdir myapp/orders myapp/inventory

# 3. Run
uvicorn myapp:app --reload

# That's it.
```

### 13.2 Brownfield Adoption (the path that matters)

```bash
# 1. Install
uv add modulith

# 2. Audit existing structure
modulith audit > MIGRATION.md
# Review the proposed structure with the team

# 3. Restructure files into subpackages (no code changes yet)
mv app/views/orders.py myapp/orders/api.py
# ... etc

# 4. Generate baseline of existing violations
modulith verify --mode=ratchet --update-baseline

# 5. Add to CI
# verify: modulith verify --mode=ratchet
# Existing violations grandfathered; new ones blocked

# 6. Tighten gradually over weeks
# Each PR that fixes a baseline violation removes it via --update-baseline
```

### 13.3 The Three-Step Migration

A team migrating an existing FastAPI app:

1. **Install + audit + ratchet.** Boundaries enforced going forward; existing violations grandfathered.
2. **Add events incrementally.** Pick one cross-module call at a time; replace direct call with `publish` + `@listener`. Each migration is a single PR.
3. **Enable outbox.** Set `outbox = "postgres"`, run migrations, verify outbox-readiness via `modulith doctor`. Production-grade event delivery.

Step 4 (process-per-module) and step 5 (true microservices extraction) are optional later moves justified by data, not architecture.

---

## Part XIV — The Seven Gap Mitigations

From the brutal-truth analysis. Each gap has a concrete mitigation.

### Gap 1: Cross-module event imports leak the source module

**Mitigation:** the contracts module pattern ([§5.3](#53-the-contracts-module-pattern)). Events live in `myapp.contracts.*`, a sink in the dependency graph. Verifier treats it specially. For distributed deployments, `contracts` becomes versioned with `schema_version` headers.

### Gap 2: The "modulith now, microservices later" promise has a hidden cliff

**Mitigation:** module-level data ownership rules ([§8.1](#81-the-default-rules)) + `modulith doctor` ([§8.5](#85-the-doctor-command)). Users see their split-readiness as a number. We document the cliff explicitly: "no rewrites for the messaging layer; database boundaries are a separate decision."

### Gap 3: The async assumption is hostile to existing FastAPI codebases

**Mitigation:** `publish_sync()` and sync `@listener` support ([§5.2](#52-sync-vs-async)). The framework detects the calling context (running loop or not) and does the right thing. Sync listeners run in the executor, async run directly.

### Gap 4: Decorator-based listener registration creates import-order dependencies

**Mitigation:** the manifest file ([§5.4](#54-the-manifest-file)) and a startup verification check that compares declared listeners to actually-registered listeners. Mismatches fail loudly with file:line.

### Gap 5: Testing is genuinely harder than the docs admit

**Mitigation:** `pytest-modulith` plugin ([Part XI](#part-xi--testing)). Auto-reset between tests, subprocess-per-test for isolation, Scenario API for event-driven flows.

### Gap 6: No story for adopting modulith on existing codebases

**Mitigation:** ratcheting verifier ([§8.3](#83-ratcheting-mode)) + `modulith audit` ([§8.4](#84-the-audit-tool)). Teams adopt on Friday, ratchet on Monday, tighten over weeks.

### Gap 7: The plugin ecosystem might never form

**Mitigation:** reframe the positioning. Plugin system is for internal modularity, not for community ecosystem. We ship 5 first-party adapters (Postgres, Redis, Kafka, OTel, RabbitMQ) covering 95% of users. Community plugins are nice-to-have, not required for success.

---

## Part XV — Implementation Roadmap

Time-boxed phases. Each has explicit kill criteria.

### Phase 0: Foundation ✅ DONE

- Plugin contract (10 hookspecs, 3 protocols)
- Auto-discovery + lazy bootstrap
- Configuration system
- In-memory event bus
- Module structure conventions
- 29 passing tests

**Status:** Complete in this codebase.

### Phase 1: v1 Essentials (4-6 weeks)

The minimum scope where modulith provides value over "FastAPI plus folders."

**Must-ship:**
1. **Sync entrypoint** (`publish_sync()`, sync `@listener` support) — `modulith/sync.py`. ~150 lines.
2. **Manifest support** (`declare_module()`) — `modulith/manifest.py`. ~120 lines.
3. **Built-in verifier with AST analysis** — `modulith/builtin/verifier.py`. ~200 lines (will need to split).
4. **Ratcheting verifier** with baseline file — extends the verifier. ~80 lines.
5. **Postgres outbox adapter** with SQLAlchemy integration — `modulith/adapters/postgres_outbox.py`. ~200 lines split across files.
6. **The CLI** (`dev`, `run`, `verify`, `info`) — `modulith/cli.py`. ~150 lines.
7. **Documentation generator** — `modulith/builtin/docs.py`. ~150 lines.
8. **Real documentation** — README, architecture guide, migration guide, API reference. 2 weeks of writing.

**Kill criterion:** if the outbox doesn't pass crash-recovery tests by week 3, stop and reassess.

### Phase 2: v1.1 Polish (2-3 weeks)

**Should-ship:**
1. **`pytest-modulith`** as a separate package. ~200 lines.
2. **Scenario API**. ~100 lines.
3. **Audit tool** (`modulith audit`) — `modulith/audit.py`. ~150 lines.
4. **Doctor command** (`modulith doctor`) — `modulith/doctor.py`. ~120 lines.
5. **OTel auto-instrumentation** — `modulith/builtin/observability.py`. ~120 lines.
6. **Redis Streams broker** — `modulith/adapters/redis_broker.py`. ~80 lines.

**Kill criterion:** if Phase 1 didn't get adoption (zero external users) by end of Phase 2 prep, reassess strategy before continuing.

### Phase 3: v2 Process-Per-Module (3-4 weeks)

**Differentiator features:**
1. **Worker module** (`modulith/_worker.py`). ~80 lines.
2. **Supervisor** (`modulith/supervisor.py`). ~200 lines.
3. **Reverse proxy** (`modulith/proxy.py`). ~120 lines.
4. **Topology configuration + CLI flags**. ~50 lines.
5. **Cross-process event integration** — extends event bus to use brokers when topology != "single". ~100 lines.

**Kill criterion:** if v1 + v1.1 hit production usage and process-per-module isn't requested, defer indefinitely.

### Phase 4: Ecosystem Adapters (ongoing)

- Kafka broker
- RabbitMQ broker
- MongoDB outbox store
- Subinterpreter topology (when 3.13 ecosystem is ready)

### Phase Budget Summary

| Phase | Duration | Cumulative |
|---|---|---|
| Phase 0 | (done) | 0 |
| Phase 1 | 4-6 weeks | 4-6 weeks |
| Phase 2 | 2-3 weeks | 6-9 weeks |
| Phase 3 | 3-4 weeks | 9-13 weeks |

**Realistic v1 ship: 3 months from Phase 1 start.** Extension to v2: 3-4 months total.

---

## Part XVI — File Inventory

### Status legend
- ✅ — Built, tested, line-budget within target
- 🚧 — Skeleton with detailed implementation guide
- ⏳ — Planned, not yet started
- 📦 — Separate package

### Core package: `modulith/`

| File | Status | Lines | Notes |
|---|---|---|---|
| `__init__.py` | ✅ | 75 | Public API exports |
| `types.py` | ✅ | 113 | ModuleInfo, EventPublication, Violation |
| `protocols.py` | ✅ | 151 | PublicationStore, EventSerializer, Broker |
| `hooks.py` | ✅ | 198 | The 10 hookspecs |
| `markers.py` | ✅ | 27 | hookimpl re-export |
| `brokers.py` | ✅ | 128 | BrokerRegistry |
| `manager.py` | ✅ | 119 | create_plugin_manager |
| `config.py` | ✅ | 176 | Configuration + load_configuration |
| `discovery.py` | ✅ | 134 | detect_application_package |
| `event_bus.py` | ✅ | 95 | InMemoryEventBus |
| `runtime.py` | ✅ | 174 | Runtime singleton |
| `decorators.py` | ✅ | 117 | @event, @listener, publish, configure |
| `sync.py` | 🚧 | ~150 | publish_sync, sync listener support (Phase 1) |
| `manifest.py` | 🚧 | ~120 | declare_module API (Phase 1) |
| `audit.py` | ⏳ | ~150 | Codebase analysis (Phase 2) |
| `doctor.py` | ⏳ | ~120 | Health diagnostics (Phase 2) |
| `cli.py` | 🚧 | ~150 | Typer-based CLI (Phase 1) |
| `_worker.py` | ⏳ | ~80 | Per-module FastAPI app generator (Phase 3) |
| `supervisor.py` | ⏳ | ~200 | Process orchestration (Phase 3) |
| `proxy.py` | ⏳ | ~120 | Reverse proxy (Phase 3) |
| `testing.py` | ⏳ | ~100 | pytest plugin entry points (Phase 2) |

### Built-in plugins: `modulith/builtin/`

| File | Status | Lines | Notes |
|---|---|---|---|
| `__init__.py` | ✅ | 11 | Namespace package |
| `discovery.py` | ✅ | 74 | Default subpackage walker |
| `verifier.py` | 🚧 | ~200 | AST-based boundary verification (Phase 1) |
| `outbox.py` | 🚧 | ~150 | Outbox plugin core, calls store adapter (Phase 1) |
| `observability.py` | ⏳ | ~120 | OTel auto-instrumentation (Phase 2) |
| `docs.py` | 🚧 | ~150 | Mermaid + canvas generation (Phase 1) |

### Storage adapters: `modulith/adapters/`

| File | Status | Lines | Notes |
|---|---|---|---|
| `__init__.py` | ⏳ | ~10 | Namespace package |
| `postgres_outbox.py` | 🚧 | ~180 | SQLAlchemy + Postgres PublicationStore (Phase 1) |
| `redis_broker.py` | ⏳ | ~80 | Redis Streams Broker (Phase 2) |
| `kafka_broker.py` | ⏳ | ~80 | Kafka Broker (Phase 4) |

### Tests: `tests/`

| File | Status | Notes |
|---|---|---|
| `test_plugin_manager.py` | ✅ | 7 tests, plugin manager + broker registry |
| `test_config.py` | ✅ | 12 tests, config loading and validation |
| `test_discovery.py` | ✅ | 6 tests, package detection |
| `test_zero_config.py` | ✅ | 4 tests, end-to-end zero-config flow |
| `test_sync.py` | ⏳ | Sync entrypoint and sync listeners |
| `test_outbox.py` | ⏳ | Outbox + crash recovery |
| `test_verifier.py` | ⏳ | Verifier rules + ratcheting |
| `test_docs.py` | ⏳ | Documentation generation |
| `test_cli.py` | ⏳ | CLI commands |

### Examples: `examples/`

| File | Status | Notes |
|---|---|---|
| `redis_streams_broker.py` | ✅ | Complete broker adapter |
| `naming_convention_verifier.py` | ✅ | Custom verification rule |

### Top-level

| File | Status | Notes |
|---|---|---|
| `pyproject.toml` | 🚧 | Project metadata, dependencies, entry points, CLI script |
| `README.md` | 🚧 | The first thing users read |
| `SPEC.md` | ✅ | This document |
| `ROADMAP.md` | 🚧 | Phase plan with checkboxes |
| `MIGRATION_GUIDE.md` | ⏳ | How to adopt on existing codebases |
| `LICENSE` | ⏳ | Likely Apache 2.0 |

### Separate packages (Phase 2+)

| Package | Status | Notes |
|---|---|---|
| `pytest-modulith` | 📦 | Test plugin |
| `modulith-postgres` | 📦 | Postgres outbox (could ship as `modulith[postgres]` extra) |
| `modulith-redis` | 📦 | Redis broker |
| `modulith-kafka` | 📦 | Kafka broker |

---

## Part XVII — Brutal Truths and Decision Points

### 17.1 The Adoption Risk

The realistic addressable market is small. Five Python projects have attempted similar designs over the last decade; none are dominant. The pattern of "modular monolith with optional process split" specifically: nobody has cracked it. Maybe because it's hard. Maybe because the market doesn't actually want it as much as we think.

**Mitigation:** ship internal first, validate with real load, then open-source with evidence.

### 17.2 The Scope Risk

Phase 1 alone is 4-6 weeks. Total to v2: 3-4 months. Most side projects of this scope never ship. The dominant failure mode is not designing badly — it's adding scope while shipping nothing.

**Mitigation:** ruthless time-boxing. Each phase has a kill criterion. The phases are sized so any one of them shipping alone has value. Phase 1 ships v1, full stop. Phase 2 ships polish. Phase 3 ships the differentiator. Each is independently valuable.

### 17.3 The Internal-First Recommendation

The honest recommendation: **don't ship as open source first.** Build it as an internal library at the team's company first. Reasons:

- Real first user, real production load, real codebase to test against
- Plugin contract gets validated by hostile reality
- Defaults get tuned by actual usage
- Outbox gets battle-tested against real database failures
- Migration story emerges from doing actual migrations

Then, if it proves valuable, open-source it. Pytest, Django, and most successful libraries followed this path. Open-sourcing first is romantic but it's how libraries die — built for hypothetical users, no real feedback, lose motivation, archive.

### 17.4 The Most Important Question

Will this actually ship, or will it be designed for six months and then stopped?

If yes-ship: pick the smallest possible v1 (contract spec + auto-discovery + outbox + docs + sync support), time-box to two months, ship it ugly, iterate.

If unsure: the value of what's been done is the *thinking*. The architecture sketches inform how to structure consolidation work even without ever publishing a library. That's a legitimate outcome.

If exploration: also legitimate. The clearest design thinking happens in the early scoping, and that has value independent of shipping.

The failure mode to avoid: keep designing, add scope, never quite ship. Portfolio-piece syndrome. Decided up front and revisited at every phase boundary.

---

## Appendices

### Appendix A — Complete pyproject.toml

See `pyproject.toml` in the source tree. Key sections:

```toml
[project]
name = "modulith"
version = "0.1.0"
description = "Modular monolith pattern for Python"
requires-python = ">=3.11"
dependencies = ["pluggy>=1.3"]

[project.optional-dependencies]
postgres = ["sqlalchemy>=2.0", "asyncpg>=0.29"]
redis = ["redis>=5.0"]
kafka = ["aiokafka>=0.10"]
otel = ["opentelemetry-api", "opentelemetry-sdk"]
fastapi = ["fastapi>=0.110", "uvicorn>=0.27"]
cli = ["typer>=0.12"]
all = ["modulith[postgres,redis,otel,fastapi,cli]"]

[project.scripts]
modulith = "modulith.cli:app"

[project.entry-points."modulith"]
# Built-in plugins. Users disable via [tool.modulith].disable.
# (None needed here — built-ins are loaded by manager.BUILTIN_PLUGINS.)

[tool.modulith]
# Defaults for the modulith project itself if used self-referentially.
# Real apps put their own [tool.modulith] section here.
```

### Appendix B — Test Strategy

Three layers:

1. **Unit tests** (`tests/test_*.py`) — fast, isolated, no I/O. Cover individual modules.
2. **Integration tests** — exercise the full bootstrap + dispatch flow. Use the fake-app fixture in `test_zero_config.py` as the template.
3. **End-to-end tests** — a real Postgres database, real Redis, real subprocess workers. Run in CI only. Test crash recovery, broker failures, outbox retries.

Minimum bar for shipping v1: 90%+ line coverage on the core package, 100% on the outbox plugin (the hardest piece), passing crash-recovery tests with random kill timing.

### Appendix C — Documentation Strategy

Documentation makes or breaks adoption.

- **README.md** — the 5-minute pitch + zero-config example + link to deeper docs
- **Architecture guide** — for users who want to understand how it works
- **Migration guide** — for brownfield adoption
- **API reference** — auto-generated from docstrings via mkdocstrings
- **Cookbook** — short examples of common patterns (publish-then-listen, outbox with FastAPI, process-per-module deployment)
- **Contributing** — how to write a plugin, how to write an adapter

Hosted on Read the Docs or GitHub Pages. Generated from `docs/` with mkdocs + mkdocstrings.

### Appendix D — Versioning Policy

- **0.x.y** during pre-1.0 — anything can change with warnings, additions are `y`, breaking changes are `x`
- **1.0.0** — first stable release. Hookspecs and protocols frozen.
- **2.0.0** — only when the contract genuinely needs a breaking change. Maintain 1.x for at least 6 months after.

The plugin contract is the most stable part. We treat it like a public API of stdlib: additions free, deprecations slow, removals catastrophic.

### Appendix E — Decision Log

For future-self and contributors. Major decisions and their rationale.

- **Why pluggy?** Most pythonic plugin system, used by pytest for a decade. Rebuilding it badly is real risk; rebuilding it well is just rebuilding pluggy.
- **Why three plugin shapes?** Drivers, dispatch, hooks have genuinely different shapes. Forcing all into one mechanism makes the wrong one awkward.
- **Why lazy bootstrap?** "Just import and use" is the adoption-critical UX. Eager bootstrap requires explicit setup which adds learning curve.
- **Why contracts module?** Spring uses module-public events; we add a separate contracts package because it's cleaner for distributed deployment versioning.
- **Why ratcheting verifier?** Brownfield is where adoption happens. Without ratcheting, modulith is for new projects only.
- **Why subprocess-per-test in pytest plugin?** Python's import system is global and side-effectful. Subprocess is the only correct isolation; cost is acceptable for integration tests.
- **Why Mermaid over PlantUML?** Renders natively on GitHub/GitLab. PlantUML needs a separate server.
- **Why time-box phases?** Side projects of this scope don't ship. Phases with kill criteria are the only mechanism that produces a v1.

---

*End of specification. The accompanying source tree contains the buildable codebase referenced throughout.*
