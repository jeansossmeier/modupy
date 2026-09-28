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
- **Broker**: in-memory for `single`; durable local `shm` for `processes` unless
  a configured URL/DSN selects `database`
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

Users must be able to disable any built-in feature and replace it with their own implementation using the same hookspec contract. The shipped mechanism is `configure(disable_plugins=["modulith.builtin.verifier", ...])` at application startup (or `create_plugin_manager(disable=[...])` when embedding) — there is no `[tool.modulith].disable` pyproject key; the configuration loader rejects unknown keys loudly. If this isn't true, we've built two-tier architecture (privileged core + second-class plugins) and people will route around it.

### 3.5 Async-Native, Sync-Compatible

Modern Python is async; SQLAlchemy 2.0 is async-first. The framework assumes `asyncio` internally. But we ship `publish_sync()` and accept sync `@listener` functions because most real Python apps are mixed sync/async, and forcing async-only refactors is hostile to adoption.

### 3.6 Honest About What We Don't Do

The single most important framing principle: **we don't lie about our limitations.** The "modulith now, microservices later" pitch has a hidden cliff (shared databases, shared transactions). We document the cliff, give users tools to measure their proximity to it, and let them make informed decisions. The first time we oversell and a user hits the cliff in production, we lose them and they tell their network. Honest framing is risk management.

---

## Part IV — The Plugin Contract

The plugin contract is the most stable part of modulith. Once published, every plugin ever written depends on it. Additions are fine; signature changes are major-version events.

### 4.1 The Thirteen Hookspecs

Defined in `modulith/hooks.py`. Each is a stable, versioned contract.

**Module lifecycle (2):**

1. `modulith_discover_modules(app_package: str) -> list[ModuleInfo] | None` — `firstresult=True`. Override module discovery; the default walks subpackages.

2. `modulith_after_module_load(module: ModuleInfo) -> None` — react to module load completion. Plugins use this for startup metrics, registration, etc.

**Verification (1):**

3. `modulith_verify_module(module: ModuleInfo, all_modules: list[ModuleInfo]) -> list[Violation]` — aggregate. Plugins return violations; results combine into the full report.

**Event lifecycle (6):**

4. `modulith_before_event_published(event: Any) -> None` — pre-publish validation/enrichment. Raising aborts publication.

5. `modulith_after_event_published(event: Any, publication: EventPublication | EventPublishReceipt) -> None` — post-publish observability. `EventPublication` on the in-memory path, `EventPublishReceipt` wrapping the persisted rows on the durable path.

6. `modulith_on_publish_error(event: Any, exception: BaseException) -> None` — fires when a publish fails between the two hooks above (outbox persist, event serialization, an inline broker route). `modulith_after_event_published` is scoped to a successful publish, so this is the paired hook for closing whatever `modulith_before_event_published` opened — a publish span, most notably. Observe-only: implementations that raise are logged and swallowed, never masking the original failure.

7. `modulith_on_listener_dispatch(event: Any, listener_name: str, publication: EventPublication) -> None` — per-listener tracing/correlation.

8. `modulith_on_listener_complete(event: Any, listener_name: str, publication: EventPublication, exception: BaseException | None) -> None` — fires after every listener invocation, success or failure (`exception` is None on success). Pairs with `modulith_on_listener_dispatch` so observability plugins can close the spans they open there.

9. `modulith_on_listener_error(event: Any, listener_name: str, publication: EventPublication, exception: BaseException) -> None` — listener failure handling.

**Externalization (3):**

10. `modulith_resolve_event_target(event: Any) -> str | None` — `firstresult=True`. Dynamic routing override; first non-None wins.

11. `modulith_register_brokers(registry: BrokerRegistry) -> None` — broker (producer) adapters register at startup.

12. `modulith_register_consumers(registry: ConsumerRegistry) -> None` — cross-process consumer factories register at startup, right after brokers. The consumer-side mirror of `modulith_register_brokers`; the process-per-module worker builds one `Consumer` per module from the registered factory.

**Documentation (1):**

13. `modulith_render_documentation(modules: list[ModuleInfo], output_dir: str) -> list[str]` — aggregate. Plugins write artifacts and return paths.

### 4.2 The Four Driver Protocols

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

The configured serializer governs outbox **storage** only. Broker
**transport** is not pluggable in v1: the wire format is fixed JSON
(`JsonEventSerializer`), spoken identically by the direct publish path, the
durable broker-route path, and the cross-process worker consumer — so a
binary storage serializer (Avro, Protobuf) never leaks onto the wire.

`event_type` is the fully-qualified class name recorded on the publication.
Treat it as untrusted input whenever payloads can originate outside the
trusted process boundary (a shared outbox table, a broker): the default
`JsonEventSerializer` resolves the class via `importlib`, so it accepts an
`allowed_event_types` allowlist (classes or fully-qualified names) and
rejects any other `event_type` with `ValueError` before resolving the
class. The cross-process worker applies this automatically, allowlisting
exactly the event types its listeners consume.

Fields decode by their annotations; `NewType` and `type` aliases decode as
the type they name. Two field shapes carry a type tag,
`{"__modulith_union_type__": "<module>.<qualname>", "value": ...}`: a
multi-member union, and a nested dataclass holding an instance of a
subclass of its declared class. A value of exactly the declared class stays
untagged. A subclass tag is matched only against subclasses of the declared
class already imported in the consuming process, never imported by name, so
the consumer must import the module defining the subclass. A tag naming
anything else raises `ValueError`.

**`Broker`** — external message broker (producer side):
```
async publish(target: str, payload: bytes, headers: dict[str, str] | None) -> None
async close() -> None
```

**`Consumer`** — cross-process consumer (the broker's consumer half):
```
async start() -> None
async stop() -> None
```
One `Consumer` wins per scheme, mirroring `Broker`. In process-per-module
topology each worker builds one from a `ConsumerSpec` (via the factory
registered by `modulith_register_consumers`) and only start()/stop()s it — the
consumer owns its own poll/claim/ack loop and dispatches to the local bus.
Delivery is at-least-once, so the listeners it feeds must be idempotent.

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

Applications adopting the process-per-module topology (Part IX) use a fifth name:

**`@externalized`** — marks an event as *externalized*: routed to the configured
broker so workers in other processes can consume it, even when it also has local
listeners (fan-out). `@externalized(target="scheme:destination")` overrides the
destination per event. Single-process applications never need it — the four
names above are the complete single-process API. See [§9.2](#92-the-worker-pattern).

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

`publish_sync(event, *, timeout=30.0)` blocks until dispatch completes.
The `timeout` (seconds) protects against listener deadlocks: on expiry the
dispatch is cancelled (best-effort) and the call raises `TimeoutError`;
`timeout=None` disables the bound. Calling it from *inside* async code on
the loop's own thread raises `RuntimeError` — use `await publish(event)`
there.

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

For the distributed case, `contracts` becomes versioned. Today the broker carries an `event_type` header (the fully-qualified class name) that consumers use to resolve and deserialize each message; arbitrary additional headers pass through the generic `headers` dict. A dedicated `schema_version` header is **planned, not yet provided** — until it lands, consumers that need version checks build them on the headers passthrough (and the doctor's schema-drift check flags event-definition changes as the cue to version consciously, see [§8.5](#85-the-doctor-command)).

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

Discovery imports each module *package* (its `__init__.py`), not every submodule: `@listener` functions in `handlers.py` register only if the package imports them (`from . import handlers`) or a `_manifest.py` declares them.

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

Every *scalar* `Configuration` field has a `MODULITH_<KEY>` env var equivalent: `MODULITH_PACKAGE`, `MODULITH_CONTRACTS_MODULE`, `MODULITH_OUTBOX`, `MODULITH_TOPOLOGY`, `MODULITH_BROKER`, `MODULITH_PRODUCTION`, `MODULITH_AUTO_DISCOVER`, `MODULITH_OBSERVABILITY`, `MODULITH_VERIFY_MANIFESTS`, `MODULITH_SUBSCRIPTION_SOURCE`, `MODULITH_ACTUATOR_MODE`, `MODULITH_STRICT_BOUNDARIES`. Booleans accept `1`/`true`/`yes` and `0`/`false`/`no` (case-insensitive); any other non-empty value raises `ConfigurationError`. The dict-typed fields (`outbox_options`, `broker_options`, `workers`) have **no generic** env var — they come from the `[tool.modulith.*]` subtables in pyproject.toml. Adapter-specific env vars are separate contracts: SHM and database options use `MODULITH_BROKER_<KEY>`, Redis Streams reads `REDIS_URL`, `MODULITH_CONSUMER_GROUP`, `MODULITH_STREAM_PREFIX`, and `MODULITH_STREAM_MAXLEN`, and the packaged alembic runner reads `MODULITH_DB_URL`.

Validation happens before construction. Unknown keys raise `ConfigurationError` with the list of valid keys (catches typos). Production mode + default memory outbox raises (forces explicit opt-in for unsafe defaults). Process topology defaults to local `shm`; an URL/DSN without an explicit broker selects `database`. Explicit `shm` accepts filesystem paths only and rejects DSNs and SQLAlchemy/network URLs. SQL schema names must be portable unquoted identifiers at every entry point: loaded configuration, broker environment overrides, direct `DatabaseBroker` construction, `MODULITH_DB_SCHEMA`, and Alembic `-x schema=...`.

The `explicit_keys: frozenset[str]` field tracks which values were set vs defaulted. Used by the production safety check.

### 6.4 Auto-Discovery

In `modulith/discovery.py`. Two strategies:

1. **Call-stack walking** (`_detect_from_caller_stack`): use `sys._getframe()` to walk back from the modulith bootstrap call. Skip frames inside modulith itself, stdlib, and site-packages. Return the top-level package of the first user-code frame.

2. **pyproject.toml** (`_detect_from_pyproject_name`): walk up from cwd looking for `pyproject.toml`, read `[project].name`, and replace hyphens with underscores — the conventional distribution-name → import-package mapping. (This is *not* PEP 503, which governs package-index name normalization and collapses hyphens/dots/underscores to `-`, the opposite direction.)

If both fail, raise `ConfigurationError` with all three escape hatches in the message.

### 6.5 The Friendly Banner

Logged at INFO via the `modulith` logger:

```
modulith: detected application package 'myapp'
modulith: discovered 3 module(s): orders, inventory, reports
modulith: outbox=memory, broker=memory, topology=single
modulith: outbox disabled — set [tool.modulith].outbox = 'postgres' for durable event delivery
modulith: ready
```

(The contracts subpackage, when present, is discovered and listed as a module too.)

Five log lines that tell the user exactly what's active and how to change it. If they don't want any of it, the message tells them where to turn it off.

The banner goes through the standard `modulith` logger at INFO level — it inherits the application's logging config and is **not** printed directly. Python's root logger surfaces only WARNING+ by default (and uvicorn configures only its own loggers), so the hosting application must enable INFO logging — e.g. `logging.basicConfig(level=logging.INFO)` at startup — for the banner to appear.

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

When `publish()` is called outside a transaction context, the bus dispatches directly without persistence (the outbox doesn't apply). On this direct path, an event that routes to a cross-process broker is sent inline (awaited), and a broker publish failure **propagates to the caller** — fail-loud by design: with no outbox row persisted, a swallowed send would lose the event for every remote consumer with zero trace. Callers that need `publish()` decoupled from broker availability use the transactional outbox path (bind a session + durable store), where the send happens after commit with retry/dead-letter handling.

### 7.3 Completion Modes

Three modes, selected with the `completion_mode` argument to `outbox.configure()` (`modulith/builtin/outbox.py`) when the application wires the store at startup.

In pyproject.toml, `[tool.modulith.outbox_options]` is the *reserved* home for outbox tuning. The subtable is parsed and validated (`outbox` itself is the scalar adapter-selection key — `outbox = "postgres"` — and TOML forbids one key being both a scalar and a table, so the options subtable is `outbox_options`. A legacy `[tool.modulith.outbox]` subtable is rejected with a `ConfigurationError` pointing at the correct spelling, and a pyproject.toml that fails to parse — including the scalar/table collision — is a loud `ConfigurationError`, never silently-ignored config). Pass claim/completion options through to `outbox.configure(...)` when wiring the store (`claim_strategy` defaults to `"lease"`; alternatives `"advisory_lock"` and `"none"`). Until an automatic config→configure bridge lands, apps must forward those keys explicitly.

- **`update`** (default) — set `completed_at`. Old records remain for inspection until a maintenance job purges them.
- **`delete`** — remove the row on success. Lower overhead, no historical visibility.
- **`archive`** — copy to `event_publications_archive` table, delete from primary. Best for high-volume systems where you want history without performance impact.

### 7.4 The Retry Loop

A background task started by the outbox plugin polls `find_incomplete(older_than=...)` periodically:

- On startup: `older_than=timedelta(0)` to catch crash recovery
- During normal operation: `older_than=timedelta(seconds=30)` to avoid thrashing fresh events

Failed dispatches stay incomplete with `attempt_count` incremented and `last_error` set. Retries use exponential backoff capped at the configured max (`max_retry_backoff_seconds`, default 5 minutes). Once `attempt_count` reaches `dead_letter_after_attempts` (unset by default, which resolves to the store's own setting if it has one, else 10), the record is moved to a dead-letter status (column flag) and surfaced via the actuator.

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
3. **Declared dependencies match observed** — if a manifest declares `declared_dependencies=["payments"]`, only those modules may be imported (when manifest is present)
4. **Events flow through contracts module** — cross-module type imports must come from `myapp.contracts.*`, not from another module's package
5. **Module data ownership** — when manifests declare `owns_tables=[...]`, queries against another module's tables are violations, including a `ForeignKey("table.col")` string literal pointing at a table another module owns; a module with a non-empty `owns_tables` also gets a warning for any table it defines but omits from that list, so the manifest stays a complete inventory

### 8.2 The AST-Based Verifier

Implementation: walk every `.py` file under the application package with `ast.parse()`. Collect all `Import` and `ImportFrom` nodes. For each, check:

- Is the source inside an application module?
- Is the target inside a different module's `_internal` package?
- Is the target a different module's package (not contracts)?

Emit `Violation` for each rule failure. The verifier hookspec is aggregating, so multiple plugins (built-in + custom) all contribute.

For data ownership rules, parse SQLAlchemy queries via static analysis of `.select()`, `.from_()`, `.execute()` calls. This is approximate — full coverage requires runtime checks via SQLAlchemy events, which is the v1.1 enhancement.

### 8.3 Ratcheting Mode

For brownfield adoption, run:

```bash
modulith verify --mode=ratchet --baseline=.modulith-baseline.json
```

`[tool.modulith.verify]` is reserved for a future config-backed default. The
current implementation intentionally ignores that subtable for forward
compatibility; use the CLI flags above today.

The baseline file records existing violations. The verifier:
- Passes any violation listed in the baseline (grandfathered)
- Fails any new violation
- `modulith verify --update-baseline` regenerates the file after refactoring

Same pattern as `mypy --strict` rolling out gradually. The baseline diff in git review shows what got fixed and what got worse. This is the single biggest adoption lever — without it, modulith is "for new projects only."

### 8.4 The Audit Tool

`modulith audit` analyzes an existing codebase non-destructively:

- Proposed module structure based on folder layout: each top-level subdirectory of the audited root is a module candidate. At a project root whose only application directory is one package, or `src/` holding one package, the audited root is that package; tests, docs, scripts, examples, migrations, virtualenvs, hidden and build directories are ignored when deciding. The command prints the root it chose.
- List of cross-module imports that would become violations
- List of shared database tables that need ownership decisions
- Modulith-readiness score (0-100): percentage of cross-module interactions that go through events vs direct calls. With fewer than two module candidates the score is reported as not applicable, with a warning.

Output is Markdown. Teams can run it on Friday afternoon, generate a baseline, have green CI on Monday, then tighten over weeks.

### 8.5 The Doctor Command

`modulith doctor` reports operational and architectural health:

- **Boundary health**: violation count, baseline drift
- **Process-split readiness**: percentage of cross-module interactions that are events vs direct calls (the "are you ready to split this module?" metric), plus per-module counts of cross-module table references and of tables not prefixed with the module's name — table-only coupling reports a warning even when there are no import/event interactions, and the "microservice-ready" tier requires zero cross-module table references
- **Schema drift**: events whose field definitions (name, annotation, default — fingerprinted via AST) changed since the last doctor run. The check is an unconditional fingerprint diff against a cache file (`.modulith-schemas.json`): it flags *every* definition change as the cue to version consciously — it does not read or compare any `schema_version` attribute
- **Outbox health**: incomplete, completed, and dead-lettered counts
- **Listener registration coverage**: declared listeners vs actually-registered listeners
- **SHM notifier**: whether each SHM broker's hint ring actually attached (a `shm_capacity` change on an existing hint file leaves the notifier dead — delivery still works, only slower)
- **Actuator token**: under `topology = "processes"`, whether the actuator would start unmounted (`auto` mode, no `MODULITH_ACTUATOR_TOKEN`, non-loopback bind) or refuse to start (`token` mode, no token, reported as an error)
- **Single-host broker**: a per-host broker (`shm`, or `database` on embedded SQLite) configured under a detected container runtime — a warning under Docker, an error under Kubernetes when `production = true`
- **Redis retention**: a `redis-streams` `max_stream_len` below the safe minimum, tightened by a live pending+lag backlog query when a client is reachable

Same machinery as `modulith doctor` in Spring Modulith's spirit but expanded to operational concerns.

---

## Part IX — Process-Per-Module Runtime

The v2 wedge. The feature that makes "modulith now, microservices later" credible.

### 9.1 The Topology Decision

Three options, ranked by setup ease:

- **A. One process per module, local broker for IPC** ✅ **chosen.** Each module runs as its own uvicorn worker. Communication defaults to the durable local SHM/SQLite broker; configured URL/DSN options select the database broker, while Redis Streams remains explicit. Reuses outbox + externalization machinery. Latency is workload- and host-dependent and must be measured, not assumed.
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
- Routes this module's cross-module *publishes* out through the broker, and
  (via the lifespan) starts a `BrokerConsumer` that subscribes to the streams
  for the events this module's listeners consume, deserializes each via its
  `event_type` header, and dispatches it to the local listeners
- Returns a FastAPI app exposing only that module's router

Standard uvicorn machinery from there — workers, reload, signals, graceful shutdown.

**Externalized events.** An event crosses to the broker when it has no local
listener (a cross-module event whose only consumer lives in another worker —
routed automatically under the default scheme), or when it carries an explicit
externalization signal:

```python
from dataclasses import dataclass
from modulith import event, externalized

@externalized                       # fan-out: local listeners AND remote workers
@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str

@externalized(target="redis-streams:orders.priority")  # explicit destination
@event
@dataclass(frozen=True)
class PriorityOrderPlaced:
    order_id: str
```

Dynamic routing (tenant-aware topics, A/B channels) goes through the
`modulith_resolve_event_target` hook, which takes precedence over the static
annotation. When the transactional outbox is enabled, broker routing is
commit-gated like local dispatch: the route is persisted as its own outbox
row inside the business transaction and delivered to the broker after commit
— a rollback discards it, so remote consumers never see an un-committed event.
Publishing a cross-process event whose target scheme has no registered broker
adapter raises `ConfigurationError` (uniformly for the default scheme and
explicit targets).

### 9.3 The Supervisor

`modulith/supervisor.py` orchestrates workers in process-per-module mode:

- Spawns one subprocess per module via `asyncio.create_subprocess_exec`
- Each is a uvicorn invocation pointing at `modulith._worker:create_app`
- Stdout/stderr stream back with module-name prefixes
- SIGTERM cascades; crashed workers restart with exponential backoff
  (crash detection is process exit — there is no periodic health-check polling)
- Topology changes (worker count, module isolation) without app code changes

Usage:
```bash
modulith run app.main --topology=processes
modulith run app.main --workers='{"reports": 4, "default": 1}'
modulith dev app.main --isolate=reports  # only reports runs; every other module is not started (its routes 404 through the proxy)
```

### 9.4 The Reverse Proxy

`modulith/proxy.py` is a Starlette ASGI app that routes requests to workers by URL prefix:

```
/orders/*    → http://127.0.0.1:9001
/inventory/* → http://127.0.0.1:9002
/reports/*   → http://127.0.0.1:9003
/_modulith/* → supervisor's own actuator
```

Implementation: `httpx.AsyncClient` for streaming proxy. The proxy bounds
buffered request bodies, logs upstream URLs without query strings, and can
guard `/_modulith/*` actuator metadata with an optional bearer token. The
proxy also aggregates worker health: the `/_modulith/health` actuator route
queries each worker's `/health` endpoint on demand. WebSocket support is a
v2.1 enhancement.

### 9.5 Topology Configuration

```toml
[tool.modulith]
topology = "processes"       # or "single" ("subinterpreters" is reserved, not yet implemented)
# broker omitted             # defaults to local durable "shm"

[tool.modulith.workers]
default = 1
reports = 4
```

There is no `[tool.modulith.supervisor]` subtable — configuration resolution reads only the `outbox_options`, `broker_options`, and `workers` subtables. The supervisor's restart policy is built in, not configurable via pyproject (`modulith/supervisor.py`): per-instance exponential backoff starting at 1s, doubling to a 60s cap, with a crash-loop circuit breaker that stops respawning an instance after more than 5 crashes in a row with no healthy run in between (the spacing between crashes is irrelevant — a module crashing every few minutes trips it just the same), and a backoff reset once an instance has stayed up past the healthy-uptime threshold (defaults to the 60s cap), which also clears the crash streak. Crash detection is process-exit-based — the supervisor awaits each worker process; there is no periodic health-check polling.

Startup failure modes for the cross-process broker are deliberately loud:

- `topology = "processes"` with no broker or URL/DSN selects local `shm`; a
  URL/DSN instead selects `database`. Explicit `broker = "memory"` raises
  because an in-memory broker cannot carry events between processes.
- Explicit `broker = "shm"` rejects DSNs and SQLAlchemy/network URLs; use its
  canonical filesystem options or select `database`.
- `topology = "subinterpreters"` parses as a known topology but is rejected
  with "not yet implemented" (reserved for a future release; ROADMAP Phase 4).
- A cross-process topology whose configured broker *scheme* has no registered
  adapter (typo, missing plugin, missing extra) logs a WARNING in the startup
  banner, and any cross-process publish in that state raises
  `ConfigurationError` rather than dropping the event.

---

## Part X — Built-in Adapters

Each adapter ships as an optional *extra* of the single `modupy` distribution so dependencies stay optional. `pip install 'modupy[postgres]'` pulls in the SQLAlchemy adapter; without it, the outbox can't use Postgres but everything else works. (Splitting adapters into separately-published packages remains a possible later move — see [Part XVI](#part-xvi--file-inventory) — but is not the shipped model.)

### 10.1 Postgres Outbox Store

Extra: `modupy[postgres]` (`modulith/adapters/postgres_outbox.py`). Implements `PublicationStore` against a SQLAlchemy async engine. Ships:

- Schema migrations (alembic, packaged under `modulith/adapters/migrations/`) for `event_publications` and `event_publications_archive`
- The session-event integration described in [§7.2](#72-sqlalchemy-integration)
- Health check endpoint integration

The alembic migration is the source of truth for the schema (the ORM
metadata and migration `0001_initial` are asserted identical against a real
Postgres by the integration suite). Equivalent DDL:

```sql
CREATE TABLE event_publications (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload BYTEA NOT NULL,       -- bytes, not JSONB: binary serializers supported
    listener TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMPTZ,  -- when the most recent retry ran; drives backoff
    is_dead_lettered BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_pending ON event_publications (published_at)
    WHERE completed_at IS NULL;

-- Populated by the "archive" completion mode.
CREATE TABLE event_publications_archive (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    listener TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMPTZ
);
```

Apply it with the packaged alembic config rather than hand-running SQL
(`MODULITH_DB_URL` or `-x url=...` supplies the database URL):

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  upgrade head
```

### 10.2 Redis Streams Broker

Extra: `modupy[redis]` (`modulith/adapters/redis_broker.py`). Implements `Broker` against `redis.asyncio`. It is an explicit networked choice for process-per-module deployments.

Select the broker by name, and supply connection options under the
`[tool.modulith.broker]` subtable. TOML forbids one key (`broker`) being both a
string and a table in the same file, so when you use the options subtable the
broker *name* comes from `MODULITH_BROKER` or `configure(broker="redis-streams")`:

```toml
# Name only (no connection options): scalar form.
[tool.modulith]
broker = "redis-streams"
```

```toml
# With connection options: subtable form. Set the name out-of-band, e.g.
#   export MODULITH_BROKER=redis-streams
[tool.modulith.broker]
url = "redis://localhost:6379"
consumer_group = "modulith-orders"
```

Each option also has an environment variable that takes precedence at deploy
time: `REDIS_URL`, `MODULITH_CONSUMER_GROUP`, `MODULITH_STREAM_PREFIX`,
`MODULITH_STREAM_MAXLEN`. (Values are literal — there is no `${VAR}`
interpolation inside the TOML.)

Retention caveat: `max_stream_len` / `MODULITH_STREAM_MAXLEN` is enforced via
`XADD MAXLEN ~`, which trims by stream length alone and is blind to
consumer-group pending state — an undersized cap lets a publish burst silently
trim entries that were delivered but never ACK'd (permanently losing them
despite the XAUTOCLAIM recovery path) or never delivered at all. The consumer
surfaces such losses at ERROR level (via XAUTOCLAIM's deleted-ids element).
Size `max_stream_len` well above the worst-case backlog (publish rate ×
consumer downtime/latency). The dead-letter stream is likewise bounded
(`dlq_max_stream_len`, default 10× `max_stream_len`) and best-effort, not a
durable audit log — size it to the forensic retention window you need.

### 10.2a Durable Local SHM Broker

Extra: none; `modulith/adapters/shm_broker.py` and `_shm_*.py` use only the
standard library. This is the implicit broker for `topology = "processes"` when
no URL/DSN is configured, and it is local-host only.

The scheme name does not define the durability boundary. SQLite is
authoritative for publications, subscriptions, claims, retries, and completion.
Every successful publish commits SQLite before writing a best-effort sequence
hint to the file-backed mmap ring. The ring never contains payload or delivery
state; missing, torn, stale, wrapped, or incompatible hints fall back to a
periodic SQLite safety poll.

Publications are retained for 24 hours. A consumer group that subscribes after
publication receives one replay before expiry, preventing silent loss during
worker startup. Delivery is at-least-once: a crash after listener completion
but before the fenced acknowledgement commits can cause a duplicate.

Canonical `state_dir`, `sqlite_path`, and `hint_path` resolve to absolute,
package-namespaced paths under a private per-user directory (`0700` directories
and `0600` files on POSIX). Explicit SHM rejects DSNs and SQLAlchemy/network
URLs. WAL with `synchronous=NORMAL` survives application/process restart on the
same disk; `FULL` is the explicit opt-in for OS-failure and power-loss
durability.

Two byte limits bound the authoritative store. `max_payload_bytes` defaults to
16 MiB and cannot exceed 1 GiB; payload validation occurs before the publish
transaction. `max_store_bytes` defaults to 1 GiB and cannot exceed 1 TiB; it is
translated to SQLite `max_page_count`, so page exhaustion rolls back and
rejects the publish as backpressure. Both have
`MODULITH_BROKER_MAX_PAYLOAD_BYTES` / `MODULITH_BROKER_MAX_STORE_BYTES`
overrides. `shm_slot_size` is deprecated and ignored because mmap hint slots
are fixed-size sequence records.

### 10.3 Kafka Broker (planned — not shipped)

**Roadmap item (Phase 4), not a shipped adapter.** Will implement `Broker` against `aiokafka` for teams already running Kafka. No `kafka_broker.py` exists and there is intentionally no `kafka` extra in pyproject.toml — we don't advertise a dependency for a feature that does not exist. It returns when the adapter lands.

### 10.4 OpenTelemetry Observability

Extra: `modupy[otel]` (built-in plugin `modulith/builtin/observability.py`; a silent no-op when OTel isn't installed, or installed without a configured tracer provider). Auto-instrumentation emits two span types via the paired event-lifecycle hooks:

- `modulith.event.publish` — one per publication, attributes `event.type`, `event.module`, `modulith.duration_ms`. On the durable (outbox) path the span brackets the persistence step; a persist/serialize/broker-route failure still ends the span, with the exception recorded and status ERROR (the span never leaks).
- `modulith.event.dispatch` — one per listener invocation, attributes `event.type`, `listener.name`, `publication.id`; status ERROR (with recorded exception) when the listener raises. Parenting depends on the path: on the **in-memory path** the dispatch span is a child of the publish span; on the **durable (outbox) path** listener dispatch runs after the business transaction commits, in a different context, so those dispatch spans are **not** parented to the publish span — correlate them via `publication.id` instead.

### 10.5 Documentation Generator

`modulith/builtin/docs.py`. Generates:

- `docs/architecture.mmd` — Mermaid C4 component diagram
- `docs/modules/<name>.md` — Application Module Canvas (public API, events published, events consumed, dependencies, internals)
- `docs/events.mmd` — Sequence diagram of event flows

Mermaid over PlantUML because it renders natively on GitHub/GitLab. Canvas is markdown so it diffs cleanly in PRs.

---

## Part XI — Testing

### 11.1 The pytest Plugin

Ships bundled in the main distribution as `modulith/testing.py`, installed via the `modupy[test]` extra (registered under pytest's `pytest11` entry point, so the fixtures are available automatically). A standalone `pytest-modulith` package is a planned later split, not current reality. Provides:

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
    def within(seconds: float) -> Any  # terminal: returns the matched event;
                                       # raises AssertionError when it never arrives
```

`within()` is the terminal operation: it fires the trigger and polls for the expected event, with the trigger and the poll sharing one `seconds` budget. Handles the eventual-consistency dance — async listeners need awaiting in tests. Internally polls the published events list with timeout, no `asyncio.sleep` in user code.

### 11.3 Module-Isolated Tests

Implementation strategy: subprocess-per-test for the strictest isolation mode. Fork overhead is fine for integration tests in CI; not for unit tests on save.

For non-isolated tests, the plugin handles state reset via session fixtures: clears event bus, drops listeners, restores `sys.modules` to a snapshot. The user never thinks about it.

### 11.4 Subprocess-Per-Test Mode

Activated via `@pytest.mark.modulith_isolated`. The plugin re-invokes pytest on that single test in a child process (an env-var guard prevents recursion) and synthesizes the test report from the child's exit code. State leaks are impossible. Cost: a full interpreter + pytest startup per test — fine for integration tests, not for unit tests on save.

Each isolated subprocess is bounded by the `modulith_isolated_timeout` ini option (seconds, default `300`): a hung child is killed and reported as a failure of that one test — with its captured stdout/stderr — instead of blocking the suite forever. Tune it in pytest configuration, e.g.:

```toml
[tool.pytest.ini_options]
modulith_isolated_timeout = "120"
```

---

## Part XII — The CLI

`modulith` is a typer-based CLI. Distributed via `[project.scripts]` in pyproject.toml.

### Commands

```
modulith dev APP_MODULE [--topology=single|processes] [--isolate=MODULE] [--reload/--no-reload] [--host=HOST] [--port=PORT]
modulith run APP_MODULE [--topology=single|processes] [--workers=JSON] [--host=HOST] [--port=PORT]
modulith verify [--mode=strict|ratchet] [--baseline=PATH] [--update-baseline] [--fail-on-warnings]
modulith docs [--output-dir=DIR]
modulith audit [PATH] [--output=FILE]
modulith extract MODULE [--output=DIR] [--force]
modulith k8s-manifest [--output=FILE] [--image=IMAGE] [--namespace=NAME] [--port=PORT]
modulith openapi [--output=FILE] [--title=TITLE] [--api-version=VERSION]
modulith doctor
modulith outbox status
modulith outbox retry <id>
modulith outbox purge --older-than=DURATION
modulith outbox dead-letter [--list|--retry-all]
modulith info  # show detected config, modules, plugins
```

`dev` and `run` take a required positional `APP_MODULE` (the ASGI app, e.g. `myapp.main:app`); `audit` takes an optional positional `PATH` (the codebase root, default `.`; a project root resolves to its single application package, see [§8.4](#84-the-audit-tool)).

`extract`, `k8s-manifest`, and `openapi` bootstrap and import configured
application modules to derive artifacts; they are build-time tools for trusted
source. Extraction writes a wheel-buildable project through a staging
directory and rejects output symlinks, output inside the source package,
non-empty targets, and source symlinks that escape the package. Kubernetes
names are RFC-1123 labels with stable hashes for long inputs, ports must be
1–65535, only supported broker environment contracts are emitted, and the
contracts module is passed explicitly. OpenAPI generation requires the
`fastapi` extra and rejects incompatible collisions or duplicate operation IDs
instead of silently discarding definitions.

### Exit codes

Uniform across every command:

- **0** — success. Warnings may still have been reported (verify's WARNING-severity violations without `--fail-on-warnings`, `dev`'s startup boundary warnings, doctor's warn-tier checks).
- **1** — violations or user error *within a recognized command line*: failed verification, invalid flag **values**/arguments (typo'd `--mode`/`--topology` values are rejected loudly, never silently defaulted), configuration errors, unknown ids, missing uvicorn, unwritable `--baseline` paths.
- **2** — unexpected internal errors (a modulith bug; traceback printed to stderr) **and CLI usage errors** (a missing required argument, an unknown option): the CLI is built on click, whose convention exits 2 for usage errors — modulith follows it rather than fighting the framework.

`modulith verify` exits 0 when no ERROR-severity violations are reported (strict) or none are new relative to the baseline (ratchet); `--fail-on-warnings` opts in to failing on WARNING-severity findings too. `modulith doctor` exits 1 only when a check reports an error, so both drop into CI as a single line.

### `modulith dev` semantics

`modulith dev` is *almost* `uvicorn --reload` with quality-of-life additions:
- Prints the discovered module list at startup
- Runs the boundary verifier at startup and echoes violations as **non-fatal warnings** on stderr — the "warnings in dev, hard checks via `modulith verify` in CI" promise of [§3.2](#32-defaults-so-good-you-dont-change-them). A dev server must start even when the project is half-configured, so a failing check downgrades to a note; it never blocks the launch
- Shows a friendly banner with topology and detected adapters

**It is not a different way to run the app; it's a nicer way.** Users with muscle memory for `uvicorn` keep using `uvicorn`. The CLI is a progressive enhancement.

### `modulith run` semantics

`modulith run` is `modulith dev` minus reload, plus production-mode toggles. In `--topology=processes`, it spawns the supervisor. Designed to be the actual production entrypoint for users who want modulith to manage their topology, but optional — running each worker as a vanilla uvicorn process is also supported.

---

## Part XIII — Migration Strategy

### 13.1 Greenfield Adoption

```bash
# 1. Install
uv add modupy

# 2. Define modules as subpackages (zero config)
mkdir myapp/orders myapp/inventory

# 3. Run (the FastAPI app object lives in myapp/main.py)
uvicorn myapp.main:app --reload

# That's it.
```

### 13.2 Brownfield Adoption (the path that matters)

```bash
# 1. Install (the CLI needs the cli extra)
uv add 'modupy[cli]'

# 2. Audit existing structure (writes MIGRATION.md by default; --output to change.
#    Don't shell-redirect stdout onto the same file — the command already writes
#    the report there and echoes a short summary to stdout.)
modulith audit
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

### 13.3 The Three-Phase Migration

A compressed summary of MIGRATION_GUIDE.md's seven steps — three phases, deliberately *not* numbered 1:1 with the guide's step headings:

1. **Install + audit + ratchet** (guide Steps 1–3). Boundaries enforced going forward; existing violations grandfathered.
2. **Add events incrementally** (guide Step 4). Pick one cross-module call at a time; replace direct call with `publish` + `@listener`. Each migration is a single PR.
3. **Enable outbox** (guide Step 5). Set `outbox = "postgres"`, run migrations, verify outbox-readiness via `modulith doctor`. Production-grade event delivery.

Process-per-module (guide Step 6) and true microservices extraction (guide Step 7) are optional later moves justified by data, not architecture.

### 13.4 Migrating the Legacy SHM Store

Stop the full process fleet and back up its private state directory before the
first new worker opens the old overflow-only v0 database. First open migrates
the `shm_message` schema transactionally. Only messages that spilled to that
SQLite store are recoverable; payloads that existed only in the old ring cannot
be recovered. Never mix old and new workers against the same state files.

---

## Part XIV — The Seven Gap Mitigations

From the brutal-truth analysis. Each gap has a concrete mitigation.

### Gap 1: Cross-module event imports leak the source module

**Mitigation:** the contracts module pattern ([§5.3](#53-the-contracts-module-pattern)). Events live in `myapp.contracts.*`, a sink in the dependency graph. Verifier treats it specially. For distributed deployments, `contracts` becomes versioned (a dedicated `schema_version` broker header is planned — see [§5.3](#53-the-contracts-module-pattern) for what ships today).

### Gap 2: The "modulith now, microservices later" promise has a hidden cliff

**Mitigation:** module-level data ownership rules ([§8.1](#81-the-default-rules)) + `modulith doctor` ([§8.5](#85-the-doctor-command)). Users see their split-readiness as a number. We document the cliff explicitly: "no rewrites for the messaging layer; database boundaries are a separate decision." Tooling now closes part of that data half: `doctor`'s process-split readiness check counts cross-module table references (not just imports) and reports tables not prefixed with their owning module's name; the verifier's `data-ownership` rule detects `ForeignKey("table.col")` string literals pointing at another module's table, not just `Table()`/`__tablename__` declarations; a per-module Postgres schema knob (`broker_options.schema`/`MODULITH_BROKER_SCHEMA` for the broker, `-x schema=`/`MODULITH_DB_SCHEMA` for migrations) gives modules physically separate storage; and `modulith extract` refuses (without `--force`) to scaffold a module that still shares a table with another module. What remains manual: actually moving a shared table's data to its owning module, and choosing the schema-vs-prefix convention per table — the tooling detects and reports the coupling, it does not resolve it.

Enabling a named migration schema does not move existing data. If the target
has no Alembic history while `public` contains Modulith tables or history, the
migration refuses to create a second history until operators back up,
explicitly move and verify the data, and rerun it.

### Gap 3: The async assumption is hostile to existing FastAPI codebases

**Mitigation:** `publish_sync()` and sync `@listener` support ([§5.2](#52-sync-vs-async)). The framework detects the calling context (running loop or not) and does the right thing. Sync listeners run in the executor, async run directly.

### Gap 4: Decorator-based listener registration creates import-order dependencies

**Mitigation:** the manifest file ([§5.4](#54-the-manifest-file)) and a startup verification check that compares declared listeners to actually-registered listeners. Mismatches fail loudly with file:line.

### Gap 5: Testing is genuinely harder than the docs admit

**Mitigation:** the bundled pytest plugin, installed via `modupy[test]` ([Part XI](#part-xi--testing)). Auto-reset between tests, subprocess-per-test for isolation, Scenario API for event-driven flows.

### Gap 6: No story for adopting modulith on existing codebases

**Mitigation:** ratcheting verifier ([§8.3](#83-ratcheting-mode)) + `modulith audit` ([§8.4](#84-the-audit-tool)). Teams adopt on Friday, ratchet on Monday, tighten over weeks.

### Gap 7: The plugin ecosystem might never form

**Mitigation:** reframe the positioning. Plugin system is for internal modularity, not for community ecosystem. First-party adapters cover the Postgres outbox, durable local SHM, Redis Streams, relational-database brokering, and OpenTelemetry; Kafka and RabbitMQ remain Phase 4. Community plugins are nice-to-have, not required for success.

---

## Part XV — Implementation Roadmap

> **Planning snapshot.** This part is the original phase plan, kept for the
> rationale and kill criteria. **[ROADMAP.md](ROADMAP.md) is the live status
> source** — per its checklist, Phases 0–3 are all code-complete; Phase 4
> (ecosystem adapters) is the open, demand-driven remainder.

Time-boxed phases. Each has explicit kill criteria.

### Phase 0: Foundation ✅ DONE

- Plugin contract (13 hookspecs, 5 protocols)
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
1. **The pytest plugin** (planned then as a separate `pytest-modulith` package; shipped instead as the `modupy[test]` extra). ~200 lines.
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

> **What each file is, not how big it is.** The original planning table carried
> a per-file line-count estimate. Those estimates drifted by multiples as the
> code landed, and a hand-typed line count is not something a reader can act on
> — `wc -l` answers it exactly and never goes stale — so the column is dropped
> rather than re-guessed. For live delivery status, [ROADMAP.md](ROADMAP.md) is
> the single source of truth; the Status column below matches it.

### Status legend
- ✅ — Built and tested
- ⏳ — Planned, not yet started

### Core package: `modulith/`

| File | Status | Notes |
|---|---|---|
| `__init__.py` | ✅ | Public API exports |
| `types.py` | ✅ | Public dataclasses and enums for module metadata, event publications and boundary violations |
| `protocols.py` | ✅ | `PublicationStore`, `EventSerializer`, `Broker`, `Consumer`, `HealthAwareConsumer` |
| `hooks.py` | ✅ | The 13 hookspecs |
| `markers.py` | ✅ | hookimpl re-export |
| `brokers.py` | ✅ | BrokerRegistry |
| `manager.py` | ✅ | create_plugin_manager |
| `config.py` | ✅ | Configuration + load_configuration |
| `discovery.py` | ✅ | detect_application_package |
| `event_bus.py` | ✅ | InMemoryEventBus |
| `runtime.py` | ✅ | Runtime singleton |
| `decorators.py` | ✅ | @event, @listener, publish, configure |
| `serializers.py` | ✅ | JsonEventSerializer (default EventSerializer, `allowed_event_types` allowlist) |
| `sync.py` | ✅ | publish_sync, sync listener support (Phase 1) |
| `manifest.py` | ✅ | declare_module API (Phase 1) |
| `audit.py` | ✅ | Codebase analysis (Phase 2) |
| `doctor.py` | ✅ | Health diagnostics (Phase 2) |
| `cli.py` | ✅ | Typer-based CLI (Phase 1) |
| `_worker.py` | ✅ | Per-module FastAPI app generator (Phase 3) |
| `_consumer.py` | ✅ | BrokerConsumer: per-worker stream subscription + dispatch (Phase 3) |
| `supervisor.py` | ✅ | Process orchestration (Phase 3) |
| `proxy.py` | ✅ | Reverse proxy (Phase 3) |
| `testing.py` | ✅ | pytest plugin entry points (Phase 2) |

### Built-in plugins: `modulith/builtin/`

| File | Status | Notes |
|---|---|---|
| `__init__.py` | ✅ | Namespace package |
| `discovery.py` | ✅ | Default subpackage walker |
| `verifier.py` | ✅ | AST-based boundary verification (Phase 1) |
| `outbox.py` | ✅ | Outbox plugin core, calls store adapter (Phase 1) |
| `observability.py` | ✅ | OTel auto-instrumentation (Phase 2) |
| `docs.py` | ✅ | Mermaid + canvas generation (Phase 1) |

### Storage adapters: `modulith/adapters/`

| File | Status | Notes |
|---|---|---|
| `__init__.py` | ✅ | Namespace package |
| `postgres_outbox.py` | ✅ | SQLAlchemy + Postgres PublicationStore, alembic migrations (Phase 1) |
| `redis_broker.py` | ✅ | Redis Streams Broker (Phase 2) |
| `db_broker.py` | ✅ | Postgres/MySQL/SQLite database broker |
| `shm_broker.py` + `_shm_*.py` | ✅ | SQLite-authoritative local broker + advisory mmap hints |
| `_state_path.py` | ✅ | Private package-namespaced broker state paths |
| `kafka_broker.py` | ⏳ | Kafka Broker (Phase 4 — not shipped, see §10.3) |

### Tests: `tests/`

The suite has grown far past this planning table (~45 test modules, including
testcontainers-backed integration suites — see Appendix B); the original
per-file plan is omitted rather than maintained here in parallel.

### Examples: `examples/`

| File | Status | Notes |
|---|---|---|
| `redis_streams_broker.py` | ✅ | Complete broker adapter (example scheme `redis-streams-example`) |
| `naming_convention_verifier.py` | ✅ | Custom verification rule |
| `demo_app/` | ✅ | Runnable three-module shop wired purely through events |

### Top-level

| File | Status | Notes |
|---|---|---|
| `pyproject.toml` | ✅ | Project metadata, dependencies, entry points, CLI script |
| `README.md` | ✅ | The first thing users read |
| `SPEC.md` | ✅ | This document |
| `ROADMAP.md` | ✅ | Phase plan with checkboxes (live status source) |
| `MIGRATION_GUIDE.md` | ✅ | How to adopt on existing codebases |
| `LICENSE` | ✅ | Apache 2.0 |

### Separate packages (re-scoped: shipped as extras)

The original plan floated separately-published packages. The shipped decision
is **extras of the single `modupy` distribution** (see Part X): the test
plugin is `modupy[test]` (standalone `pytest-modulith` remains a possible
v2 split), the Postgres outbox is `modupy[postgres]`, the Redis broker is
`modupy[redis]`. A Kafka adapter (whether extra or package) is Phase 4.

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
name = "modupy"
version = "0.1.0"
description = "Modular monolith pattern for Python"
requires-python = ">=3.11"
dependencies = ["pluggy>=1.3"]

[project.optional-dependencies]
postgres = ["sqlalchemy>=2.0", "asyncpg>=0.29", "alembic>=1.13"]
redis = ["redis>=5.0.1"]
database = ["sqlalchemy>=2.0", "asyncpg>=0.29", "aiomysql>=0.2", "aiosqlite>=0.19", "..."]
# local SHM broker has no extra; it is stdlib-only
# no kafka extra — the adapter is Phase 4, unshipped (§10.3)
otel = ["opentelemetry-api", "opentelemetry-sdk"]
fastapi = ["fastapi>=0.110", "uvicorn>=0.27", "httpx>=0.26"]
cli = ["typer>=0.12", "rich>=13.0"]
test = ["pytest", "pytest-asyncio", "..."]  # see pyproject.toml for the full pins
all = ["modupy[postgres,redis,database,otel,fastapi,cli,test]"]

[project.scripts]
modulith = "modulith.cli:main"

[project.entry-points."modulith"]
# Built-in plugins. Users disable via configure(disable_plugins=[...]) at
# startup — [tool.modulith] has no 'disable' key (unknown keys are rejected).
# (None needed here — built-ins are loaded by manager.BUILTIN_PLUGINS.)

[tool.modulith]
# Defaults for the modulith project itself if used self-referentially.
# Real apps put their own [tool.modulith] section here.
```

### Appendix B — Test Strategy

Three layers:

1. **Unit tests** (`tests/test_*.py`) — fast, isolated, no I/O. Cover individual modules. The Postgres outbox adapter is exercised here against in-memory SQLite so the bootstrap/retry/dead-letter logic is covered with zero external services.
2. **Integration tests** — exercise the full bootstrap + dispatch flow. Use the fake-app fixture in `test_zero_config.py` as the template.
3. **End-to-end tests** (`tests/test_*_e2e.py`, `test_*_integration.py`, `test_migration_postgres.py`) — real Postgres, real Redis, and real `uvicorn` subprocess workers, provisioned on demand by [testcontainers](https://testcontainers.com) (`postgres:16` + `redis:7`). They cover the outbox against real Postgres (including the Alembic migration and `FOR UPDATE SKIP LOCKED`), Redis Streams consumer-group delivery / `XAUTOCLAIM` reclaim / dead-lettering, cross-process event delivery, and the process-per-module supervisor + proxy. Marked `@pytest.mark.integration` and gated behind Docker — they auto-skip when no daemon is reachable, so the default `pytest` run stays hermetic.

Running them:

```bash
pip install -e '.[integration]'   # testcontainers + asyncpg + psycopg[binary]
pytest -m integration             # spins up Postgres + Redis containers
```

Set `MODULITH_TEST_POSTGRES_URL` / `MODULITH_TEST_REDIS_URL` to run against pre-existing services instead of containers (e.g. a CI service container).

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
