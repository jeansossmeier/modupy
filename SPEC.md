# modupy — Design Document

> A Spring Modulith-inspired modular monolith framework for Python. This is the design document: the reasoning behind modupy's design. Where it and the code disagree, the code and [docs/STABILITY.md](docs/STABILITY.md) are right.

**Where to start.** Install and quick start are in [README.md](README.md), and what is built and what comes next is in [ROADMAP.md](ROADMAP.md). This document is for readers who want the reasoning behind the design: Parts I to III give the vision and philosophy, Parts IV, V and XII cover the plugin contract, the user-facing API and the CLI, and Parts VI to X explain how the runtime, the outbox, boundary verification, the process-per-module runtime and the adapters work.

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
- [Appendices](#appendices)

**Terms used throughout**

- **Outbox** — a database table that records each published event in the same transaction as your data, so delivery survives a crash ([Part VII](#part-vii--the-transactional-outbox)).
- **Hookspec** — a pluggy hook definition: a function signature that plugins implement to extend modupy ([§4.1](#41-the-thirteen-hookspecs)).
- **Contracts module** — the shared package that holds the events modules exchange; every module may import it and it imports no module ([§5.3](#53-the-contracts-module-pattern)).
- **Ratchet** — a verification mode that records today's boundary violations in a baseline file and fails only on new ones ([§8.3](#83-ratcheting-mode)).
- **SHM** — short for shared memory: the built-in local broker for process-per-module. It keeps messages in SQLite and uses a memory-mapped file only as a wake-up hint ([§10.2a](#102a-durable-local-shm-broker)).

---

## Part I — Vision and Scope

### 1.1 What This Is

modupy (which installs the `modulith` package) is a Python library that supports the modular monolith architectural pattern. It provides:

- **Module structure with enforced boundaries** — packages within an application that have public APIs and private internals, with violations caught at verification time.
- **Event-driven inter-module communication** — modules talk to each other through events, not direct calls, keeping coupling low.
- **Transactional outbox** — events published inside a database transaction are durably stored and delivered at-least-once, surviving crashes.
- **Optional process-per-module runtime** — when a module needs its own CPU/memory budget, promote it to its own process without rewriting code.
- **Auto-discovery and zero-config setup** — install, define modules as subpackages, run with uvicorn as you always have. The framework configures itself.

It is inspired by Spring Modulith, which provides similar capabilities for Spring Boot. Where Java idioms don't translate cleanly to Python, we choose the most pythonic equivalent rather than porting verbatim.

### 1.2 The Three-Tier Promise

The project's value proposition is a single sentence with three escape valves:

> **`modulith dev` for development, `modulith dev --topology=processes` when one module needs its own CPU, `modulith extract` when one module needs its own container — same code, no rewrites for the messaging layer.**

![The same three modules in three shapes: one process on day one, one process per module when a feature gets busy, and payments split off into its own service](docs/images/growth.svg)

The phrase "no rewrites for the messaging layer" is doing important work. We promise zero-rewrite for events, listeners, and module structure. We do not promise zero-rewrite for shared databases, shared transactions, or shared in-memory state. Those are separate decisions a user makes consciously, ideally early.

### 1.3 The One-Sentence Pitch

> A modular monolith for Python that grows with you: in-process today, multi-process tomorrow, microservices when you actually need them.

### 1.4 What Success Looks Like

A team installs modupy, restructures their FastAPI app into subpackages, sprinkles `@event` and `@listener` decorators, and ships to production. Six months later, one module needs more CPU; they flip a flag and that module runs in its own process. Two years later, one module needs to be its own service for organizational reasons; they extract it with the contracts module already documenting the API surface. At every step, the code that didn't need to change didn't change.

---

## Part II — Audience and Non-Goals

### 2.1 Who This Is For

The realistic addressable market: **teams of 3-15 engineers building B2B SaaS in Python who are scaling a single application past one team's worth of code and want to delay microservices for as long as possible.** That's the segment where modupy is materially better than "FastAPI plus folders."

Specifically:
- Teams on Python 3.11+: events and boundary verification work in any Python application, and the process-per-module runtime builds FastAPI workers, so it needs FastAPI
- Teams with a Postgres, MySQL or SQLite database, async-friendly stack
- Teams that have started feeling pain from cross-cutting changes touching multiple folders
- Teams that have considered microservices and decided the operational cost isn't worth it yet
- Teams in DDD-adjacent territory who already think in bounded contexts

### 2.2 Who This Is Not For

Equally important to be honest about:
- **Solo developers on small apps** — you don't have the structural pain modupy solves
- **Teams already on microservices** — you've paid the cost; coming back to a monolith is rare
- **Teams using Django** — different conventions, different ORM, different ecosystem, and the process runtime builds FastAPI workers. A separate Django integration is listed under Phase 4 in [ROADMAP.md](ROADMAP.md); it does not exist yet
- **Sync-only codebases that won't go async** — the outbox runs on an async SQLAlchemy engine
- **Teams that need in-process parallelism** — modupy parallelises by giving each module its own process, not by running threads in one interpreter

### 2.3 The Adoption Calculus

For modupy to succeed:
- The benefit must clear the bar of "what FastAPI plus folders gives you for free"
- The migration cost on existing codebases must be small enough to try without a rewrite, which is what the ratcheting verifier and the audit tool are for ([§8.3](#83-ratcheting-mode), [§8.4](#84-the-audit-tool))
- The transactional outbox specifically must deliver at least once and recover from crashes, because it is the piece teams cannot easily build themselves

If those three things hold, modupy is genuinely useful. If any one fails, modupy is architecture for its own sake.

---

## Part III — Design Philosophy

### 3.1 Convention Over Configuration

Spring needs `@ApplicationModule` because Java packages are flat. Python already has the convention: top-level subpackages are modules, underscore-prefixed names are private. We use what's already there.

Every default decision has a corresponding escape hatch, **and the escape hatch is discoverable from the default behavior.** The startup banner shows active config and names the config key to change. Error messages name the config keys that would have prevented the error.

### 3.2 Defaults So Good You Don't Change Them

The defaults stack:
- **Discovery**: subpackages of the root package, ignoring underscore-prefixed
- **Event bus**: in-memory async bus, no broker required
- **Outbox**: disabled by default; turning it on takes several steps, listed in [§7.2](#72-sqlalchemy-integration)
- **Topology**: single-process; flip to processes with one flag
- **Broker**: in-memory for `single`; durable local `shm` for `processes` unless
  a configured URL/DSN selects `database`
- **Observability**: auto-enabled if OpenTelemetry is installed, silent no-op if not; `observability = false` turns it off, `true` requires OpenTelemetry
- **Verification**: warnings in dev, hard checks via `modulith verify` in CI
- **Logging**: standard library `logging`, inherits app's config

A first-time user with no `[tool.modulith]` section at all has a fully-working app.

### 3.3 Three Plugin Shapes, Three Mechanisms

Plugins fall into three shapes; forcing all three into one mechanism makes the wrong one awkward:

| Shape | Examples | Mechanism |
|---|---|---|
| **Driver** (one wins) | `PublicationStore`, `EventSerializer` | `typing.Protocol` + explicit wiring at startup |
| **Dispatch** (route by key) | Brokers (Kafka, SQS, RabbitMQ, …) | `BrokerRegistry` + scheme prefix |
| **Hook** (everyone runs) | Lifecycle, verification, observability | `pluggy` hookspecs |

### 3.4 The Architectural Test

> Built-in plugins are not privileged, just first-party.

Users must be able to disable any built-in feature and replace it with their own implementation using the same hookspec contract. The shipped mechanism is `configure(disable_plugins=["modulith.builtin.verifier", ...])` at application startup (or `create_plugin_manager(disable=[...])` when embedding) — there is no `[tool.modulith].disable` pyproject key; the configuration loader rejects unknown keys loudly. If this isn't true, we've built two-tier architecture (privileged core + second-class plugins) and people will route around it.

### 3.5 Async-Native, Sync-Compatible

Modern Python is async; SQLAlchemy 2.0 is async-first. The framework assumes `asyncio` internally. But we ship `publish_sync()` and accept sync `@listener` functions because most real Python apps are mixed sync/async, and forcing async-only refactors is hostile to adoption.

### 3.6 Honest About What We Don't Do

The single most important framing principle: **we don't lie about our limitations.** The "modupy now, microservices later" pitch has a hidden cliff (shared databases, shared transactions). We document the cliff, give users tools to measure their proximity to it, and let them make informed decisions. The first time we oversell and a user hits the cliff in production, we lose them and they tell their network. Honest framing is risk management.

---

## Part IV — The Plugin Contract

The plugin contract is the most stable part of modupy. Once published, every plugin ever written depends on it. Additions are fine; a signature change goes through the [deprecation policy](docs/STABILITY.md#deprecation-policy) before 1.0 and is a major-version event after it.

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

### 4.2 The Five Driver Protocols

Defined in `modulith/protocols.py`. Marked `runtime_checkable` for diagnostics; adapters use duck typing, no inheritance required.

**`PublicationStore`** — outbox storage:
```
async save(publication: EventPublication) -> None
async mark_complete(publication_id: UUID) -> None
async find_incomplete(older_than: timedelta) -> list[EventPublication]
async archive(publication_id: UUID) -> None
async delete(publication_id: UUID) -> None
```

`save` inserts or updates by id. It saves a new record inside the business transaction. The outbox calls it again, outside that transaction, to record a failed attempt (`attempt_count`, `last_error`, `last_attempt_at`) or to reopen a dead-lettered record. A re-save never reopens a completed record.

**`EventSerializer`** — event encoding:
```
serialize(event: Any) -> bytes
deserialize(data: bytes, event_type: str) -> Any
```

The configured serializer governs outbox **storage** only. Broker
**transport** is not pluggable: the wire format is fixed JSON
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

Fields decode by their annotations:

- A `NewType` and a non-generic `type` alias (a `TypeAliasType`, including
  chains of them) decode as the type they name.
- A parameterized generic alias such as `Pair[datetime]` is not unwrapped, and
  an alias whose value cannot be evaluated at runtime (a `TYPE_CHECKING`-only
  name) or that refers back to itself is left opaque: those fields pass
  through as their JSON values.
- A parameterized generic dataclass, such as `Box[int]` for a
  `@dataclass class Box(Generic[T])`, is not reconstructed, even from a value
  of exactly that class: the listener receives the plain `dict` of its stored
  fields (`{"v": 3}`), every value still in its JSON form (a `datetime`,
  `Decimal` or `UUID` is a string, a nested dataclass a `dict`). This holds
  wherever the hint appears, for example as a `list` element, a `dict` value
  or an `Optional` member. Annotate the field with a non-generic dataclass to
  receive an instance.
- A dataclass field declared `field(init=False)` is stored, and decode sets it
  on the instance after `__init__` ran, coerced by its annotation (assigned
  with `object.__setattr__`, so a frozen dataclass works too). An `InitVar`
  is not a field and its value is not stored: `serialize` raises `TypeError`
  naming the class and field for an `InitVar` without a default, so the
  failure surfaces at publish time rather than in the consuming process. An
  `InitVar` with a default is accepted and takes that default on decode. This
  applies to nested dataclasses too.
- Two field shapes carry a type tag,
  `{"__modulith_union_type__": "<module>.<qualname>", "value": ...}`: a
  multi-member union, and a nested dataclass holding an instance of a subclass
  of its declared class. A value of exactly the declared class stays untagged.
- A subclass tag is matched only against subclasses of the declared class
  already imported in the consuming process, never imported by name. A tag
  that matches no imported subclass decodes as the declared class when its
  fields fit that class, logging one WARNING per tag; import the module
  defining the subclass in the consumer (a process worker imports only the
  contracts package and its own module) to keep the subclass type. When the
  fields do not fit, decoding raises `TypeError`. A tag that more than one
  imported subclass carries raises `ValueError` naming the candidates.
  Serializing a value that is only a virtual subclass of the declared class
  (registered with an ABC, or accepted by an `__instancecheck__` override)
  raises `TypeError`, since no consumer could decode its tag.
- In a union field, a tag that matches no member and no member's imported
  subclass decodes against the union's single dataclass member the same way;
  with no dataclass member, or more than one, it raises `ValueError`. A tagged
  value in a union never reaches the listener as the raw tagged dict: its
  reconstruction errors propagate.

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

**`HealthAwareConsumer`** — optional capability layered on `Consumer`:
```
health() -> ConsumerHealth   # ready: bool, status, detail
```
A consumer that implements `health()` drives the worker's `/health` endpoint,
which answers 503 while the consumer reports not ready. For one that does not,
the endpoint answers 200 with `status: "unknown"` and a warning, so a readiness
probe cannot see that consumer's failures.

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
- **No event loop running on the calling thread** (a plain script, or a sync FastAPI view in Starlette's threadpool while the app's loop runs on another thread) → dispatches on modupy's own persistent daemon-thread loop and blocks until done. It never runs on the app's loop, so an async resource tied to the app's loop, such as an `AsyncEngine` the app also uses, raises "attached to a different loop"; recipe 4 in [docs/COOKBOOK.md](docs/COOKBOOK.md) covers the caveat
- **Called from inside a sync listener** → dispatches on a fresh short-lived thread with its own loop, so a nested publish does not compete for the executor that is running its caller

Sync listeners are accepted:
```python
@listener
def reserve_stock(event: OrderCreated) -> None:  # sync def, not async def
    ...
```
Sync listeners run in the event loop's executor; async listeners run directly.

The transactional outbox does not pick up a session on its own: a publish is recorded in it only inside a session bound with `bind_session()`, and dispatch happens after commit via SQLAlchemy's `after_commit` event. [§7.2](#72-sqlalchemy-integration) lists every step to turn the outbox on.

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
│   ├── __init__.py       # public API surface; re-exports `router`
│   ├── _manifest.py      # optional declarative manifest
│   ├── _internal/        # private — not importable by other modules
│   │   ├── persistence.py
│   │   └── domain.py
│   ├── handlers.py       # @listener functions
│   └── api.py            # defines the FastAPI `router`
└── main.py               # FastAPI app; includes each module's router
```

Discovery imports each module *package* (its `__init__.py`), not every submodule: `@listener` functions in `handlers.py` register only if the package imports them (`from . import handlers`) or a `_manifest.py` declares them.

A module with routes re-exports its `router` from `__init__.py`, because a module running in its own process serves exactly that `router` under `/<module>`. Nothing mounts `api.py` by itself: in a single process `main.py` includes each router.

Conventions enforced by the verifier:
- Only `myapp.orders.__init__` and explicitly-named submodules are importable from outside
- `myapp.orders._internal.*` is never importable from outside `myapp.orders.*`
- `myapp.orders` may not import from `myapp.inventory._internal.*`
- Cross-module imports of events go through `myapp.contracts.*`

---

## Part VI — Runtime Architecture

### 6.1 Lazy Bootstrap

The runtime does no work on `import modulith`. The first `publish()`, or an explicit `bootstrap()`, triggers bootstrap, which runs once. `@listener` registration only queues the listener until then, and `configure()` must run before bootstrap: afterwards it raises `ConfigurationError`.

Bootstrap sequence (in `Runtime._bootstrap()`):
1. Load configuration (pyproject + env + overrides)
2. Auto-detect application package if not configured
3. Create plugin manager, load built-ins and entry points, skipping plugins disabled with `configure(disable_plugins=[...])`
4. Create event bus (in-memory by default), kept local until bootstrap succeeds
5. Create the broker and consumer registries and run the `modulith_register_brokers` and `modulith_register_consumers` hooks
6. Run module discovery hook (default: walk subpackages) when `auto_discover` is on; importing the modules queues their `@listener` functions
7. Flush the queued listeners into the bus
8. Verify manifests when `verify_manifests` is on
9. Run the boundary scan when `strict_boundaries` is on, failing on any violation
10. Fire `modulith_after_module_load` for each module
11. Bind the outbox store built from `outbox_url`, when one is configured and `auto_discover` is on
12. Commit point: install the bus, registries and modules on the runtime, log the friendly startup banner, mark complete; subsequent calls take the fast path

### 6.2 The Singleton Pattern

A module-level `_runtime: Runtime` instance lives in `modulith/runtime.py`. Decorators and `publish()` reach through it. Double-checked locking guards bootstrap against concurrent first-uses. After bootstrap, the bootstrapped flag is read without locking.

**Critical correctness rule:** `register_listener()` gates on whether the event bus has been **published**, not on whether bootstrap is **complete**. During discovery, modules are imported, which fires their `@listener` decorators. The bus is built locally and published only at the commit point, so listeners registered until then queue and are flushed into it in registration order. Registration runs under the runtime lock: a registration racing the flush either arrives before it or waits until bootstrap finishes and registers directly, so no listener lands in a queue that was already flushed. A failed bootstrap leaves the runtime as it was before the attempt, so it can be retried. The queue keeps every listener except those of a module whose import failed, which the retry registers again when it re-imports the module.

### 6.3 Configuration Resolution

In `modulith/config.py`. Resolution order (highest priority first):

1. Explicit kwargs to `load_configuration()` / `configure()`
2. `MODULITH_*` environment variables
3. `[tool.modulith]` section in pyproject.toml
4. Hardcoded defaults

Every *scalar* `Configuration` field has a `MODULITH_<KEY>` env var equivalent: `MODULITH_PACKAGE`, `MODULITH_CONTRACTS_MODULE`, `MODULITH_OUTBOX`, `MODULITH_OUTBOX_URL`, `MODULITH_TOPOLOGY`, `MODULITH_BROKER`, `MODULITH_SUBSCRIPTION_SOURCE`, `MODULITH_ACTUATOR_MODE`, `MODULITH_WORKER_PORT_BASE` (an integer), `MODULITH_PRODUCTION`, `MODULITH_AUTO_DISCOVER`, `MODULITH_OBSERVABILITY`, `MODULITH_VERIFY_MANIFESTS`, `MODULITH_STRICT_BOUNDARIES`. Booleans accept `1`/`true`/`yes` and `0`/`false`/`no` (case-insensitive); any other non-empty value raises `ConfigurationError`. The dict-typed fields (`outbox_options`, `broker_options`, `workers`, `subscriptions`) have **no generic** env var — they come from the `[tool.modulith.*]` subtables in pyproject.toml, listed below. Adapter-specific env vars are separate contracts: SHM and database options use `MODULITH_BROKER_<KEY>`, Redis Streams reads `REDIS_URL`, `MODULITH_STREAM_PREFIX`, `MODULITH_STREAM_MAXLEN`, `MODULITH_BROKER_DLQ_MAX_STREAM_LEN`, and `MODULITH_BROKER_MAX_PAYLOAD_BYTES`, and the packaged alembic runner reads `MODULITH_DB_URL`.

Each subtable fills one `Configuration` field:

| Subtable | Field | Holds |
|---|---|---|
| `[tool.modulith.outbox_options]` | `outbox_options` | outbox tuning ([§7.3](#73-completion-modes)); the legacy spelling `[tool.modulith.outbox]` is rejected |
| `[tool.modulith.broker_options]`, or its alias `[tool.modulith.broker]` | `broker_options` | broker connection settings ([§10.2](#102-redis-streams-broker), [§10.2a](#102a-durable-local-shm-broker)); writing both spellings raises `ConfigurationError` |
| `[tool.modulith.workers]` | `workers` | worker count per module ([§9.5](#95-topology-configuration)) |
| `[tool.modulith.subscriptions]` | `subscriptions` | broker targets per module, read when `subscription_source = "config"` |
| `[tool.modulith.verify]` | `verify_disabled_rules` | only `disabled_rules` is read ([§8.3](#83-ratcheting-mode)); the other keys are ignored |

A subtable spelled close to a real one (`worker` for `workers`) raises with a suggestion, and any other unrecognised subtable is ignored.

Validation happens before construction. Unknown keys raise `ConfigurationError` with the list of valid keys (catches typos). Production mode + default memory outbox raises (forces explicit opt-in for unsafe defaults). Process topology defaults to local `shm`; an URL/DSN without an explicit broker selects `database`. Explicit `shm` accepts filesystem paths only and rejects DSNs and SQLAlchemy/network URLs. SQL schema names must be portable unquoted identifiers at every entry point: loaded configuration, broker environment overrides, direct `DatabaseBroker` construction, `MODULITH_DB_SCHEMA`, and Alembic `-x schema=...`.

The `explicit_keys: frozenset[str]` field tracks which values were set vs defaulted. Used by the production safety check.

### 6.4 Auto-Discovery

In `modulith/discovery.py`. Two strategies:

1. **Call-stack walking** (`_detect_from_caller_stack`): use `sys._getframe()` to walk back from the modupy bootstrap call. Skip frames inside the `modulith` package, the stdlib and `__main__`, but not site-packages: an installed application lives there. Return the top-level package of the first frame left. A third-party library that bootstraps modupy for the application can therefore be detected as the application, so installed deployments set the package explicitly.

2. **pyproject.toml** (`_detect_from_pyproject_name`): walk up from cwd looking for `pyproject.toml`, read `[project].name`, and replace hyphens with underscores — the conventional distribution-name → import-package mapping. (This is *not* PEP 503, which governs package-index name normalization and collapses hyphens/dots/underscores to `-`, the opposite direction.)

If both fail, raise `ConfigurationError` with all three escape hatches in the message.

### 6.5 The Friendly Banner

Logged at INFO via the `modulith` logger:

```
modulith: detected application package 'myapp'
modulith: discovered 3 module(s): orders, inventory, reports
modulith: outbox=memory, broker=memory, topology=single
modulith: outbox disabled — for durable delivery, set [tool.modulith].outbox = 'postgres' and outbox_url (or MODULITH_OUTBOX_URL), run `modulith migrate`, and wire the session and lifespan as modupy's README section "Never lose an event" shows
modulith: ready
```

(The contracts subpackage, when present, is discovered and listed as a module too. [§7.2](#72-sqlalchemy-integration) details each step the outbox line names.)

Five log lines that tell the user exactly what's active and how to change it. If they don't want any of it, the message tells them where to turn it off.

The banner goes through the standard `modulith` logger at INFO level — it inherits the application's logging config and is **not** printed directly. Python's root logger surfaces only WARNING+ by default (and uvicorn configures only its own loggers), so the hosting application must enable INFO logging — e.g. `logging.basicConfig(level=logging.INFO)` at startup — for the banner to appear.

---

## Part VII — The Transactional Outbox

This is the technically hardest piece and the one feature users can't easily build themselves. Spring Modulith's Event Publication Registry is the reference implementation; we provide its Python equivalent.

### 7.1 The Problem It Solves

Without an outbox: publish an event inside a DB transaction. Transaction commits. Process crashes before the event reaches its listener. Event is lost. Or: transaction rolls back, but the event was already dispatched. Inconsistent state.

With an outbox: publish writes to a database table in the same transaction as the business work. After commit, a dispatcher picks up the row and delivers to the listener. If the dispatcher crashes mid-delivery, the row stays incomplete, and the first sweep that runs after the dead process's lease expires or its advisory lock's session ends retries it.

![One commit saves the order and one event_publications row per listener; after the commit each listener runs in the background, and a failing one is retried, then dead-lettered](docs/images/outbox.svg)

**Guarantee: at-least-once delivery, transaction-aligned.** Listeners must be idempotent.

### 7.2 SQLAlchemy Integration

**Turning the outbox on.** The outbox is off by default, and `outbox = "postgres"` alone does not make events durable:

1. Install the extra for the store, `modupy[postgres]` (`modupy[database]` for MySQL or SQLite), and `modupy[cli]` for the `modulith` command.
2. Set `outbox = "postgres"` and `outbox_url` (or `MODULITH_OUTBOX_URL`) under `[tool.modulith]`.
3. Run `modulith migrate` to create the tables.
4. Publish inside a session bound with `bind_session`/`unbind_session`, both from `modulith.builtin.outbox`. A `publish()` outside a bound session is delivered directly and saves nothing.
5. In the app's lifespan, call `bootstrap()` and `outbox.start()` on startup and `await outbox.shutdown()` on exit.

[README.md](README.md#2-never-lose-an-event), "2. Never lose an event", shows each step in code, and recipe 6 in [docs/COOKBOOK.md](docs/COOKBOOK.md) covers the options.

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
- If a session is bound with `bind_session()`, the event is serialized and added as a record in the same session, queued in `session.info["_modulith_pending"]`.
- After commit, the queued records are dispatched.
- If commit fails (rollback), the session pops the pending list and nothing is dispatched.

When `publish()` is called outside a transaction context, the bus dispatches directly without persistence (the outbox doesn't apply). On this direct path, an event that routes to a cross-process broker is sent inline (awaited), and a broker publish failure **propagates to the caller** — fail-loud by design: with no outbox row persisted, a swallowed send would lose the event for every remote consumer with zero trace. Callers that need `publish()` decoupled from broker availability use the transactional outbox path (bind a session + durable store), where the send happens after commit with retry/dead-letter handling.

### 7.3 Completion Modes

Three modes, selected with `completion_mode`, either as the argument to `outbox.configure()` (`modulith/builtin/outbox.py`) when the application wires the store at startup, or in pyproject.toml when the runtime binds the store from `outbox_url`:

- **`update`** (default) — set `completed_at`. Old records remain for inspection until a maintenance job purges them.
- **`delete`** — remove the row on success. Lower overhead, no historical visibility.
- **`archive`** — copy to `event_publications_archive` table, delete from primary. Best for high-volume systems where you want history without performance impact.

In pyproject.toml, `[tool.modulith.outbox_options]` is the home for outbox tuning. It is `outbox_options` and not `outbox` because `outbox = "postgres"` already uses that key as a scalar, and TOML forbids one key being both a scalar and a table. A legacy `[tool.modulith.outbox]` subtable is rejected with a `ConfigurationError` pointing at the correct spelling, and so is a pyproject.toml that fails to parse.

When the runtime binds the store from `outbox_url`, it validates these keys and passes them to `outbox.configure()`:

| Key | Default | Meaning |
|---|---|---|
| `completion_mode` | `"update"` | `"update"`, `"delete"` or `"archive"`, as above |
| `claim_strategy` | `"lease"` | how concurrent sweepers coordinate: `"lease"`, `"advisory_lock"` (Postgres only) or `"none"` ([§7.4](#74-the-retry-loop)) |
| `claim_lease_seconds` | 60 | how long a claimed batch belongs to one sweeper; at most 86400 (one day) |
| `claim_batch_size` | 100 | records claimed per sweep |
| `dead_letter_after_attempts` | store's own setting, else 10 | attempts before a record is dead-lettered |
| `retry_interval_seconds` | 30 | pause between sweeps |
| `retry_stale_seconds` | 30 | minimum age of a record before the retry loop picks it up |
| `max_retry_backoff_seconds` | 300 | cap on the backoff between attempts at one record |
| `sqlite_wal` | unset | `true` puts a SQLite `outbox_url` database in WAL journal mode; any other database ignores it |

Any other key in the table is accepted and ignored. An application that binds its own store passes the same settings to `outbox.configure(...)` as keyword arguments.

### 7.4 The Retry Loop

A background task started by the outbox plugin sweeps for incomplete publications:

- On startup: one sweep with `older_than=timedelta(0)` over the records a previous process left incomplete. A record the dead process still holds, under a lease or an advisory lock, waits: the first sweep that runs after the lease expires or the lock's session ends retries it. Recovery is immediate, with no grace period for fresh records: under `lease`, a record published just before the sweep claims its batch is delivered behind the older records of that batch, because its own after-commit delivery finds it claimed and leaves it to the sweep. The cost is delay, not loss
- During normal operation: a sweep every `retry_interval_seconds` (30 by default) over records at least `retry_stale_seconds` (30 by default) old, to avoid thrashing fresh events

How a sweep finds its records depends on `claim_strategy`:

- **`lease`** (default) — the store claims up to `claim_batch_size` records with `claim_batch`, marking each with the claimant and a lease of `claim_lease_seconds`. The sweeper renews the lease while it dispatches and fences its completion write with the claim token, so a sweeper whose lease has lapsed cannot overwrite a newer claimant's work.
- **`advisory_lock`** — the sweeper reads candidates with `find_incomplete(older_than=...)` and holds a Postgres advisory lock for each dispatch.
- **`none`** — the sweeper reads candidates with `find_incomplete(older_than=...)` and dispatches them with no coordination. Two sweepers can dispatch the same record, which is why listeners must be idempotent; `outbox.configure()` logs a warning when it is chosen.

A store without the matching capability falls back to the `find_incomplete` path.

Failed dispatches stay incomplete with `attempt_count` incremented and `last_error` set. Retries use exponential backoff capped at the configured max (`max_retry_backoff_seconds`, default 5 minutes). Once `attempt_count` reaches `dead_letter_after_attempts` (unset by default, which resolves to the store's own setting if it has one, else 10), the record is moved to a dead-letter status (column flag). Rows already at or over a lowered value are dead-lettered at the next sweep. Dead letters are listed by `modulith outbox dead-letter` and `modulith outbox failing`, and `modulith doctor` counts them. The actuator routes (`/_modulith/topology`, `/_modulith/live`, `/_modulith/health`) do not report them.

### 7.5 Maintenance Operations

Exposed via the CLI and as plugin-callable APIs:

- `modulith outbox status` — counts of incomplete, completed, dead-lettered
- `modulith outbox retry <id>` — force retry of a specific publication
- `modulith outbox purge --older-than=30d` — clean up completed records
- `modulith outbox dead-letter` — list dead-lettered events for manual inspection; `--retry-all` resubmits them
- `modulith outbox failing` — list publications that are failing but not yet dead-lettered, with attempts, last error and next retry time

---

## Part VIII — Boundary Verification

### 8.1 The Default Rules

The built-in verifier in `modulith/builtin/verifier.py` ships these rules:

1. **No cross-module internal imports** — `myapp.orders` cannot import from `myapp.inventory._internal.*`
2. **No cyclic dependencies** — the module dependency graph must be a DAG
3. **Declared dependencies match observed** — if a manifest declares `declared_dependencies=["payments"]`, only those modules may be imported (when manifest is present)
4. **Events flow through contracts module** — cross-module type imports must come from `myapp.contracts.*`, not from another module's package
5. **Module data ownership** — when manifests declare `owns_tables=[...]`, a `Table(...)` definition, a `__tablename__` assignment or a `ForeignKey("table.col")` string literal that names a table another module owns is flagged, as is a table that two manifests both claim; a module with a non-empty `owns_tables` also gets a warning for any table it defines but omits from that list, so the manifest stays a complete inventory. The check is best-effort and reports warnings only ([§8.2](#82-the-ast-based-verifier))
6. **Contracts is a sink** — every module may import from the contracts module, and the contracts module may not import any application module

![payments may import the public API of orders and the events in contracts, but modulith verify refuses an import of a private name such as _orders](docs/images/boundaries.svg)

### 8.2 The AST-Based Verifier

Implementation: walk every `.py` file under the application package with `ast.parse()`. Collect all `Import` and `ImportFrom` nodes. For each, check:

- Is the source inside an application module?
- Is the target inside a different module's `_internal` package?
- Is the target a different module's package (not contracts)?

Emit `Violation` for each rule failure. The verifier hookspec is aggregating, so multiple plugins (built-in + custom) all contribute.

The data-ownership rule does not analyse queries. It collects the table names written as string literals in `Table(...)` calls, `__tablename__` assignments and `ForeignKey(...)` or `ForeignKeyConstraint(...)` arguments, and compares them with the manifests' `owns_tables`. It is best-effort and reports warnings, not errors: a table named in raw SQL or computed at runtime is not seen, and nothing checks the SQL a module runs. `modulith verify --fail-on-warnings` makes the warnings fail the build.

### 8.3 Ratcheting Mode

For brownfield adoption, run:

```bash
modulith verify --mode=ratchet --baseline=.modulith-baseline.json
```

`[tool.modulith.verify]` is otherwise reserved for future config-backed defaults;
use the CLI flags above today. Its one live key is `disabled_rules`, a list of
rule names that turns those rules off by name, plugin-contributed rules
included, everywhere verification runs (`modulith verify`, `modulith doctor` and
the `strict_boundaries` startup check). `parse-error` cannot be disabled.

The baseline file records existing violations. The verifier:
- Passes any violation listed in the baseline (grandfathered)
- Fails any new violation
- `modulith verify --update-baseline` regenerates the file after refactoring

Same pattern as `mypy --strict` rolling out gradually. The baseline diff in git review shows what got fixed and what got worse. This is the single biggest adoption lever — without it, modupy is "for new projects only."

### 8.4 The Audit Tool

`modulith audit` analyzes an existing codebase non-destructively:

- Proposed module structure based on folder layout: each top-level subdirectory of the audited root is a module candidate. At a project root whose only application directory is one package, or `src/` holding one package, the audited root is that package; tests, docs, scripts, examples, migrations, virtualenvs, hidden and build directories are ignored when deciding. The command prints the root it chose.
- List of cross-module imports that would become violations
- List of shared database tables that need ownership decisions
- modupy-readiness score (0-100): percentage of cross-module interactions that go through events vs direct calls. With fewer than two module candidates the score is reported as not applicable, with a warning. It is also not applicable, with a warning naming the packages, when no import crosses candidates but some imports name packages below the audited folder that are not module candidates.

Output is Markdown, written to `MIGRATION.md` or the file `--output` names. An existing output file is never replaced silently: the command exits 1 naming the file and leaves it unchanged unless `--force` is passed. Teams can run it on Friday afternoon, generate a baseline, have green CI on Monday, then tighten over weeks; [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) walks through the whole path.

### 8.5 The Doctor Command

`modulith doctor` reports operational and architectural health:

- **Boundary health**: violation count, and drift from the ratchet baseline file that `--baseline=PATH` names (default `.modulith-baseline.json`, the same option and default as `modulith verify`): violations the baseline lists only warn, a violation it does not list is an error, and a missing file counts as an empty baseline
- **Process-split readiness**: percentage of cross-module interactions that are events vs direct calls (the "are you ready to split this module?" metric), plus per-module counts of cross-module table references and of tables not prefixed with the module's name — table-only coupling reports a warning even when there are no import/event interactions, and the "microservice-ready" tier requires zero cross-module table references
- **Schema drift**: events whose field definitions (name, annotation, default — fingerprinted via AST) changed since the last doctor run. The check is an unconditional fingerprint diff against a cache file (`.modulith-schemas.json`): it flags *every* definition change as the cue to version consciously — it does not read or compare any `schema_version` attribute
- **Outbox health**: incomplete, completed, and dead-lettered counts
- **Listener registration coverage**: declared listeners vs actually-registered listeners
- **SHM notifier**: whether each SHM broker's hint ring actually attached (a `shm_capacity` change on an existing hint file leaves the notifier dead — delivery still works, only slower)
- **Actuator token**: under `topology = "processes"`, whether the actuator would start unmounted (`auto` mode, no `MODULITH_ACTUATOR_TOKEN`, non-loopback bind) or refuse to start (`token` mode, no token, reported as an error)
- **Single-host broker**: a per-host broker (`shm`, or `database` on embedded SQLite) configured under a detected container runtime — a warning under Docker, an error under Kubernetes when `production = true`
- **Redis retention**: a `redis-streams` `max_stream_len` below the safe minimum, tightened by a live pending+lag backlog query when a client is reachable

---

## Part IX — Process-Per-Module Runtime

The feature that makes "modupy now, microservices later" credible.

![modulith run starts a main process holding the proxy on port 8000 and the supervisor, plus one worker process per module, connected by the built-in SHM broker](docs/images/processes.svg)

### 9.1 The Topology Decision

Three options, ranked by setup ease:

- **A. One process per module, local broker for IPC** ✅ **chosen.** Each module runs as its own uvicorn worker. Communication defaults to the durable local SHM/SQLite broker; configured URL/DSN options select the database broker, while Redis Streams remains explicit. Reuses outbox + externalization machinery. Latency is workload- and host-dependent and must be measured, not assumed.
- **B. Unix domain sockets** — lower latency, no broker dependency, but you lose durability without keeping Postgres in the loop. Net complexity gain is small.
- **C. Subinterpreters (PEP 734, Python 3.14)** — a separate GIL per module inside one process, no IPC. Ecosystem support is still thin. Worth designing toward; not worth shipping on. Free-threaded Python builds, which remove the GIL altogether, are a separate feature and a separate condition.

### 9.2 The Worker Pattern

`modulith/_worker.py` is invoked by uvicorn:

```bash
MODULITH_MODULE=orders MODULITH_APP_PACKAGE=myapp \
    uvicorn modulith._worker:create_app --factory --host 127.0.0.1 --port 9001
```

The supervisor ([§9.3](#93-the-supervisor)) sets both variables for every worker it starts; set them by hand only to run one worker on its own.

`create_app()`:
- Reads `MODULITH_MODULE` and `MODULITH_APP_PACKAGE` from env, and raises if either is missing
- Imports that module's package. Sibling module packages it imports are
  loaded too, but a listener runs only in the worker of the module whose
  import registered it, including a module package an entry-point plugin
  imports during bootstrap. Two shapes are not owned by a single module:
  - Listeners registered outside any module import (plugins, hooks) are
    untagged. They are local in every worker, so a non-externalized event
    they handle is never routed to the broker, and a listener for that
    event in another worker does not receive it.
  - A listener in a plain, non-package file (`app/shared.py`) or in a
    namespace folder without `__init__.py` (`app/common/audit.py`) belongs
    to the module package whose import first loads that file in each
    process: the innermost module package on the import stack at that
    moment. Each worker decides this on its own. If two modules each import
    the file directly, both workers own it, so an event delivered through
    the broker runs it once in each. A worker whose own module imports the
    file after a sibling's import loaded it (`from app import orders` first)
    does not run it. If the application package's own `__init__.py` or the
    contracts package loads the file first, no module owns it: it is
    untagged like a plugin listener, runs in every worker, and the
    non-externalized events it handles are never routed. To run such a
    listener in exactly one module's worker, define it inside that module
    package.

  Keep listeners inside module packages, and mark an event `@externalized`
  when modules in other workers handle it
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
explicit targets). So does a target from the `modulith_resolve_event_target`
hook whose scheme or destination is empty.

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
modulith run app.main:app --topology=processes
modulith run app.main:app --topology=processes --workers='{"reports": 4, "default": 1}'
modulith dev app.main:app --isolate=reports  # only reports runs; every other module is not started (its routes 404 through the proxy)
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
queries each worker's `/health` endpoint on demand. WebSocket requests are not
proxied: the proxy speaks HTTP only.

### 9.5 Topology Configuration

```toml
[tool.modulith]
topology = "processes"       # or "single" ("subinterpreters" is reserved, not yet implemented)
# broker omitted             # defaults to local durable "shm"

[tool.modulith.workers]
default = 1
reports = 4
```

There is no `[tool.modulith.supervisor]` subtable; the subtables configuration resolution reads are listed in [§6.3](#63-configuration-resolution). The supervisor's restart policy is built in, not configurable via pyproject (`modulith/supervisor.py`): per-instance exponential backoff starting at 1s, doubling to a 60s cap, with a crash-loop circuit breaker that stops respawning an instance after more than 5 crashes in a row with no healthy run in between (the spacing between crashes is irrelevant — a module crashing every few minutes trips it just the same), and a backoff reset once an instance has stayed up past the healthy-uptime threshold (defaults to the 60s cap), which also clears the crash streak. Crash detection is process-exit-based — the supervisor awaits each worker process; there is no periodic health-check polling.

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

The alembic migrations are the source of truth for the schema, including the
claim columns and sweep indexes that lease claiming needs; the integration
suite asserts that the ORM metadata and the migrations at `head` are
identical against a real Postgres. Do not recreate the tables by hand. Apply
the migrations with `modulith migrate`, which reads only configuration, so it
runs before any table exists. Without `--url` it migrates `outbox_url` (or
`MODULITH_OUTBOX_URL`), swapping the async driver for the sync one Alembic
connects with; `--schema` selects a Postgres schema:

```bash
modulith migrate --url 'postgresql+psycopg://user:pass@localhost/mydb'
```

The chain tracks its revision in `modulith_alembic_version`, not Alembic's
default `alembic_version`, so it runs in a database that has its own Alembic
history.

### 10.2 Redis Streams Broker

Extra: `modupy[redis]` (`modulith/adapters/redis_broker.py`). Implements `Broker` against `redis.asyncio`. It is an explicit networked choice for process-per-module deployments.

Select the broker by name, and supply connection options under the
`[tool.modulith.broker]` subtable (an alias of `[tool.modulith.broker_options]`,
see [§6.3](#63-configuration-resolution)). TOML forbids one key (`broker`) being both a
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
```

Each option also has an environment variable that takes precedence at deploy
time: `REDIS_URL`, `MODULITH_STREAM_PREFIX`, `MODULITH_STREAM_MAXLEN`. An empty
or whitespace-only value counts as unset, so the TOML option or the default
applies. (Values are literal — there is no `${VAR}` interpolation inside the
TOML.)

Retention caveat: `max_stream_len` / `MODULITH_STREAM_MAXLEN` is enforced via
`XADD MAXLEN ~`, which trims by stream length alone and is blind to
consumer-group pending state — an undersized cap lets a publish burst silently
trim entries that were delivered but never ACK'd (permanently losing them
despite the XAUTOCLAIM recovery path) or never delivered at all. Only a trimmed
pending entry is reported: the consumer logs it at ERROR level (via
XAUTOCLAIM's deleted-ids element on Redis 7; on Redis 6.2, which answers with a
nil row and no id, it finds the entry through the pending list, confirms with
XRANGE that it is gone, and acknowledges it before logging). An entry trimmed
before a consumer group read it is not in that group's pending list, so its
loss is not reported.
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

Publications are retained for `orphan_retention_seconds` (default one hour),
even after every group has acknowledged them. A consumer group that subscribes
after publication receives one replay before expiry while the store has room
below its publish budget (see `max_store_bytes` below), preventing silent loss
during worker startup. Delivery is at-least-once: a crash after listener completion
but before the fenced acknowledgement commits can cause a duplicate.

Canonical `state_dir`, `sqlite_path`, and `hint_path` resolve to absolute,
package-namespaced paths under a private per-user directory (`0700` directories
and `0600` files on POSIX, owned by the effective user; an ancestor directory writable by other users without the sticky bit is rejected; a group-writable ancestor passes only when its group is the effective user's own and that user or root owns it). Explicit SHM rejects DSNs and SQLAlchemy/network
URLs. WAL with `synchronous=NORMAL` survives application/process restart on the
same disk; `FULL` is the explicit opt-in for OS-failure and power-loss
durability.

Two byte limits bound the authoritative store. Each has an environment
override, `MODULITH_BROKER_MAX_PAYLOAD_BYTES` and `MODULITH_BROKER_MAX_STORE_BYTES`:

- **`max_payload_bytes`** defaults to 16 MiB and cannot exceed 1,000,000,000
  bytes, the largest blob SQLite stores. Payload validation occurs before the
  publish transaction. `load_configuration` rejects a bool, a float, a value
  below 1 or a value above that limit at start-up; a blank value counts as
  unset.
- **`max_store_bytes`** defaults to 1 GiB and cannot exceed 1 TiB. It is
  translated to SQLite `max_page_count` on the `sqlite_path` store
  (`.modulith-shm-broker.db` by default; the `-wal` file is not counted). It bounds what publishes add: a publish that would leave
  `page_count - freelist_count` above the configured page count minus a
  consumer reserve (32 pages, or one eighth of the pages below 256 pages)
  rolls back and is rejected as backpressure.

Consumers are not held to that budget:

- Consumer writes (claims, renewals, claim releases, acks, fails, dead-letters,
  dead-letter retries, prunes, heartbeat touches and group drops) and
  recording a subscription are never refused this way. One that hits
  `max_page_count` is retried with the limit lifted, so consumers drain a
  backlog that filled the store, including a store opened above its limit, a
  group always subscribes, and the file can grow past `max_store_bytes` while
  they do. If SQLite does not raise the limit, the write fails with the
  store-full error and a note naming the SQLite version.
- A subscribe replay is bounded instead. It replays the retained publications
  the group lacks (no delivery or completion tombstone), oldest first, and
  stops 8 pages below the publish budget, so a small publish that fit before
  the replay still fits right after it. Under `completion_mode="mark"` it uses
  at most half the room left, which covers claiming and acking its own rows
  when one group drains them and its listeners succeed.
- Draining can still take the store past the budget, as any consumer write
  can: failed and dead-lettered rows keep their error text, and under
  `completion_mode="mark"` claiming and acking grows every row, including
  rows other groups replayed. Publishes are then refused until prune frees
  pages or `max_store_bytes` is raised.
- A replay cut short logs one WARNING naming the group, the target and the
  replayed and skipped counts; the target then counts as subscribed, so the
  skipped publications reach that group only through another replay. The
  WARNING's recovery drains the group's backlog on the target before
  `modulith broker drop-group --target`, which deletes the group's pending and
  claimed deliveries there, replayed ones included.

### 10.3 Kafka Broker (planned — not shipped)

**Roadmap item (Phase 4), not a shipped adapter.** Will implement `Broker` against `aiokafka` for teams already running Kafka. No `kafka_broker.py` exists and there is intentionally no `kafka` extra in pyproject.toml — we don't advertise a dependency for a feature that does not exist.

### 10.4 OpenTelemetry Observability

Extra: `modupy[otel]` (built-in plugin `modulith/builtin/observability.py`; a silent no-op when OTel isn't installed, or installed without a configured tracer provider). `Configuration.observability` switches it: `None` (default) auto-detects as above; `False` skips loading the plugin, so no spans are created; `True` makes bootstrap raise `ConfigurationError` (naming `pip install 'modupy[otel]'`) when OTel is not importable. Auto-instrumentation emits two span types via the paired event-lifecycle hooks:

- `modulith.event.publish` — one per publication, attributes `event.type`, `event.module`, `modulith.duration_ms`. On the durable (outbox) path the span brackets the persistence step; a persist/serialize/broker-route failure still ends the span, with the exception recorded and status ERROR (the span never leaks). The plugin catches `Exception` around its OTel calls in the publish hooks and logs a WARNING (`modulith.observability`), so a raising span processor or sampler never fails `publish()`.
- `modulith.event.dispatch` — one per listener invocation, attributes `event.type`, `listener.name`, `publication.id`. `listener.name` is the outbox's stored listener id on every delivery path (in-memory, outbox, broker): `module.qualname`, with an `owner:` prefix for a bound method or callable instance. Status ERROR (with recorded exception) when the listener raises. Parenting depends on the delivery path:
  - **In-memory path** — the dispatch span is a child of the live publish span.
  - **Inside the listener** — on every path, `modulith_on_listener_dispatch` makes the dispatch span the current OTel span (`opentelemetry.context.attach`, token kept in a ContextVar) and `modulith_on_listener_complete` detaches it, also when the listener raised. A span the listener starts is a child of the dispatch span.
  - **Durable (outbox) path** — dispatch runs after the business transaction commits, or on a retry, in a different context. Each outbox row therefore stores the W3C trace context (`traceparent`, plus `tracestate` when set) of the publish span that created it, in `EventPublication.trace_context`, and the dispatch span of every such delivery, after commit and on retry, is a child of that publish span in the same trace. A row without a stored context (tracing was off, or a malformed value) gives a dispatch span with no parent; correlate those via `publication.id`.
  - **Broker hop** — every message carries the publish span's context in `traceparent` and `tracestate` headers (`tracestate` only when non-empty): the outbox broker-route sender takes them from the row's stored `trace_context`, so a retried send repeats identical headers, and the inline route takes them from the live publish span. A consumer passes the received headers to `Runtime.dispatch_local`, which sets them as the `trace_context` of the `EventPublication` its dispatch hooks receive, so the consumer's dispatch spans are children of the producer's publish span in the same trace. A message without usable headers gives a dispatch span with no parent, as does a third-party broker adapter that drops headers.

### 10.5 Documentation Generator

`modulith/builtin/docs.py`. Generates:

- `docs/modulith/architecture.mmd` — Mermaid flowchart of modules and their dependencies
- `docs/modulith/modules/<name>.md` — Application Module Canvas (public API, events published, events consumed, dependencies, owned tables, internal files)
- `docs/modulith/events.mmd` — Sequence diagram of event flows

A module with a `_manifest.py` gets the canvas's events, dependencies and owned tables from it. Without one, the canvas reads the source instead: the module's `@event` classes are listed under `## Events Published`, the event type annotated on the first parameter of each `@listener` under `## Events Consumed`, and dependencies and owned tables are left out. The configured contracts module (`[tool.modulith].contracts_module`) defines the shared event types and publishes none itself, so without a manifest its `@event` classes are listed under `## Events Defined` instead.

The default output directory is `docs/modulith`; `modulith docs --output-dir=DIR` redirects it.

Mermaid over PlantUML because it renders natively on GitHub/GitLab. Canvas is markdown so it diffs cleanly in PRs.

---

## Part XI — Testing

### 11.1 The pytest Plugin

Ships bundled in the main distribution as `modulith/testing.py`, installed via the `modupy[test]` extra (registered under pytest's `pytest11` entry point, so the fixtures are available automatically). A standalone `pytest-modupy` package is a planned later split, not current reality. Provides:

```python
# Per-test isolation (opt-in: request the fixture by name)
def test_orders_publishes_correctly(modulith_app):
    # Fresh runtime singleton; modules first imported here are dropped on teardown
    from myapp.orders import create_order
    asyncio.run(create_order("123"))
    assert modulith_app.published_events_of_type(OrderCreated) == [...]

# Module-isolated tests
def test_orders_in_isolation(modulith_module):
    with modulith_module("myapp.orders", mock_modules=["myapp.inventory", "myapp.payments"]):
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

Module isolation means a test runs against one module while the rest of the application is stubbed out, and the import state it changed is reset afterwards. The fixtures below do this in the test's own process; for the strictest isolation a test runs in a child process of its own ([§11.4](#114-subprocess-per-test-mode)).

For non-isolated tests, the fixtures handle state reset, but only for the tests that request them. `modulith_app` resets the runtime singleton before and after the test and, on teardown, drops every module of the application package first imported during it: the package the test configured or bootstrapped, or else the one `[tool.modulith] package`, `MODULITH_PACKAGE` or `[project] name` names. Third-party, stdlib and `modulith` modules stay loaded. `modulith_module` removes the application package's modules from `sys.modules` for the duration of the `with` block, installs `MagicMock` stand-ins for `mock_modules`, and restores `sys.modules` and the manifest registry on exit. Names are dotted module paths: `"myapp.orders"`, not `"orders"`.

### 11.4 Subprocess-Per-Test Mode

Activated via `@pytest.mark.modulith_isolated`. The plugin re-invokes pytest on that single test in a child process (an env-var guard prevents recursion) and synthesizes the test report from the child's exit code and the outcome the child records: a skipped or xfailed child stays skipped or xfailed, and a child that runs no test fails. Under pytest-rerunfailures the retries run inside the child, and the child's last attempt decides the outcome. A test in its own process shares no in-process state with the others (the runtime singleton, `sys.modules`, the manifest registry); state outside the interpreter, such as a database, files and ports, is still shared. Cost: a full interpreter + pytest startup per test — fine for integration tests, not for unit tests on save.

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
modulith dev APP_MODULE [--topology=single|processes] [--isolate=MODULE] [--reload/--no-reload] [--host=HOST] [--port=PORT] [--log-level=LEVEL] [--worker-port-base=PORT]
modulith run APP_MODULE [--topology=single|processes] [--workers=JSON] [--host=HOST] [--port=PORT] [--log-level=LEVEL] [--worker-port-base=PORT]
modulith verify [--mode=strict|ratchet] [--baseline=PATH] [--update-baseline] [--fail-on-warnings]
modulith docs [--output-dir=DIR]
modulith audit [PATH] [--output=FILE] [--force]
modulith extract MODULE [--output=DIR] [--force]
modulith k8s-manifest [--output=FILE] [--image=IMAGE] [--namespace=NAME] [--port=PORT] [--host=HOST]
modulith openapi [--output=FILE] [--title=TITLE] [--api-version=VERSION]
modulith doctor [--baseline=PATH]
modulith migrate [REVISION] [--url=URL] [--schema=SCHEMA]
modulith outbox status
modulith outbox retry <id>
modulith outbox purge --older-than=DURATION
modulith outbox dead-letter [--list|--retry-all]
modulith outbox failing
modulith broker dead-letter [--list|--retry-all]
modulith broker drop-group GROUP [--target=TARGET] [--force] [--yes]
modulith info  # show detected config, modules, plugins
```

`dev` and `run` take a required positional `APP_MODULE` (the ASGI app, e.g. `myapp.main:app`); `audit` takes an optional positional `PATH` (the codebase root, default `.`; a project root resolves to its single application package, see [§8.4](#84-the-audit-tool)).

`migrate` applies the packaged outbox and broker migrations and `REVISION` defaults to `head` ([§10.1](#101-postgres-outbox-store)). `outbox failing` lists publications that are failing but not yet dead-lettered ([§7.5](#75-maintenance-operations)). `broker dead-letter` lists dead-lettered broker deliveries, or resubmits them with `--retry-all`, on the database, shm and redis-streams brokers. `broker drop-group` removes a retired module's consumer group, with its subscriptions and undelivered work, from the shm or database broker; `--target` limits it to one target (repeatable), it refuses a group a current module or a recent consumer still uses unless `--force`, and it asks for confirmation unless `--yes`, checking again after the answer that the group did not become live while the prompt waited.

`extract`, `k8s-manifest`, and `openapi` bootstrap and import configured
application modules to derive artifacts; they are build-time tools for trusted
source. Extraction writes a wheel-buildable project through a staging
directory and rejects output symlinks, output inside the source package,
non-empty targets, and any symlink in the copied source. Kubernetes
names are RFC-1123 labels with stable hashes for long inputs, ports must be
1–65535, only supported broker environment contracts are emitted, and the
contracts module is passed explicitly. OpenAPI generation requires the
`fastapi` extra and rejects incompatible collisions or duplicate operation IDs
instead of silently discarding definitions.

### Exit codes

Uniform across every command:

- **0** — success. Warnings may still have been reported (verify's WARNING-severity violations without `--fail-on-warnings`, `dev`'s startup boundary warnings, doctor's warn-tier checks).
- **1** — violations or user error *within a recognized command line*: failed verification, invalid flag **values**/arguments (typo'd `--mode`/`--topology` values are rejected loudly, never silently defaulted), configuration errors, unknown ids, missing uvicorn, unwritable `--baseline` paths.
- **2** — unexpected internal errors (a modupy bug; traceback printed to stderr) **and CLI usage errors** (a missing required argument, an unknown option): the CLI is built on click, whose convention exits 2 for usage errors — modupy follows it rather than fighting the framework.

`modulith verify` exits 0 when no ERROR-severity violations are reported (strict) or none are new relative to the baseline (ratchet); `--fail-on-warnings` opts in to failing on WARNING-severity findings too. `modulith doctor` exits 1 only when a check reports an error, so both drop into CI as a single line.

### `modulith dev` semantics

`modulith dev` is *almost* `uvicorn --reload` with quality-of-life additions:
- Prints the discovered module list at startup
- Runs the boundary verifier at startup and echoes violations as **non-fatal warnings** on stderr — the "warnings in dev, hard checks via `modulith verify` in CI" promise of [§3.2](#32-defaults-so-good-you-dont-change-them). A dev server must start even when the project is half-configured, so a failing check downgrades to a note; it never blocks the launch
- Shows a friendly banner with topology and detected adapters

**It is not a different way to run the app; it's a nicer way.** Users with muscle memory for `uvicorn` keep using `uvicorn`. The CLI is a progressive enhancement.

### `modulith run` semantics

`modulith run` is `modulith dev` minus reload, plus production-mode toggles. In `--topology=processes`, it spawns the supervisor. Designed to be the actual production entrypoint for users who want modupy to manage their topology, but optional — running each worker as a vanilla uvicorn process is also supported.

---

## Part XIII — Migration Strategy

### 13.1 Greenfield Adoption

```bash
# 1. Install
uv add modupy

# 2. Define modules as subpackages (zero config). Each needs an __init__.py:
#    a directory without one is not a package, and discovery skips it.
mkdir -p myapp/contracts myapp/orders myapp/inventory
touch myapp/__init__.py myapp/contracts/__init__.py myapp/orders/__init__.py myapp/inventory/__init__.py

# 3. Add a first event, listener and publish (below), then run
#    (the FastAPI app object lives in myapp/main.py)
uvicorn myapp.main:app --reload
```

```python
# myapp/contracts/__init__.py
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str


# myapp/inventory/__init__.py
from modulith import listener

from myapp.contracts import OrderCreated


@listener
async def reserve_stock(event: OrderCreated) -> None:
    print(f"reserving stock for {event.order_id}")


# myapp/orders/__init__.py
from modulith import publish

from myapp.contracts import OrderCreated


async def create_order(order_id: str) -> None:
    await publish(OrderCreated(order_id=order_id))
```

The first `publish()` bootstraps modupy ([§6.1](#61-lazy-bootstrap)): it discovers `contracts`, `inventory` and `orders`, registers `reserve_stock`, and calls it. Neither module imports the other, only the event in `contracts`.

### 13.2 Brownfield Adoption (the path that matters)

```bash
# 1. Install (the CLI needs the cli extra)
uv add 'modupy[cli]'

# 2. Audit existing structure (writes MIGRATION.md by default; --output to change;
#    --force to replace a file that already exists.
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

[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) has the full seven steps. In three phases:

1. **Install + audit + ratchet** (guide Steps 1–3). Boundaries enforced going forward; existing violations grandfathered, as in [§13.2](#132-brownfield-adoption-the-path-that-matters).
2. **Add events incrementally** (guide Step 4). Pick one cross-module call at a time; replace direct call with `publish` + `@listener`. Each migration is a single PR.
3. **Enable outbox** (guide Step 5). The steps are in [§7.2](#72-sqlalchemy-integration); verify outbox readiness with `modulith doctor`. Delivery is at least once, so listeners must be safe to run twice.

Process-per-module (guide Step 6) and true microservices extraction (guide Step 7) are optional later moves justified by data, not architecture.

---

## Part XIV — The Seven Gap Mitigations

Seven weak points in the design, each with a concrete mitigation.

### Gap 1: Cross-module event imports leak the source module

**Mitigation:** the contracts module pattern ([§5.3](#53-the-contracts-module-pattern)). Events live in `myapp.contracts.*`, a sink in the dependency graph. Verifier treats it specially. For distributed deployments, `contracts` becomes versioned (a dedicated `schema_version` broker header is planned — see [§5.3](#53-the-contracts-module-pattern) for what ships today).

### Gap 2: The "modupy now, microservices later" promise has a hidden cliff

**Mitigation:** module-level data ownership rules ([§8.1](#81-the-default-rules)) + `modulith doctor` ([§8.5](#85-the-doctor-command)). Users see their split-readiness as a number. We document the cliff explicitly: "no rewrites for the messaging layer; database boundaries are a separate decision." The tooling covers part of the data half:

- `doctor`'s process-split readiness check counts cross-module table references (not just imports) and reports tables not prefixed with their owning module's name
- the verifier's `data-ownership` rule detects `ForeignKey("table.col")` string literals pointing at another module's table, not just `Table()`/`__tablename__` declarations
- a per-module Postgres schema setting (`broker_options.schema`/`MODULITH_BROKER_SCHEMA` for the broker, `modulith migrate --schema`/`MODULITH_DB_SCHEMA` for migrations) gives modules physically separate storage
- `modulith extract` refuses (without `--force`) to scaffold a module that still shares a table with another module

What remains manual: actually moving a shared table's data to its owning module, and choosing the schema-vs-prefix convention per table. The tooling detects and reports the coupling; it does not resolve it.

Enabling a named migration schema does not move existing data. If the target
has no Alembic history while `public` contains modupy tables or history, the
migration refuses to create a second history until operators back up,
explicitly move and verify the data, and rerun it.

### Gap 3: The async assumption is hostile to existing FastAPI codebases

**Mitigation:** `publish_sync()` and sync `@listener` support ([§5.2](#52-sync-vs-async)). The framework detects the calling context (running loop or not) and does the right thing. Sync listeners run in the executor, async run directly.

### Gap 4: Decorator-based listener registration creates import-order dependencies

**Mitigation:** the manifest file ([§5.4](#54-the-manifest-file)) and a startup verification check that compares declared listeners to actually-registered listeners. Mismatches fail loudly with file:line.

### Gap 5: Testing is genuinely harder than the docs admit

**Mitigation:** the bundled pytest plugin, installed via `modupy[test]` ([Part XI](#part-xi--testing)). Auto-reset between tests, subprocess-per-test for isolation, Scenario API for event-driven flows.

### Gap 6: No story for adopting modupy on existing codebases

**Mitigation:** ratcheting verifier ([§8.3](#83-ratcheting-mode)) + `modulith audit` ([§8.4](#84-the-audit-tool)). The adoption path is in [§13.2](#132-brownfield-adoption-the-path-that-matters) and [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md).

### Gap 7: The plugin ecosystem might never form

**Mitigation:** reframe the positioning. Plugin system is for internal modularity, not for community ecosystem. First-party adapters cover the Postgres outbox, durable local SHM, Redis Streams, relational-database brokering, and OpenTelemetry; Kafka and RabbitMQ remain Phase 4. Community plugins are nice-to-have, not required for success.

---

## Part XV — Implementation Roadmap

The phases and their live status are in [ROADMAP.md](ROADMAP.md). Phase 0 (Foundation), Phase 1 (Core), Phase 2 (Tooling and observability) and Phase 3 (Process-per-module runtime) are code complete. Phase 4 (Ecosystem adapters) is built when users ask for it.

---

## Part XVI — File Inventory

What each file is. [ROADMAP.md](ROADMAP.md) holds the live delivery status, and the Status column below matches it.

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
| `__main__.py` | ✅ | `python -m modulith`, the same entry point as the `modulith` command |
| `_claims.py` | ✅ | Claim strategies (`lease`, `advisory_lock`, `none`) and the store capabilities the outbox sweep checks for |
| `_health_failures.py` | ✅ | Broker-operation failures that hold a consumer's health at `degraded` |
| `_shutdown.py` | ✅ | Bounded cancellation for consumer shutdown |
| `extract.py` | ✅ | `modulith extract`: scaffold a standalone service from one module |
| `k8s.py` | ✅ | `modulith k8s-manifest`: a Deployment and Service per module plus one Ingress |
| `openapi.py` | ✅ | `modulith openapi`: build each module's OpenAPI document and merge them |
| `py.typed` | ✅ | PEP 561 marker: type checkers read the package's inline annotations |

### Built-in plugins: `modulith/builtin/`

| File | Status | Notes |
|---|---|---|
| `__init__.py` | ✅ | Regular package: its docstring says the built-ins are ordinary first-party plugins with no special privileges |
| `discovery.py` | ✅ | Default subpackage walker |
| `verifier.py` | ✅ | AST-based boundary verification (Phase 1) |
| `outbox.py` | ✅ | Outbox plugin core, calls store adapter (Phase 1) |
| `observability.py` | ✅ | OTel auto-instrumentation (Phase 2) |
| `docs.py` | ✅ | Mermaid + canvas generation (Phase 1) |

### Storage adapters: `modulith/adapters/`

| File | Status | Notes |
|---|---|---|
| `__init__.py` | ✅ | Regular package: its docstring lists the shipped adapters and the extra each needs |
| `postgres_outbox.py` | ✅ | SQLAlchemy + Postgres PublicationStore, alembic migrations (Phase 1) |
| `redis_broker.py` | ✅ | Redis Streams Broker (Phase 2) |
| `db_broker.py` | ✅ | Postgres/MySQL/SQLite database broker |
| `shm_broker.py` + `_shm_*.py` | ✅ | SQLite-authoritative local broker + advisory mmap hints |
| `_state_path.py` | ✅ | Private package-namespaced broker state paths |
| `_polling_consumer.py` | ✅ | Shared lifecycle of the durable polling consumers behind the SHM and database brokers |
| `_consumer_protocol.py` | ✅ | `PollingBroker`: the store operations a polling consumer needs |
| `_delivery_dispatch.py` | ✅ | Concurrent delivery and fenced completion for polling consumers |
| `_dead_letter.py` | ✅ | The dead-letter shape `modulith broker dead-letter` reads from any adapter |
| `alembic.ini` | ✅ | Alembic configuration for the packaged migrations; its `sqlalchemy.url` is empty because `migrations/env.py` takes the URL from `-x url=` or `MODULITH_DB_URL` |
| `migrations/` | ✅ | The packaged Alembic environment and revision chain for the outbox and broker schemas: `env.py`, `script.py.mako`, `version_table.py` (keeps the revision in `modulith_alembic_version`) and `versions/` |
| `kafka_broker.py` | ⏳ | Kafka Broker (Phase 4 — not shipped, see §10.3) |

### Tests: `tests/`

One `test_*.py` module per area, including testcontainers-backed integration
suites (see Appendix B). The directory is the inventory.

### Examples: `examples/`

Three runnable examples of growing scale, then three single-file extension references.

| Example | Status | Notes |
|---|---|---|
| `quickstart/` (package `myapp`) | ✅ | Small: three modules plus contracts, wired purely through events; the same code runs single-process and under `--topology=processes`. No infrastructure |
| `demo_app/` (package `shop`) | ✅ | Mid: three modules plus contracts with per-module persistence, idempotent listeners, the durable outbox through `outbox_url`, `modulith migrate` and the outbox CLI, and process-per-module on the default SHM broker. A SQLite file; Docker only to swap in Postgres and Redis |
| `marketplace/` (packages `marketplace`, `marketplace_platform`) | ✅ | Large: seven modules plus contracts, with a separate platform package registered as a plugin. Docker Postgres |
| `naming_convention_verifier.py` | ✅ | Reference: a custom verification rule |
| `redis_streams_broker.py` | ✅ | Reference: a producer-side broker adapter (example scheme `redis-streams-example`); cross-process delivery also needs a consumer for the same scheme |
| `versioned_json_serializer.py` | ✅ | Reference: a custom outbox storage serializer |

### Top-level

| File | Status | Notes |
|---|---|---|
| `pyproject.toml` | ✅ | Project metadata, dependencies, entry points, CLI script |
| `README.md` | ✅ | The first thing users read |
| `SPEC.md` | ✅ | This document |
| `ROADMAP.md` | ✅ | Phase plan with checkboxes (live status source) |
| `MIGRATION_GUIDE.md` | ✅ | How to adopt on existing codebases |
| `LICENSE` | ✅ | Apache 2.0 |

### Packaging

modupy is one distribution, and its optional parts are **extras** (see
[Part X](#part-x--built-in-adapters)): the test plugin is `modupy[test]`, the
Postgres outbox is `modupy[postgres]`, the Redis broker is `modupy[redis]`. A
standalone `pytest-modupy` package remains a possible later split, and a Kafka
adapter (whether extra or package) is Phase 4.

---

## Appendices

### Appendix A — pyproject.toml

[`pyproject.toml`](pyproject.toml) holds the version, the dependencies, every extra and the entry points. This document does not copy them, so they cannot drift.

### Appendix B — Test Strategy

Three layers:

1. **Unit tests** (`tests/test_*.py`) — fast, isolated, no I/O. Cover individual modules. The Postgres outbox adapter is exercised here against in-memory SQLite so the bootstrap/retry/dead-letter logic is covered with zero external services.
2. **Integration tests** — exercise the full bootstrap + dispatch flow. Use the fake-app fixture in `test_zero_config.py` as the template.
3. **End-to-end tests** (`tests/test_*_e2e.py`, `test_*_integration.py`, `test_migration_postgres.py`) — real Postgres, MySQL and Redis, and real `uvicorn` subprocess workers, provisioned on demand by [testcontainers](https://testcontainers.com) (`postgres:16`, `mysql:8.0` and `redis:7`). They cover the outbox against real Postgres (including the Alembic migration and `FOR UPDATE SKIP LOCKED`), Redis Streams consumer-group delivery / `XAUTOCLAIM` reclaim / dead-lettering, cross-process event delivery, and the process-per-module supervisor + proxy. Marked `@pytest.mark.integration` and gated behind Docker — they auto-skip when no daemon is reachable, so the default `pytest` run stays hermetic.

Running them:

```bash
pip install -e '.[integration]'   # the `integration` extra in pyproject.toml: testcontainers and the database drivers
pytest -m integration             # spins up Postgres, MySQL and Redis containers
```

Set `MODULITH_TEST_POSTGRES_URL` / `MODULITH_TEST_MYSQL_URL` / `MODULITH_TEST_REDIS_URL` to run against pre-existing services instead of containers (e.g. a CI service container).

Coverage floors enforced in CI: 90% line coverage on the core package and 100% on the outbox plugin (the hardest piece), alongside crash-recovery tests with random kill timing.

### Appendix C — Documentation

Documentation makes or breaks adoption. The set that ships in the repository:

- **[README.md](README.md)** — the 5-minute pitch + zero-config example + link to deeper docs
- **[Architecture guide](docs/ARCHITECTURE.md)** — for users who want to understand how it works
- **[Migration guide](MIGRATION_GUIDE.md)** — for brownfield adoption
- **[API reference](docs/API_REFERENCE.md)** — generated from docstrings by `scripts/gen_api_reference.py`
- **[Cookbook](docs/COOKBOOK.md)** — short recipes for common patterns (publish-then-listen, outbox with FastAPI, process-per-module deployment)
- **[Deployment guide](docs/DEPLOYMENT.md)** — single-process, process-per-module and distributed topologies
- **[Stability guide](docs/STABILITY.md)** — API stability and versioning
- **[Contributing](CONTRIBUTING.md)** — development setup, tests and the pull request checklist

### Appendix D — Versioning Policy

The versioning policy, and which parts of the API are stable, are in [docs/STABILITY.md](docs/STABILITY.md).

### Appendix E — Decision Log

Major decisions and their rationale.

- **Why pluggy?** Most pythonic plugin system, used by pytest for a decade. Rebuilding it badly is real risk; rebuilding it well is just rebuilding pluggy.
- **Why three plugin shapes?** Drivers, dispatch, hooks have genuinely different shapes. Forcing all into one mechanism makes the wrong one awkward.
- **Why lazy bootstrap?** "Just import and use" is the adoption-critical UX. Eager bootstrap requires explicit setup which adds learning curve.
- **Why contracts module?** Spring uses module-public events; we add a separate contracts package because it's cleaner for distributed deployment versioning.
- **Why ratcheting verifier?** Brownfield is where adoption happens. Without ratcheting, modupy is for new projects only.
- **Why subprocess-per-test in pytest plugin?** Python's import system is global and side-effectful. Subprocess is the only correct isolation; cost is acceptable for integration tests.
- **Why Mermaid over PlantUML?** Renders natively on GitHub/GitLab. PlantUML needs a separate server.

---

*End of the design document. The source tree contains the code referenced throughout.*
