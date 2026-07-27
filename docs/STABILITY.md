# modulith Stability & Versioning

This guide explains API stability guarantees for modulith 0.x and what to expect as we move toward 1.0.

---

## Pre-1.0 SemVer: 0.x releases

modulith follows [Semantic Versioning](https://semver.org/). Until 1.0, **breaking changes may occur in minor releases** (0.9 → 0.10) and **are always documented in CHANGELOG.md**. Patch releases (0.9.0 → 0.9.1) never break the public API.

| Release | Breaking changes allowed? | Documented in | Upgrade effort |
|---------|--------------------------|---------------|----------------|
| 0.9.0 → 0.10.0 | **Yes** | CHANGELOG.md | Check for migrations |
| 0.9.0 → 0.9.1 | No | CHANGELOG.md (bugfixes) | Safe |
| 0.9 → 1.0 | Final, then no | CHANGELOG.md + [MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md) | Review migration guide |

---

## Stable surface: public API

The public API is defined by exports in `modulith/__init__.py`:

**Application authors** (most users):
- `event`, `listener`, `publish`, `publish_sync` — event-driven decorators and dispatch
- `externalized` — marks an event as routed to the broker; the one extra name the process-per-module topology needs
- `bootstrap`, `configure` — initialization and runtime config
- `Configuration`, `ConfigurationError` — config contract and errors
- `PublishSyncTimeout` — raised by `publish_sync()` when its timeout elapses

**Plugin authors** (extending modulith):
- `hookimpl` — hook-implementation marker (plugins implement hookspecs in `modulith.hooks`)
- Protocols: `Broker`, `Consumer`, `EventSerializer`, `PublicationStore` — driver contracts
- `Manifest`, `declare_module`, `get_manifest` — module registration and introspection
- `create_plugin_manager` — build a pluggy manager pre-registered with modulith's hookspecs

**Registry** (advanced, rarely needed):
- `BrokerRegistry`, `ConsumerRegistry`, `ConsumerSpec` — broker and consumer management
- Exception types: `DuplicateBrokerError`, `UnknownBrokerError`, `DuplicateConsumerError`, `UnknownConsumerError`

**Contract types** (plugin/extension data):
- `EventPublication`, `ModuleInfo`, `Violation`, `ViolationSeverity` — event and manifest types

**Stability**: These exports are considered stable. **Signature changes or removals require a major version bump (0.x → 1.0+)**, though 0.x minors may add optional parameters or new overloads if backward-compatible. The list above is `modulith.__all__` minus `__version__`; anything reachable only by a deeper import path is covered by one of the weaker tiers below.

---

## Stable surface: hookspec signatures and the types in them

`modulith.hooks` declares the 13 hookspecs a plugin implements, and
`modulith.protocols` declares the driver contracts. Both carry the same
guarantee as the public API above: a hookspec's name or parameter list changes
only on a major bump.

That guarantee extends to types a hookspec's signature names but
`modulith/__init__.py` deliberately does **not** re-export — import them from
the module that defines them:

- `modulith.types.EventPublishReceipt` — what `modulith_after_event_published` receives on the durable outbox path (an `EventPublication` on the in-memory path). Kept out of the top-level package on purpose: application authors never construct one, and plugin authors reach it with `isinstance(publication, EventPublishReceipt)`. `modulith/builtin/observability.py` is the working example.
- `modulith.protocols.HealthAwareConsumer` — the optional `health()` capability a consumer may implement; worker readiness checks use it when present.
- `modulith.protocols.ConsumerHealth` / `ConsumerStatus` — the frozen readiness snapshot `health()` returns, and the literal status set (`starting`, `ready`, `degraded`, `failed`, `stopped`, `unknown`).

**Stability**: same as the public API. Note that `docs/API_REFERENCE.md` is generated strictly from `modulith.__all__`, so these names are documented here rather than there.

---

## Stable surface: the pytest plugin (`modulith.testing`)

`modulith.testing` is registered through the `pytest11` entry point, so its
fixtures and markers load in any pytest run where modupy is installed — the
`modupy[test]` extra only adds the libraries the fixtures need, it does not
gate registration.

- Fixtures: `modulith_app`, `modulith_module`, `scenario`
- Classes: `ModulithTestApp`, `Scenario` (what those fixtures hand you)
- Markers: `modulith_isolated`, `modulith_no_outbox`
- Ini option: `modulith_isolated_timeout` (seconds an isolated test's subprocess may run; default 300)

**Stability**: same as the public API — a rename or signature change is a major-version event, because test suites depend on these by name in every test function's arguments. The plugin is expected to move into a standalone `pytest-modulith` distribution in a future release; that split will keep the fixture, marker, and ini-option names identical, and will be documented in CHANGELOG.md.

---

## Documented wiring surface: `modulith.builtin.*` and `modulith.serializers`

Drivers are wired **explicitly** at startup — there is no entry-point auto-discovery for them — so any durable deployment imports these directly (see [ARCHITECTURE.md §5.2](ARCHITECTURE.md#52-driver-protocols-exactly-one-wins)):

- `modulith.builtin.outbox` — `configure()`, `status()`, `force_retry()`, `list_dead_lettered()`, `retry_all_dead_lettered()`, `purge_completed()`, `shutdown()`
- `modulith.serializers` — `JsonEventSerializer`

**Stability**: weaker than the public API above, stronger than the adapter internals below. Signatures may change in a 0.x minor, but every change is documented in CHANGELOG.md with an upgrade note. Underscore-prefixed names are excluded — in particular `modulith.builtin.outbox._current_session`, which exists so adapters can bind to it; applications use `bind_session()`/`unbind_session()` from `modulith.adapters.postgres_outbox`.

---

## Experimental surface: broker adapter internals

Modules under `modulith/adapters/*` are **implementation details** and may change without a major bump:

- `modulith.adapters.postgres_outbox` — outbox store for Postgres, MySQL, and SQLite (one adapter; the URL picks the dialect)
- `modulith.adapters.db_broker` — relational-database broker internals
- `modulith.adapters.redis_broker` — Redis Streams broker internals
- `modulith.adapters.shm_broker` — local durable shared-memory broker internals
- `modulith.adapters._*` — private helpers shared between the above

**What's stable**: The `PublicationStore`, `Broker`, and `Consumer` protocols these adapters implement. If you implement a custom store or broker, your code depends on the protocol, not the adapter internals.

**What's experimental**: Adapter configuration classes, internal helper functions, and schema details are not part of the public API and may shift between 0.x minors.

---

## Deprecation policy

**One minor-release warning window**: If we deprecate a public API item (e.g., a function parameter), it will:

1. Raise `DeprecationWarning` at runtime (where feasible)
2. Be marked `@deprecated` in docstrings
3. Appear in CHANGELOG.md under "Deprecated"
4. Remain functional for **at least one minor release** (e.g., deprecated in 0.9, removed no earlier than 0.11)

Example timeline:
- **0.9.0**: `publish_sync(timeout=...)` parameter works, `DeprecationWarning` issued, CHANGELOG notes deprecation
- **0.9.1 – 0.10.x**: Parameter still works, warning continues
- **0.11.0** (or later): Parameter removed, breaking change documented in [MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md)

(Illustration only — `publish_sync(timeout=...)` is not deprecated. Do not
confuse `MIGRATION_GUIDE.md`, the upstream upgrade guide, with `MIGRATION.md`,
the default output filename of your own `modulith audit` run.)

This gives users a clear upgrade path without surprise breakage in patch releases.

---

## Getting help

- **API questions**: See [API_REFERENCE.md](API_REFERENCE.md)
- **Design & architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md) and [SPEC.md](../SPEC.md)
- **How-to guides**: See [COOKBOOK.md](COOKBOOK.md)
- **Release notes**: See [CHANGELOG.md](../CHANGELOG.md)
