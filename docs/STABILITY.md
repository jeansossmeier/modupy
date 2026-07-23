# modulith Stability & Versioning

This guide explains API stability guarantees for modulith 0.x and what to expect as we move toward 1.0.

---

## Pre-1.0 SemVer: 0.x releases

modulith follows [Semantic Versioning](https://semver.org/). Until 1.0, **breaking changes may occur in minor releases** (0.9 → 0.10) and **are always documented in CHANGELOG.md**. Patch releases (0.9.0 → 0.9.1) never break the public API.

| Release | Breaking changes allowed? | Documented in | Upgrade effort |
|---------|--------------------------|---------------|----------------|
| 0.9.0 → 0.10.0 | **Yes** | CHANGELOG.md | Check for migrations |
| 0.9.0 → 0.9.1 | No | CHANGELOG.md (bugfixes) | Safe |
| 0.9 → 1.0 | Final, then no | CHANGELOG.md + MIGRATION.md | Review migration guide |

---

## Stable surface: public API

The public API is defined by exports in `modulith/__init__.py`:

**Application authors** (most users):
- `event`, `listener`, `publish`, `publish_sync` — event-driven decorators and dispatch
- `bootstrap`, `configure` — initialization and runtime config
- `Configuration`, `ConfigurationError` — config contract and errors

**Plugin authors** (extending modulith):
- `hookimpl` — hook-implementation marker (plugins implement hookspecs in `modulith.hooks`)
- Protocols: `Broker`, `Consumer`, `EventSerializer`, `PublicationStore` — driver contracts
- `Manifest`, `declare_module`, `get_manifest` — module registration and introspection

**Registry** (advanced, rarely needed):
- `BrokerRegistry`, `ConsumerRegistry`, `ConsumerSpec` — broker and consumer management
- Exception types: `DuplicateBrokerError`, `UnknownBrokerError`, etc.

**Contract types** (plugin/extension data):
- `EventPublication`, `ModuleInfo`, `Violation`, `ViolationSeverity` — event and manifest types

**Stability**: These exports are considered stable. **Signature changes or removals require a major version bump (0.x → 1.0+)**, though 0.x minors may add optional parameters or new overloads if backward-compatible.

---

## Experimental surface: broker adapter internals

Modules under `modulith/adapters/*` are **implementation details** and may change without a major bump:

- `modulith.adapters.postgres` — Postgres outbox adapter internals
- `modulith.adapters.sqlite` — SQLite outbox adapter internals
- `modulith.adapters.redis` — Redis broker adapter internals
- `modulith.adapters.broker_*` — Other broker adapters

**What's stable**: The `Broker` and `Consumer` protocols these adapters implement. If you implement a custom broker, your code depends on the protocol, not the adapter internals.

**What's experimental**: Adapter configuration classes, internal helper functions, and schema details are not part of the public API and may shift between 0.x minors.

---

## Deprecation policy

**One minor-release warning window**: If we deprecate a public API item (e.g., a function parameter), it will:

1. Raise `DeprecationWarning` at runtime (where feasible)
2. Be marked `@deprecated` in docstrings
3. Appear in CHANGELOG.md under "Deprecated"
4. Remain functional for **at least one minor release** (e.g., deprecated in 0.9, removed no earlier than 0.11)

Example timeline:
- **0.9.0**: `publish(timeout=...)` parameter works, `DeprecationWarning` issued, CHANGELOG notes deprecation
- **0.9.1 – 0.10.x**: Parameter still works, warning continues
- **0.11.0** (or later): Parameter removed, breaking change documented in MIGRATION.md

This gives users a clear upgrade path without surprise breakage in patch releases.

---

## Getting help

- **API questions**: See [API_REFERENCE.md](API_REFERENCE.md)
- **Design & architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md) and [SPEC.md](../SPEC.md)
- **How-to guides**: See [COOKBOOK.md](COOKBOOK.md)
- **Release notes**: See [CHANGELOG.md](../CHANGELOG.md)
