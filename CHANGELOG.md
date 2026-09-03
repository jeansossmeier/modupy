# Changelog

All notable changes to modulith are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.10.0] — 2026-09-02

### Added

- `modulith extract <module>` — scaffold a wheel-buildable standalone service (`pyproject.toml`, `Dockerfile`, `README.md`, `.env.example`) from one module; boundary/shared-table blockers require `--force`, while unsafe output paths and escaping source symlinks are always rejected
- `modulith k8s-manifest` — generate RFC-1123-safe Deployment, Service, and Ingress names; validate ports; pass the contracts module and supported broker settings; reference connection secrets without embedding credentials
- `modulith openapi` — merge module OpenAPI documents into one build-time spec, prefix schema names, and reject incompatible collisions or duplicate operation IDs
- `modulith doctor` — three new checks: **actuator token** (including an error when token mode lacks a token), **single-host broker**, and **redis retention**; process-split readiness also reports table-only cross-module coupling
- Per-module Postgres schema — `broker_options.schema` / `MODULITH_BROKER_SCHEMA` for the database broker, and alembic `-x schema=` / `MODULITH_DB_SCHEMA` for migrations, applied via `schema_translate_map` so `Table`/`MetaData` definitions stay unchanged

### Changed

- `modulith doctor`'s process-split readiness check now also counts cross-module table references (a coupling direct imports can't see) and reports, per module, tables not prefixed with the module's own name; the "microservice-ready" tier now additionally requires zero cross-module table references, otherwise the headline reads "process-split ready (shared tables block extraction)" instead
- The verifier's `data-ownership` rule now detects `ForeignKey("table.col")` string literals referencing another module's table, not just `Table()`/`__tablename__` declarations, and warns when a module with a non-empty `owns_tables` defines a table it doesn't declare there
- `modulith audit`'s shared-table detection now also follows `ForeignKey` string literals, mirroring the verifier
- SQL schema identifiers are validated consistently through configuration, environment variables, Alembic `-x`, and direct database-broker construction
- Enabling a named migration schema now refuses to abandon existing Modulith tables or Alembic history in `public`; data movement remains an explicit operator migration
- Artifact generators import application modules and therefore require trusted source; `openapi` reports an actionable `modupy[fastapi]` installation error when FastAPI is unavailable
- `MODULITH_DEV_WARN_ONLY` is limited to single-process `modulith dev`; process topology and `modulith run` continue enforcing strict boundaries
- `JsonEventSerializer.deserialize` now enforces a payload cap — `MODULITH_BROKER_MAX_PAYLOAD_BYTES`, else `broker_options["max_payload_bytes"]`, else 16 MiB — and raises `ConfigurationError` for an oversized payload where it previously decoded unconditionally; the cap resolves lazily from the loaded broker config at first deserialize (or from the new `max_payload_bytes` constructor argument) instead of freezing a default before configuration is available

### Fixed

- Database broker: schema-aware alembic revisions skip tables/indexes the broker already bootstrapped instead of erroring; a stale claim past `max_attempts` is now reclaimed, retried, and dead-lettered instead of stuck forever; the broker schema falls back to `MODULITH_DB_SCHEMA` when unset; a database broker or store used from a second event loop now warns once instead of deadlocking on schema/session setup
- Outbox: the retry loop recreates itself if its event loop closes instead of dying silently; `shutdown()` no longer raises on an already-closed loop; lease renewal re-raises a real shutdown cancellation instead of swallowing it and hanging shutdown forever; `claim_lease_seconds` must be finite and positive; `status()` and the doctor outbox check now include archived-row counts (`count_archived`)
- SHM broker: the ring file's temporary descriptor is closed before it is linked into place, so a racing peer on Windows no longer sees `st_nlink == 2` and rejects the hint file; completion pruning under retention 0 also removes rows completed in the same clock tick as the prune call; per-event-loop store locks evict closed loops instead of leaking; `claim_batch` respects `max_attempts` end-to-end so a reclaimed row is retried and eventually dead-lettered instead of looping forever; pruning runs every 100 publishes instead of every publish; added a backfilled expiry index and a deterministic claim-cost test
- Redis broker/consumer: `MODULITH_BROKER_MAX_PAYLOAD_BYTES` and the dead-letter max-stream-len env var are read ahead of `broker_options`; the dead-letter script checks for an existing dedup entry before appending, so a replayed acknowledgment can no longer duplicate a dead-letter record
- Polling consumer: idle backoff never drops below the configured poll interval, so raising `poll_interval_ms` above the previous 0.5s floor actually reduces poll frequency
- Reverse proxy: requests round-robin across a module's healthy replica instances instead of always hitting the first one, and a replica marked down is retried after a cooldown instead of staying excluded forever
- Supervisor: failed worker instances are now reported through `/_modulith/health`; `stop()` no longer sends a second, functionally-identical kill signal on Windows, where `terminate()` and `kill()` are the same hard stop
- Config: a whitespace-only `MODULITH_*` env var is treated as unset, matching the empty-string contract; `[tool.modulith.verify].disabled_rules` is honored by the verifier itself (previously ignored), and an unknown rule name in it now warns
- `sync.publish_sync`'s nested-loop dispatch bounds its `loop_ready` wait so a failure before the nested loop starts raises the documented timeout instead of hanging forever
- `@listener` recognizes an async-callable class instance (not just a plain async function) as an async handler
- Observability hooks now match module boundaries the same way the verifier does
- Generated docs and canvases are written as UTF-8 explicitly
- k8s manifest generation: the Ingress path is the raw module name (YAML-quoted) instead of a hyphenated one that could mismatch the worker's actual route prefix, and a non-identifier module name is now rejected instead of producing a broken manifest
- `modulith extract`: a parse failure during extraction is now a blocker (`--force`-overridable) instead of silently skipped; the contracts module is exempt from the shared-table scan; a module/helper name that doesn't resolve to a real importable package is now an error instead of writing an empty extraction tree
- Verifier: `importlib.import_module()`/`__import__()` string-literal imports now count as cross-module imports like a normal `import` statement
- Testing plugin: the manifest registry is snapshotted and restored alongside `sys.modules` between tests, and `modulith.testing` no longer leaks helper imports (`Any`, `MagicMock`, `dataclass`, …) into its public namespace — a guard test now enforces that, like the top-level package's
- `modulith dev --isolate` now states, in `--help` and on stderr, that every other discovered module is not started and its routes 404
- Packaging: dropped a `py.typed` include that pointed at a directory the wheel doesn't ship; `.opencode/` is excluded from the sdist; CI and release inspection now require `alembic.ini` and `py.typed` to be present in the built distribution
- Raised the `redis` extra's floor to `redis>=5.0.1` (the actual tested minimum)
- The issue template's "Question or usage help" contact link now points at a working `issues/new?labels=question` URL instead of the disabled Discussions tab
- Consumer `stop()` (database, SHM and Redis Streams consumers) can no longer hang forever on a poll task whose cancellation is absorbed — SQLAlchemy shields a cancelled connection's graceful close, and a driver that never finishes it swallowed the only cancel. `stop()` now re-cancels after 10 s and, if the task still ignores that, logs an error and abandons it after another 10 s; cancelling the stopping task itself still reaches the poll task first

## [0.9.0] — 2026-07-22

**Pre-1.0 feature-complete alpha release.** All Phase 0–3 features are implemented and tested; the core API is stable enough for early adopters, though the public API may still shift before 1.0. See [STABILITY.md](docs/STABILITY.md) for pre-1.0 SemVer guarantees.

### Added

#### Core Framework
- **Module system** — Auto-discovery of subpackages as modules with configurable naming
- **Event-driven boundaries** — `@event`, `@listener`, `publish()` API with async/sync support
- **Boundary verifier** — Six default rules for cross-module dependency compliance
  - `no-internal-imports` — no reaching into another module's private packages or `_`-prefixed names
  - `use-contracts` — shared types come from the contracts module; no wildcard cross-module imports
  - `undeclared-dependency` — every cross-module import must appear in the manifest's `declared_dependencies`
  - `data-ownership` — exactly one module owns a table; others reach it through events or its public API
  - `contracts-is-sink` — the contracts module may not import application modules
  - `no-cyclic-dependency` — the module dependency graph stays acyclic
  - Manual baseline ratcheting for gradual remediation
- **Module manifests** — `declare_module()` with startup verification of contract satisfaction
- **Plugin system** — Pluggy-based hooks for custom brokers, verifiers, serializers, and lifecycle

#### Durability & Outbox
- **Transactional outbox pattern** — Atomic publish-with-transaction for at-least-once delivery
- **Postgres adapter** — SQLAlchemy + async driver for production outbox store
- **SQLite outbox** — The same adapter pointed at a `sqlite+aiosqlite://` URL, for zero-infrastructure deployments; there is no separate SQLite module
- **Crash recovery** — Automatic replay of uncommitted events on restart
- **Retry loop** — Exponential backoff with configurable max attempts and dead-letter store

#### Brokers
- **In-memory broker** — Fast, non-durable, for development and testing
- **Database broker** — Generic SQL broker with `FOR UPDATE SKIP LOCKED` lock-free fan-out
  - Supports Postgres, MySQL, SQLite (embedded or networked)
  - Consumer group semantics via claim leases
  - Automatic rebalancing on consumer crash
  - Built-in dead-letter and age-based pruning
- **Redis Streams broker** — High-throughput, distributed
  - Consumer group management with automatic pending-entry recovery
  - Dead-letter handling for failed listeners
  - Soft import (optional dependency)

#### Process Topology
- **Single-process (default)** — All modules in one FastAPI app
- **Process-per-module** — Modules as separate worker processes behind a reverse proxy
  - Worker supervisor with crash recovery and graceful shutdown
  - Configurable module-to-worker assignment
  - Cross-process event delivery via broker
  - Health check propagation
- **Topology CLI** — `modulith run --topology {single,processes}`

#### CLI
- `modulith dev` — Development server with auto-reload banner
- `modulith run` — Production mode with optional process topology
- `modulith verify` — Boundary compliance checks with ratchet baseline
- `modulith docs` — Auto-generate Mermaid architecture diagrams + event flow
- `modulith info` — Introspect detected package, modules, manifests, plugins
- `modulith doctor` — Health checks on brokers, stores, and driver wiring
- `modulith audit` — AST-based codebase scan for brownfield migration insights
- `modulith outbox {status,retry,purge,dead-letter}` — Operational commands for event management

#### Testing
- **pytest plugin** — Auto-discovery and fixture injection
- **Fixtures**
  - `modulith_app` — Fresh runtime per test
  - `modulith_module` — Module-isolated runtime with manifests cleared
  - `scenario` — Fluent event-driven test API with `publish().expect_event().within()`
- **Subprocess isolation** — `@pytest.mark.modulith_isolated` for testing topology in CI
- **Captured state** — `ModulithTestApp.published_events`, `.listener_calls`, and `.published_events_of_type()` for assertions

#### Observability
- **OpenTelemetry integration** — Spans for publish, listen, and outbox dispatch
- **Soft OTel import** — Silent no-op if not installed
- **Instrumentation** — Events and listeners emit to configured exporters (console, Jaeger, Prometheus)

#### Documentation
- **Architecture guide** — Internal design, plugin contracts, broker comparison
- **Migration guide** — Strategies for adopting modulith in existing FastAPI applications
- **API reference** — Auto-generated from docstrings for all public APIs
- **Cookbook** — 11 recipes covering common patterns
- **Deployment guide** — Scaling from monolith to process-per-module, Docker, Kubernetes, operational playbooks
- **Examples** — Runnable demo app (`examples/demo_app/shop`) demonstrating all modes

### Fixed

- Verifier false positives from aliased imports (now correctly traces through `__all__` and re-exports)
- `modulith outbox purge` left `event_publications_archive` untouched — under `completion_mode="archive"` the primary table holds no completed rows, so the purge reported a truthful-looking zero every night while the archive grew without bound. It now deletes from both tables and returns the combined row count
- Outbox claim-lease handling under concurrent claims
- Broker consumer group rebalancing on member timeout
- Health check propagation in multi-worker topology
- Graceful shutdown cascade in supervisor (waits for in-flight listeners)

### Security

- Input validation for configuration strings and broker URLs
- SQL injection prevention via SQLAlchemy ORM (parameterized queries)
- Event payload size limits to prevent unbounded memory use

### Documentation

- Full restructuring to separate concerns (internal architecture, operational guides, API)
- Deployment patterns with scaling strategies
- Troubleshooting guide for common production scenarios
- Version constraints and supported Python versions (3.11+)

### Performance

- Outbox scan indexes (migration `0005_outbox_scan_indexes`) — a partial expression index on the sweep's claim ordering, so a sweep no longer reads and sorts every pending row on each pass (Postgres only: it needs both functional and partial index support), plus a `completed_at` index on the archive table for the purge scan on every dialect
- Batch event dispatch for database and Redis brokers (configurable batch size)
- Connection pooling for database brokers
- Claim-lease-based lock-free fan-out (Postgres broker)
- Concurrent listener invocation within a worker (configurable concurrency)

---

## v0.9.0 Statistics

Measured at the 0.9.0 tag; these drift with every release and are not a
contract.

- **Lines of code (core):** ~20,600 across 54 modules in `modulith/`
- **Test suite:** 1,439 tests in the default (non-integration) suite, 0 failures
- **Test code:** ~37,300 lines in `tests/`
- **Type safety:** 100% typed, mypy `--strict` passing
- **Documentation:** ~5,400 lines of Markdown (README, SPEC, guides, cookbook)

---

## Roadmap

### v1.1 (Demand-Driven)
- Additional broker adapters: Kafka, RabbitMQ, AWS SQS
- Alternative outbox stores: MongoDB, DynamoDB
- Subinterpreter topology (when Python 3.13+ ecosystem ready)
- Django integration package

### v2.0 (Post-Adoption)
- Performance optimization based on production workload telemetry
- Extended plugin APIs for custom storage and transport layers
- GraphQL subscription support for event-driven subscriptions

See [ROADMAP.md](ROADMAP.md) for detailed phase breakdowns and kill criteria.

---

## Migration Guide

**For existing FastAPI applications:**
- Adopt modulith as a dependency; no refactor required to start
- Gradually migrate request handlers to event-driven modules
- Use outbox for durability; process topology for horizontal scale
- See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)

**For new applications:**
- Start with `modupy[fastapi,cli]` and in-memory broker
- Add durability via `modupy[postgres]` when needed
- Scale to processes with the `database` broker and `--topology processes`

---

## Installation

```bash
pip install modupy

# With FastAPI and CLI:
pip install 'modupy[fastapi,cli]'

# With Postgres outbox:
pip install 'modupy[postgres]'

# With database broker:
pip install 'modupy[database]'

# Full stack (all adapters):
pip install 'modupy[all]'
```

**Requires:** Python 3.11+
