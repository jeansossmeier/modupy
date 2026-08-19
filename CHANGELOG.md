# Changelog

All notable changes to modulith are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.10.0] — 2026-08-19

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
- **Idempotency guards** — Per-event deduplication via hash

#### Brokers
- **In-memory broker** — Fast, non-durable, for development and testing
- **Database broker** — Generic SQL broker with `FOR UPDATE SKIP LOCKED` lock-free fan-out
  - Supports Postgres, MySQL, SQLite (embedded or networked)
  - Consumer group semantics via claim leases
  - Automatic rebalancing on consumer crash
  - Built-in dead-letter and age-based pruning
- **Redis Streams broker** — High-throughput, distributed
  - Consumer group management with automatic pending-entry recovery
  - Configurable batch size and concurrency
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
- XSS prevention in auto-generated documentation (HTML-safe Mermaid links)

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
