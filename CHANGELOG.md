# Changelog

All notable changes to modulith are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [1.0.0] — 2026-07-22

**First stable release.** Modulith is production-ready with all Phase 0–3 features complete.

### Added

#### Core Framework
- **Module system** — Auto-discovery of subpackages as modules with configurable naming
- **Event-driven boundaries** — `@event`, `@listener`, `publish()` API with async/sync support
- **Boundary verifier** — Five default rules for cross-module dependency compliance
  - No cross-module imports (structural)
  - No public imports (API leakage)
  - No circular module dependencies (graph cycles)
  - Manual baseline ratcheting for gradual remediation
- **Module manifests** — `declare_module()` with startup verification of contract satisfaction
- **Plugin system** — Pluggy-based hooks for custom brokers, verifiers, serializers, and lifecycle

#### Durability & Outbox
- **Transactional outbox pattern** — Atomic publish-with-transaction for at-least-once delivery
- **Postgres adapter** — SQLAlchemy + async driver for production outbox store
- **SQLite adapter** — Embedded database for zero-infrastructure deployments
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
- **Helper assertions** — `event_published()`, `listener_called()`

#### Observability
- **OpenTelemetry integration** — Spans for publish, listen, and outbox dispatch
- **Soft OTel import** — Silent no-op if not installed
- **Instrumentation** — Events and listeners emit to configured exporters (console, Jaeger, Prometheus)

#### Documentation
- **Architecture guide** — Internal design, plugin contracts, broker comparison
- **Migration guide** — Strategies for adopting modulith in existing FastAPI applications
- **API reference** — Auto-generated from docstrings for all public APIs
- **Cookbook** — 11+ recipes covering common patterns
- **Deployment guide** — Scaling from monolith to process-per-module, Docker, Kubernetes, operational playbooks
- **Examples** — Runnable demo app (`examples/demo_app/shop`) demonstrating all modes

### Fixed

- Verifier false positives from aliased imports (now correctly traces through `__all__` and re-exports)
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

- Batch event dispatch for database and Redis brokers (configurable batch size)
- Connection pooling for database brokers
- Claim-lease-based lock-free fan-out (Postgres broker)
- Concurrent listener invocation within a worker (configurable concurrency)

---

## v1.0.0 Statistics

- **Lines of code (core):** ~4,500 (modulith/)
- **Test coverage:** 1,376 tests, 0 failures
- **Type safety:** 100% typed, mypy `--strict` passing
- **Documentation:** 400+ pages (guides, API, cookbook, examples)
- **Time to first event:** <50ms (in-memory), <100ms (database), <200ms (Redis)

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
- Start with `modulith[fastapi,cli]` and in-memory broker
- Add durability via `modulith[postgres]` when needed
- Scale to processes via `MODULITH_BROKER=database --topology processes`

---

## Installation

```bash
pip install modulith

# With FastAPI and CLI:
pip install 'modulith[fastapi,cli]'

# With Postgres outbox:
pip install 'modulith[postgres]'

# With database broker:
pip install 'modulith[database]'

# Full stack (all adapters):
pip install 'modulith[all]'
```

**Requires:** Python 3.11+
