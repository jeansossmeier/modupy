# Roadmap

> Time-boxed delivery plan. Each phase has explicit kill criteria.
> Side projects of this scope don't ship without ruthless scoping.

The full design is in [SPEC.md](SPEC.md). This document is the
execution checklist that maps phases to deliverables.

---

## Phase 0 — Foundation ✅ COMPLETE

The plugin contract, configuration, auto-discovery, and in-memory
event bus. The minimum that proves the architecture works.

- [x] `modulith/__init__.py` — public API exports
- [x] `modulith/types.py` — `ModuleInfo`, `EventPublication`, `Violation`
- [x] `modulith/protocols.py` — driver protocols
- [x] `modulith/hooks.py` — the 10 hookspecs
- [x] `modulith/markers.py` — `@hookimpl` re-export
- [x] `modulith/brokers.py` — `BrokerRegistry`
- [x] `modulith/manager.py` — plugin manager factory
- [x] `modulith/config.py` — configuration loading + validation
- [x] `modulith/discovery.py` — application package detection
- [x] `modulith/event_bus.py` — `InMemoryEventBus`
- [x] `modulith/runtime.py` — runtime singleton with lazy bootstrap
- [x] `modulith/decorators.py` — `@event`, `@listener`, `publish`, `configure`
- [x] `modulith/builtin/discovery.py` — default subpackage walker
- [x] `tests/test_*.py` — 29 tests, all passing
- [x] `examples/redis_streams_broker.py` — reference broker implementation
- [x] `examples/naming_convention_verifier.py` — reference verifier plugin

---

## Phase 1 — v1 Essentials (4-6 weeks)

The minimum scope where modulith provides value over "FastAPI plus folders."

### Sync entrypoint (Gap 3)
- [ ] `modulith/sync.py` — `publish_sync()`, persistent event loop, sync listener wrapping
- [ ] `modulith/decorators.py` — accept sync `@listener` functions
- [ ] `tests/test_sync.py` — sync publish, sync listeners, mixed flows

### Manifests (Gap 4)
- [ ] `modulith/manifest.py` — `declare_module()` API, `Manifest` dataclass
- [ ] `modulith/runtime.py` — verify_manifest at startup
- [ ] `tests/test_manifest.py` — declared-vs-registered checks

### Boundary verifier (core feature)
- [ ] `modulith/builtin/verifier.py` — AST walker, the five default rules
- [ ] Cycle detection
- [ ] `tests/test_verifier.py` — each rule, cycle detection

### Ratcheting verifier (Gap 6)
- [ ] Baseline file format + load/save
- [ ] `modulith verify --mode=ratchet` semantics
- [ ] `modulith verify --update-baseline` regenerates the file
- [ ] `tests/test_ratchet.py`

### Transactional outbox (the differentiator)
- [ ] `modulith/builtin/outbox.py` — outbox plugin, retry loop, completion modes
- [ ] `modulith/adapters/postgres_outbox.py` — Postgres + SQLAlchemy adapter
- [ ] Schema migrations (alembic)
- [ ] `tests/test_outbox.py` — crash recovery, retry, dead-lettering

### CLI
- [ ] `modulith/cli.py` — typer-based commands
- [ ] `modulith dev` — like `uvicorn --reload` with banner
- [ ] `modulith run` — production mode
- [ ] `modulith verify` — boundary checks
- [ ] `modulith docs` — generate documentation
- [ ] `modulith outbox {status,retry,purge}` — operational commands
- [ ] `modulith info` — show detected config

### Documentation generator
- [ ] `modulith/builtin/docs.py` — Mermaid diagrams + Markdown canvases
- [ ] Architecture diagram
- [ ] Per-module canvas
- [ ] Event flow sequence diagram

### Real documentation (the project lives or dies on this)
- [ ] Updated `README.md` with v1 surface
- [ ] Architecture guide (how modulith works internally)
- [ ] Migration guide for existing FastAPI apps
- [ ] API reference (auto-generated from docstrings)
- [ ] Cookbook with 5-10 common patterns

### Phase 1 kill criteria

**Stop and reassess if any of these miss:**
- The outbox doesn't pass crash-recovery tests by week 3
- Sync entrypoint doesn't integrate cleanly with FastAPI sync views by week 4
- The verifier produces too many false positives to be useful in real codebases by week 5

---

## Phase 2 — v1.1 Polish (2-3 weeks)

Quality-of-life improvements that take v1 from "usable" to "good."

### Testing
- [ ] `modulith/testing.py` — pytest plugin with fixtures
- [ ] `modulith_app` fixture — fresh runtime per test
- [ ] `modulith_module` fixture — module-isolated tests
- [ ] `scenario` fixture — fluent event-driven test API
- [ ] `@pytest.mark.modulith_isolated` — subprocess isolation
- [ ] `tests/test_testing_plugin.py` — meta tests

### Audit + Doctor
- [ ] `modulith/audit.py` — codebase analysis for brownfield migration
- [ ] `modulith/doctor.py` — health diagnostics
- [ ] `modulith audit` CLI command
- [ ] `modulith doctor` CLI command
- [ ] Tests for both

### Observability
- [ ] `modulith/builtin/observability.py` — OpenTelemetry auto-instrumentation
- [ ] Spans for publish + dispatch
- [ ] Soft OTel import (silent no-op if not installed)
- [ ] Tests with mock tracer provider

### Redis Streams broker
- [ ] `modulith/adapters/redis_broker.py` — production-grade implementation
- [ ] Consumer group handling
- [ ] Pending entry recovery on restart
- [ ] Dead-letter handling
- [ ] Integration test with real Redis

### Phase 2 kill criteria

**Stop and reassess if any of these hold:**
- Phase 1 didn't get adoption (zero non-team users) by end of Phase 2 prep
- Test plugin is too magical to debug — users would rather wire fixtures themselves

---

## Phase 3 — v2 Process-Per-Module (3-4 weeks)

The differentiator. Makes "modulith now, microservices later" credible.

- [ ] `modulith/_worker.py` — per-module FastAPI app generator
- [ ] `modulith/supervisor.py` — process orchestration with crash recovery
- [ ] `modulith/proxy.py` — reverse proxy for routing requests to workers
- [ ] Cross-process event integration (events flow through broker when topology != "single")
- [ ] Topology configuration in pyproject + CLI flags
- [ ] Health check propagation
- [ ] Graceful shutdown cascade
- [ ] Documentation: deployment patterns, scaling stories

### Phase 3 kill criteria

**Defer indefinitely if:**
- v1 + v1.1 hit production usage and process-per-module isn't requested
- Subinterpreters become viable (3.13 free-threaded reaches good ecosystem support)
  — at that point process-per-module is obsolete; pivot to subinterpreter mode

---

## Phase 4 — Ecosystem Adapters (ongoing, post-v1)

- [ ] Kafka broker (`modulith[kafka]`)
- [ ] RabbitMQ broker (`modulith[rabbitmq]`)
- [ ] MongoDB outbox store (`modulith[mongo]`)
- [ ] AWS SQS broker (`modulith[sqs]`)
- [ ] Subinterpreter topology (when 3.13 ecosystem ready)
- [ ] Django integration (separate package: `django-modulith`)

These are demand-driven. Ship them when users actually ask, not before.

---

## Total budget

| Phase | Duration | Cumulative |
|---|---|---|
| Phase 0 | (done) | 0 |
| Phase 1 | 4-6 weeks | 4-6 weeks |
| Phase 2 | 2-3 weeks | 6-9 weeks |
| Phase 3 | 3-4 weeks | 9-13 weeks |

**Realistic v1 ship: 3 months from Phase 1 start.**
**Extension to v2: 3-4 months total.**

The honest expectation: the dominant failure mode for projects of this
scope is shipping nothing. Every phase has independent value: Phase 1
shipping alone is a real product. Each phase's kill criterion is a
real off-ramp, not a formality.

---

## Decisions to revisit at each phase boundary

1. **Internal-first or open-source-first?** Recommendation in SPEC.md
   §17.3 is internal-first. Reconsider at start of each phase based on
   what the team actually needs.
2. **Is the addressable market real?** Adoption signal at Phase 1 ship
   determines whether Phase 2 happens.
3. **Is process-per-module worth the complexity?** Phase 3 is the
   biggest commitment; if no one's asking by Phase 2, defer.
4. **What's the next adapter?** Demand-driven. Watch what users ask for.
