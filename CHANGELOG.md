# Changelog

All notable changes to modulith are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Upgrade notes

- Durable outbox with callable-instance or bound-method listeners: drain the outbox before upgrading. Their stored listener ids change, so incomplete rows stored under the old ids match no listener after the upgrade and dead-letter, and replaying them fails the same way
- Rolling deploys whose events carry a subclass instance in a field declared as its base class: upgrade consumers before producers, because a consumer from before the upgrade cannot decode the new subclass envelope
- Process-per-module topology with an `outbox` other than `memory`: set `outbox_url` (`MODULITH_OUTBOX_URL`) or bind a store with `outbox.configure()` in every worker, because a worker without a bound store now refuses to start
- Database broker with Alembic-managed tables: run migration `0006_broker_dispatch_started`; self-bootstrapped tables gain the column automatically
- SHM broker in production: set `state_dir`, or pin `sqlite_path`/`url` to an absolute path. The default store location is keyed on the package's install path, so a redeploy to a new path starts an empty store
- Process-per-module topology with a module named `health`: that module's root GET route no longer answers `GET /health`, because the worker's own `/health` answers that path first. Serve it from another path, such as `/health/`; startup logs a WARNING naming the hidden route
- Durable outbox with `claim_strategy="advisory_lock"`: advisory locks now take connections from a second pool sized like the store engine's. With a `QueuePool` (the async engine default), budget up to twice `pool_size` + `max_overflow` Postgres connections per process; `NullPool` and `max_overflow=-1` bound neither pool, so a burst opens one lock connection per in-flight row

### Added

- `modulith broker drop-group <group>` removes a consumer group's subscriptions and pending deliveries on the SHM and database brokers after confirmation, and `--target <target>` removes only that target's subscription and deliveries. It names the store it acts on (the SHM file, or the database URL with its password masked), never creates a missing store or schema, warns when the group is the only subscriber of a target, stating what later publishes to it will do under the broker's policy in effect, warns when the database broker's `expected_consumer_groups` still lists the group and will keep queuing messages for it, and exits 1 when nothing matched. Without `--force` it refuses a group the current deployment still derives or that a consumer served in the last 24 hours. Running consumers re-stamp their subscriptions hourly, but consumers from earlier releases do not, so upgrade them first. `modulith run` warns about consumer groups that no current module derives and no consumer has served in 24 hours, because a retired module's group keeps every publication pinned. Nothing is removed automatically
- Durable outbox: `outbox = "postgres"` plus the new `outbox_url` (`MODULITH_OUTBOX_URL`) builds and binds the store in every process, including process-topology workers and the `modulith outbox` CLI, and applies the claim keys of `[tool.modulith.outbox_options]` (`claim_strategy`, `claim_lease_seconds`, `claim_batch_size`) to it; an explicit `outbox.configure()` still wins. The new `outbox.start()` starts the retry loop from an ASGI lifespan, and workers call it at startup

### Changed

- **Breaking:** a process-topology worker whose `outbox` is not `memory` refuses to start without a bound store (see Upgrade notes)
- SHM broker: startup logs the resolved store path at INFO, naming the SQLite file and whether its location is the default or explicit. `modulith run --topology processes` warns when `state_dir` is not set and no absolute `sqlite_path`/`url` pins the store. README, DEPLOYMENT and COOKBOOK say to set `state_dir` in production
- Database broker, process topology: a worker whose module has no listeners now builds an empty consumer, without a poll task, so startup can warn about subscriptions its group still holds, as SHM workers already did. Its `/health` therefore reports that consumer's readiness (`{"status": "ready", "ready": true}`) instead of `{"status": "ok"}`
- Database and SHM consumers store at most the first 500 characters of a failing listener's error, as the outbox does; the full text is still logged

### Fixed

- Durable outbox: one publication is no longer delivered twice when after-commit dispatch races a sweep. Under `claim_strategy="lease"`, after-commit dispatch claims its row with the sweep's lease, and a sweep's lease renewal fails on a completed row; a row a crashed process was delivering is recovered once its lease expires, normally within `claim_lease_seconds` plus `retry_interval_seconds`. Under `claim_strategy="advisory_lock"`, the sweep re-reads each row after locking and skips one a peer already completed, dead-lettered or deleted, or that is still backing off, and after-commit dispatch holds the row's advisory lock and skips a row a peer's sweep holds. Advisory locks use a second connection pool (see Upgrade notes); an after-commit dispatch or a sweep that waits past `pool_timeout` for a lock connection logs one WARNING and leaves the row, uncharged, to a later sweep
- Durable outbox (`"lease"` claim strategy): a lease renewal that raises, for example on a dropped database connection, no longer stops renewals or surfaces as an error after a successful delivery; it is logged and retried until the lease runs out
- Durable outbox (SQLAlchemy store, `"lease"` claim strategy): on MySQL and SQLite, concurrent sweepers could claim and deliver the same publications. Off Postgres, `claim_batch` now takes each row with a conditional `UPDATE` that re-checks the lease and keeps only the rows it won. Postgres keeps `FOR UPDATE SKIP LOCKED`
- Durable outbox: callable-instance listeners (objects defining `__call__`) now get distinct, restart-stable stored ids; before, two of them for one event shared an id, so one ran twice and the other never ran. Callable-instance and bound-method listeners are stored as `<class module>.<ClassName>`, prefixed with the registering module package (`orders:shared.X`) when registered from a module, so one class instantiated in two modules keeps separate rows; plain-function ids are unchanged. Two listeners that would still share an id raise `ConfigurationError` on the first durable publish (see Upgrade notes)
- Durable outbox: a publish that reaches a bound session after its last commit, which includes every FastAPI `BackgroundTasks` publish under the old COOKBOOK `get_db`, is no longer discarded silently. A rollback or close that discards outbox publications logs a WARNING naming the event types, and the COOKBOOK's `get_db` now commits after `yield`, so such publishes are delivered
- Durable outbox: a task started with `asyncio.create_task` inside a request bound with `bind_session` no longer loses events it publishes after `unbind_session`; those publishes now dispatch directly, like any unbound publish. Before, they were added to the finished request's session and silently dropped. Internal: `modulith.builtin.outbox._current_session` now holds a binding holder; adapters that read it directly should call `outbox._bound_session()`
- Durable outbox: a worker sharing an outbox table with other workers never charges attempts on rows for listeners it does not run
- A broker target with whitespace around its scheme or destination, such as `@externalized(target="redis-streams: orders.placed")`, published to a destination no consumer read, silently losing the event on the Redis, SHM and database brokers. Publishing, consuming, runtime routing and outbox dispatch now strip both parts and split on the first colon only; `@externalized` stores the normalized target and raises `ConfigurationError` at decoration time when the scheme or destination is empty
- Database and SHM brokers: a consumer claims only deliveries for targets it currently consumes. After a listener moves or is removed, including on the upgrade past the sibling-listener fix below, the group's consumer no longer dead-letters every publish to the old target with an ERROR. It warns once at start with the pending count, and `modulith broker drop-group <group> --target <target>` removes that target's subscription and deliveries. Nothing is removed automatically
- Database and SHM brokers: a consumer that crash-looped or kept restarting on one poison message no longer dead-letters the other messages claimed in the same batch that had not started dispatching; an attempt is charged only when a message's dispatch has started. The database broker gains `broker_message.dispatch_started` through migration `0006_broker_dispatch_started` (self-bootstrapped tables gain it automatically); SHM stores gain the column when opened
- Database broker: `publish_sync()` against Postgres or MySQL no longer fails with "attached to a different loop" when a worker's consumer shares the broker; calls from another event loop now run on the loop that owns the engine's connection pool, which must stay running and unblocked
- Database broker: MySQL servers older than 8.0.1 and MariaDB servers older than 10.6 are rejected with a `ConfigurationError` at consumer start naming the server version and the minimum; before, every claim failed with a `SKIP LOCKED` syntax error. The release is read from the leading numbers of the server's `VERSION()`, after MariaDB's `5.5.5-` prefix, so distribution-suffixed and MariaDB Enterprise (`10.6.12-8-MariaDB-enterprise`) version strings are recognized
- Database broker: in-memory SQLite URLs select `StaticPool` explicitly instead of relying on SQLAlchemy 2.1's deprecated inference from `mode=memory`
- SHM broker: a full store no longer blocks its consumers. Publishes are refused a small consumer reserve (32 pages, or one eighth of a store under 256 pages) below `max_store_bytes`, while claims, acks, fails, dead-letters and prunes always commit, past `max_store_bytes` if they must. Consumers therefore drain the backlog in either completion mode, including a store filled by an earlier release or reopened with a lower limit; before, every claim failed with "database or disk is full". `max_store_bytes` bounds what publishes add, not the file: consumer writes can grow `broker.db` past it, and the WAL file is not counted. A subscribe into a full store no longer fails and stops the worker: the group's subscription is always recorded, and the replay of a newly subscribed target's retained publications adds only those the group lacks, oldest first, and stops 8 pages below the publish budget, so a small publish that fit before the replay still fits after it. Under `completion_mode="mark"` the replay uses at most half the room left below that limit, because claiming and acking its rows grows them in place. A replay cut short logs one WARNING per target naming the group, the target, the replayed and skipped counts, and how to recover the skipped publications without losing work. When SQLite will not raise its page limit, the store-full error names the SQLite version
- SHM broker: `orphan_retention_seconds` is now an SHM broker option (`MODULITH_BROKER_ORPHAN_RETENTION_SECONDS`); the default stays 24 hours. The store-full error and the docs state the sizing rule, sustainable publish rate ≈ `max_store_bytes / (bytes per publication × orphan_retention_seconds)`, cover backlog, retired groups, and mark-mode and dead-letter retention, and point to `modulith broker drop-group`; they no longer suggest pruning, which cannot free retained publications. `orphan_retention_seconds` above 100 years is rejected at construction on both brokers; on the database broker it used to raise `OverflowError` at publish
- Redis Streams broker: `max_stream_len` / `MODULITH_STREAM_MAXLEN` and `dlq_max_stream_len` / `MODULITH_BROKER_DLQ_MAX_STREAM_LEN` must now be positive integers. Zero, negative or non-numeric values raise `ConfigurationError` at broker construction; before, 0 made every XADD trim the stream to empty, so published events were silently lost
- Redis Streams broker: a non-numeric `MODULITH_BROKER_MAX_PAYLOAD_BYTES` now raises `ConfigurationError` at startup instead of a bare `ValueError`
- Consumers (database, SHM, Redis Streams): a transient failure to ack, fail, dead-letter or renew no longer leaves `/health` degraded forever. The failure clears when the same operation next succeeds on that target, or expires after the redelivery window (`reclaim_stale_seconds`; Redis `reclaim_min_idle_ms`); on Redis it also clears once the message is no longer pending. An operation that keeps failing keeps the consumer degraded
- Consumers: a listener that never returns, or a Redis server that stops answering, no longer leaves a consumer silently stalled while `/health` reports ready. The database and SHM consumers report `degraded` once a batch outlives the renew deadline and log the stuck event type and row. The Redis consumer reports `degraded` when no read completes for five block intervals. The Redis client now defaults `socket_timeout` to the block time plus 5 s and enables `socket_keepalive`; values in the URL win
- Process-per-module topology: a worker now runs only the listeners of its own module, including a module package that an entry-point plugin imports during bootstrap. Before, a sibling module the worker's package imported had its listeners run in that worker too, so one event could run a listener twice, once in the wrong process; transactional (outbox) publishes also persisted rows for those sibling listeners. Each worker's consumer now accepts only its own modules' event types. SPEC and the migration guide now say that plugin and hook listeners suppress broker routing of the non-externalized events they handle, and that a listener in a plain, non-package file or in a namespace folder without `__init__.py` runs in every worker that imports it
- Process-per-module topology: the worker port base is configurable (`worker_port_base`, `MODULITH_WORKER_PORT_BASE`, `modulith run --worker-port-base`, default 9001), and `modulith run` refuses a proxy port that collides with a worker port before spawning anything. The proxy checks each worker's per-deployment identity before forwarding to it, again after the worker is marked down or foreign, and as soon as its worker process exits, whether or not the supervisor restarts it, and answers 503 "foreign deployment" for another deployment's worker. The check catches an accidental port collision; its token is not a secret and does not defend against a hostile local process. The check never fails a slow worker: it is bounded at 30 s and 64 KiB, and past the deadline the request gets 504 without the worker being marked down. Requests that arrive while a worker's identity check is in flight wait on that one check, so a stalled worker ties up a single probe connection. A module named `health` can no longer shadow the worker's own `/health`: the worker answers `GET /health` itself, and startup logs a WARNING naming the module route that path hides; the module's other routes, including `/health/`, are unaffected
- Process-per-module supervisor: a worker's exit is noticed at once even when a helper process the worker started still holds its output pipes. Before, the restart, the crash-loop breaker and `stop()` all waited for that helper to exit, possibly forever. When such a helper still holds the worker's port, the restart waits up to `restart_max_delay` for the port to be released and logs one WARNING; the wait does not count as a crash, and past it the respawn goes ahead
- Process-per-module proxy: the deployment is no longer capped at 100 in-flight requests. The upstream pool is bounded by `MODULITH_PROXY_MAX_CONNECTIONS` (default 1000). A full pool answers 503 without marking the worker down and logs a WARNING naming the pool (request or health-probe) and its limit; health probes use their own pool
- Serializers: a nested dataclass field declared as a base class now round-trips a subclass instance; the concrete class is resolved only among the declared class's already-imported subclasses, never imported from the payload. `NewType` fields (plain, Optional, list) and type aliases decode as the type they name. Values of exactly the declared class keep their untagged encoding, so stored rows still decode. A consumer that has not imported the subclass decodes the value as the declared class when its fields fit, logging one WARNING per unknown tag. A tagged value in an Optional or union field that fails to reconstruct raises; it never reaches the listener as a raw dict. An alias that cannot be evaluated, or a cyclic one, is treated as opaque (see Upgrade notes)
- `modulith audit` without a PATH now audits the application package, `src/<pkg>` or the single top-level package (even beside script folders such as `tools/`), and prints the root it chose; a PATH you pass is audited as given. Before, run from a project root it proposed the whole application as one module and scored any codebase 100/100. With fewer than two module candidates, or when no import crosses module candidates but some reach folders below the audited root that are not candidates, it warns and reports the readiness score as not applicable. Unreadable directories are skipped, a file PATH exits 1, and the report names the audited folder without a machine-specific absolute path
- `modulith verify` and `modulith extract` now find an application package under a PEP 420 namespace root when another installed portion of that root comes earlier on `sys.path`, the order an editable install's `.pth` file produces. Before, `verify` silently checked no imports in that layout, so it passed apps that broke the boundary rules, and `extract` exited 1 with "could not resolve package directory". In this layout `verify` reports violation locations relative to the project's source root, so a ratchet baseline written in one checkout still grandfathers its violations in another
- `modulith extract` now copies every package-level helper the extracted module, its contracts and its helpers import, transitively, including helpers imported as `from <pkg> import <submodule>`, which it used to drop silently. An import of another declared module now blocks extraction unless `--force` is passed; the contracts package is copied, not blocked. Before publishing, extract imports the extracted copy, never the monolith, in a subprocess and exits 1 naming the failing import, so the service's third-party dependencies must be installed where it runs. `--force` never skips this check. The check works when the project and Python sit on different Windows drives, and it still catches first-party code imported from the source tree when the project lies inside the virtualenv or interpreter prefix, including on Windows, where `site.getsitepackages()` lists the virtualenv root. When the application resolves to an installed, non-editable copy in site-packages, only its own top-level package counts as first-party, so other installed distributions are not reported. Contracts are resolved the way Python imports them, and a contracts directory without `__init__.py` has its files scanned for helper imports. The package path and every helper's parent folder get an `__init__.py` only where the source has one, so a PEP 420 namespace root stays a namespace, its other installed portions stay importable, and a namespace helper folder does not become an extra module in the service
- Tests marked `@pytest.mark.modulith_isolated` that skip or xfail now report SKIPPED / XFAIL / XPASS instead of PASSED, and an isolated child that exits 0 without running or finishing the test, including through `os._exit(0)` or `pytest.exit(returncode=0)` in its body, fails it. The child's outcome travels over a private result file, so a user's `--junitxml` is unaffected
- Demo app: in durable mode `POST /orders` commits inside the route before responding, so a failed commit returns an error instead of 200 for an order that was rolled back. The inventory listener records an order as handled only after `StockReserved` is published, so a redelivery after a failed publish publishes again
- Dependencies: the `postgres`, `database` and `test` extras require `sqlalchemy[asyncio]`, because SQLAlchemy 2.1 installs `greenlet` only through that extra and the async adapters cannot import without it; the `database` and `integration` extras cap PyMySQL below 1.2.1, whose releases break aiomysql 0.3.2 on every MySQL write

### Security

- The process-per-module reverse proxy now builds each upstream URL from the matched backend's scheme, host and port, and matches routing rules on the same bytes it forwards. A request-target that does not start with `/`, or that contains a `.`/`..` path segment (literal or percent-encoded), now gets 400 and reaches no worker. Before, such a target could send the request, client headers included, to another host, or reach a worker's internal `/health`
- The process-per-module proxy no longer stores upstream cookies. Before, its shared HTTP client kept every `Set-Cookie` a worker sent and replayed it on later requests from other clients and on health probes, so one user's session cookie could reach requests made for another user. A client's own `Cookie` header and upstream `Set-Cookie` responses still pass through unchanged
- The proxy's upstream clients ignore `HTTP_PROXY`/`ALL_PROXY` from the environment. Before, with the common `NO_PROXY=localhost` (which does not match `127.0.0.1`), every proxied request and its credential headers went to the environment's proxy

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
