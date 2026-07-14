# modulith — 100% Production-Readiness Implementation Plan

> **Mission:** Take `modulith` from "Phase 0 done, rest skeletal" to "v1 production-ready
> framework that delivers on its three-tier promise."
> **Brutal-truth grade (updated 2026-06-26):** Phases 1–3 are now **code-complete** — every
> former skeleton is implemented (0 `NotImplementedError` across `modulith/`; +3769 LOC since
> Phase 0). Behavioral verification (full `ruff`/`mypy --strict`/`pytest` gate suite on the
> expanded surface) completed 2026-06-26 — all gates green after declaring 2 missing deps + formatting; see STATUS.
> **Target ship:** v1 (Phase 0 + 1) in 4–6 weeks. v1.1 (Phase 2) +2–3 weeks.
> v2 (Phase 3) +3–4 weeks.

---

## STATUS (live)

**Pre-Phase-1 Hardening: ✓ COMPLETE (2026-05-07)**

| Task | Status | Notes |
|---|---|---|
| T0.1 — Fix `@listener` wrapping (B1) | ✓ | `inspect.unwrap()` peels `@functools.wraps`; 8 decorator tests |
| T0.2 — `EventPublication` field optionalization | ✓ | `event_type`/`listener`/`published_at` now optional; 7 type tests |
| T0.3 — Env-var bool coercion (B5) | ✓ | Phantom bug — already returned bool; locked in by identity test |
| T0.4 — `tests/conftest.py` shared fixtures | ✓ | `make_fake_app()` factory; 6 smoke tests; `fake_app` back-compat shim |
| T0.5 — CI scaffolding | ✓ | `.github/workflows/ci.yml` — 4 jobs (test matrix 3.11/3.12/3.13, lint, typecheck, build) |
| T0.6 — Pin runtime deps + extras upper bounds | ✓ | `modulith[all,kafka]` resolves; ruff per-file ignores + mypy excludes for stub files |
| T0.7 — Fix `_read_pyproject()` subtable handling | ✓ | P0 bug discovered during T0.3: TOML subtables (`[tool.modulith.outbox]`, `.workers`) crashed bootstrap on this repo's own `pyproject.toml`. Fixed via `SUBTABLE_FIELD` mapping. |

**Verification gate (all green)**
- `pytest`: 57 passed (29 baseline + 28 new)
- `ruff check`: clean (was 75 issues)
- `ruff format --check`: 38 files clean
- `mypy --strict`: clean on 15 Phase 0 source files (was 43 errors)
- Wheel build: clean
- Bootstrap from repo root: clean (was previously crashing on subtables)

**Phase 1 dependency order:** Manifest → Sync → Outbox plugin → Postgres adapter → Verifier → Docs gen → CLI

**Status (2026-06-26):** Phases 1–3 **code-complete** (0 `NotImplementedError`) and **all gates green** — independently verified in a clean Python 3.11 `.[dev]` venv: `ruff check` ✓, `ruff format --check` ✓, `mypy --strict` ✓ (33 files), `pytest` ✓ (269 passed; 1 Redis integration test skipped, gated on `MODULITH_TEST_REDIS_URL`). Reaching green required three fixes applied this session: declared `alembic` (→ `postgres` extra) and `aiosqlite` (→ `test` extra) — both imported by shipped code/tests but previously in no extra — and ran `ruff format` on 19 files.

---

## 0. Context

`modulith` is a Python framework implementing the modular monolith pattern (Spring Modulith
for Python). The SPEC.md is exhaustive (1,164 lines) and the design is sound. Phase 0
(plugin contract, auto-discovery, in-memory bus, configuration) is genuinely complete with
29 passing tests. **Everything else *was* a high-quality skeleton** — the stub files contained
detailed TODO comments specifying implementations step-by-step; those have since been implemented
(see STATUS, 2026-06-26).

The user's explicit ask: brutal truth, granular tasks, production-ready, useful.
This plan is the execution surface for that.

**Why this plan exists:** Without a granular implementation map, this codebase will follow
the failure mode SPEC §1.5 names explicitly — "Eight months of design, no v1, no users,
repo archived in 2027." The phases below are time-boxed with kill criteria; each phase
ships independently valuable software.

---

## 1. Brutal Truth Assessment

### 1.1 What works (Phase 0 — verified)

| Area | Status | Evidence |
|---|---|---|
| Public API exports | ✓ Complete | `modulith/__init__.py` exports 14 symbols cleanly |
| `@event`, `@listener`, `publish`, `configure` | ✓ Complete | `decorators.py`; tested in `test_zero_config.py` |
| Configuration loading | ✓ Complete | `config.py`; 13 tests in `test_config.py` |
| Auto-discovery | ✓ Complete | `discovery.py` + `builtin/discovery.py`; tests in `test_discovery.py` |
| In-memory event bus | ✓ Complete | `event_bus.py` — async-correct, exception-safe |
| Plugin manager | ✓ Complete | `manager.py`; 6 tests in `test_plugin_manager.py` |
| Broker registry | ✓ Complete | `brokers.py` — scheme dispatch, dup/unknown errors |
| Hookspecs (10) | ✓ Complete | `hooks.py` — clean contracts, TYPE_CHECKING guard |
| Driver protocols (3) | ✓ Complete | `protocols.py` — runtime_checkable |
| Types | ✓ Complete | `types.py` — frozen dataclasses, enums |
| Runtime singleton + lazy bootstrap | ✓ Complete | `runtime.py` — double-checked locking correct |

**Reference implementations (work but not production-wired):**
- `examples/redis_streams_broker.py` (77 lines) — complete working Redis Streams broker
- `examples/naming_convention_verifier.py` — complete custom verifier plugin

### 1.2 What does NOT work (the gap)

| File | Status | Notes |
|---|---|---|
| `modulith/sync.py` | STUB | `publish_sync()`, `wrap_sync_listener()` raise NotImplementedError |
| `modulith/manifest.py` | STUB | `declare_module()`, `verify_manifest()` raise NotImplementedError |
| `modulith/cli.py` | STUB | All 10 commands raise NotImplementedError |
| `modulith/audit.py` | STUB | All 6 functions raise NotImplementedError (Phase 2) |
| `modulith/doctor.py` | STUB | All 6 health checks raise NotImplementedError (Phase 2) |
| `modulith/testing.py` | STUB | pytest fixtures, scenario API stubbed (Phase 2) |
| `modulith/_worker.py` | STUB | `create_app()` raises NotImplementedError (Phase 3) |
| `modulith/supervisor.py` | STUB | All 7 methods raise NotImplementedError (Phase 3) |
| `modulith/proxy.py` | STUB | `create_proxy_app()` raises NotImplementedError (Phase 3) |
| `modulith/builtin/outbox.py` | STUB | Hook impls raise NotImplementedError when called |
| `modulith/builtin/verifier.py` | STUB | All 5 rules + AST walker stubbed |
| `modulith/builtin/observability.py` | STUB | OTel hooks raise NotImplementedError |
| `modulith/builtin/docs.py` | PARTIAL | Plumbing works; diagram generators are TODO stubs |
| `modulith/adapters/postgres_outbox.py` | STUB | All 6 PublicationStore methods empty |
| `modulith/adapters/redis_broker.py` | STUB | Constructor raises NotImplementedError |

### 1.3 Latent bugs (visible by inspection — fix during implementation)

- **B1** `decorators.py:57` — `inspect.iscoroutinefunction(func)` fails when `@listener`
  wraps a `@functools.wraps`-decorated function. Use `inspect.unwrap()` first.
- **B2** `runtime.py:_bootstrap()` — never loads manifests after discovery. When manifest
  feature ships, add manifest loading + `verify_manifest()` call to step 6.
- **B3** `manager.py:42-44` — `BUILTIN_PLUGINS` only includes discovery (intentional;
  others not yet implemented). When implementing each built-in, register it here.
- **B4** `testing.py:163-168` — `Scenario.call()` stores positional args but loses kwargs.
  Add `self._initial_call_kwargs = kwargs`.
- **B5** `config.py:146` — env-var "true"/"yes" stored as string, not bool. Coerce explicitly.
- **B6** `EventPublication.event_type` & `.listener` fields — required strings but no code
  populates them at publish time. Wire population in `Runtime.publish()` and outbox plugin.

### 1.4 What "100% production-ready" means

Production-ready ≠ feature-complete. It means the **claimed features** in README.md and
SPEC.md actually work, with:

1. **Behavioral test coverage ≥ 90%** on shipped subsystems (per SPEC Appendix B).
2. **Crash recovery test passing** for the outbox (kill -9 between save and dispatch).
3. **Real Postgres + Redis integration tests** in CI (testcontainers).
4. **Documentation complete**: README usage, architecture guide, migration guide,
   API reference, cookbook with 5+ recipes.
5. **CI gates**: pytest, ruff, mypy --strict, modulith verify (self-applied), build wheel.
6. **No NotImplementedError reachable from any documented public API**.
7. **One real reference application** (`examples/demo_app/`) demonstrating all features across documented deployment modes (in-memory, durable outbox, process-per-module) with runnable recipes per mode.

### 1.5 Risk register

| ID | Risk | Probability | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Scope creep — Phase 1 expands and never ships | HIGH | FATAL | Time-boxing in §6; kill criteria at week 3 (outbox), 4 (sync), 5 (verifier) |
| R2 | Outbox crash-recovery proves harder than estimated | MED | HIGH | Build crash test on day 1 (T1.5.1); use as forcing function |
| R3 | SQLAlchemy `after_commit` event semantics differ across versions | MED | MED | Pin `sqlalchemy>=2.0,<3.0`; test against 2.0 latest |
| R4 | Verifier produces too many false positives | MED | MED | Ratcheting mode + `disabled_rules` config; test against this repo + 2 brownfield apps |
| R5 | `publish_sync()` in FastAPI sync views deadlocks | LOW | HIGH | Detection branch tested separately; integration test with FastAPI sync TestClient |
| R6 | Adoption risk — zero non-author users by Phase 2 end | HIGH | HIGH | Build internal pilot first per SPEC §17.3; don't open-source until validated |
| R7 | Plugin contract requires breaking change post-1.0 | LOW | FATAL | 10 hookspecs frozen NOW; additions only via new hookspecs |

---

## 2. Strategy

**Sequence:** Pre-Phase-1 Hardening → Phase 1 (v1) → Phase 2 (v1.1) → Phase 3 (v2).

**Within Phase 1, dependency order:**
```
Manifest → Sync → Outbox plugin → Postgres adapter → Verifier → Docs gen → CLI
            ↘                  ↗
             Testing harness (parallel)
```

Manifest first because verifier depends on it. Sync second because outbox depends on
session contextvar discipline. Outbox plugin before adapter because adapter assumes
plugin's `_current_session` and dispatch hook semantics. Verifier in parallel with outbox
once manifest is done. Docs gen depends on manifest. CLI last (wires everything).

**Per-task acceptance pattern:** Each task has (1) file path, (2) function signature,
(3) acceptance criteria, (4) test path, (5) effort estimate, (6) dependencies.

**Test discipline:** Every implementation task ships with a test in the same PR.
Tests live next to the existing pattern (`tests/test_*.py`). Use the `fake_app` fixture
pattern from `tests/test_zero_config.py` for integration tests.

---

## 3. Pre-Phase-1 Hardening (Day 0–2)

Small, blocking fixes and infrastructure before serious implementation begins.

### T0.1 — Fix `@listener` decorator wrapping (B1)
- **File:** `modulith/decorators.py:57`
- **Change:** Replace `inspect.iscoroutinefunction(func)` with check that uses
  `inspect.unwrap(func)` to peel through `functools.wraps` chains.
- **Acceptance:** Test in `tests/test_decorators.py` (new) — `@listener` accepts
  `@wraps`-decorated async function without TypeError.
- **Effort:** 15 min + test.

### T0.2 — Fix `EventPublication.event_type` / `.listener` population
- **Files:** `modulith/types.py`, `modulith/runtime.py`, `modulith/event_bus.py`.
- **Change:** Make `event_type` and `listener` optional in `EventPublication` dataclass
  for now (they're required by outbox, populated at outbox-save time). Verify nothing
  currently constructs `EventPublication` outside type stubs — if so, fix call sites.
- **Acceptance:** `EventPublication(id=uuid4(), payload=b"")` works; outbox plugin sets
  the rest at save time.
- **Effort:** 30 min + smoke test.

### T0.3 — Coerce env-var booleans (B5)
- **File:** `modulith/config.py:146`
- **Change:** `result["production"] = True` (bool, not the string).
- **Acceptance:** New test in `test_config.py` asserting `cfg.production is True`
  (identity, not truthy).
- **Effort:** 5 min + test.

### T0.4 — Add `tests/conftest.py` with shared fixtures
- **File:** `tests/conftest.py` (new)
- **Content:** Move the `fake_app` factory pattern from `test_zero_config.py` into a
  shared fixture so future test files reuse it. Keep the existing test passing.
- **Acceptance:** All current 29+ tests still pass; new `make_fake_app(modules: dict)`
  helper available.
- **Effort:** 1 hour.

### T0.5 — Set up CI scaffolding
- **Files:** `.github/workflows/ci.yml` (new), `tox.ini` or pytest cfg verified.
- **Content:** GitHub Actions workflow running:
  - `pytest -v` on Python 3.11, 3.12, 3.13
  - `ruff check .`
  - `mypy --strict modulith/`
  - Build wheel via `pip install build && python -m build`
- **Acceptance:** CI green on `main` after baseline.
- **Effort:** 1–2 hours.

### T0.6 — Pin runtime deps explicitly in `pyproject.toml`
- **File:** `pyproject.toml`
- **Change:** Verify the version pins in extras are tight (`sqlalchemy>=2.0,<3.0`,
  `redis>=5.0,<6.0`, `aiokafka>=0.10,<0.13`, etc.). Add upper bounds.
- **Acceptance:** `pip install modulith[all]` resolves on a fresh venv.
- **Effort:** 30 min.

---

## 4. Phase 1 — v1 Essentials (4–6 weeks)

### 4.1 Manifest System (week 1, ~150 LOC)

#### T1.1.1 — Implement `declare_module()`
- **File:** `modulith/manifest.py:78-101`
- **Spec:**
  1. Use `sys._getframe(1)` to get caller's frame.
  2. Read `frame.f_globals["__name__"]`.
  3. Strip trailing `._manifest` suffix → derive package name.
  4. Build `Manifest` dataclass with all fields.
  5. Raise `ConfigurationError` if `_manifests[package]` already exists.
  6. Store in `_manifests[package]`.
- **Acceptance:** `declare_module(publishes=["X"])` from `myapp/orders/_manifest.py`
  creates `Manifest(package="myapp.orders", publishes=("X",))`.
- **Test:** `tests/test_manifest.py` (new) — calling from a fake module via
  `runpy.run_module()` produces correct manifest; double-call raises.
- **Effort:** 2 hours.

#### T1.1.2 — Implement `verify_manifest()`
- **File:** `modulith/manifest.py:114-132`
- **Spec:** Per the docstring TODO — check listeners-declared-vs-registered, return
  list of error strings.
- **Acceptance:** Manifest declaring missing listener returns error mentioning the
  listener name.
- **Test:** `tests/test_manifest.py` — manifest with non-registered listener returns
  one error; correct manifest returns empty list.
- **Effort:** 1 hour.

#### T1.1.3 — Wire manifest loading into bootstrap (B2)
- **File:** `modulith/runtime.py:_bootstrap()` (insert step 6.5 between steps 6 and 7)
- **Spec:**
  1. After module discovery (step 6), iterate `manifest.all_manifests()`.
  2. Build a set of currently-registered listener callables from event bus.
  3. For each manifest, call `verify_manifest(manifest, registered_set)`.
  4. If any errors, raise `ConfigurationError` listing all errors with module names.
- **Acceptance:** Module declaring listener that didn't register fails bootstrap with
  clear error message.
- **Test:** `tests/test_manifest.py` — fake_app with bad manifest fails bootstrap.
- **Effort:** 1 hour.

#### T1.1.4 — Discover `_manifest.py` files during module import
- **File:** `modulith/builtin/discovery.py` — extend to also import `<module>._manifest`
  if it exists, as part of module loading.
- **Acceptance:** `_manifest.py` is auto-imported when its parent module is discovered;
  manifest registers as a side effect.
- **Test:** Existing fake_app fixture extended with `_manifest.py` — assert manifest in
  `all_manifests()` after bootstrap.
- **Effort:** 30 min + test.

### 4.2 Sync Entrypoint (week 1–2, ~150 LOC)

#### T1.2.1 — Implement `_get_or_create_loop()`
- **File:** `modulith/sync.py:44-65`
- **Status:** Already mostly implemented; verify thread-safety + add cleanup hook.
- **Acceptance:** Repeated calls return same loop; loop runs on daemon thread.
- **Test:** `tests/test_sync.py` (new) — `_get_or_create_loop()` is idempotent.
- **Effort:** 30 min.

#### T1.2.2 — Implement `publish_sync()`
- **File:** `modulith/sync.py:73-123`
- **Spec:**
  1. Build coroutine: `coro = _runtime.publish(event)`.
  2. Try `asyncio.get_running_loop()`. On RuntimeError, set `running_loop = None`.
  3. If `running_loop`: `future = asyncio.run_coroutine_threadsafe(coro, running_loop)`.
  4. Else: `loop = _get_or_create_loop()`; `future = run_coroutine_threadsafe(coro, loop)`.
  5. `future.result(timeout=timeout)` — catch `concurrent.futures.TimeoutError`,
     log warning, raise `TimeoutError`.
- **Acceptance:** Works from script (no loop), works from FastAPI sync view (loop in
  another thread), respects timeout.
- **Tests:**
  - `tests/test_sync.py::test_publish_sync_no_loop` — pure sync context.
  - `tests/test_sync.py::test_publish_sync_with_running_loop` — async test that creates
    a thread, calls `publish_sync` from it.
  - `tests/test_sync.py::test_publish_sync_timeout` — listener that hangs raises
    `TimeoutError`.
- **Effort:** 3 hours including tests.

#### T1.2.3 — Implement `wrap_sync_listener()`
- **File:** `modulith/sync.py:134-156`
- **Spec:**
  1. `@functools.wraps(func)` to preserve `__name__`, `__qualname__`, `__annotations__`.
  2. Define `async def wrapper(event)`:
     ```python
     loop = asyncio.get_running_loop()
     ctx = contextvars.copy_context()
     await loop.run_in_executor(None, ctx.run, func, event)
     ```
  3. Return wrapper.
- **Acceptance:** Sync listener registered via `@listener` runs in executor; contextvars
  propagate (so `_current_session` is visible to sync DB code).
- **Test:** `tests/test_sync.py::test_sync_listener_runs_in_executor` —
  `threading.current_thread()` inside listener differs from main loop's thread.
- **Effort:** 2 hours including tests.

#### T1.2.4 — Update `@listener` to accept sync functions
- **File:** `modulith/decorators.py:48-95`
- **Change:** Replace the `if not inspect.iscoroutinefunction(unwrapped): raise TypeError`
  block with: if sync, call `wrap_sync_listener(func)` and register the wrapped form;
  preserve original for testing visibility.
- **Acceptance:** `@listener def handler(event: X) -> None` (sync) works.
- **Test:** `tests/test_sync.py::test_sync_listener_via_decorator` — full E2E.
- **Effort:** 1 hour.

### 4.3 Outbox Plugin Core (week 2, ~250 LOC)

#### T1.3.1 — Implement `outbox.configure()`
- **File:** `modulith/builtin/outbox.py:57-78`
- **Spec:**
  1. Bind globals: `_store`, `_serializer`, `_dead_letter_after_attempts`,
     `_retry_interval_seconds`.
  2. Start retry loop as background task: `asyncio.create_task(_retry_loop())`,
     store reference for shutdown.
  3. Crash recovery: schedule one-shot dispatch sweep with `older_than=timedelta(0)`.
  4. Register a `_runtime` shutdown hook to cancel retry task gracefully.
- **Acceptance:** Calling `outbox.configure(store, serializer)` does not raise; retry
  task starts; subsequent `find_incomplete()` calls fire from the loop.
- **Test:** `tests/test_outbox.py::test_configure_starts_retry_loop` — use a stub store
  that records `find_incomplete` calls; assert called within retry interval.
- **Effort:** 3 hours including tests.

#### T1.3.2 — Implement `modulith_before_event_published` hook
- **File:** `modulith/builtin/outbox.py:85-112`
- **Spec:**
  1. Read `_current_session.get()`. If None → return (in-memory bus dispatches directly).
  2. Resolve listeners for `type(event)` from event bus.
  3. For each listener:
     - Build `EventPublication(id=uuid4(), event_type=fqcn, payload=serializer.serialize(event),
       listener=listener.__qualname__, published_at=utcnow(), completed_at=None)`.
     - `await _store.save(pub)`.
     - Append `pub.id` to `session.info["_modulith_pending"]` list.
- **Acceptance:** Publishing inside a transaction context creates one publication per
  listener; nothing dispatches before commit.
- **Test:** `tests/test_outbox.py::test_publish_in_transaction_persists` — bind fake
  session, publish event, assert listeners array on session.info.
- **Effort:** 4 hours including tests.

#### T1.3.3 — Implement `_dispatch_publication()`
- **File:** `modulith/builtin/outbox.py:128-146`
- **Spec:** Per docstring TODO — deserialize, lookup listener by name, call, mark
  complete or dead-letter on failure.
- **Acceptance:** Successful dispatch calls `mark_complete`. Failed dispatch increments
  `attempt_count`, sets `last_error`, leaves incomplete (or dead-letters at threshold).
- **Test:** `tests/test_outbox.py::test_dispatch_success_marks_complete` and
  `tests/test_outbox.py::test_dispatch_failure_increments_count`.
- **Effort:** 3 hours.

#### T1.3.4 — Implement `_retry_loop()`
- **File:** `modulith/builtin/outbox.py:149-166`
- **Spec:** Per docstring TODO — sleep, find incomplete, dispatch with backoff,
  loop. Catch `CancelledError` and re-raise.
- **Acceptance:** Loop polls every `_retry_interval_seconds`; respects exponential
  backoff per publication; cancels cleanly on shutdown.
- **Test:** `tests/test_outbox.py::test_retry_loop_processes_incomplete` — pre-load
  store with one incomplete publication, start loop, await dispatch.
- **Effort:** 3 hours.

#### T1.3.5 — Implement maintenance APIs
- **File:** `modulith/builtin/outbox.py:173-185`
- **Spec:** `status()`, `force_retry()`, `purge_completed()` per docstring.
- **Acceptance:** Each returns expected counts/effects against a stub store.
- **Test:** `tests/test_outbox.py::test_maintenance_apis`.
- **Effort:** 2 hours.

#### T1.3.6 — Register outbox in `BUILTIN_PLUGINS` (B3)
- **File:** `modulith/manager.py:42-44`
- **Change:** Add `"modulith.builtin.outbox"` to the tuple.
- **Acceptance:** Plugin manager loads outbox plugin by default.
- **Test:** `tests/test_plugin_manager.py` — assert outbox hook is registered.
- **Effort:** 5 min + test.

### 4.4 Postgres Outbox Adapter (week 2–3, ~250 LOC)

#### T1.4.1 — Define SQLAlchemy schema
- **File:** `modulith/adapters/postgres_outbox.py` (replace commented section lines 41–68)
- **Spec:** Implement `EventPublicationRow` per the commented-out spec. Use proper
  SQLAlchemy 2.0 declarative mapping (Mapped, mapped_column).
- **Acceptance:** `Base.metadata.create_all(engine)` produces correct table + partial index.
- **Test:** `tests/test_postgres_adapter.py::test_schema_creates_cleanly` (using
  `pytest-postgresql` or `testcontainers-postgres`).
- **Effort:** 1.5 hours.

#### T1.4.2 — Provide alembic migration
- **File:** `modulith/adapters/migrations/versions/0001_initial.py` (new)
- **Spec:** Alembic migration creating `event_publications` table + partial index +
  `event_publications_archive` table.
- **Acceptance:** `alembic upgrade head` against empty DB succeeds.
- **Effort:** 1 hour.

#### T1.4.3 — Implement `_install_session_hooks()`
- **File:** `modulith/adapters/postgres_outbox.py:105-127`
- **Spec:** Per docstring — `sa_event.listens_for(session_class, "after_commit")` that
  pops `_modulith_pending` and schedules dispatch via `asyncio.create_task` on the
  running loop. **Critical:** the after_commit callback runs synchronously; the
  dispatch must be scheduled, not awaited.
- **Acceptance:** Commit fires dispatch; rollback drops pending without dispatch.
- **Test:** `tests/test_postgres_adapter.py::test_after_commit_dispatches`,
  `::test_rollback_drops_pending`.
- **Effort:** 4 hours.

#### T1.4.4 — Implement `save()`, `mark_complete()`, `find_incomplete()`,
   `archive()`, `delete()`
- **File:** `modulith/adapters/postgres_outbox.py:129-177`
- **Spec:** Per docstring TODOs. Use `_current_session.get()` for `save`; new short
  session for the others.
- **Acceptance:** Each method's contract verified against real Postgres.
- **Test:** `tests/test_postgres_adapter.py` — one test per method.
- **Effort:** 5 hours including tests.

#### T1.4.5 — Crash-recovery integration test (THE forcing function)
- **File:** `tests/test_outbox_crash_recovery.py` (new, marked `@pytest.mark.integration`)
- **Spec:**
  1. Spawn subprocess running app: publish 100 events in a transaction.
  2. After commit but before retry loop completes, send SIGKILL.
  3. Restart in new process. Verify all 100 events eventually deliver to listener.
  4. Verify no duplicates beyond at-least-once expectations (idempotency tagging).
- **Acceptance:** Test passes in CI on every run; no flake.
- **Effort:** 1 day. **This is the kill criterion test for week 3.**

### 4.5 JSON EventSerializer

#### T1.5.1 — Implement default JSON serializer
- **File:** `modulith/serializers.py` (new, ~80 LOC)
- **Spec:**
  - `JsonEventSerializer` class implementing `EventSerializer` Protocol.
  - `serialize(event)`: extract dataclass fields → `json.dumps(...).encode()`.
  - `deserialize(data, event_type)`: `importlib.import_module()` + `getattr` to find
    the class; `json.loads` + `cls(**dict)`.
  - Handle `datetime`, `UUID`, `Decimal` via custom encoder/decoder.
- **Acceptance:** Round-trip serialization preserves dataclass equality.
- **Test:** `tests/test_serializers.py` (new) — round-trip + edge types.
- **Effort:** 3 hours.

### 4.6 Boundary Verifier (week 3–4, ~350 LOC)

#### T1.6.1 — Implement `_collect_imports()`
- **File:** `modulith/builtin/verifier.py:94-122`
- **Spec:** Per docstring — `Path.rglob`, `ast.parse`, walk Import + ImportFrom, resolve
  relatives, skip TYPE_CHECKING blocks, return `list[ImportRecord]`.
- **Acceptance:** Given a fake module tree, returns expected import set.
- **Test:** `tests/test_verifier.py::test_collect_imports` — synthetic directory with
  known imports, assert exact `ImportRecord` list.
- **Effort:** 4 hours.

#### T1.6.2 — Implement Rule 1: `_check_no_internal_imports()`
- **File:** `modulith/builtin/verifier.py:129-141`
- **Spec:** For each ImportRecord, check if `target_module` starts with
  `<other_module_package>._internal`.
- **Acceptance:** `myapp.orders` importing `myapp.inventory._internal.foo` produces a
  Violation.
- **Test:** `tests/test_verifier.py::test_internal_import_violation`.
- **Effort:** 1.5 hours.

#### T1.6.3 — Implement Rule 4: `_check_uses_contracts_module()`
- **File:** `modulith/builtin/verifier.py:144-161`
- **Spec:** Per docstring — flag uppercase-starting names imported from another
  module's package (not contracts).
- **Acceptance:** `from myapp.inventory import StockItem` (where StockItem is uppercase)
  produces Violation.
- **Test:** `tests/test_verifier.py::test_contracts_violation`.
- **Effort:** 2 hours.

#### T1.6.4 — Implement Rule 3: `_check_declared_dependencies()`
- **File:** `modulith/builtin/verifier.py:164-177`
- **Spec:** Per docstring — only flag if manifest exists and target outside declared deps.
- **Acceptance:** Manifest declaring `["payments"]` flags imports from `inventory`.
- **Test:** `tests/test_verifier.py::test_declared_dependencies`.
- **Effort:** 2 hours.

#### T1.6.5 — Implement `detect_cycles()` (Rule 2)
- **File:** `modulith/builtin/verifier.py:184-197`
- **Spec:** Implement Tarjan's SCC inline (~30 LOC) — avoid networkx dependency.
- **Acceptance:** Cycle A→B→A detected; non-cyclic graph returns empty.
- **Test:** `tests/test_verifier.py::test_cycle_detection` (3 cases: linear, simple
  cycle, complex multi-cycle).
- **Effort:** 3 hours.

#### T1.6.6 — Implement baseline file management
- **File:** `modulith/builtin/verifier.py:213-244`
- **Spec:** `load_baseline()`, `filter_against_baseline()`, `write_baseline()` per
  docstrings. Stable JSON with sorted keys.
- **Acceptance:** Baseline round-trips; filtering removes grandfathered violations.
- **Test:** `tests/test_verifier.py::test_baseline_*` (3 tests).
- **Effort:** 3 hours.

#### T1.6.7 — Add Rule 5: data ownership (best-effort)
- **File:** `modulith/builtin/verifier.py` (new function)
- **Spec:** Static analysis for `Table("orders")`, `select(orders_table)`, etc. Cross-
  reference with manifests' `owns_tables`. Flag mismatches as warnings.
- **Acceptance:** Test against fake module declaring `owns_tables=["orders"]` and
  another module containing `Table("orders")` — produces warning.
- **Test:** `tests/test_verifier.py::test_data_ownership_warning`.
- **Note:** Mark as best-effort; full coverage is v1.1 with runtime SQLAlchemy events.
- **Effort:** 4 hours.

#### T1.6.8 — Register verifier in `BUILTIN_PLUGINS`
- **File:** `modulith/manager.py:42-44`
- **Change:** Add `"modulith.builtin.verifier"`.
- **Acceptance:** `modulith verify` invokes the verifier.
- **Effort:** 5 min.

### 4.7 Documentation Generator (week 4, ~200 LOC)

#### T1.7.1 — Implement architecture diagram
- **File:** `modulith/builtin/docs.py:73-97`
- **Spec:** Build edges from manifest dependencies + observed cross-module imports;
  render as Mermaid C4.
- **Acceptance:** Generated `.mmd` file renders a connected graph in a Mermaid viewer.
- **Test:** `tests/test_docs.py::test_architecture_diagram` — string-match key lines.
- **Effort:** 3 hours.

#### T1.7.2 — Implement module canvas
- **File:** `modulith/builtin/docs.py:100-163`
- **Spec:** Per docstring — pull from manifest, generate Markdown with Public API,
  Events Published, Events Consumed, Owned Tables, Dependencies, Internal Files.
- **Acceptance:** Canvas for a fake module shows all declared sections.
- **Test:** `tests/test_docs.py::test_module_canvas`.
- **Effort:** 3 hours.

#### T1.7.3 — Implement event flow diagram
- **File:** `modulith/builtin/docs.py:166-187`
- **Spec:** Sequence diagram: for each event type, show publishers → bus → listeners.
- **Acceptance:** Mermaid sequence diagram with arrows.
- **Test:** `tests/test_docs.py::test_event_flow`.
- **Effort:** 2 hours.

#### T1.7.4 — Register docs plugin in `BUILTIN_PLUGINS`
- **File:** `modulith/manager.py:42-44`
- **Change:** Add `"modulith.builtin.docs"`.
- **Effort:** 5 min.

### 4.8 CLI (week 5, ~250 LOC)

#### T1.8.1 — Implement `modulith info`
- **File:** `modulith/cli.py:215-232`
- **Spec:** Bootstrap, print package, modules, config, plugins, brokers, manifest status.
- **Acceptance:** Output is human-readable; exit 0 on success.
- **Test:** `tests/test_cli.py::test_info` (use typer's `CliRunner`).
- **Effort:** 2 hours.

#### T1.8.2 — Implement `modulith dev` (single-process path only for v1)
- **File:** `modulith/cli.py:46-70`
- **Spec:** `os.execvp("uvicorn", ["uvicorn", app_module, "--reload", ...])` after
  printing banner. Process-per-module path: defer to Phase 3.
- **Acceptance:** `modulith dev myapp:app` execs uvicorn correctly.
- **Test:** `tests/test_cli.py::test_dev_invokes_uvicorn` (mock execvp).
- **Effort:** 2 hours.

#### T1.8.3 — Implement `modulith run` (single-process path only for v1)
- **File:** `modulith/cli.py:77-91`
- **Spec:** Like `dev` minus `--reload`.
- **Effort:** 1 hour.

#### T1.8.4 — Implement `modulith verify`
- **File:** `modulith/cli.py:98-121`
- **Spec:** Bootstrap, iterate modules, call `modulith_verify_module` hook for each,
  run `detect_cycles()`, aggregate. If `mode=="ratchet"`, load baseline + filter.
  If `--update-baseline`, write baseline. Print grouped output. Exit 0 if clean else 1.
- **Acceptance:** CI-ready exit codes; output matches Spring Modulith aesthetic.
- **Test:** `tests/test_cli.py::test_verify_*` (clean, dirty, ratchet, update).
- **Effort:** 4 hours.

#### T1.8.5 — Implement `modulith docs`
- **File:** `modulith/cli.py:128-139`
- **Spec:** Bootstrap, call `modulith_render_documentation` aggregate hook,
  print produced files.
- **Acceptance:** Files appear in target directory.
- **Test:** `tests/test_cli.py::test_docs_command`.
- **Effort:** 1 hour.

#### T1.8.6 — Implement `modulith outbox status/retry/purge`
- **File:** `modulith/cli.py:191-208`
- **Spec:** Each calls the corresponding outbox plugin function. Need a way to
  bootstrap with the configured outbox; reuse `_runtime.ensure_bootstrapped()`.
- **Acceptance:** Each subcommand works against a real Postgres in integration test.
- **Test:** `tests/test_cli.py::test_outbox_*` + integration in
  `tests/test_postgres_adapter.py`.
- **Effort:** 3 hours.

### 4.9 Real Documentation (week 5–6, no code)

#### T1.9.1 — Update README.md with v1 surface
- **File:** `README.md`
- **Spec:** Replace "Phase 0 done, Phase 1 in active development" status with v1 ship
  status. Update CLI table, install instructions, comparison table. Add real
  screenshots of `modulith info` output, `modulith verify` output, generated docs.
- **Acceptance:** A new user can copy-paste their way to a working app in under 5 min.
- **Effort:** 1 day.

#### T1.9.2 — Architecture guide
- **File:** `docs/architecture.md` (new)
- **Spec:** ~3,000 words. Bootstrap sequence, plugin contract, event flow,
  outbox lifecycle, verification model. Diagrams via Mermaid.
- **Effort:** 1.5 days.

#### T1.9.3 — Migration guide enhancements
- **File:** `MIGRATION_GUIDE.md` (existing — enhance with screenshots and a worked
  example: real FastAPI app of moderate size, before/after diff).
- **Effort:** 1 day.

#### T1.9.4 — API reference
- **File:** `docs/api/` directory + `mkdocs.yml`
- **Spec:** Set up mkdocs + mkdocstrings. Auto-generate from docstrings. Manual ToC
  organizing public API by audience (app authors, plugin authors, adapter authors).
- **Acceptance:** `mkdocs serve` renders cleanly; nav matches SPEC's three audiences.
- **Effort:** 1 day.

#### T1.9.5 — Cookbook (5 recipes minimum)
- **File:** `docs/cookbook/` directory
- **Recipes:**
  1. Basic publish/listen with FastAPI
  2. Outbox with Postgres + SQLAlchemy
  3. Custom verifier rule (extends `examples/naming_convention_verifier.py`)
  4. Sync FastAPI views with `publish_sync()`
  5. Adopting on existing codebase (link to MIGRATION_GUIDE)
- **Effort:** 1.5 days.

### 4.10 Phase 1 verification gates

Before declaring Phase 1 done, ALL of the following must be green:

- [ ] All 29 existing tests still pass
- [ ] New tests: ≥ 60 covering all Phase 1 features
- [ ] Behavioral coverage ≥ 90% on `modulith/` (measured by `pytest-cov`)
- [ ] **Crash recovery integration test passes consistently** (T1.4.5)
- [ ] `mypy --strict modulith/` clean
- [ ] `ruff check .` clean
- [ ] `modulith verify` self-applied to this repo: clean (or baselined)
- [ ] CI green on Python 3.11, 3.12, 3.13
- [ ] At least one external user (or internal pilot) successfully ran a real app

---

## 5. Phase 2 — v1.1 Polish (2–3 weeks)

### 5.1 pytest-modulith plugin (week 7, ~250 LOC)

#### T2.1.1 — `modulith_app` fixture
- **File:** `modulith/testing.py:60-84`
- **Spec:** Per-test runtime reset; spy plugin recording all hook calls; restoration
  of `sys.modules` snapshot.
- **Effort:** 4 hours.

#### T2.1.2 — `modulith_module` fixture (context manager)
- **File:** `modulith/testing.py:91-108`
- **Spec:** Module isolation — load only one module; mock others by name.
- **Effort:** 4 hours.

#### T2.1.3 — `Scenario.within()`
- **File:** `modulith/testing.py:180-200`
- **Spec:** Per docstring — trigger the publish/call, poll for expected event with
  timeout, assert.
- **Effort:** 3 hours.

#### T2.1.4 — Fix `Scenario.call()` kwargs (B4)
- **File:** `modulith/testing.py:163-168`
- **Change:** Add `self._initial_call_kwargs = kwargs`; use in `within()`.
- **Effort:** 15 min.

#### T2.1.5 — Subprocess-per-test isolation marker
- **File:** `modulith/testing.py` + pytest hook
- **Spec:** `@pytest.mark.modulith_isolated` runs each marked test in its own subprocess.
- **Effort:** 4 hours.

#### T2.1.6 — Tests for the testing plugin (meta)
- **File:** `tests/test_testing_plugin.py`
- **Effort:** 1 day.

### 5.2 Audit Tool (week 7–8, ~200 LOC)

#### T2.2.1 — Implement `audit_codebase()`
- **File:** `modulith/audit.py`
- **Spec:** Per the per-function TODOs in the file:
  - `_propose_modules()` — folder layout + import-graph clustering
  - `_collect_cross_module_imports()` — AST walk for direct deps
  - `_collect_shared_tables()` — find SQLAlchemy `Table()` calls referenced from multiple
    folders
  - `_collect_listener_candidates()` — pattern-match `on_*`, `handle_*` functions
  - `_compute_readiness_score()` — % of cross-module via events vs direct
- **Effort:** 3 days.

#### T2.2.2 — Implement `render_report()`
- **File:** `modulith/audit.py`
- **Spec:** Markdown with proposed module structure, violations to expect, tables to
  decide, score with breakdown.
- **Effort:** 4 hours.

#### T2.2.3 — Wire `modulith audit` CLI
- **File:** `modulith/cli.py:146-160`
- **Effort:** 1 hour.

#### T2.2.4 — Tests for audit
- **File:** `tests/test_audit.py`
- **Effort:** 1 day.

### 5.3 Doctor Command (week 8, ~150 LOC)

#### T2.3.1 — Implement health checks
- **File:** `modulith/doctor.py`
- **Spec:** Per per-function TODOs — boundary health, split readiness, schema drift,
  outbox health, listener registration coverage.
- **Effort:** 2 days.

#### T2.3.2 — Wire `modulith doctor` CLI + tests
- **Files:** `modulith/cli.py:167-180`, `tests/test_doctor.py`
- **Effort:** 1 day.

### 5.4 OpenTelemetry Observability (week 8–9, ~150 LOC)

#### T2.4.1 — Decide hookspec extension
- **Decision needed:** SPEC notes that `modulith_on_listener_dispatch` lacks a paired
  "after_dispatch" hook, blocking proper span ending. Add 11th hookspec
  `modulith_on_listener_complete(event, listener_name, publication, exception=None)`
  before implementation.
- **File:** `modulith/hooks.py`
- **Effort:** 1 hour. **Note:** This is technically a contract change. Since v1 is
  pre-release, acceptable.

#### T2.4.2 — Implement OTel hooks
- **File:** `modulith/builtin/observability.py:76-149`
- **Spec:** Per TODOs — start spans, set attributes, end spans on complete/error.
- **Effort:** 1.5 days.

#### T2.4.3 — Register observability plugin
- **File:** `modulith/manager.py:42-44`
- **Effort:** 5 min.

#### T2.4.4 — Tests with mock tracer
- **File:** `tests/test_observability.py`
- **Effort:** 4 hours.

### 5.5 Production Redis Streams Broker (week 9, ~120 LOC)

#### T2.5.1 — Promote example to production adapter
- **Source:** `examples/redis_streams_broker.py` (the working reference).
- **Target:** `modulith/adapters/redis_broker.py`
- **Spec:** Copy + add: consumer groups (XGROUP CREATE on init), pending entry
  recovery on restart (XCLAIM stale), dead-letter via `MAXLEN ~`, structured
  logging.
- **Effort:** 1.5 days.

#### T2.5.2 — Integration tests with real Redis
- **File:** `tests/test_redis_broker.py` (new, marked integration)
- **Spec:** Use `testcontainers-redis`. Test publish, consume, restart-replay,
  consumer-group failover.
- **Effort:** 1 day.

### 5.6 Phase 2 verification gates

- [ ] All Phase 1 gates still green
- [ ] New tests: ≥ 30 for Phase 2 features
- [ ] Real Redis integration test passes in CI
- [ ] OTel example with Jaeger renders span tree correctly
- [ ] At least 2 external/internal users on Phase 1, requesting (or already using)
      Phase 2 features. **If zero adopters → kill criterion triggered; reassess.**

---

## 6. Phase 3 — v2 Process-Per-Module (3–4 weeks)

### 6.1 Worker module (week 10, ~120 LOC)

#### T3.1.1 — Implement `_worker.create_app()`
- **File:** `modulith/_worker.py:30-96`
- **Spec:** Per docstring 8-step TODO. Read env, configure runtime with
  `auto_discover=False`, selectively import the configured module + contracts,
  build FastAPI app with health endpoint and module's router, subscribe to broker
  for cross-module events.
- **Effort:** 1 day.

#### T3.1.2 — Tests
- **File:** `tests/test_worker.py`
- **Effort:** 4 hours.

### 6.2 Supervisor (week 10–11, ~250 LOC)

#### T3.2.1 — Implement `Supervisor` (subprocess management)
- **File:** `modulith/supervisor.py`
- **Spec:** Per per-method TODOs. `_spawn`, `_monitor_worker`, `_forward_logs`,
  `start`, `stop`. Exponential backoff per spec.
- **Effort:** 3 days.

#### T3.2.2 — Implement `derive_specs_from_config()`
- **File:** `modulith/supervisor.py:204-211`
- **Effort:** 4 hours.

#### T3.2.3 — Tests (subprocess-based, marked integration)
- **File:** `tests/test_supervisor.py`
- **Spec:** Test: starts workers, stops gracefully, restarts crashed worker.
- **Effort:** 1.5 days.

### 6.3 Reverse Proxy (week 11–12, ~150 LOC)

#### T3.3.1 — Implement `create_proxy_app()`
- **File:** `modulith/proxy.py:34-95`
- **Spec:** Per docstring TODO — FastAPI catch-all route, httpx.AsyncClient,
  hop-by-hop header filtering, streaming.
- **Effort:** 1.5 days.

#### T3.3.2 — Implement `_match_rule()`
- **File:** `modulith/proxy.py:98-106`
- **Effort:** 30 min.

#### T3.3.3 — Implement actuator endpoints (`/_modulith/topology`, `/_modulith/health`)
- **File:** `modulith/proxy.py`
- **Effort:** 4 hours.

#### T3.3.4 — Tests
- **File:** `tests/test_proxy.py`
- **Effort:** 1 day.

### 6.4 Cross-process event integration (week 12, ~100 LOC)

#### T3.4.1 — Update event bus to route through broker when topology != "single"
- **File:** `modulith/event_bus.py` (extend) + `modulith/runtime.py:_bootstrap()`
  (conditional broker bus creation).
- **Spec:** When `topology="processes"`, in the `_worker` process, only listen on
  the events declared `consumes` for the local module. Other publish() calls go to
  broker.
- **Effort:** 2 days.

#### T3.4.2 — Tests
- **File:** `tests/test_cross_process.py` (integration)
- **Effort:** 1 day.

### 6.5 CLI completion (week 12–13)

#### T3.5.1 — Wire `modulith dev/run` process-per-module path
- **File:** `modulith/cli.py:46-91` (extend with topology branch)
- **Effort:** 4 hours.

### 6.6 Phase 3 verification gates

- [ ] All prior gates still green
- [ ] Real-app integration test: 3 modules, 3 processes, broker routing, end-to-end
- [ ] Crash test: kill one worker, supervisor restarts within 5s, no event loss
- [ ] Process-per-module example app added to `examples/`

---

## 7. Cross-Cutting Concerns

### 7.1 CI/CD pipeline (continuous, started in T0.5)

- GitHub Actions: pytest matrix, lint, mypy, build wheel
- Integration tests: testcontainers (Postgres, Redis) — runs on PR + nightly
- Crash recovery test: dedicated job, 5 retries to surface flake
- Coverage report → Codecov
- Auto-deploy to TestPyPI on tag; PyPI on stable tag (manual approval)

### 7.2 Release engineering

- Versioning: 0.1.0 → 0.2.0 (Phase 1 ship) → 0.3.0 (Phase 2) → 1.0.0 (after internal
  validation, per SPEC §17.3).
- CHANGELOG.md generated from commits using conventional commits.
- Release notes drafted per phase ship.

### 7.3 Self-application

`modulith` should use `modulith` on its own codebase from Phase 1 ship onward:
- `modulith/` root has its own `_manifest.py` files for `builtin/`, `adapters/`.
- `modulith verify` runs in CI.
- Generated docs published to `docs/site/`.

This is the most credible dogfooding signal possible.

### 7.4 Reference application

Build `examples/demo_app/` — a real working FastAPI app with three modules
(orders, inventory, payments), Postgres outbox, demonstrating every claim in the
README. Used as:
- The MIGRATION_GUIDE worked example
- A subject for crash-recovery integration tests
- A demo for adoption pitching

---

## 8. Production-Readiness Final Checklist

Beyond per-phase gates, the v1.0.0 release requires:

- [ ] Crash recovery test in CI: 1,000 iterations passing without manual intervention
- [ ] Docs deployed to readthedocs.io with full nav
- [ ] PyPI package installable: `pip install modulith[all]` resolves and imports
- [ ] `modulith --version` reports correct version
- [ ] `modulith info` produces useful output for a fresh app
- [ ] `LICENSE` file present (Apache 2.0)
- [ ] CONTRIBUTING.md added
- [ ] Security policy (`SECURITY.md`)
- [ ] Issue templates + PR template in `.github/`
- [ ] Coverage badge, CI badge, PyPI badge in README
- [ ] At minimum one external production user with a positive testimonial OR
      explicitly defer open-source per SPEC §17.3

---

## 9. Critical Files Map (priority-ordered)

### Pre-Phase-1 hardening
1. `modulith/decorators.py:57` — fix `@listener` wrapping (B1)
2. `modulith/types.py` — make `event_type`/`listener` optional in `EventPublication`
3. `modulith/config.py:146` — coerce env booleans (B5)
4. `tests/conftest.py` — extract shared fixtures
5. `.github/workflows/ci.yml` — CI scaffolding

### Phase 1 (in dependency order)
6. `modulith/manifest.py` — `declare_module`, `verify_manifest`
7. `modulith/runtime.py:_bootstrap()` — wire manifest loading (B2)
8. `modulith/sync.py` — `publish_sync`, `wrap_sync_listener`
9. `modulith/decorators.py` — accept sync listeners
10. `modulith/serializers.py` — JSON event serializer (new file)
11. `modulith/builtin/outbox.py` — outbox plugin
12. `modulith/manager.py:42-44` — register outbox (B3)
13. `modulith/adapters/postgres_outbox.py` — Postgres adapter
14. `modulith/adapters/migrations/` — alembic migration (new directory)
15. `modulith/builtin/verifier.py` — boundary verifier
16. `modulith/manager.py` — register verifier
17. `modulith/builtin/docs.py` — docs generator
18. `modulith/manager.py` — register docs
19. `modulith/cli.py` — CLI commands
20. `README.md`, `docs/`, `MIGRATION_GUIDE.md` — documentation

### Phase 2
21. `modulith/testing.py` — pytest plugin
22. `modulith/audit.py` — codebase audit
23. `modulith/doctor.py` — health checks
24. `modulith/builtin/observability.py` — OTel
25. `modulith/hooks.py` — add `modulith_on_listener_complete` hookspec
26. `modulith/adapters/redis_broker.py` — Redis production adapter

### Phase 3
27. `modulith/_worker.py` — worker factory
28. `modulith/supervisor.py` — process supervisor
29. `modulith/proxy.py` — reverse proxy
30. `modulith/event_bus.py` — broker-backed bus extension
31. `modulith/cli.py` — process-per-module CLI paths

---

## 10. Verification Plan (How to Test)

### Unit tests (per task)
Each task includes its test in the same file/PR. Run via `pytest tests/test_<feature>.py`.

### Integration tests (per phase)
Marked `@pytest.mark.integration`; run via `pytest -m integration`. Require:
- `testcontainers-postgres` for outbox tests
- `testcontainers-redis` for broker tests
- Local Python 3.11+ subprocess capability

### End-to-end tests (Phase 3 + final)
Reference app in `examples/demo_app/`. Drive via `subprocess.Popen` invocations of
`modulith run`. Validate:
- Single-process mode: 3 modules, events flow in-memory, outbox persists
- Process mode: 3 modules in 3 processes, events flow via Redis, supervisor manages
- Crash recovery: kill one worker mid-burst; supervisor restarts; no events lost

### Performance tests (Phase 1 + Phase 3)
- Outbox throughput: ≥ 10,000 publications/sec on commodity hardware (single-publisher)
- Cross-process latency: p95 ≤ 5ms on localhost via Redis Streams
- Bootstrap time: ≤ 200ms cold start for app with 5 modules

### Chaos tests (Phase 3, optional but recommended)
- Random kill -9 of workers
- Network partition between supervisor and worker (iptables)
- Postgres unavailable for 30s during outbox dispatch

### Self-application gate
`modulith verify` run on this repo passes. `modulith docs` produces clean output for
this repo. This is the most credible production-readiness signal.

---

## 11. Time Budget (calibrated to SPEC §XV)

| Phase | Duration | Cumulative | Output |
|---|---|---|---|
| Pre-Phase-1 | 2 days | 2 days | Hardened foundation, CI green |
| Phase 1 | 4–6 weeks | ~6 weeks | v1.0 (or 0.2.0 release) |
| Phase 2 | 2–3 weeks | ~9 weeks | v1.1 (0.3.0 release) |
| Phase 3 | 3–4 weeks | ~13 weeks | v2.0 (1.0.0 release after validation) |

**Realistic v1 ship: 6 weeks from Phase 1 start, assuming full-time focused work.**

Per SPEC §17.4: the failure mode to avoid is designing forever and never shipping.
Each phase has independent value; Phase 1 alone is a real product.

---

## 12. Decision Points (revisit at each phase boundary)

1. **Internal-first vs open-source-first?** Default: internal per SPEC §17.3.
2. **Add Rule 5 (data ownership) static analysis depth?** Default: best-effort in v1;
   runtime SQLAlchemy events in v1.1.
3. **OTel hookspec change (T2.4.1)?** Pre-1.0; acceptable. Post-1.0: only via new
   hookspec.
4. **Phase 3 commit?** Defer if Phase 1+2 hit production but no one asks for processes
   (per SPEC §XV Phase 3 kill criterion).
5. **MongoDB outbox + Kafka broker?** Demand-driven (Phase 4); ship only if asked.

---

## 13. What I'm NOT including (deliberately out of scope)

- **Subinterpreter topology** (PEP 734) — wait for 3.13 ecosystem maturity
- **Django integration** — separate package post-v1; SPEC explicitly excludes Django
- **MongoDB outbox** — Phase 4, demand-driven
- **GraphQL/gRPC adapters** — out of scope for v1; design has no opinion yet
- **Auto-scaling supervisor** — Phase 3 ships fixed worker counts; auto-scale is v2.1+
- **Horizontal multi-host topology** — single-host only; multi-host is microservices

---

## 14. Quick reference: per-phase ship criteria

**Phase 1 (v1) ships when:**
- All Phase 1 verification gates green
- Reference app in `examples/demo_app/` works end-to-end
- README updated; install instructions accurate
- One internal pilot using outbox in production

**Phase 2 (v1.1) ships when:**
- pytest-modulith installable and used in this repo's own tests
- `modulith audit` produces actionable output for one real brownfield codebase
- OTel spans visible in Jaeger/Tempo for the demo app

**Phase 3 (v2) ships when:**
- 3-process demo app handles 1,000 RPS without event loss
- Crash recovery: 1,000-iteration kill test passes
- Documentation includes deployment guide for process mode

---

*End of plan. Total estimated effort: 9–13 weeks for v2 production-ready, 6 weeks for
v1. Calibration assumes single full-time focus; double for part-time.*
