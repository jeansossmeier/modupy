"""The `modulith doctor` command implementation.

Reports operational and architectural health of a running modulith
application. Useful for both first-run sanity checks and ongoing
monitoring.

Output sections:

  1. Boundary health — violation count, baseline drift
  2. Process-split readiness — % of cross-module interactions via events
  3. Schema drift — events whose definitions changed since the last check
  4. Outbox health — incomplete/dead-letter counts
  5. Listener registration — declared vs actually registered
  6. SHM notifier — whether each SHM broker's hint ring attached
  7. Actuator token — missing bearer token on a multi-process actuator proxy
  8. Single-host broker — a per-host broker (shm/sqlite) under a container runtime
  9. Redis retention — stream max_stream_len sizing and live backlog risk

Each section either reports OK with summary stats or surfaces problems.
A check that raises is reported as an error rather than aborting the run,
so one broken check never hides the others.
"""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .runtime import Runtime

logger = logging.getLogger("modulith.doctor")

_BASELINE_PATH = Path(".modulith-baseline.json")
_SCHEMA_CACHE_PATH = Path(".modulith-schemas.json")

# Outbox-health thresholds (operate on unbounded counts from the store).
# A standing dead-letter pile this large is an outage; a large incomplete
# backlog means the retry loop can't keep up. Heuristics, not hard limits.
_DEAD_LETTER_ERROR_THRESHOLD = 100
_INCOMPLETE_BACKLOG_THRESHOLD = 1000

# An unresponsive store (network partition, stalled connection pool) must
# not hang `modulith doctor` forever — bound the query and report the
# timeout as an error instead.
_OUTBOX_HEALTH_TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# Health report aggregation
# ---------------------------------------------------------------------------


@dataclass
class HealthCheck:
    """One named check with its status and findings."""

    name: str
    status: str  # "ok" | "warn" | "error"
    summary: str  # one-line summary
    details: list[str] = field(default_factory=list)  # multi-line if findings


@dataclass
class HealthReport:
    """Aggregated output of all checks."""

    checks: list[HealthCheck]

    @property
    def overall_status(self) -> str:
        """Worst status across all checks."""
        if any(c.status == "error" for c in self.checks):
            return "error"
        if any(c.status == "warn" for c in self.checks):
            return "warn"
        return "ok"


# ---------------------------------------------------------------------------
# Top-level doctor command
# ---------------------------------------------------------------------------


def run_doctor() -> HealthReport:
    """Bootstrap, run every health check, and return the aggregated report.

    Each check runs independently — an exception in one is captured as an
    error HealthCheck so the others still run.
    """
    from .runtime import _runtime

    _runtime.ensure_bootstrapped()

    checks_spec: list[tuple[str, Callable[[Runtime], HealthCheck]]] = [
        ("boundary health", _check_boundary_health),
        ("process-split readiness", _check_split_readiness),
        ("schema drift", _check_schema_drift),
        ("outbox health", _check_outbox_health),
        ("listener registration", _check_listener_registration),
        ("shm notifier", _check_shm_notifier),
        ("actuator token", _check_actuator_token),
        ("single-host broker", _check_single_host_broker),
        ("redis retention", _check_redis_retention),
    ]

    checks: list[HealthCheck] = []
    for name, check in checks_spec:
        try:
            checks.append(check(_runtime))
        except Exception as exc:  # one broken check must not abort the rest
            logger.exception("doctor check %r failed", name)
            checks.append(HealthCheck(name=name, status="error", summary=f"check raised: {exc}"))
    return HealthReport(checks=checks)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _parse_file(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError, OSError) as exc:
        logger.debug("doctor: skipping unparseable file %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def _check_boundary_health(rt: Runtime) -> HealthCheck:
    """Run the verifier; report violation count and baseline drift."""
    from .builtin.verifier import detect_cycles, filter_against_baseline, load_baseline
    from .types import ViolationSeverity

    modules = rt.modules
    pm = rt.plugin_manager
    violations = []
    for module in modules:
        for result in pm.hook.modulith_verify_module(module=module, all_modules=modules):
            violations.extend(result)
    violations.extend(detect_cycles(modules))

    errors = [v for v in violations if v.severity is ViolationSeverity.ERROR]
    warnings = [v for v in violations if v.severity is ViolationSeverity.WARNING]
    if not errors:
        # Warning-only violations (e.g. data-ownership) must not read as "ok" —
        # they're the framework's best-effort signal and were previously
        # invisible in the overall status. Surface them as a distinct warn.
        if warnings:
            return HealthCheck(
                "boundary health",
                "warn",
                f"0 error(s), {len(warnings)} warning(s)",
                [v.message for v in warnings[:5]],
            )
        return HealthCheck("boundary health", "ok", "0 violation(s)")

    if _BASELINE_PATH.exists():
        baseline = load_baseline(_BASELINE_PATH)
        new = filter_against_baseline(errors, baseline)
        if new:
            return HealthCheck(
                "boundary health",
                "error",
                f"{len(errors)} violation(s), {len(new)} new (not in baseline)",
                [v.message for v in new[:5]],
            )
        return HealthCheck("boundary health", "warn", f"{len(errors)} violation(s), all baselined")

    return HealthCheck(
        "boundary health",
        "error",
        f"{len(errors)} violation(s)",
        [v.message for v in errors[:5]],
    )


def _count_publish_calls(rt: Runtime, module: Any) -> int:
    """Count ``publish``/``publish_sync`` calls in a module's source."""
    from .builtin.verifier import _package_dir

    root = _package_dir(module.package)
    if root is None:
        return 0
    count = 0
    for path in sorted(root.rglob("*.py")):
        tree = _parse_file(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if name in ("publish", "publish_sync"):
                count += 1
    return count


def _check_split_readiness(rt: Runtime) -> HealthCheck:
    """Percentage of cross-module interactions that flow through events.

    The "are you ready to split this module into its own process?" number.
    Direct cross-module imports are couplings that would break under a
    process split; ``publish()`` calls are the event-shaped interactions
    that survive it.
    """
    from .builtin.verifier import _collect_imports, _configured_contracts_module, _owning_module

    modules = rt.modules
    contracts_module = _configured_contracts_module()
    direct_by_module: dict[str, int] = {}
    direct_total = 0
    publish_total = 0

    for module in modules:
        direct = 0
        for record in _collect_imports(module):
            owner = _owning_module(record.target_module, modules)
            # The contracts module is the sanctioned shared dependency that
            # survives a process split — importing it is the prescribed pattern,
            # not coupling. Mirror the verifier's rule-4 exemption so the
            # readiness score doesn't penalize the recommended structure.
            if owner is not None and owner.name != module.name and owner.name != contracts_module:
                direct += 1
        direct_by_module[module.name] = direct
        direct_total += direct
        publish_total += _count_publish_calls(rt, module)

    total = direct_total + publish_total
    if total == 0:
        return HealthCheck("process-split readiness", "ok", "no cross-module interactions detected")

    score = round(100 * publish_total / total)
    # MIGRATION_GUIDE.md's documented milestones are inclusive: 80%+ = ready
    # to split a module into its own process, 95%+ = ready to extract a
    # microservice. Below that the score is an informational
    # maturity signal and caps at "warn" — never "error": doctor doubles as a
    # CI gate, and a low score is the framework's own recommended starting
    # state for a migration, not a defect.
    if score >= 95:
        status, tier = "ok", " — microservice-ready"
    elif score >= 80:
        status, tier = "ok", " — process-split ready"
    else:
        status, tier = "warn", ""
    top = sorted(direct_by_module.items(), key=lambda kv: -kv[1])[:5]
    details = [f"{name}: {n} direct cross-module import(s)" for name, n in top if n]
    return HealthCheck(
        "process-split readiness",
        status,
        f"{score}% of cross-module interactions via events{tier}",
        details,
    )


def _event_fingerprints(rt: Runtime) -> dict[str, str]:
    """Map ``module.EventClass`` -> a hash of its declared fields."""
    from .builtin.verifier import _package_dir

    fingerprints: dict[str, str] = {}
    for module in rt.modules:
        root = _package_dir(module.package)
        if root is None:
            continue
        for path in sorted(root.rglob("*.py")):
            tree = _parse_file(path)
            if tree is None:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef) and _has_event_decorator(node):
                    fields = []
                    for stmt in node.body:
                        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                            # Capture the default too: `x: str` → `x: str = ""`
                            # flips a field from required to optional — a real
                            # wire-compatibility change the fingerprint must see.
                            default = ast.unparse(stmt.value) if stmt.value is not None else None
                            fields.append((stmt.target.id, ast.unparse(stmt.annotation), default))
                    digest = hashlib.sha256(repr(sorted(fields)).encode("utf-8")).hexdigest()[:12]
                    fingerprints[f"{module.name}.{node.name}"] = digest
    return fingerprints


def _has_event_decorator(node: ast.ClassDef) -> bool:
    for decorator in node.decorator_list:
        if isinstance(decorator, ast.Name) and decorator.id == "event":
            return True
        if isinstance(decorator, ast.Attribute) and decorator.attr == "event":
            return True
    return False


def _check_schema_drift(rt: Runtime) -> HealthCheck:
    """Warn when an event's fields changed since the last doctor run.

    Fingerprints are cached in ``.modulith-schemas.json``. The first run
    records the baseline; later runs compare and then refresh it, so each
    change is surfaced exactly once (the cue to bump a schema version and
    plan a compatible rollout).
    """
    current = _event_fingerprints(rt)

    if not _SCHEMA_CACHE_PATH.exists():
        _SCHEMA_CACHE_PATH.write_text(
            json.dumps(current, indent=2, sort_keys=True), encoding="utf-8"
        )
        return HealthCheck("schema drift", "ok", f"recorded {len(current)} event schema(s)")

    try:
        cached = json.loads(_SCHEMA_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        # A corrupt cache is itself a finding, not silent noise — surface it
        # and leave the file untouched so it can be inspected. Overwriting it
        # here would destroy the evidence of whatever corrupted it.
        return HealthCheck(
            "schema drift",
            "error",
            f"cache at {_SCHEMA_CACHE_PATH} is corrupt and was left untouched",
            [str(exc)],
        )

    drifted = sorted(k for k, digest in current.items() if k in cached and cached[k] != digest)
    removed = sorted(k for k in cached if k not in current)
    _SCHEMA_CACHE_PATH.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")

    if drifted or removed:
        details = [f"{k} changed" for k in drifted[:5]] + [f"{k} removed" for k in removed[:5]]
        summary_parts = []
        if drifted:
            summary_parts.append(f"{len(drifted)} event schema(s) changed")
        if removed:
            summary_parts.append(f"{len(removed)} event schema(s) removed")
        return HealthCheck(
            "schema drift",
            "warn",
            " and ".join(summary_parts) + " since last check",
            details,
        )
    return HealthCheck("schema drift", "ok", f"{len(current)} event schema(s) stable")


def _check_outbox_health(rt: Runtime) -> HealthCheck:
    """Query the outbox for stuck or dead-lettered events."""
    import asyncio

    cfg = rt.config
    if cfg is None or cfg.outbox == "memory":
        return HealthCheck("outbox health", "ok", "in-memory bus, no durable outbox")

    from .builtin import outbox

    if outbox._store is None:
        return HealthCheck(
            "outbox health",
            "warn",
            f"outbox configured as {cfg.outbox!r} but no store is wired",
        )

    # status() now reports UNBOUNDED counts (via the store's count_open /
    # count_dead_lettered), not a sample capped at find_incomplete's LIMIT 100 —
    # so a real backlog (50k stuck rows) is visible here instead of reading 100.
    # Bounded by a timeout: an unresponsive store (network partition, stalled
    # connection pool) must not hang `doctor` forever.
    try:
        counts = asyncio.run(asyncio.wait_for(outbox.status(), timeout=_OUTBOX_HEALTH_TIMEOUT))
    except TimeoutError:
        return HealthCheck(
            "outbox health",
            "error",
            f"outbox status query timed out after {_OUTBOX_HEALTH_TIMEOUT}s",
        )
    dead = counts["dead_lettered"]
    incomplete = counts["incomplete"]
    summary = f"{incomplete} incomplete, {counts['completed']} completed, {dead} dead-lettered"
    if dead > _DEAD_LETTER_ERROR_THRESHOLD:
        return HealthCheck(
            "outbox health",
            "error",
            summary,
            [f"dead-letter backlog exceeds {_DEAD_LETTER_ERROR_THRESHOLD}"],
        )
    if dead > 0 or incomplete > _INCOMPLETE_BACKLOG_THRESHOLD:
        details: list[str] = []
        if incomplete > _INCOMPLETE_BACKLOG_THRESHOLD:
            details.append(
                f"{incomplete} incomplete publications — the retry loop may be falling behind"
            )
        return HealthCheck("outbox health", "warn", summary, details)
    return HealthCheck("outbox health", "ok", summary)


def _check_listener_registration(rt: Runtime) -> HealthCheck:
    """Compare manifest-declared listeners to actually-registered ones."""
    from . import manifest as manifest_module

    bus = rt.event_bus
    registered: set[Callable[..., Any]] = set()
    if bus is not None:
        for handlers in bus._handlers.values():
            registered.update(handlers)

    manifests = manifest_module.all_manifests()
    if not manifests:
        return HealthCheck("listener registration", "ok", "no manifests declared")

    errors: list[str] = []
    for pkg, m in manifests.items():
        errors.extend(f"[{pkg}] {e}" for e in manifest_module.verify_manifest(m, registered))

    if errors:
        return HealthCheck("listener registration", "error", f"{len(errors)} issue(s)", errors[:5])
    declared = sum(len(m.listeners) for m in manifests.values())
    return HealthCheck(
        "listener registration", "ok", f"{declared} declared listener(s), all registered"
    )


def _check_shm_notifier(rt: Runtime) -> HealthCheck:
    """Report whether each SHM broker's hint ring actually attached.

    A hint file is never re-created once it exists, and attaching requires its
    header to match the configured ``shm_capacity`` exactly — so changing that
    capacity on a deployment whose file already exists leaves the notifier dead
    for every process that starts afterwards. Delivery is unaffected (SQLite
    stays authoritative), but every consumer falls back to its safety poll,
    which costs a poll interval of latency per message. The adapter logs one
    WARNING when it happens (modulith/adapters/shm_broker.py), invisible to
    anyone attaching to an already-running deployment; this makes it a standing
    signal instead.

    Warn, never error: the application is correct, only slower — and doctor
    doubles as a CI gate.

    Detected by duck-typing the broker's hint ring rather than importing the
    adapter, so the check costs nothing for deployments that use a different
    broker (or none).
    """
    registry = rt.broker_registry
    if registry is None:
        return HealthCheck("shm notifier", "ok", "no broker registry (single-process)")

    attached = 0
    detached: list[str] = []
    for scheme in registry.schemes():
        ring = getattr(registry.get(scheme), "_ring", None)
        if ring is None:
            continue  # not an SHM broker — it has no hint ring to attach
        if ring.available:
            attached += 1
        else:
            detached.append(
                f"[{scheme}] hint file {ring.name} not attached at capacity={ring.capacity} "
                "— consumers are polling. Stop every worker and delete the file "
                "to have it re-created at the configured capacity."
            )

    if not detached:
        if attached == 0:
            return HealthCheck("shm notifier", "ok", "no shm broker registered")
        return HealthCheck("shm notifier", "ok", f"{attached} hint ring(s) attached")
    return HealthCheck(
        "shm notifier",
        "warn",
        f"{len(detached)} of {len(detached) + attached} shm broker(s) have no working notifier",
        detached,
    )


# ---------------------------------------------------------------------------
# Actuator token
# ---------------------------------------------------------------------------


def _check_actuator_token(rt: Runtime) -> HealthCheck:
    """Warn when a multi-process deployment would start with a token gap.

    Mirrors the branches ``supervisor._resolve_actuator`` takes without
    importing or calling it: that function raises for
    ``actuator_mode='token'`` and logs to a handler-less root logger for
    ``'auto'``, neither of which belongs in a read-only diagnostic.

    ``run_doctor()`` only sees the pyproject/env configuration, not
    ``modulith run``'s ``--topology``/``--host`` flags — a deployment that
    overrides ``--host`` away from the ``run`` command's ``0.0.0.0`` default
    is a known blind spot, named in the warning text below.
    """
    cfg = rt.config
    if cfg is None or cfg.topology != "processes":
        return HealthCheck("actuator token", "ok", "single-process topology, no actuator proxy")

    mode = cfg.actuator_mode
    if mode in ("disabled", "open"):
        return HealthCheck("actuator token", "ok", f"actuator_mode={mode!r}")

    token = os.environ.get("MODULITH_ACTUATOR_TOKEN") or None
    if token:
        return HealthCheck("actuator token", "ok", f"actuator_mode={mode!r}, token configured")

    if mode == "token":
        return HealthCheck(
            "actuator token",
            "warn",
            "actuator_mode='token' with no MODULITH_ACTUATOR_TOKEN configured — "
            "modulith run --topology processes will refuse to start (ConfigurationError)",
        )

    # "auto"
    return HealthCheck(
        "actuator token",
        "warn",
        "actuator_mode='auto' with no MODULITH_ACTUATOR_TOKEN configured — "
        "/_modulith/* is left UNMOUNTED (404) when production=True or the proxy "
        "binds a non-loopback host; 'modulith run' binds 0.0.0.0 by default",
        [
            "k8s liveness/readiness probes on those paths will fail. Known limit: "
            "doctor only sees pyproject/env configuration, not 'modulith run "
            "--topology/--host' flags. Set MODULITH_ACTUATOR_TOKEN, or "
            "actuator_mode='open' to serve it unauthenticated."
        ],
    )


# ---------------------------------------------------------------------------
# Single-host broker
# ---------------------------------------------------------------------------

# Module constants so tests can monkeypatch every detection source — CI
# itself often runs inside a container, so a negative test must be able to
# neutralise all three independently of what the real host happens to be.
_CONTAINER_MARKERS = (Path("/.dockerenv"), Path("/run/.containerenv"))
_CGROUP_PATH = Path("/proc/1/cgroup")


def _container_runtime() -> str | None:
    """Best-effort detection of the container runtime hosting this process."""
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return "kubernetes"
    if any(marker.exists() for marker in _CONTAINER_MARKERS):
        return "docker"
    try:
        cgroup_text = _CGROUP_PATH.read_text(encoding="utf-8")
    except OSError:
        cgroup_text = ""
    if "kubepods" in cgroup_text:
        return "kubernetes"
    if "docker" in cgroup_text or "containerd" in cgroup_text:
        return "docker"
    return None


def _check_single_host_broker(rt: Runtime) -> HealthCheck:
    """Warn/error when a per-host broker is configured under a container
    runtime that routinely schedules each module onto a different host.

    Complementary to the production+database+no-URL guard in
    ``config.py``'s ``_validate`` — that guard only fires with no URL at
    all; this check fires even with an explicit *local file* URL, and also
    covers ``broker='shm'``, which the config-time guard never sees.
    """
    cfg = rt.config
    if cfg is None or cfg.topology != "processes":
        return HealthCheck("single-host broker", "ok", "single-process topology")

    local = cfg.broker == "shm"
    if not local and cfg.broker == "database":
        registry = rt.broker_registry
        broker = registry.get("database") if registry is not None else None
        local = bool(getattr(broker, "_is_sqlite", False))

    if not local:
        return HealthCheck("single-host broker", "ok", f"broker={cfg.broker!r} is host-independent")

    runtime = _container_runtime()
    if runtime is None:
        return HealthCheck(
            "single-host broker", "ok", f"broker={cfg.broker!r}, no container runtime detected"
        )

    detail = (
        f"broker={cfg.broker!r} keeps its state in a local file — per-module pods on "
        "different hosts can't share it, so cross-module events are never delivered. "
        "Use broker='database' with a Postgres/MySQL URL, or broker='redis-streams'."
    )
    if runtime == "kubernetes":
        status = "error" if cfg.production else "warn"
        summary = f"broker={cfg.broker!r} is single-host but running under kubernetes"
    else:
        status = "warn"
        summary = (
            f"broker={cfg.broker!r} is single-host but running under docker — a single "
            "container or compose volume can be a legitimate single-host deployment"
        )
    return HealthCheck("single-host broker", status, summary, [detail])


# ---------------------------------------------------------------------------
# Redis retention
# ---------------------------------------------------------------------------


def _check_redis_retention(rt: Runtime) -> HealthCheck:
    """Warn when a redis-streams broker's retention window is undersized.

    Static sizing check: reads the broker's configured ``max_stream_len``
    and flags it against the adapter's own default. A live backlog check
    against the running server is a separate, timeout-bounded query and is
    not part of this function.
    """
    cfg = rt.config
    if cfg is None or cfg.broker != "redis-streams":
        return HealthCheck("redis retention", "ok", "broker is not redis-streams")

    registry = rt.broker_registry
    broker = registry.get("redis-streams") if registry is not None else None
    maxlen = getattr(broker, "_max_stream_len", None)
    if maxlen is None:
        return HealthCheck("redis retention", "ok", "redis-streams broker not registered")

    from .adapters.redis_broker import _DEFAULT_MAXLEN

    if maxlen >= _DEFAULT_MAXLEN:
        return HealthCheck("redis retention", "ok", f"max_stream_len={maxlen}")

    reclaim_min_idle_ms = (cfg.broker_options or {}).get("reclaim_min_idle_ms", 60_000)
    return HealthCheck(
        "redis retention",
        "warn",
        f"max_stream_len={maxlen} is below the {_DEFAULT_MAXLEN} default",
        [
            "size max_stream_len >= publish_rate x (consumer_downtime + "
            f"processing_latency + reclaim_min_idle_ms={reclaim_min_idle_ms}) — an "
            "undersized retention window silently drops still-pending entries."
        ],
    )


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------


_STATUS_ICON = {"ok": "✓", "warn": "⚠", "error": "✗"}


def render_report(report: HealthReport) -> str:
    """Render a HealthReport as plain terminal output (icons + details)."""
    lines = ["modulith doctor", ""]
    for check in report.checks:
        icon = _STATUS_ICON.get(check.status, "?")
        lines.append(f"{icon} {check.name} — {check.summary}")
        lines.extend(f"    {detail}" for detail in check.details)
    lines.append("")
    lines.append(f"overall: {report.overall_status}")
    return "\n".join(lines) + "\n"


__all__ = [
    "HealthCheck",
    "HealthReport",
    "render_report",
    "run_doctor",
]
