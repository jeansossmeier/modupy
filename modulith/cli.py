"""The modulith CLI.

Built on typer. Provides commands for development, running, verification,
documentation generation, and operational maintenance.

Critical design principle: the CLI is a *progressive enhancement*, not
a requirement. Users with muscle memory for `uvicorn` keep using it.
`modulith dev` is *almost* `uvicorn --reload` with quality-of-life
additions; running an app without the CLI works the same way.

Every command that needs the live module model bootstraps the runtime
(``_runtime.ensure_bootstrapped()``) and reads its public accessors —
the CLI itself knows nothing about discovery, verification rules, or
storage backends. It just orchestrates the already-tested subsystems
through the plugin hooks and the outbox maintenance API.

Distribution: shipped via [project.scripts] in pyproject.toml so
`pip install modulith[cli]` makes `modulith` available on PATH.

Exit-code scheme (uniform across commands; see ``main``): 0 = success,
1 = violations or user error within a recognized command line, 2 =
unexpected internal errors AND CLI usage errors (missing required
argument, unknown option) — the latter is click's convention, which
modulith follows rather than fighting the framework (W3 R3-F3).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import traceback
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

# typer is an optional dependency — only loaded when the CLI is invoked.
try:
    import typer
except ImportError:
    print(
        "modulith CLI requires the 'cli' extra. Install with:\n  pip install 'modulith[cli]'",
        file=sys.stderr,
    )
    sys.exit(1)

from .builtin import outbox, verifier
from .config import ConfigurationError, load_configuration
from .discovery import _detect_from_pyproject_name
from .manifest import get_manifest
from .runtime import Runtime, _runtime
from .types import Violation, ViolationSeverity

app = typer.Typer(
    name="modulith",
    help="Modular monolith pattern for Python.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    """Eager ``--version`` handler: print the version and exit before any command."""
    if value:
        from . import __version__

        typer.echo(f"modulith {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the modulith version and exit.",
    ),
) -> None:
    """Modular monolith pattern for Python."""


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _bootstrap_or_exit() -> Runtime:
    """Bootstrap the runtime, converting config errors into a clean exit.

    Bootstrap imports the application's modules and wires the plugin
    manager — everything ``info``/``verify``/``docs`` need. A
    ConfigurationError here means the user's project isn't set up (no
    detectable package, failing manifest); surface its actionable message
    and exit 1 rather than dumping a traceback.

    CLI bootstrap must never fall back to the caller-stack package heuristic:
    every frame above a CLI command belongs to typer/click, so the stack walk
    would "detect" the CLI framework itself as the application package and
    the command would silently run against the wrong package — ``verify``
    exited 0 without ever scanning the real app (A9-r2-94). Resolve the
    package up front instead: explicit configuration wins, then pyproject
    ``[project].name``, otherwise exit 1 with actionable guidance.
    """
    if not _runtime._bootstrapped:
        try:
            resolved = load_configuration(**_runtime._config_overrides)
        except ConfigurationError as exc:
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(code=1) from None
        if resolved.package is None:
            pkg = _detect_from_pyproject_name()
            if pkg is None:
                typer.echo(
                    "error: could not determine the application package. Set "
                    "[tool.modulith].package or [project].name in pyproject.toml, "
                    "or the MODULITH_PACKAGE environment variable.",
                    err=True,
                )
                raise typer.Exit(code=1)
            _runtime.configure(package=pkg)
    try:
        _runtime.ensure_bootstrapped()
    except ConfigurationError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    return _runtime


def _package_from_app_module(app_module: str) -> str:
    """Infer the root package from ``package.module:app`` CLI syntax."""
    module_path = app_module.partition(":")[0]
    return module_path.split(".", 1)[0]


def _configure_process_runtime(app_module: str) -> None:
    """Apply process-topology CLI intent before bootstrapping the runtime.

    The package comes from explicit configuration when present; otherwise it
    is derived from ``app_module``. When both exist and disagree, warn loudly:
    the process topology runs the *configured* package's modules and never
    reads ``app_module`` again, so silence here launched a different app's
    workers on a typo'd argument (A9-r4-182).
    """
    try:
        resolved = load_configuration(topology="processes")
    except ConfigurationError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    implied = _package_from_app_module(app_module)
    overrides: dict[str, Any] = {"topology": "processes"}
    if resolved.package is None:
        if not implied:
            typer.echo(
                f"invalid app module {app_module!r}: expected 'package.module:app'",
                err=True,
            )
            raise typer.Exit(code=1)
        overrides["package"] = implied
    elif implied and implied != resolved.package:
        typer.echo(
            f"warning: app module {app_module!r} implies package {implied!r}, but the "
            f"configured package is {resolved.package!r} — the process topology runs "
            "the configured package's modules and ignores the app-module argument",
            err=True,
        )
    _runtime.configure(**overrides)


def _exec_uvicorn(argv: list[str]) -> None:
    """Replace this process with uvicorn, mapping launch failure to exit 1.

    ``os.execvp`` only returns by raising. A missing uvicorn binary
    (FileNotFoundError) — or any other OSError launching it — is an
    environment/user error per the documented exit-code scheme, so it must
    surface as an actionable message with exit 1, never the raw traceback +
    exit 2 reserved for internal bugs (S3-r2-124).
    """
    try:
        os.execvp("uvicorn", argv)
    except OSError as exc:
        typer.echo(
            f"error: could not launch 'uvicorn' ({exc}). Install it in this "
            "environment — e.g. `pip install uvicorn` or `pip install "
            "'modulith[fastapi]'` — or run your ASGI server directly.",
            err=True,
        )
        raise typer.Exit(code=1) from None


_TOPOLOGIES = ("single", "processes")


def _validate_topology(topology: str) -> None:
    """Reject anything outside the two supported topologies (A9-r1-30).

    Anything unrecognized used to route to the process-per-module supervisor
    (the branch was ``!= "single"``), silently launching the wrong
    architecture on a typo.
    """
    if topology not in _TOPOLOGIES:
        typer.echo(
            f"invalid --topology {topology!r}: expected 'single' or 'processes'",
            err=True,
        )
        raise typer.Exit(code=1)


def _validate_app_module(app_module: str) -> None:
    """Reject empty app-module arguments with a clean CLI error (A9-r3-144)."""
    if not app_module.strip():
        typer.echo(
            f"invalid app module {app_module!r}: expected 'package.module:app'",
            err=True,
        )
        raise typer.Exit(code=1)


_DURATION_RE = re.compile(r"^(\d+)([dhms])$")
_DURATION_UNIT = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}


def _parse_duration(text: str) -> timedelta:
    """Parse a compact duration like ``30d``/``24h``/``30m``/``90s``.

    Raises ValueError on anything else so the caller can render a clean
    CLI error instead of constructing a bogus timedelta.
    """
    match = _DURATION_RE.match(text.strip())
    if match is None:
        raise ValueError(
            f"invalid duration {text!r}; expected a count and unit like "
            "'30d', '24h', '30m', or '90s'"
        )
    amount = int(match.group(1))
    unit = _DURATION_UNIT[match.group(2)]
    try:
        return timedelta(**{unit: amount})
    except OverflowError:
        # A syntactically-valid but astronomically-large count overflows the C
        # int backing timedelta. OverflowError is not a ValueError, so re-raise
        # as one to honor this function's clean-CLI-error contract.
        raise ValueError(f"duration {text!r} is too large") from None


def _print_violations(violations: list[Violation]) -> None:
    """Print violations grouped by module, mirroring Spring Modulith's report."""
    if not violations:
        typer.echo("✓ no boundary violations")
        return

    by_module: dict[str, list[Violation]] = defaultdict(list)
    for v in violations:
        by_module[v.module].append(v)

    for module in sorted(by_module):
        typer.echo(f"\n{module}:")
        for v in by_module[module]:
            severity = v.severity.value.upper()
            location = f"  ({v.location})" if v.location else ""
            typer.echo(f"  [{severity}] {v.rule}: {v.message}{location}")

    errors = sum(1 for v in violations if v.severity is ViolationSeverity.ERROR)
    warnings = len(violations) - errors
    typer.echo(f"\n{errors} error(s), {warnings} warning(s)")


def _write_baseline_or_exit(baseline: Path, violations: list[Violation]) -> None:
    """Record the current violation set as the accepted ratchet baseline.

    A --baseline path in a nonexistent directory (or otherwise unwritable)
    is a user error per the documented exit codes — exit 1 with guidance,
    never the raw traceback + exit 2 reserved for internal bugs (W3 R3-F4).
    """
    try:
        verifier.write_baseline(baseline, violations)
    except OSError as exc:
        typer.echo(
            f"error: could not write baseline file {baseline} ({exc}). "
            "Create the directory or pass a writable --baseline path.",
            err=True,
        )
        raise typer.Exit(code=1) from None
    typer.echo(f"baseline updated: {len(violations)} violation(s) recorded in {baseline}")


def _collect_violations(rt: Runtime) -> list[Violation]:
    """Aggregate every boundary rule: plugin verify hooks + cycle detection."""
    modules = rt.modules
    pm = rt.plugin_manager
    violations: list[Violation] = []
    for module in modules:
        for result in pm.hook.modulith_verify_module(module=module, all_modules=modules):
            violations.extend(result)
    violations.extend(verifier.detect_cycles(modules))
    return violations


def _echo_violation_warnings(violations: list[Violation]) -> None:
    """Echo violations as dev-time warnings on stderr (never exits)."""
    if not violations:
        return
    typer.echo(
        f"modulith: {len(violations)} boundary violation(s) — non-fatal in dev, "
        "run `modulith verify` for the hard check:",
        err=True,
    )
    for v in violations:
        location = f" ({v.location})" if v.location else ""
        typer.echo(
            f"  [{v.severity.value.upper()}] {v.module}: {v.rule}: {v.message}{location}",
            err=True,
        )


def _echo_dev_verify_warnings(app_module: str) -> None:
    """Best-effort boundary check at ``modulith dev`` startup (S2-r4-195).

    Implements SPEC §3.2's "warnings in dev, hard checks via `modulith
    verify` in CI": bootstrap the runtime (deriving the package from
    ``app_module`` when nothing is configured), print the discovered module
    list, and echo any boundary violations as warnings. Never fatal — a dev
    server must start even when the project is half-configured, so every
    failure here downgrades to a note on stderr.
    """
    added_package = False
    try:
        added_package = _configure_dev_package(app_module)
        _runtime.ensure_bootstrapped()
        modules = _runtime.modules
        violations = _collect_violations(_runtime)
    except Exception as exc:  # dev-time signal only — never block the server
        if added_package:
            # Leave the (un-bootstrapped) runtime as we found it.
            _runtime._config_overrides.pop("package", None)
        typer.echo(f"modulith: boundary check skipped ({exc})", err=True)
        return
    names = ", ".join(sorted(m.name for m in modules)) or "(none discovered)"
    typer.echo(f"modulith: discovered modules: {names}")
    _echo_violation_warnings(violations)


def _configure_dev_package(app_module: str) -> bool:
    """Name a package for the dev warn-pass when configuration doesn't.

    Returns True when this call added the override, so a failed bootstrap
    can remove it again and leave the runtime exactly as it was.
    """
    if _runtime._bootstrapped:
        return False
    resolved = load_configuration(**_runtime._config_overrides)
    if resolved.package is not None:
        return False
    pkg = _package_from_app_module(app_module)
    if not pkg:
        raise ConfigurationError(f"cannot derive an application package from {app_module!r}")
    _runtime.configure(package=pkg)
    return True


def _require_outbox_store() -> None:
    """Exit 1 with guidance when no durable outbox store is configured.

    The maintenance commands operate on the store the application wires at
    startup (e.g. ``PostgresPublicationStore``). The default ``memory``
    outbox persists nothing, so there is nothing to inspect or act on.
    """
    if outbox._store is None:
        typer.echo(
            "no outbox store configured — the outbox commands operate on the "
            "durable PublicationStore your application wires at startup (e.g. "
            "PostgresPublicationStore). The default 'memory' outbox keeps "
            "nothing to inspect; set [tool.modulith].outbox = 'postgres'.",
            err=True,
        )
        raise typer.Exit(code=1)


def _parse_workers_json(workers_json: str) -> dict[str, Any]:
    """Parse ``--workers`` into a dict, exiting 1 on anything else.

    JSON that parses but isn't an object (a list, number, string…) used to
    crash the supervisor with an AttributeError traceback (A9-r1-31) — the
    option's contract is an object like ``{"reports": 4}``.
    """
    try:
        parsed = json.loads(workers_json)
    except json.JSONDecodeError as exc:
        typer.echo(f"invalid --workers JSON: {exc}", err=True)
        raise typer.Exit(code=1) from None
    if not isinstance(parsed, dict):
        typer.echo(
            'invalid --workers JSON: expected an object like {"reports": 4}, '
            f"got {type(parsed).__name__}",
            err=True,
        )
        raise typer.Exit(code=1)
    return parsed


def _run_process_topology(
    *,
    app_module: str,
    workers_json: str | None,
    isolate: str | None,
    host: str,
    port: int,
    verify_warn: bool = False,
) -> None:
    """Spin up the process-per-module runtime: one worker per module + proxy.

    Bootstraps to resolve the application package, derives a ``WorkerSpec``
    per discovered module (honoring ``--workers`` counts and ``--isolate``),
    then runs the supervisor and reverse proxy on ``(host, port)``. Blocks
    until a shutdown signal arrives. With ``verify_warn`` (``modulith dev``),
    boundary violations are echoed as non-fatal warnings after bootstrap.
    """
    from .supervisor import derive_specs_from_config, run_supervised

    # Argument validation precedes environment preconditions (bootstrap).
    workers_map: dict[str, Any] | None = None
    if workers_json:
        workers_map = _parse_workers_json(workers_json)

    _configure_process_runtime(app_module)
    rt = _bootstrap_or_exit()
    if verify_warn:
        try:
            _echo_violation_warnings(_collect_violations(rt))
        except Exception as exc:  # dev-time signal only — never fatal
            typer.echo(f"modulith: boundary check skipped ({exc})", err=True)
    cfg = rt.config
    assert cfg is not None  # ensure_bootstrapped guarantees this

    # Forward the resolved broker config to every worker as MODULITH_BROKER_*
    # env vars. A worker re-bootstraps from scratch (auto_discover=False,
    # explicit package/topology kwargs) and cannot reliably re-derive the
    # broker_options the parent resolved — its CWD/pyproject discovery may
    # differ, and its own configure() call doesn't read them — so without this
    # a URL/DSN (or any tuning key) set in pyproject or resolved at the parent
    # never reaches the worker's broker/consumer, and e.g. the database broker
    # fails to build an engine.
    #
    # Precedence matches _broker_opt (env > broker_options): never overwrite a
    # non-empty MODULITH_BROKER_* already in the parent's environment. Omit
    # MODULITH_BROKER when the parent auto-defaulted it so workers also see
    # is_explicit("broker") == False. Structured option values (dicts/lists)
    # are JSON-encoded — str(dict) is not json.loads()-able.
    worker_env: dict[str, str] = {}
    if cfg.is_explicit("broker"):
        worker_env["MODULITH_BROKER"] = cfg.broker
    for key, value in (cfg.broker_options or {}).items():
        if value is None:
            continue
        env_key = f"MODULITH_BROKER_{key.upper()}"
        if os.environ.get(env_key):
            continue  # parent env already wins
        if isinstance(value, (dict, list)):
            worker_env[env_key] = json.dumps(value)
        else:
            worker_env[env_key] = str(value)
    if cfg.broker == "shm":
        from .adapters.shm_broker import _resolve_shm_paths

        state_dir, sqlite_path, hint_path = _resolve_shm_paths(
            cfg.package,
            cfg.broker_options or {},
        )
        for env_key, path in (
            ("MODULITH_BROKER_STATE_DIR", state_dir),
            ("MODULITH_BROKER_SQLITE_PATH", sqlite_path),
            ("MODULITH_BROKER_HINT_PATH", hint_path),
        ):
            if not os.environ.get(env_key):
                worker_env[env_key] = str(path)

    config: dict[str, Any] = {
        "package": cfg.package,
        # --workers replaces the pyproject [tool.modulith.workers] table
        # entirely — a full override, not a per-module patch (A9-r5-216).
        "workers": workers_map if workers_map is not None else dict(cfg.workers),
        "env": worker_env,
    }
    if isolate:
        config["isolate"] = [isolate]

    specs = derive_specs_from_config(config)
    if not specs:
        typer.echo("no modules discovered to run", err=True)
        raise typer.Exit(code=1)

    layout = ", ".join(f"{s.module_name}:{s.port}" for s in specs)
    typer.echo(
        f"modulith → process-per-module: {len(specs)} worker(s) [{layout}], "
        f"reverse proxy on http://{host}:{port}"
    )
    asyncio.run(
        run_supervised(
            specs, host, port, actuator_mode=cfg.actuator_mode, production=cfg.production
        )
    )


# ---------------------------------------------------------------------------
# modulith dev — like uvicorn --reload but with the modulith banner
# ---------------------------------------------------------------------------


@app.command()
def dev(
    app_module: str = typer.Argument(..., help="ASGI app, e.g. 'myapp:app'"),
    topology: str = typer.Option("single", help="single | processes"),
    isolate: str | None = typer.Option(None, help="Module to isolate in its own process"),
    reload: bool = typer.Option(True, help="Reload on file changes"),
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
) -> None:
    """Run the application in development mode.

    Startup runs the boundary verifier and echoes violations as *non-fatal*
    warnings — the dev-time signal promised by SPEC §3.2 (``modulith verify``
    remains the hard CI gate). The single-process path execs uvicorn
    (optionally with ``--reload``), so the dev experience is identical to
    running uvicorn directly. The process-per-module path (``--topology
    processes`` / ``--isolate``) runs the supervisor + reverse proxy: one
    worker subprocess per module behind a routing proxy on ``(host, port)``.
    (``--reload`` does not apply to the process topology in v1.)

    Exit codes: 0 on a clean launch (dev-time verifier warnings never fail
    the command), 1 on invalid arguments or configuration errors, 2 on
    unexpected internal errors.
    """
    _validate_topology(topology)
    _validate_app_module(app_module)
    if topology == "processes" or isolate is not None:
        _run_process_topology(
            app_module=app_module,
            workers_json=None,
            isolate=isolate,
            host=host,
            port=port,
            verify_warn=True,
        )
        return

    _echo_dev_verify_warnings(app_module)
    argv = ["uvicorn", app_module, "--host", host, "--port", str(port)]
    if reload:
        argv.append("--reload")
    typer.echo(
        f"modulith dev → {app_module} on http://{host}:{port} (reload={'on' if reload else 'off'})"
    )
    _exec_uvicorn(argv)


# ---------------------------------------------------------------------------
# modulith run — production mode (no reload)
# ---------------------------------------------------------------------------


@app.command()
def run(
    app_module: str = typer.Argument(...),
    topology: str = typer.Option("single", help="single | processes"),
    workers: str | None = typer.Option(
        None,
        help='JSON: {"reports": 4} (replaces the pyproject [tool.modulith.workers] table entirely)',
    ),
    host: str = typer.Option("0.0.0.0"),
    port: int = typer.Option(8000),
) -> None:
    """Run the application in production mode.

    Like ``dev`` minus reload (and minus the dev-time verifier warnings).
    Per-module worker counts (``--workers``) apply to the process-per-module
    topology: each module's worker count comes from the JSON map (with a
    ``default`` fallback). Passing ``--workers`` replaces the pyproject
    ``[tool.modulith.workers]`` table entirely — a full override, not a
    per-module patch. The single-process path execs a plain uvicorn.

    Exit codes: 0 on a clean launch, 1 on invalid arguments or configuration
    errors, 2 on unexpected internal errors.
    """
    _validate_topology(topology)
    _validate_app_module(app_module)
    if topology == "processes":
        _run_process_topology(
            app_module=app_module,
            workers_json=workers,
            isolate=None,
            host=host,
            port=port,
        )
        return

    argv = ["uvicorn", app_module, "--host", host, "--port", str(port)]
    typer.echo(f"modulith run → {app_module} on http://{host}:{port}")
    _exec_uvicorn(argv)


# ---------------------------------------------------------------------------
# modulith verify — boundary checks for CI
# ---------------------------------------------------------------------------


@app.command()
def verify(
    mode: str = typer.Option("strict", help="strict | ratchet"),
    baseline: Path = typer.Option(Path(".modulith-baseline.json")),
    update_baseline: bool = typer.Option(False, "--update-baseline"),
    fail_on_warnings: bool = typer.Option(
        False,
        "--fail-on-warnings",
        help="Also exit 1 when WARNING-severity violations are reported.",
    ),
) -> None:
    """Run boundary verification.

    Exit code 0 when no ERROR-severity violations are reported (strict) or
    none are new relative to the baseline (ratchet) — WARNING-severity
    violations are printed but pass unless ``--fail-on-warnings`` is set.
    Exit code 1 on violations or invalid usage; 2 on unexpected internal
    errors. Designed to drop into CI as a single line. ``--update-baseline``
    records the current violation set as the accepted baseline and exits 0.
    """
    # Argument validation precedes bootstrap: a typo'd mode used to fall
    # through to strict semantics, ignoring the baseline (A9-r3-142).
    if mode not in ("strict", "ratchet"):
        typer.echo(f"invalid --mode {mode!r}: expected 'strict' or 'ratchet'", err=True)
        raise typer.Exit(code=1)

    rt = _bootstrap_or_exit()
    violations = _collect_violations(rt)

    if update_baseline:
        _write_baseline_or_exit(baseline, violations)
        return

    if mode == "ratchet":
        try:
            grandfathered = verifier.load_baseline(baseline)
        except ConfigurationError as exc:
            # A corrupt/schema-mismatched baseline is a user error: surface
            # load_baseline's actionable message (it names the path and the
            # regeneration command) and exit 1 — never the raw traceback +
            # exit 2 reserved for internal bugs (G11 disclosure).
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(code=1) from None
        reported = verifier.filter_against_baseline(violations, grandfathered)
    else:
        reported = violations

    _print_violations(reported)

    if any(v.severity is ViolationSeverity.ERROR for v in reported):
        raise typer.Exit(code=1)
    if fail_on_warnings and reported:
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# modulith docs — generate architecture documentation
# ---------------------------------------------------------------------------


@app.command()
def docs(
    output_dir: Path = typer.Option(Path("docs/modulith")),
) -> None:
    """Generate Mermaid diagrams and module canvases.

    Exit codes: 0 on success, 1 on configuration errors (e.g. a discovery
    hook produced duplicate or unsafe module names — see the built-in docs
    generator's validation), 2 on unexpected internal errors.
    """
    rt = _bootstrap_or_exit()
    modules = rt.modules
    pm = rt.plugin_manager

    produced: list[str] = []
    try:
        for result in pm.hook.modulith_render_documentation(
            modules=modules, output_dir=str(output_dir)
        ):
            produced.extend(result)
    except ConfigurationError as exc:
        # The built-in generator validates module names (duplicates, unsafe
        # path segments — A11-r4-188/189) and raises ConfigurationError with
        # an actionable message: a user/config error per the documented exit
        # codes, not the exit-2 traceback reserved for internal bugs.
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    except OSError as exc:
        # An --output-dir that collides with an existing file, or is
        # otherwise unwritable (permissions, read-only filesystem), is a
        # user/filesystem error per the documented exit codes — exit 1 with
        # guidance, never the raw traceback + exit 2 reserved for internal
        # bugs.
        typer.echo(
            f"error: could not write documentation to {output_dir} ({exc}). "
            "Pass a writable --output-dir that is not an existing file.",
            err=True,
        )
        raise typer.Exit(code=1) from None

    if not produced:
        typer.echo("no documentation artifacts produced")
        return
    typer.echo(f"generated {len(produced)} file(s) in {output_dir}:")
    for name in produced:
        typer.echo(f"  {name}")


# ---------------------------------------------------------------------------
# modulith audit — analyze an existing codebase (Phase 2)
# ---------------------------------------------------------------------------


@app.command()
def audit(
    path: Path = typer.Argument(Path("."), help="Codebase root to analyze"),
    output: Path = typer.Option(Path("MIGRATION.md")),
) -> None:
    """Analyze an existing codebase for modulith readiness.

    Non-destructive — only reads files (parsed via ``ast``, never imported).
    Produces a Markdown report with the proposed module structure, the
    cross-module imports that would become violations, shared tables that
    need ownership decisions, and a 0-100 readiness score.
    """
    from .audit import audit_codebase, render_report

    if not path.exists():
        typer.echo(f"path does not exist: {path}", err=True)
        raise typer.Exit(code=1)

    try:
        cfg = load_configuration()
    except ConfigurationError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    result = audit_codebase(path, contracts_module=cfg.contracts_module)
    try:
        output.write_text(render_report(result), encoding="utf-8")
    except OSError as exc:
        # An --output path in a nonexistent directory (or otherwise
        # unwritable) is a user/filesystem error per the documented exit
        # codes — exit 1 with guidance, never the raw traceback + exit 2
        # reserved for internal bugs (mirrors _write_baseline_or_exit).
        typer.echo(
            f"error: could not write audit report to {output} ({exc}). "
            "Create the directory or pass a writable --output path.",
            err=True,
        )
        raise typer.Exit(code=1) from None
    typer.echo(f"readiness score: {result.readiness_score}/100")
    typer.echo(
        f"{len(result.cross_module_imports)} cross-module import pattern(s), "
        f"{len(result.shared_tables)} shared table(s)"
    )
    typer.echo(f"wrote audit report to {output}")


# ---------------------------------------------------------------------------
# modulith doctor — operational health check (Phase 2)
# ---------------------------------------------------------------------------


@app.command()
def doctor() -> None:
    """Report architectural and operational health.

    Runs five checks — boundary health, process-split readiness, schema
    drift, outbox health, and listener registration — and prints a report.
    Exits 1 if any check reports an error, so it doubles as a CI gate
    (warnings are reported but pass); 2 on unexpected internal errors.
    """
    from .doctor import render_report, run_doctor

    _bootstrap_or_exit()
    report = run_doctor()
    typer.echo(render_report(report))
    if report.overall_status == "error":
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# modulith outbox — operational commands for the transactional outbox
# ---------------------------------------------------------------------------

outbox_app = typer.Typer(help="Outbox operational commands.")
app.add_typer(outbox_app, name="outbox")


@outbox_app.command("status")
def outbox_status() -> None:
    """Show outbox counts: incomplete, completed, dead-lettered."""
    _bootstrap_or_exit()
    _require_outbox_store()
    counts = asyncio.run(outbox.status())
    typer.echo(f"incomplete:    {counts['incomplete']}")
    typer.echo(f"completed:     {counts['completed']}")
    typer.echo(f"dead-lettered: {counts['dead_lettered']}")


async def _force_retry_known(pub_id: UUID) -> bool:
    """Retry ``pub_id`` via the outbox; False when no such publication exists.

    ``outbox.force_retry`` only *logs* a warning on the not-found path —
    invisible whenever the application configures its own logging — so the
    CLI checks existence itself and reports honestly (A9-r3-143). Mirrors
    ``force_retry``'s own lookup strategy: prefer the store's ``find_by_id``
    direct point lookup when available, since ``find_incomplete``/
    ``list_dead_lettered`` are both capped windows (LIMIT 100) that can miss
    a targeted row sitting further back in a large backlog. Stores without
    ``find_by_id`` fall back to the bounded scan.
    """
    store = outbox._store
    assert store is not None  # _require_outbox_store already ran
    finder = getattr(store, "find_by_id", None)
    if finder is not None:
        pub = await finder(pub_id)
        if pub is None or pub.completed_at is not None:
            return False
        await outbox.force_retry(pub_id)
        return True
    candidates = list(await store.find_incomplete(timedelta(0)))
    candidates += await outbox.list_dead_lettered()
    if not any(pub.id == pub_id for pub in candidates):
        return False
    await outbox.force_retry(pub_id)
    return True


@outbox_app.command("retry")
def outbox_retry(publication_id: str) -> None:
    """Force retry of a specific publication, bypassing backoff.

    Exit code 1 when the id is not a UUID or names no retryable publication
    (unknown, or already complete) — success is only reported for ids that
    were actually resubmitted.
    """
    # Argument validation precedes environment preconditions.
    try:
        pub_id = UUID(publication_id)
    except ValueError:
        typer.echo(f"invalid publication id {publication_id!r}: expected a UUID", err=True)
        raise typer.Exit(code=1) from None
    _bootstrap_or_exit()
    _require_outbox_store()
    if not asyncio.run(_force_retry_known(pub_id)):
        typer.echo(
            f"publication {pub_id} not found (or already complete) — nothing was retried",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(f"requested retry of publication {pub_id}")


@outbox_app.command("purge")
def outbox_purge(
    older_than: str = typer.Option("30d", help="e.g. 7d, 24h, 30m"),
) -> None:
    """Delete completed publications older than threshold."""
    _bootstrap_or_exit()
    _require_outbox_store()
    try:
        threshold = _parse_duration(older_than)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    deleted = asyncio.run(outbox.purge_completed(threshold))
    typer.echo(f"purged {deleted} completed publication(s) older than {older_than}")


@outbox_app.command("dead-letter")
def outbox_dead_letter(
    retry_all: bool = typer.Option(
        False, "--retry-all", help="Resubmit every dead-lettered publication."
    ),
    list_dead: bool = typer.Option(
        False, "--list", help="List dead-lettered publications (default)."
    ),
) -> None:
    """List dead-lettered events for manual inspection, or resubmit them all.

    Listing is the default; ``--list`` makes it explicit. ``--list`` and
    ``--retry-all`` are mutually exclusive — passing both is an error rather
    than silently doing one (the previously-inert ``--list`` masked this).
    """
    # Argument validation precedes environment preconditions: the flag
    # conflict must be reported even when no store is configured (A9-r3-145).
    if retry_all and list_dead:
        typer.echo("--list and --retry-all are mutually exclusive", err=True)
        raise typer.Exit(code=1)

    _bootstrap_or_exit()
    _require_outbox_store()

    if retry_all:
        count = asyncio.run(outbox.retry_all_dead_lettered())
        typer.echo(f"resubmitted {count} dead-lettered publication(s)")
        return

    dead = asyncio.run(outbox.list_dead_lettered())
    if not dead:
        typer.echo("no dead-lettered publications")
        return
    typer.echo(f"{len(dead)} dead-lettered publication(s):")
    for pub in dead:
        typer.echo(
            f"  {pub.id}  {pub.event_type}  listener={pub.listener}  "
            f"attempts={pub.attempt_count}  last_error={pub.last_error}"
        )


# ---------------------------------------------------------------------------
# modulith info — show detected configuration
# ---------------------------------------------------------------------------


@app.command()
def info() -> None:
    """Print detected configuration, modules, and active plugins.

    Useful for debugging "why isn't my plugin loading" / "what package
    did modulith detect" questions.
    """
    rt = _bootstrap_or_exit()
    cfg = rt.config
    assert cfg is not None  # ensure_bootstrapped guarantees this
    modules = rt.modules
    pm = rt.plugin_manager
    registry = rt.broker_registry

    typer.echo("modulith")
    typer.echo(f"  package: {cfg.package}")
    typer.echo("")

    typer.echo(f"  modules ({len(modules)}):")
    if modules:
        for module in sorted(modules, key=lambda m: m.name):
            manifest = "manifest" if get_manifest(module.package) else "no manifest"
            typer.echo(f"    - {module.name}  ({module.package})  [{manifest}]")
    else:
        typer.echo("    (none discovered)")
    typer.echo("")

    typer.echo("  configuration:")
    typer.echo(f"    outbox:        {cfg.outbox}")
    typer.echo(f"    broker:        {cfg.broker}")
    typer.echo(f"    topology:      {cfg.topology}")
    typer.echo(f"    observability: {cfg.observability}")
    typer.echo(f"    production:    {cfg.production}")
    typer.echo("")

    plugins = sorted(name for name, _ in pm.list_name_plugin())
    typer.echo(f"  plugins ({len(plugins)}):")
    for name in plugins:
        typer.echo(f"    - {name}")
    typer.echo("")

    schemes = registry.schemes() if registry is not None else []
    typer.echo(f"  brokers: {', '.join(schemes) if schemes else '(none registered)'}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """Console-script entry point.

    Exit codes: 0 = success (warnings may still be reported), 1 = violations
    or user error within a recognized command line (commands raise
    ``typer.Exit(1)``), 2 = unexpected internal errors (traceback printed to
    stderr) and CLI usage errors — a missing required argument or unknown
    option exits 2 per click's convention, which the CLI deliberately
    follows (W3 R3-F3).
    """
    try:
        app()
    except Exception:  # final safety net: internal bugs exit 2, distinctly
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
