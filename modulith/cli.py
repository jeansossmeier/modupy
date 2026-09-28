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
`pip install modupy[cli]` makes `modulith` available on PATH.

Exit-code scheme (uniform across commands; see ``main``): 0 = success,
1 = violations or user error within a recognized command line, 2 =
unexpected internal errors AND CLI usage errors (missing required
argument, unknown option) — the latter is click's convention, which
modulith follows rather than fighting the framework.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import re
import shutil
import sys
import tomllib
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
        "modulith CLI requires the 'cli' extra. Install with:\n  pip install 'modupy[cli]'",
        file=sys.stderr,
    )
    sys.exit(1)

from .builtin import outbox, verifier
from .config import ConfigurationError, _find_pyproject, load_configuration
from .discovery import _detect_from_pyproject_name
from .manifest import get_manifest
from .runtime import Runtime, _runtime
from .types import Violation, ViolationSeverity

app = typer.Typer(
    name="modulith",
    help="Modular monolith pattern for Python.",
    no_args_is_help=True,
)


logger = logging.getLogger(__name__)


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


def _add_project_root_to_syspath() -> None:
    """Make the project the CLI was launched in importable.

    A console script's ``sys.path[0]`` is the directory holding the script
    (``.venv/bin`` after ``pip install``), never the working directory. So an
    application package that sits next to ``pyproject.toml`` — the layout every
    quickstart produces — is invisible to ``import``, and every command that
    bootstraps the runtime dies with ``ModuleNotFoundError`` on the very
    package the configuration names. Configuration discovery already anchors on
    the nearest ``pyproject.toml`` walking up from the cwd; put that same
    directory on the path so the package it declares can actually be imported,
    rather than making users prefix each command with ``PYTHONPATH=.``.

    Idempotent, and deliberately at the front: a root already on ``sys.path``
    is left where it is, so ``python -m modulith.cli`` and an application
    installed into site-packages both keep the resolution order they had.
    """
    pyproject = _find_pyproject()
    if pyproject is None:
        return
    root = str(pyproject.parent)
    if root not in sys.path:
        sys.path.insert(0, root)


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
    exited 0 without ever scanning the real app. Resolve the package up
    front instead: explicit configuration wins, then pyproject
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
    return _ensure_bootstrapped_or_exit()


def _ensure_bootstrapped_or_exit() -> Runtime:
    """Bootstrap an already-configured runtime, exiting 1 on configuration errors.

    The half of ``_bootstrap_or_exit`` that does not re-resolve configuration.
    A caller that has already settled the application package — ``modulith
    run``/``dev`` under the process topology, via
    ``_configure_process_runtime`` — uses this instead, so one boot does not
    read pyproject.toml and sweep the environment twice to reach an answer it
    already has.
    """
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
    workers on a typo'd argument.
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


def _prefer_sibling_uvicorn() -> None:
    """Put the running interpreter's script directory first on ``PATH``.

    ``os.execvp`` resolves ``uvicorn`` through ``PATH``, which is not the
    environment modulith itself was imported from whenever this CLI is invoked
    by absolute path into a virtualenv that was never activated — a systemd
    ``ExecStart=/srv/app/venv/bin/modulith``, a container
    ``CMD ["/app/venv/bin/modulith", "run", ...]``, a Makefile recipe, a CI
    step. ``PATH`` then hands back whichever uvicorn comes first, potentially
    one bound to a different interpreter that cannot import the app at all.

    Hoisting ``sys.executable``'s directory makes the co-installed uvicorn win
    — the same one ``pip install modupy[fastapi]`` put there. When no uvicorn
    lives beside the interpreter, ``PATH`` is left untouched and resolution
    falls through to it, so an intentionally-shadowed uvicorn still runs.
    """
    bindir = os.path.dirname(sys.executable)
    # shutil.which, not an exists() check: it applies PATHEXT, so the Windows
    # Scripts\uvicorn.exe is found by the same bare name as the POSIX script.
    if bindir and shutil.which("uvicorn", path=bindir):
        os.environ["PATH"] = bindir + os.pathsep + os.environ.get("PATH", os.defpath)


def _exec_uvicorn(argv: list[str]) -> None:
    """Replace this process with uvicorn, mapping launch failure to exit 1.

    ``os.execvp`` only returns by raising. A missing uvicorn binary
    (FileNotFoundError) — or any other OSError launching it — is an
    environment/user error per the documented exit-code scheme, so it must
    surface as an actionable message with exit 1, never the raw traceback +
    exit 2 reserved for internal bugs.
    """
    _prefer_sibling_uvicorn()
    try:
        os.execvp("uvicorn", argv)
    except OSError as exc:
        typer.echo(
            f"error: could not launch 'uvicorn' ({exc}). Install it in this "
            "environment — e.g. `pip install uvicorn` or `pip install "
            "'modupy[fastapi]'` — or run your ASGI server directly.",
            err=True,
        )
        raise typer.Exit(code=1) from None


_TOPOLOGIES = ("single", "processes")


def _validate_topology(topology: str) -> None:
    """Reject anything outside the two supported topologies.

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


_LOG_LEVELS = ("debug", "info", "warning", "error", "critical")


def _configure_cli_logging(log_level: str) -> str:
    """Install this process's root log configuration; return the normalized level.

    ``dev`` and ``run`` are process entry points, so owning the root logger
    here is legitimate — a library import path must never do it. Without this
    the root logger has no handler at all, ``logging.lastResort`` discards
    everything below WARNING, and every INFO diagnostic modulith emits
    disappears: which modules expose no ``router``, which workers were
    spawned on which ports, and every line a worker subprocess wrote.

    An unrecognized name exits 1 instead of falling back to a default —
    quietly staying at INFO on a typo is exactly the invisible-logs failure
    this flag exists to end.
    """
    level = log_level.strip().lower()
    if level not in _LOG_LEVELS:
        typer.echo(
            f"invalid --log-level {log_level!r}: expected one of {', '.join(_LOG_LEVELS)}",
            err=True,
        )
        raise typer.Exit(code=1)
    logging.basicConfig(format="%(levelname)s:  %(message)s")
    # basicConfig is a no-op once the root logger has a handler, so set the
    # level separately: otherwise the flag silently does nothing whenever
    # something configured logging before this command ran.
    logging.getLogger().setLevel(level.upper())
    return level


def _validate_app_module(app_module: str) -> None:
    """Reject empty app-module arguments with a clean CLI error."""
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
    never the raw traceback + exit 2 reserved for internal bugs.
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
    """Best-effort boundary check at ``modulith dev`` startup.

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
    """Exit 1 with actionable guidance when no outbox store is bound.

    The maintenance commands act on the ``PublicationStore`` that
    ``modulith.builtin.outbox.configure(store=..., serializer=...)`` binds.
    That binding is per-process in-memory state, and a CLI invocation is its
    own process which only runs what bootstrap's discovery import executes —
    so a store wired exclusively inside an ASGI lifespan/startup hook lives in
    the server process and never in this one.

    Which is why the remedy depends on what the configuration already says.
    Pointing at ``[tool.modulith].outbox`` unconditionally is a dead end once
    that key is set: on its own it binds nothing, and bootstrap builds a store
    only when ``outbox_url`` is set too.
    """
    if outbox._store is not None:
        return
    cfg = _runtime.config
    configured = cfg.outbox if cfg is not None else "memory"
    if configured == "memory":
        cause = (
            "the default 'memory' outbox persists nothing, so there is nothing "
            "to inspect — set [tool.modulith].outbox to a durable adapter "
            "(e.g. 'postgres')"
        )
    else:
        cause = f"[tool.modulith].outbox is {configured!r} but no store is bound in this process"
    typer.echo(
        f"no outbox store: {cause}. Set [tool.modulith].outbox_url (env "
        "MODULITH_OUTBOX_URL) to the outbox database's async SQLAlchemy URL and "
        "bootstrap binds a store in every process, this one included. "
        "Otherwise a store is bound only by calling "
        "modulith.builtin.outbox.configure(store=..., serializer=...) — and the "
        "outbox commands run in their own process, seeing only what bootstrap "
        "imports, so that call has to run at module import time rather than "
        "solely in an ASGI lifespan/startup hook.",
        err=True,
    )
    raise typer.Exit(code=1)


def _parse_workers_json(workers_json: str) -> dict[str, Any]:
    """Parse ``--workers`` into a dict, exiting 1 on anything else.

    JSON that parses but isn't an object (a list, number, string…) used to
    crash the supervisor with an AttributeError traceback — the option's
    contract is an object like ``{"reports": 4}``.

    Values are validated exactly as the ``[tool.modulith.workers]`` table is
    (``config._validate``): a positive ``int``, never a bool or a numeric
    string. Without it the same bad count is a clean exit 1 from pyproject but
    a ``derive_specs_from_config`` traceback and exit 2 from the flag.
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
    for module_name, count in parsed.items():
        if type(count) is not int or count <= 0:
            typer.echo(
                "invalid --workers JSON: workers must map module names to "
                f"positive integer counts; got {module_name!r}: {count!r}",
                err=True,
            )
            raise typer.Exit(code=1)
    return parsed


_GROUP_LEDGER_BROKERS = ("shm", "database")


def _derived_consumer_groups(package: str | None, contracts_module: str) -> set[str]:
    """Consumer groups the current deployment's modules subscribe under.

    Every discovered module counts, including ones ``--isolate`` leaves out
    of this run, so isolating one module never flags its siblings.
    """
    from ._worker import consumer_group
    from .supervisor import discover_module_names

    if not package:
        return set()
    return {consumer_group(name) for name in discover_module_names(package, contracts_module)}


def _group_ledger_broker(rt: Runtime, scheme: str) -> Any | None:
    """The registered broker when it keeps a durable per-group subscription ledger."""
    registry = rt.broker_registry
    if scheme not in _GROUP_LEDGER_BROKERS or registry is None:
        return None
    if scheme not in registry.schemes():
        return None
    return registry.get(scheme)


# A group whose consumer subscribed or claimed within this window counts as
# live even when no local module derives it: an extracted service, or another
# host of a rolling deploy, still serves it. A daily traffic cycle fits inside.
_LIVE_GROUP_WINDOW_S = 24 * 60 * 60


async def _live_groups(broker: Any, derived: set[str]) -> set[str]:
    """Groups a module of this deployment derives or a consumer recently served."""
    active: set[str] = await broker.active_groups(within_seconds=_LIVE_GROUP_WINDOW_S)
    return derived | active


async def _warn_about_retired_groups(rt: Runtime, scheme: str, derived: set[str]) -> None:
    """Log one warning per group that no module derives and no consumer served lately.

    Every later publication to its targets is queued for it. Nothing is
    deleted here: a module that is only disabled for this deploy gets its
    backlog when it returns.
    """
    broker = _group_ledger_broker(rt, scheme)
    if broker is None:
        return
    try:
        backlog = await broker.group_backlog()
        live = await _live_groups(broker, derived)
    except Exception:
        logger.warning("could not read the %s broker's subscribed groups", scheme, exc_info=True)
        return
    hours = _LIVE_GROUP_WINDOW_S // 3600
    for group, pending in backlog.items():
        if group in live:
            continue
        if pending:
            held = f"it holds {pending} pending or claimed delivery(ies) that no consumer claimed"
        else:
            held = "it holds no backlog yet"
        cost = (
            "and those rows keep the shm store from pruning its publications"
            if scheme == "shm"
            else "and those rows pile up in broker_message"
        )
        logger.warning(
            "broker group %r is not derived by any module of this deployment and no "
            "consumer served it in the last %d h; %s. Every later publication to its "
            "targets is queued for it too, %s. If the module is retired for good, run: "
            "modulith broker drop-group %s",
            group,
            hours,
            held,
            cost,
            group,
        )


def _run_process_topology(
    *,
    app_module: str,
    workers_json: str | None,
    isolate: str | None,
    host: str,
    port: int,
    log_level: str,
    verify_warn: bool = False,
    warn_default_state_dir: bool = False,
    worker_port_base: int | None = None,
) -> None:
    """Spin up the process-per-module runtime: one worker per module + proxy.

    Bootstraps to resolve the application package, derives a ``WorkerSpec``
    per discovered module (honoring ``--workers`` counts and ``--isolate``),
    then runs the supervisor and reverse proxy on ``(host, port)``. Blocks
    until a shutdown signal arrives. With ``verify_warn`` (``modulith dev``),
    boundary violations are echoed as non-fatal warnings after bootstrap.

    ``log_level`` is both this process's root log level (already installed by
    the caller) and the level forwarded to every worker subprocess, so one
    flag governs the whole deployment's verbosity.
    """
    from .supervisor import derive_specs_from_config, run_supervised

    # Argument validation precedes environment preconditions (bootstrap).
    workers_map: dict[str, Any] | None = None
    if workers_json:
        workers_map = _parse_workers_json(workers_json)

    # _configure_process_runtime already resolved the configuration and
    # guaranteed a package (explicit, or derived from app_module), so the
    # package-detection probe in _bootstrap_or_exit would re-read pyproject
    # and re-sweep the environment only to reach the same answer.
    _configure_process_runtime(app_module)
    rt = _ensure_bootstrapped_or_exit()
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
    # Workers are `python -m uvicorn` processes, and uvicorn's CLI reads every
    # one of its options from a UVICORN_-prefixed environment variable, so this
    # is how --log-level reaches them: filtered at the source in the worker
    # rather than in the supervisor after the bytes have already crossed a pipe.
    worker_env: dict[str, str] = {"UVICORN_LOG_LEVEL": log_level}
    if cfg.is_explicit("broker"):
        worker_env["MODULITH_BROKER"] = cfg.broker
    # Workers bind their outbox store from these; a value already in the
    # parent's environment reaches them by inheritance and wins.
    if cfg.is_explicit("outbox") and not os.environ.get("MODULITH_OUTBOX"):
        worker_env["MODULITH_OUTBOX"] = cfg.outbox
    if cfg.outbox_url and not os.environ.get("MODULITH_OUTBOX_URL"):
        worker_env["MODULITH_OUTBOX_URL"] = cfg.outbox_url
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
        from .adapters.shm_broker import _resolve_shm_paths, _shm_store_is_defaulted

        state_dir, sqlite_path, hint_path = _resolve_shm_paths(
            cfg.package,
            cfg.broker_options or {},
        )
        if warn_default_state_dir and _shm_store_is_defaulted(cfg.package, sqlite_path):
            logger.warning(
                "SHM broker state_dir is not set: the SQLite store %s lives in the "
                "default state directory, which is keyed on the package's install "
                "path, so a redeploy to a different path (a new release directory, "
                "venv or checkout) opens an empty store and strands undelivered events "
                "in the old one. Production deploys must set "
                "[tool.modulith.broker_options].state_dir or MODULITH_BROKER_STATE_DIR, "
                "or an absolute sqlite_path.",
                sqlite_path,
            )
        for env_key, path in (
            ("MODULITH_BROKER_STATE_DIR", state_dir),
            ("MODULITH_BROKER_SQLITE_PATH", sqlite_path),
            ("MODULITH_BROKER_HINT_PATH", hint_path),
        ):
            if not os.environ.get(env_key):
                worker_env[env_key] = str(path)

    # Workers are `python -m uvicorn` subprocesses, so their sys.path starts
    # from the inherited working directory. _add_project_root_to_syspath fixed
    # the application package's importability for THIS process only; launched
    # from a subdirectory of the project, every worker dies importing the very
    # package the parent just discovered, and the restart loop hides behind a
    # proxy that stays up and answers 502. Forward the same root, prepended so
    # it wins, without discarding an operator's own PYTHONPATH.
    pyproject = _find_pyproject()
    if pyproject is not None:
        root = str(pyproject.parent)
        inherited = os.environ.get("PYTHONPATH", "")
        if root not in inherited.split(os.pathsep):
            worker_env["PYTHONPATH"] = f"{root}{os.pathsep}{inherited}" if inherited else root

    config: dict[str, Any] = {
        "package": cfg.package,
        # --workers replaces the pyproject [tool.modulith.workers] table
        # entirely — a full override, not a per-module patch.
        "workers": workers_map if workers_map is not None else dict(cfg.workers),
        "env": worker_env,
        "contracts_module": cfg.contracts_module,
        "worker_port_base": (
            worker_port_base if worker_port_base is not None else cfg.worker_port_base
        ),
    }
    if isolate:
        config["isolate"] = [isolate]
        typer.echo(
            f"modulith: --isolate={isolate!r} restricts this deployment to that module "
            "only — every other discovered module is not started and its routes 404",
            err=True,
        )

    specs = derive_specs_from_config(config)
    if not specs:
        typer.echo("no modules discovered to run", err=True)
        raise typer.Exit(code=1)

    layout = ", ".join(f"{s.module_name}:{s.port}" for s in specs)
    typer.echo(
        f"modulith → process-per-module: {len(specs)} worker(s) [{layout}], "
        f"reverse proxy on http://{host}:{port}"
    )
    derived = _derived_consumer_groups(cfg.package, cfg.contracts_module)

    async def supervise() -> None:
        await _warn_about_retired_groups(rt, cfg.broker, derived)
        await run_supervised(
            specs, host, port, actuator_mode=cfg.actuator_mode, production=cfg.production
        )

    try:
        asyncio.run(supervise())
    except ConfigurationError as exc:
        # Configuration resolved inside run_supervised (actuator mode, proxy
        # tunables) is user error like any other: the documented contract is a
        # one-line message and exit 1, never the exit-2 traceback reserved for
        # internal bugs.
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None


# ---------------------------------------------------------------------------
# modulith dev — like uvicorn --reload but with the modulith banner
# ---------------------------------------------------------------------------


@app.command()
def dev(
    app_module: str = typer.Argument(..., help="ASGI app, e.g. 'myapp:app'"),
    topology: str = typer.Option("single", help="single | processes"),
    isolate: str | None = typer.Option(
        None, help="Restrict the deployment to only this module (all others are not started)"
    ),
    reload: bool = typer.Option(True, help="Reload on file changes"),
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
    log_level: str = typer.Option(
        "info", help="debug | info | warning | error | critical (applies to workers too)"
    ),
) -> None:
    """Run the application in development mode.

    Startup runs the boundary verifier and echoes violations as *non-fatal*
    warnings — the dev-time signal promised by SPEC §3.2 (``modulith verify``
    remains the hard CI gate). The single-process path execs uvicorn
    (optionally with ``--reload``), so the dev experience is identical to
    running uvicorn directly. The process-per-module path (``--topology
    processes``) runs the supervisor + reverse proxy: one worker subprocess
    per module behind a routing proxy on ``(host, port)``. ``--isolate
    MODULE`` also selects this path but restricts it to MODULE only — every
    other discovered module is not started, and its routes 404 through the
    proxy. (``--reload`` does not apply to the process topology in v1.)

    ``--log-level`` sets this process's root log level and is passed on to
    uvicorn (and, under the process topology, to every worker subprocess), so
    the whole deployment's verbosity comes from one flag.

    Exit codes: 0 on a clean launch (dev-time verifier warnings never fail
    the command), 1 on invalid arguments or configuration errors, 2 on
    unexpected internal errors.
    """
    _validate_topology(topology)
    _validate_app_module(app_module)
    level = _configure_cli_logging(log_level)
    if topology == "processes" or isolate is not None:
        os.environ.pop("MODULITH_DEV_WARN_ONLY", None)
        _run_process_topology(
            app_module=app_module,
            workers_json=None,
            isolate=isolate,
            host=host,
            port=port,
            log_level=level,
            verify_warn=True,
        )
        return

    # Keep the marker across uvicorn's reload child for single-process dev.
    os.environ["MODULITH_DEV_WARN_ONLY"] = "1"
    _echo_dev_verify_warnings(app_module)
    argv = ["uvicorn", app_module, "--host", host, "--port", str(port), "--log-level", level]
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
        # Square brackets are rich markup tags in typer's help renderer, so a
        # bare [tool.modulith.workers] is parsed as a style and deleted from
        # the output — leaving "replaces the pyproject  table entirely" with
        # the one load-bearing name missing. \[ escapes it back to a literal.
        help=r'JSON: {"reports": 4} (replaces the pyproject \[tool.modulith.workers] table entirely)',
    ),
    host: str = typer.Option("0.0.0.0"),
    port: int = typer.Option(8000),
    log_level: str = typer.Option(
        "info", help="debug | info | warning | error | critical (applies to workers too)"
    ),
    worker_port_base: int | None = typer.Option(
        None,
        min=1,
        max=65535,
        help=r"First worker port under --topology processes (default: \[tool.modulith] "
        "worker_port_base, else 9001); replicas take the following ports",
    ),
) -> None:
    r"""Run the application in production mode.

    Like ``dev`` minus reload (and minus the dev-time verifier warnings).
    Per-module worker counts (``--workers``) apply to the process-per-module
    topology: each module's worker count comes from the JSON map (with a
    ``default`` fallback). Passing ``--workers`` replaces the pyproject
    ``\[tool.modulith.workers]`` table entirely — a full override, not a
    per-module patch. The single-process path execs a plain uvicorn.

    ``--log-level`` sets this process's root log level and is passed on to
    uvicorn (and, under the process topology, to every worker subprocess), so
    the whole deployment's verbosity comes from one flag.

    Exit codes: 0 on a clean launch, 1 on invalid arguments or configuration
    errors, 2 on unexpected internal errors.
    """
    _validate_topology(topology)
    _validate_app_module(app_module)
    level = _configure_cli_logging(log_level)
    os.environ.pop("MODULITH_DEV_WARN_ONLY", None)
    if topology == "processes":
        _run_process_topology(
            app_module=app_module,
            workers_json=workers,
            isolate=None,
            host=host,
            port=port,
            log_level=level,
            warn_default_state_dir=True,
            worker_port_base=worker_port_base,
        )
        return

    argv = ["uvicorn", app_module, "--host", host, "--port", str(port), "--log-level", level]
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
    # through to strict semantics, ignoring the baseline.
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
            # exit 2 reserved for internal bugs.
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
        # path segments) and raises ConfigurationError with an actionable
        # message: a user/config error per the documented exit codes, not
        # the exit-2 traceback reserved for internal bugs.
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
# modulith extract — scaffold a standalone service from one module
# ---------------------------------------------------------------------------


@app.command()
def extract(
    module: str = typer.Argument(..., help="Module to extract into its own service"),
    output: Path | None = typer.Option(
        None,
        "--output",
        help="Directory to write the extracted service (default: <module>-service)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help=(
            "Extract despite boundary violations, shared tables or imports of other "
            "modules; never overrides the import check"
        ),
    ),
) -> None:
    """Scaffold a standalone service (pyproject, Dockerfile, README) from one module.

    Copies the module, its contracts and the package-level helpers they
    import into --output and generates the files needed to run it as its own
    process via ``modulith._worker:create_app``. Before publishing, imports
    the extracted module in a subprocess, so the service's third-party
    dependencies must be installed. Exit codes: 0 on success, 1 on an
    unknown module, boundary violations / shared tables / imports of other
    modules not overridden by --force, or (never overridable) an extracted
    module that fails to import or loads code from the source tree outside
    the extracted service, or an existing/unsafe --output path. A module-level
    import of another module therefore fails even with --force.
    """
    from .extract import extraction_blockers, import_closure, write_extraction

    rt = _bootstrap_or_exit()
    known = {m.name for m in rt.modules}
    if module not in known:
        available = ", ".join(sorted(known)) if known else "(none discovered)"
        typer.echo(f"error: unknown module {module!r}. Available modules: {available}", err=True)
        raise typer.Exit(code=1)

    if output is None:
        output = Path(f"{module}-service")

    cfg = rt.config
    assert cfg is not None and cfg.package is not None  # guaranteed by _bootstrap_or_exit

    violations = _collect_violations(rt)
    blockers = extraction_blockers(rt, module, violations)

    target = next(m for m in rt.modules if m.name == module)
    inbound = sorted(
        {
            other.name
            for other in rt.modules
            if other.name != module
            for record in verifier._collect_imports(other)
            if record.target_module == target.package
            or record.target_module.startswith(target.package + ".")
        }
    )
    if inbound:
        typer.echo(f"note: also imported by: {', '.join(inbound)}")

    if blockers and not force:
        typer.echo("error: extraction blocked:", err=True)
        for blocker in blockers:
            typer.echo(f"  {blocker}", err=True)
        typer.echo("Pass --force to extract anyway.", err=True)
        raise typer.Exit(code=1)

    package_dir = verifier._package_dir(cfg.package)
    if package_dir is None:
        typer.echo(f"error: could not resolve package directory for {cfg.package!r}", err=True)
        raise typer.Exit(code=1)

    helpers, _siblings = import_closure(rt, module)

    notes = blockers if (blockers and force) else []
    try:
        written = write_extraction(
            cfg=cfg,
            module=module,
            package_dir=package_dir,
            output=output,
            helpers=helpers,
            notes=notes,
        )
    except (OSError, ValueError) as exc:
        typer.echo(f"error: could not extract to {output}: {exc}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(f"extracted {module!r} to {output} ({len(written)} file(s)):")
    for name in written:
        typer.echo(f"  {name}")


# ---------------------------------------------------------------------------
# modulith audit — analyze an existing codebase
# ---------------------------------------------------------------------------


@app.command()
def audit(
    path: Path | None = typer.Argument(
        None,
        help="Directory whose subdirectories are the module candidates "
        "(default: the application package under the current directory)",
    ),
    output: Path = typer.Option(Path("MIGRATION.md")),
) -> None:
    """Analyze an existing codebase for modulith readiness.

    Non-destructive — only reads files (parsed via ``ast``, never imported).
    Produces a Markdown report with the proposed module structure, the
    cross-module imports that would become violations, shared tables that
    need ownership decisions, and a 0-100 readiness score.
    """
    from .audit import audit_codebase, find_audit_root, render_report, single_module_warning

    if path is not None and not path.exists():
        typer.echo(f"path does not exist: {path}", err=True)
        raise typer.Exit(code=1)
    if path is not None and not path.is_dir():
        typer.echo(f"not a directory: {path} (pass the directory to audit)", err=True)
        raise typer.Exit(code=1)

    try:
        cfg = load_configuration()
    except ConfigurationError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    root = find_audit_root(Path(".")) if path is None else path
    result = audit_codebase(root, contracts_module=cfg.contracts_module)
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
    typer.echo(f"audited {result.audited_root}")
    warning = single_module_warning(result)
    if warning is not None:
        typer.echo(f"warning: {warning}", err=True)
    if result.score_applicable:
        typer.echo(f"readiness score: {result.readiness_score}/100")
    else:
        typer.echo("readiness score: n/a (no module boundaries to score; see the warning)")
    typer.echo(
        f"{len(result.cross_module_imports)} cross-module import pattern(s), "
        f"{len(result.shared_tables)} shared table(s)"
    )
    typer.echo(f"wrote audit report to {output}")


# ---------------------------------------------------------------------------
# modulith k8s-manifest — Kubernetes Deployment/Service/Ingress generator
# ---------------------------------------------------------------------------


@app.command("k8s-manifest")
def k8s_manifest(
    output: Path = typer.Option(Path("modulith-k8s.yaml"), help="Output path, or '-' for stdout"),
    image: str | None = typer.Option(None, help="Container image (default: '<package>:latest')"),
    namespace: str | None = typer.Option(None, help="Kubernetes namespace for every object"),
    port: int = typer.Option(8000, help="Container port every worker listens on"),
    host: str | None = typer.Option(None, help="Ingress host"),
) -> None:
    """Generate Kubernetes Deployment/Service/Ingress manifests.

    One Deployment + Service per module discovered in the process-per-module
    topology, plus a single Ingress fanning out ``/<module>`` paths to each
    module's Service. The broker connection URL is never embedded in the
    manifest — see the generated header comment for the ``kubectl create
    secret`` commands to run once per cluster/namespace.
    """
    from . import k8s

    _runtime.configure(topology="processes")
    rt = _bootstrap_or_exit()
    cfg = rt.config
    assert cfg is not None  # ensure_bootstrapped guarantees this

    from .supervisor import derive_specs_from_config

    specs = derive_specs_from_config(
        {
            "package": cfg.package,
            "workers": dict(cfg.workers),
            "contracts_module": cfg.contracts_module,
        }
    )
    if not specs:
        typer.echo("no modules discovered", err=True)
        raise typer.Exit(code=1)

    resolved_image = image or f"{k8s.k8s_name(cfg.package or '')}:latest"
    try:
        manifest = k8s.render_manifests(
            cfg, specs, image=resolved_image, port=port, namespace=namespace, host=host
        )
    except ConfigurationError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    if str(output) == "-":
        typer.echo(manifest, nl=False)
        return

    try:
        output.write_text(manifest, encoding="utf-8")
    except OSError as exc:
        typer.echo(
            f"error: could not write manifest to {output} ({exc}). "
            "Create the directory or pass a writable --output path.",
            err=True,
        )
        raise typer.Exit(code=1) from None
    typer.echo(f"wrote {len(specs)} module manifest(s) to {output}")


# ---------------------------------------------------------------------------
# modulith doctor — operational health check
# ---------------------------------------------------------------------------


@app.command()
def doctor() -> None:
    """Report architectural and operational health.

    Runs nine checks — boundary health, process-split readiness, schema
    drift, outbox health, listener registration, the SHM notifier, actuator
    token, single-host broker, and redis retention — and prints a report.
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
# modulith openapi — build-time OpenAPI aggregation across worker modules
# ---------------------------------------------------------------------------


def _project_version() -> str | None:
    """Read ``[project].version`` from the nearest pyproject.toml, if any."""
    pyproject = _find_pyproject()
    if pyproject is None:
        return None
    try:
        with pyproject.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return None
    version = data.get("project", {}).get("version")
    return str(version) if version else None


@app.command()
def openapi(
    output: Path = typer.Option(Path("openapi.json"), help="Where to write the merged spec"),
    title: str | None = typer.Option(None, help="Spec title (default: the app package name)"),
    api_version: str | None = typer.Option(
        None, help="Spec version (default: [project].version, else 0.0.0)"
    ),
) -> None:
    """Aggregate every module's OpenAPI document into one build-time spec.

    Each worker process (see ``modulith._worker.create_app``) only ever
    serves its own module's document — there is no single running process
    with the whole application's surface. This command builds that surface
    offline: importing each module, generating its document in isolation,
    and merging them (schema names prefixed per module to avoid collisions)
    into one JSON file suitable for a gateway, an API portal, or
    client-generation tooling.
    """
    from . import openapi as openapi_module

    try:
        from fastapi.openapi.models import OpenAPI as FastAPIOpenAPI
    except ImportError as exc:
        typer.echo(
            "error: OpenAPI generation requires FastAPI. "
            "Install it with: pip install 'modupy[fastapi]' "
            f"({exc})",
            err=True,
        )
        raise typer.Exit(code=1) from None

    rt = _bootstrap_or_exit()
    cfg = rt.config
    assert cfg is not None  # ensure_bootstrapped guarantees this

    docs: dict[str, dict[str, Any]] = {}
    skipped: list[str] = []
    for info in sorted(rt.modules, key=lambda m: m.name):
        if info.name == cfg.contracts_module:
            continue
        module = importlib.import_module(info.package)
        doc = openapi_module.build_module_openapi(info.name, module)
        if doc is None:
            skipped.append(info.name)
            continue
        docs[info.name] = doc

    if not docs:
        typer.echo("error: no module exposes a router — nothing to document", err=True)
        raise typer.Exit(code=1)

    try:
        merged = openapi_module.merge_openapi(
            docs,
            title=title or cfg.package or "modulith",
            version=api_version or _project_version() or "0.0.0",
        )
        try:
            FastAPIOpenAPI(**merged)
        except ValueError as exc:
            raise openapi_module.OpenAPIMergeError(
                f"invalid merged OpenAPI document: {exc}"
            ) from None
    except openapi_module.OpenAPIMergeError as exc:
        typer.echo(f"error: could not merge OpenAPI documents: {exc}", err=True)
        raise typer.Exit(code=1) from None

    try:
        output.write_text(json.dumps(merged, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        typer.echo(
            f"error: could not write OpenAPI document to {output} ({exc}). "
            "Create the directory or pass a writable --output path.",
            err=True,
        )
        raise typer.Exit(code=1) from None

    typer.echo(f"wrote OpenAPI for {len(docs)} module(s) to {output}")
    if skipped:
        typer.echo(f"skipped (no router): {', '.join(skipped)}")


# ---------------------------------------------------------------------------
# modulith outbox — operational commands for the transactional outbox
# ---------------------------------------------------------------------------

outbox_app = typer.Typer(help="Outbox operational commands.")
app.add_typer(outbox_app, name="outbox")


# ---------------------------------------------------------------------------
# modulith broker — operational commands for the cross-process broker
# ---------------------------------------------------------------------------

broker_app = typer.Typer(help="Broker operational commands.")
app.add_typer(broker_app, name="broker")


def _exit_unless_shm_store_exists() -> None:
    """Exit 1 when the shm store drop-group would act on does not exist.

    Bootstrapping the shm broker creates its state directory and SQLite
    file, so the check has to run first, from configuration alone.
    """
    from .adapters.shm_broker import shm_store_path

    try:
        resolved = load_configuration(**_runtime._config_overrides)
    except ConfigurationError:
        return  # _bootstrap_or_exit reports it
    if resolved.broker != "shm":
        return
    package = resolved.package or _detect_from_pyproject_name()
    try:
        path = shm_store_path(package, dict(resolved.broker_options or {}))
    except ConfigurationError:
        return
    if not path.is_file():
        typer.echo(
            f"error: no shm broker store at {path}; nothing was removed. Run this with "
            "the service's broker configuration (state_dir or MODULITH_BROKER_STATE_DIR).",
            err=True,
        )
        raise typer.Exit(code=1)


def _sole_subscriber_warning(group: str, sole: list[str], broker: Any, scheme: str) -> str:
    names = ", ".join(repr(t) for t in sole)
    if scheme == "database" and broker.no_subscriber_policy in ("error", "wait"):
        effect = (
            f"under the database broker's {broker.no_subscriber_policy!r} no-subscriber "
            "policy, every later publish to them raises NoSubscribersError"
        )
    elif scheme == "database":
        effect = "later publishes to them are retained until a group subscribes"
    else:
        effect = "later publishes to them are stored but reach no consumer"
    return f"warning: {group!r} is the only subscriber of {names}; after the drop, {effect}."


@broker_app.command("drop-group")
def broker_drop_group(
    group: str = typer.Argument(..., help="Consumer group, e.g. 'modulith-notifications'"),
    force: bool = typer.Option(
        False, "--force", help="Drop the group even though a current module derives it."
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
    target: list[str] | None = typer.Option(
        None,
        "--target",
        help="Remove only this target's subscription and backlog (repeatable).",
    ),
) -> None:
    """Remove a retired module's group: its subscriptions and undelivered work.

    Applies to the shm and database brokers, which fan every publication out
    to each subscribed group and never prune undelivered rows. The group's
    queued messages are deleted, not delivered; prune then reclaims them.
    With ``--target``, only those targets are removed, which is how a stale
    target reported at worker start is cleaned up; a current module's group
    then needs no ``--force``.
    """
    _runtime.configure(topology="processes")
    _exit_unless_shm_store_exists()
    rt = _bootstrap_or_exit()
    cfg = rt.config
    assert cfg is not None
    broker = _group_ledger_broker(rt, cfg.broker)
    if broker is None:
        typer.echo(
            f"error: broker {cfg.broker!r} keeps no per-group subscription ledger; "
            f"drop-group applies to {' and '.join(_GROUP_LEDGER_BROKERS)}",
            err=True,
        )
        raise typer.Exit(code=1)
    targets = list(target) if target else None
    derived = _derived_consumer_groups(cfg.package, cfg.contracts_module)
    scope = f"group {group!r}" if targets is None else f"targets {targets!r} of group {group!r}"

    async def drop() -> tuple[int, int]:
        # One event loop for every call: the database broker binds its engine
        # to the loop that first used it.
        typer.echo(f"{cfg.broker} broker store: {broker.store_location}")
        if cfg.broker == "database" and not await broker.has_schema():
            typer.echo(
                f"error: no database broker tables at {broker.store_location}; "
                "nothing was removed. Run this with the service's broker configuration.",
                err=True,
            )
            raise typer.Exit(code=1)
        if targets is None and not force and group in await _live_groups(broker, derived):
            typer.echo(
                f"error: {group!r} belongs to a module of the current deployment or a "
                f"consumer served it in the last {_LIVE_GROUP_WINDOW_S // 3600} h. Dropping "
                "it deletes its queued messages undelivered, and its running workers receive "
                "no new publications until they restart and subscribe again. Pass --force "
                "to drop it anyway.",
                err=True,
            )
            raise typer.Exit(code=1)
        sole = [
            t
            for t in await broker.sole_subscriber_targets(group)
            if targets is None or t in targets
        ]
        if sole:
            typer.echo(_sole_subscriber_warning(group, sole, broker, cfg.broker))
        prompt = f"Drop {scope} and delete its pending and claimed messages?"
        if not yes and not typer.confirm(prompt):
            typer.echo("aborted — nothing was removed", err=True)
            raise typer.Exit(code=1)
        removed: tuple[int, int] = await broker.drop_group(group, targets=targets)
        return removed

    subscriptions, deliveries = asyncio.run(drop())
    if not subscriptions and not deliveries:
        typer.echo(
            f"error: {scope} has no subscription or undelivered work in the "
            f"{cfg.broker} broker's store; nothing was removed",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(
        f"dropped {scope}: {subscriptions} subscription(s), "
        f"{deliveries} pending or claimed delivery(ies)"
    )


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
    CLI checks existence itself and reports honestly. Mirrors ``force_retry``'s
    own lookup strategy: prefer the store's ``find_by_id`` direct point lookup
    when available, since ``find_incomplete``/``list_dead_lettered`` are both
    capped windows (LIMIT 100) that can miss a targeted row sitting further
    back in a large backlog. Stores without ``find_by_id`` fall back to the
    bounded scan.
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
    # conflict must be reported even when no store is configured.
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
    follows.
    """
    try:
        _add_project_root_to_syspath()
        app()
    except Exception:  # final safety net: internal bugs exit 2, distinctly
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
