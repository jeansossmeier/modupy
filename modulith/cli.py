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
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
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
from .config import ConfigurationError
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
    """
    try:
        _runtime.ensure_bootstrapped()
    except ConfigurationError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    return _runtime


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


def _run_process_topology(
    *,
    workers_json: str | None,
    isolate: str | None,
    host: str,
    port: int,
) -> None:
    """Spin up the process-per-module runtime: one worker per module + proxy.

    Bootstraps to resolve the application package, derives a ``WorkerSpec``
    per discovered module (honoring ``--workers`` counts and ``--isolate``),
    then runs the supervisor and reverse proxy on ``(host, port)``. Blocks
    until a shutdown signal arrives.
    """
    from .supervisor import derive_specs_from_config, run_supervised

    rt = _bootstrap_or_exit()
    cfg = rt.config
    assert cfg is not None  # ensure_bootstrapped guarantees this

    config: dict[str, Any] = {"package": cfg.package}
    if workers_json:
        try:
            config["workers"] = json.loads(workers_json)
        except json.JSONDecodeError as exc:
            typer.echo(f"invalid --workers JSON: {exc}", err=True)
            raise typer.Exit(code=1) from None
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
    asyncio.run(run_supervised(specs, host, port))


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

    The single-process path execs uvicorn (optionally with ``--reload``),
    so the dev experience is identical to running uvicorn directly. The
    process-per-module path (``--topology processes`` / ``--isolate``) runs
    the supervisor + reverse proxy: one worker subprocess per module behind a
    routing proxy on ``(host, port)``. (``--reload`` does not apply to the
    process topology in v1.)
    """
    if topology != "single" or isolate is not None:
        _run_process_topology(workers_json=None, isolate=isolate, host=host, port=port)
        return

    argv = ["uvicorn", app_module, "--host", host, "--port", str(port)]
    if reload:
        argv.append("--reload")
    typer.echo(
        f"modulith dev → {app_module} on http://{host}:{port} (reload={'on' if reload else 'off'})"
    )
    os.execvp("uvicorn", argv)


# ---------------------------------------------------------------------------
# modulith run — production mode (no reload)
# ---------------------------------------------------------------------------


@app.command()
def run(
    app_module: str = typer.Argument(...),
    topology: str = typer.Option("single"),
    workers: str | None = typer.Option(None, help='JSON: {"reports": 4}'),
    host: str = typer.Option("0.0.0.0"),
    port: int = typer.Option(8000),
) -> None:
    """Run the application in production mode.

    Like ``dev`` minus reload. Per-module worker counts (``--workers``) apply
    to the process-per-module topology: each module's worker count comes from
    the JSON map (with a ``default`` fallback). The single-process path execs
    a plain uvicorn.
    """
    if topology != "single":
        _run_process_topology(workers_json=workers, isolate=None, host=host, port=port)
        return

    argv = ["uvicorn", app_module, "--host", host, "--port", str(port)]
    typer.echo(f"modulith run → {app_module} on http://{host}:{port}")
    os.execvp("uvicorn", argv)


# ---------------------------------------------------------------------------
# modulith verify — boundary checks for CI
# ---------------------------------------------------------------------------


@app.command()
def verify(
    mode: str = typer.Option("strict", help="strict | ratchet"),
    baseline: Path = typer.Option(Path(".modulith-baseline.json")),
    update_baseline: bool = typer.Option(False, "--update-baseline"),
) -> None:
    """Run boundary verification.

    Exit code 0 if clean (strict) or no *new* violations (ratchet); 1
    otherwise. Designed to drop into CI as a single line. ``--update-baseline``
    records the current violation set as the accepted baseline and exits 0.
    """
    rt = _bootstrap_or_exit()
    modules = rt.modules
    pm = rt.plugin_manager

    violations: list[Violation] = []
    for module in modules:
        for result in pm.hook.modulith_verify_module(module=module, all_modules=modules):
            violations.extend(result)
    violations.extend(verifier.detect_cycles(modules))

    if update_baseline:
        verifier.write_baseline(baseline, violations)
        typer.echo(f"baseline updated: {len(violations)} violation(s) recorded in {baseline}")
        return

    if mode == "ratchet":
        grandfathered = verifier.load_baseline(baseline)
        reported = verifier.filter_against_baseline(violations, grandfathered)
    else:
        reported = violations

    _print_violations(reported)

    if any(v.severity is ViolationSeverity.ERROR for v in reported):
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# modulith docs — generate architecture documentation
# ---------------------------------------------------------------------------


@app.command()
def docs(
    output_dir: Path = typer.Option(Path("docs/modulith")),
) -> None:
    """Generate Mermaid diagrams and module canvases."""
    rt = _bootstrap_or_exit()
    modules = rt.modules
    pm = rt.plugin_manager

    produced: list[str] = []
    for result in pm.hook.modulith_render_documentation(
        modules=modules, output_dir=str(output_dir)
    ):
        produced.extend(result)

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

    result = audit_codebase(path)
    output.write_text(render_report(result), encoding="utf-8")
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
    Exits non-zero if any check reports an error, so it doubles as a CI gate.
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


@outbox_app.command("retry")
def outbox_retry(publication_id: str) -> None:
    """Force retry of a specific publication, bypassing backoff."""
    _bootstrap_or_exit()
    _require_outbox_store()
    try:
        pub_id = UUID(publication_id)
    except ValueError:
        typer.echo(f"invalid publication id {publication_id!r}: expected a UUID", err=True)
        raise typer.Exit(code=1) from None
    asyncio.run(outbox.force_retry(pub_id))
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
    _bootstrap_or_exit()
    _require_outbox_store()

    if retry_all and list_dead:
        typer.echo("--list and --retry-all are mutually exclusive", err=True)
        raise typer.Exit(code=1)

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
    """Console-script entry point."""
    app()


if __name__ == "__main__":
    main()
