"""The modulith CLI.

Built on typer. Provides commands for development, running, verification,
documentation generation, and operational maintenance.

Implementation status: SKELETON. ~150 lines when complete.

Critical design principle: the CLI is a *progressive enhancement*, not
a requirement. Users with muscle memory for `uvicorn` keep using it.
`modulith dev` is *almost* `uvicorn --reload` with quality-of-life
additions; running an app without the CLI works the same way.

Distribution: shipped via [project.scripts] in pyproject.toml so
`pip install modulith[cli]` makes `modulith` available on PATH.
"""

from __future__ import annotations

import sys
from pathlib import Path

# typer is an optional dependency — only loaded when the CLI is invoked.
try:
    import typer
except ImportError:
    print(
        "modulith CLI requires the 'cli' extra. Install with:\n  pip install 'modulith[cli]'",
        file=sys.stderr,
    )
    sys.exit(1)


app = typer.Typer(
    name="modulith",
    help="Modular monolith pattern for Python.",
    no_args_is_help=True,
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

    IMPLEMENTATION TODO:
    1. If topology == "single" and isolate is None:
       - Print modulith banner with detected modules
       - Exec uvicorn with the given arguments
    2. If topology == "processes" or isolate is set:
       - Start the supervisor (modulith.supervisor.Supervisor)
       - Supervisor reads workers config from pyproject.toml
       - Supervisor's reverse proxy listens on the host:port
       - Each worker runs uvicorn against modulith._worker:create_app

    See modulith/supervisor.py and modulith/_worker.py for the
    process-per-module path.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


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

    IMPLEMENTATION TODO:
    Same logic as `dev` minus reload. Worker counts from --workers
    override the pyproject.toml [tool.modulith.workers] section.
    """
    raise NotImplementedError("Phase 1")


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

    Exit code 0 if no violations (strict) or no new violations (ratchet).
    Exit code 1 otherwise. Designed to drop into CI as a single line.

    IMPLEMENTATION TODO:
    1. Bootstrap modulith (loads modules, plugin manager).
    2. For each module, call pm.hook.modulith_verify_module.
    3. Run cycle detection across all modules.
    4. Aggregate all violations.
    5. If mode == "ratchet": load baseline, filter, write new baseline
       if --update-baseline.
    6. Print violations grouped by module, with file:line.
    7. Exit 0 or 1.

    See modulith/builtin/verifier.py for the implementation hooks.
    """
    raise NotImplementedError("Phase 1")


# ---------------------------------------------------------------------------
# modulith docs — generate architecture documentation
# ---------------------------------------------------------------------------


@app.command()
def docs(
    output_dir: Path = typer.Option(Path("docs/modulith")),
) -> None:
    """Generate Mermaid diagrams and module canvases.

    IMPLEMENTATION TODO:
    1. Bootstrap modulith.
    2. Call pm.hook.modulith_render_documentation(modules, output_dir).
    3. Print the list of files produced.
    """
    raise NotImplementedError("Phase 1")


# ---------------------------------------------------------------------------
# modulith audit — analyze an existing codebase (Phase 2)
# ---------------------------------------------------------------------------


@app.command()
def audit(
    output: Path = typer.Option(Path("MIGRATION.md")),
) -> None:
    """Analyze an existing codebase for modulith readiness.

    Non-destructive — only reads files. Produces a Markdown report with:
      - Proposed module structure
      - Cross-module imports that would become violations
      - Shared tables that need ownership decisions
      - Modulith-readiness score (0-100)

    IMPLEMENTATION: see modulith/audit.py (Phase 2).
    """
    raise NotImplementedError("Phase 2 — see modulith/audit.py")


# ---------------------------------------------------------------------------
# modulith doctor — operational health check (Phase 2)
# ---------------------------------------------------------------------------


@app.command()
def doctor() -> None:
    """Report architectural and operational health.

    Output sections:
      - Boundary health: violation count, baseline drift
      - Process-split readiness: % cross-module via events
      - Schema drift: events whose definitions changed
      - Outbox health: dead-lettered count, oldest incomplete age
      - Listener registration: declared vs registered

    IMPLEMENTATION: see modulith/doctor.py (Phase 2).
    """
    raise NotImplementedError("Phase 2 — see modulith/doctor.py")


# ---------------------------------------------------------------------------
# modulith outbox — operational commands for the transactional outbox
# ---------------------------------------------------------------------------

outbox_app = typer.Typer(help="Outbox operational commands.")
app.add_typer(outbox_app, name="outbox")


@outbox_app.command("status")
def outbox_status() -> None:
    """Show outbox counts: incomplete, completed, dead-lettered."""
    raise NotImplementedError("Phase 1")


@outbox_app.command("retry")
def outbox_retry(publication_id: str) -> None:
    """Force retry of a specific publication, bypassing backoff."""
    raise NotImplementedError("Phase 1")


@outbox_app.command("purge")
def outbox_purge(
    older_than: str = typer.Option("30d", help="e.g. 7d, 24h, 30m"),
) -> None:
    """Delete completed publications older than threshold."""
    raise NotImplementedError("Phase 1")


# ---------------------------------------------------------------------------
# modulith info — show detected configuration
# ---------------------------------------------------------------------------


@app.command()
def info() -> None:
    """Print detected configuration, modules, and active plugins.

    Useful for debugging "why isn't my plugin loading" / "what package
    did modulith detect" questions.

    IMPLEMENTATION TODO:
    1. Bootstrap modulith.
    2. Print:
       - Application package (auto-detected or explicit)
       - Discovered modules (one per line, with their packages)
       - Active configuration (outbox, broker, topology, observability)
       - Loaded plugins (built-ins + entry points)
       - Registered brokers (scheme -> class name)
       - Whether a manifest is present per module
    """
    raise NotImplementedError("Phase 1")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """Console-script entry point."""
    app()


if __name__ == "__main__":
    main()
