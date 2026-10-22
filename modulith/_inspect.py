"""Bootstrap the application in a throwaway process and report its modules.

``modulith run`` and ``modulith dev`` under the process topology run this as a
subprocess (``python -m modulith._inspect``), so the supervisor, which outlives
every worker and serves the proxy that every request passes through, never
imports an application module. A module's import-time cost then stays out of
that process: memory, threads it starts, or a native crash, which matter most
for an ``own_process`` module that was given a process of its own for exactly
that reason.

The child does everything that needs the modules imported:

* the full bootstrap: discovery, plus the manifest and boundary checks that
  fail it;
* the dev-time boundary warnings;
* the report of which modules serve HTTP;
* which modules consume each event type (``_event_consumers``), from which the
  supervisor tells a worker hosting several modules what to send to the broker;
* the warning about what the app the command names sets up that no worker
  runs.

It then writes ``{"modules": [sorted names], "consumers": {...}}``, as JSON, to
the file its last argument names, before it imports the app. A bootstrap failure
exits with the CLI's own message and code. The arguments are the package, the
log level, ``1`` to echo boundary warnings, the ``--isolate`` module (empty for
none), the app the command names and the output file.

Importing the app runs code no worker runs, so it is bounded: an import still
going after ``_APP_CHECK_SECONDS`` skips the warning, and the child exits
without waiting for threads the import started.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .runtime import Runtime

logger = logging.getLogger(__name__)

# Every worker waits for this child, so the app import may delay them this long.
_APP_CHECK_SECONDS = 5.0


def _event_consumers(rt: Runtime, names: list[str]) -> dict[str, list[str]]:
    """The modules whose workers consume each fully-qualified event type.

    A listener registered outside any module import runs in whichever process
    publishes, so it counts for none. One registered by a file outside its
    owner's package, such as ``app/shared.py``, belongs to the module whose
    import first loaded that file, which can differ in each worker (see
    ``Runtime._owning_module_package``), so it counts for every module.
    """
    bus = rt.event_bus
    assert bus is not None  # bootstrapped
    consumers: dict[str, list[str]] = {}
    for event_type in bus.registered_event_types():
        modules: set[str] = set()
        for handler in bus.listeners_for(event_type):
            owner = rt._listener_owners.get(handler)
            if owner is None:
                continue
            home = rt._owning_module_package((getattr(handler, "__module__", None) or "",))
            modules.update([owner.rpartition(".")[2]] if home == owner else names)
        if modules:
            consumers[f"{event_type.__module__}.{event_type.__qualname__}"] = sorted(modules)
    return consumers


def _named(obj: object) -> str:
    return str(getattr(obj, "__qualname__", obj))


def _warn_about_dropped_app_setup(app_module: str, package: str, names: list[str]) -> None:
    """Warn about what ``app_module``'s app adds that no worker runs.

    A worker builds its own FastAPI app and mounts only each module's
    ``router`` (``modulith._worker.create_app``), so the app's middleware, its
    dependencies and its exception handlers never run under the process
    topology. Its lifespan goes unreported: the one every example has only
    bootstraps and starts the outbox, which a worker does itself.
    """
    from fastapi import FastAPI, routing
    from uvicorn.importer import ImportFromStringError, import_from_string

    module_path, _, attribute = app_module.partition(":")
    if not attribute or not f"{module_path}.".startswith(f"{package}."):
        return  # not an app of the package whose modules the workers run
    try:
        app = import_from_string(app_module)
    except ImportFromStringError:
        return  # no such app, as in an extracted service; the workers need none
    if not isinstance(app, FastAPI):
        return
    # Older FastAPI flattens included routes into app.routes.
    walk: Any = getattr(routing, "iter_route_contexts", iter)
    app_dependencies = [d.dependency for d in app.router.dependencies]
    added: dict[str, list[str]] = {}
    for name in names:
        router = getattr(sys.modules.get(f"{package}.{name}"), "router", None)
        if router is None:
            continue
        # The routes as a worker mounts them, keyed by endpoint.
        worker = FastAPI()
        worker.include_router(router, prefix=f"/{name}")
        own = {
            route.endpoint: [d.dependency for d in getattr(route, "dependencies", ())]
            for route in walk(worker.routes)
            if getattr(route, "endpoint", None) is not None
        }
        extra = [
            _named(d.dependency)
            for route in walk(app.routes)
            if (endpoint := getattr(route, "endpoint", None)) in own
            for d in route.dependencies
            if d.dependency not in own[endpoint] + app_dependencies
        ]
        if extra:
            added[name] = list(dict.fromkeys(extra))
    defaults = FastAPI().exception_handlers
    found = [
        ("middleware", [_named(m.kwargs.get("dispatch") or m.cls) for m in app.user_middleware]),
        ("app dependencies", [_named(d) for d in app_dependencies]),
        *((f"dependencies added to the routes of {name}", deps) for name, deps in added.items()),
        (
            "exception handlers",
            [_named(k) for k, h in app.exception_handlers.items() if defaults.get(k) is not h],
        ),
    ]
    lines = [f"  {label}: {', '.join(items)}" for label, items in found if items]
    if not lines:
        return
    logger.warning(
        "%s sets up what no worker runs: under the process topology a worker serves "
        "only its modules' routers under /<module>, and %s never runs there.\n%s\n"
        "Put a check a module's routes need on that module's own router, "
        "APIRouter(dependencies=[Depends(...)]), raise HTTPException in the module "
        "instead of relying on an app exception handler, and put app-wide middleware "
        "such as CORS in a proxy in front of modupy. See the README's layout rules: "
        "https://github.com/jeansossmeier/modupy#lay-out-your-project",
        app_module,
        module_path,
        "\n".join(lines),
    )


def main(argv: list[str]) -> int:
    package, log_level, verify_warn, isolate, app_module, out = argv

    import typer

    from .cli import (
        _collect_violations,
        _configure_cli_logging,
        _echo_violation_warnings,
        _ensure_bootstrapped_or_exit,
    )
    from .runtime import _runtime
    from .supervisor import _log_http_surface, discover_module_names

    try:
        _configure_cli_logging(log_level)
        _runtime.configure(package=package, topology="processes")
        rt = _ensure_bootstrapped_or_exit()
    except typer.Exit as exc:
        return exc.exit_code
    if verify_warn == "1":
        try:
            _echo_violation_warnings(_collect_violations(rt))
        except Exception as exc:  # dev-time signal only — never fatal
            typer.echo(f"modulith: boundary check skipped ({exc})", err=True)
    cfg = rt.config
    assert cfg is not None  # ensure_bootstrapped guarantees this
    names = discover_module_names(package, cfg.contracts_module)
    _log_http_surface(package, [name for name in names if name == isolate or not isolate])
    # Before the app import below, so listeners it registers count for none.
    report = {"modules": names, "consumers": _event_consumers(rt, names)}
    Path(out).write_text(json.dumps(report), encoding="utf-8")
    watchdog = threading.Timer(_APP_CHECK_SECONDS, _skip_slow_check, (app_module,))
    watchdog.daemon = True
    watchdog.start()
    try:
        _warn_about_dropped_app_setup(app_module, package, names)
    except Exception as exc:  # a warning only — never fatal
        typer.echo(f"modulith: check of {app_module} skipped ({exc})", err=True)
    finally:
        watchdog.cancel()
    return 0


def _skip_slow_check(app_module: str) -> None:
    """Give up on an app import that is still going; the report is written."""
    sys.stderr.write(
        f"modulith: check of {app_module} skipped (its import took over {_APP_CHECK_SECONDS:g} s)\n"
    )
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    code = main(sys.argv[1:])
    # Threads the app's import started would otherwise keep the command waiting.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
