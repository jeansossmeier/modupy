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
* the report of which modules serve HTTP.

It then writes the sorted module names, as JSON, to the file its last argument
names. A bootstrap failure exits with the CLI's own message and code. The
arguments are the package, the log level, ``1`` to echo boundary warnings, the
``--isolate`` module (empty for none) and the output file.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    package, log_level, verify_warn, isolate, out = argv

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
    Path(out).write_text(json.dumps(names), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
