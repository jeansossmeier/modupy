"""Scaffold a standalone deployable service from one module of a modulith app.

Given one already-verified module, produces a self-contained package tree
(the module itself, its contracts, and the shared package entrypoint)
alongside a ``pyproject.toml``, ``Dockerfile``, ``README.md``, and
``.env.example`` so the module can run as its own process against
``modulith._worker:create_app``.
"""

from __future__ import annotations

import ast
import importlib.machinery
import importlib.metadata
import json
import keyword
import re
import shutil
import site
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import _configured_broker_url, _find_pyproject

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from .config import Configuration
    from .runtime import Runtime
    from .types import Violation

_PY_VERSION = "3.11"
_CONFIG_TABLE_KEYS = (
    "contracts_module",
    "outbox",
    "broker",
    "subscription_source",
    "production",
    "verify_manifests",
    "strict_boundaries",
    "observability",
)


def extraction_blockers(rt: Runtime, module: str, violations: list[Violation]) -> list[str]:
    """--force-overridable reasons extraction of *module* should be blocked.

    Two checks: outbound boundary violations *module* itself produces (not
    baseline-filtered — a grandfathered violation still breaks the
    extracted standalone app), and tables *module* shares with another
    module elsewhere in the codebase. Module existence and a non-empty
    output directory are hard gates the caller checks separately, since
    ``--force`` never overrides them.
    """
    from .audit import audit_codebase
    from .builtin.verifier import _collect_table_refs, _package_dir

    blockers = [
        f"[{v.severity.value.upper()}] {v.rule}: {v.message}"
        + (f" ({v.location})" if v.location else "")
        for v in violations
        if v.module == module
    ]

    _helpers, siblings = _closure(rt, module)
    if siblings:
        listed = ", ".join(f"{name} ({where})" for name, where in sorted(siblings.items()))
        blockers.append(
            f"imports declared module(s): {listed}; the extracted service does not contain them"
        )

    target = next((m for m in rt.modules if m.name == module), None)
    cfg = rt.config
    if target is not None and cfg is not None and cfg.package is not None:
        package_dir = _package_dir(cfg.package)
        if package_dir is not None:
            audit = audit_codebase(package_dir, contracts_module=cfg.contracts_module)
            own_tables = {table for table, _location, _kind in _collect_table_refs(target)}
            shared = own_tables & set(audit.shared_tables)
            if shared:
                blockers.append(f"shares table(s) with another module: {', '.join(sorted(shared))}")
            if audit.parse_failures:
                blockers.append(
                    f"{len(audit.parse_failures)} file(s) could not be parsed during the "
                    "shared-table scan; results are incomplete"
                )
    return blockers


def _source_path(package_dir: Path, package: str, dotted: str) -> Path | None:
    """The package directory or module file *dotted* names under *package*, if any.

    A module file is a ``.py`` source, a compiled extension module or a sourceless ``.pyc``.
    """
    if not dotted.startswith(f"{package}."):
        return None
    rel = Path(*dotted.removeprefix(f"{package}.").split("."))
    if (package_dir / rel / "__init__.py").is_file():
        return package_dir / rel
    suffixes = (
        ".py",
        *importlib.machinery.EXTENSION_SUFFIXES,
        *importlib.machinery.BYTECODE_SUFFIXES,
    )
    for suffix in suffixes:
        if (package_dir / f"{rel}{suffix}").is_file():
            return package_dir / f"{rel}{suffix}"
    return None


def _contracts_source(package_dir: Path, package: str, contracts_module: str) -> Path | None:
    """The contracts source in Python's own order: a regular package, a module, a namespace dir."""
    source = _source_path(package_dir, package, f"{package}.{contracts_module}")
    namespace_dir = package_dir.joinpath(*contracts_module.split("."))
    if source is None and namespace_dir.is_dir():
        return namespace_dir
    return source


def import_closure(rt: Runtime, module: str) -> tuple[list[str], list[str]]:
    """Helper modules the extracted copy needs, and other declared modules it imports.

    See ``_closure`` for what counts as either.
    """
    helpers, siblings = _closure(rt, module)
    return helpers, sorted(siblings)


def _closure(rt: Runtime, module: str) -> tuple[list[str], dict[str, str]]:
    """The helpers and, per imported sibling module, where its import first appears.

    A sibling maps to ``path:line``, the path relative to the directory that holds
    the application's top-level package and the line of the import that sorts first.

    Scans the module, the contracts package and every helper found, until no
    new helper appears. A helper is package-level code outside every declared
    module and outside the contracts package. ``from pkg import name`` also
    counts ``pkg.name`` when that is a module or package, since
    ``target_module`` alone names only ``pkg``. Type-only imports of another
    module are not reported: they never execute.
    """
    from .builtin.verifier import (
        _file_package,
        _ImportCollector,
        _owning_module,
        _package_dir,
        _parse_source,
    )

    cfg = rt.config
    target = next((m for m in rt.modules if m.name == module), None)
    if cfg is None or cfg.package is None or target is None:
        return [], {}
    package = cfg.package
    package_dir = _package_dir(package)
    if package_dir is None:
        return [], {}
    contracts = f"{package}.{cfg.contracts_module}"
    source_root = package_dir.parents[len(package.split(".")) - 1]

    helpers: set[str] = set()
    siblings: dict[str, tuple[str, int]] = {}
    pending = [target.package, contracts]
    scanned: set[str] = set()
    while pending:
        name = pending.pop()
        if name in scanned:
            continue
        scanned.add(name)
        if name == contracts:
            source = _contracts_source(package_dir, package, cfg.contracts_module)
        else:
            source = _source_path(package_dir, package, name)
        if source is None:
            continue
        for path in sorted(source.rglob("*.py")) if source.is_dir() else [source]:
            if path.suffix != ".py":
                continue  # a compiled module has no source to scan
            try:
                tree = _parse_source(path)
            except SyntaxError:
                continue
            collector = _ImportCollector(path, _file_package(package_dir, package, path))
            collector.visit(tree)
            for record in collector.records:
                submodules = [
                    f"{record.target_module}.{imported}" for imported in record.imported_names
                ]
                for candidate in [
                    record.target_module,
                    *(s for s in submodules if _source_path(package_dir, package, s)),
                ]:
                    if not candidate.startswith(f"{package}."):
                        continue
                    if candidate == contracts or candidate.startswith(f"{contracts}."):
                        continue
                    owner = _owning_module(candidate, rt.modules)
                    if owner is not None:
                        if owner.name != module and not record.type_only:
                            where = (path.relative_to(source_root).as_posix(), record.line)
                            siblings[owner.name] = min(siblings.get(owner.name, where), where)
                        continue
                    # Importing a.b.c runs a/__init__.py and a/b/__init__.py too, so
                    # every resolvable ancestor package is part of the closure.
                    parts = candidate.split(".")
                    for depth in range(len(package.split(".")) + 1, len(parts) + 1):
                        name = ".".join(parts[:depth])
                        if _source_path(package_dir, package, name) is not None:
                            helpers.add(name)
                            pending.append(name)
    return sorted(helpers), {name: f"{file}:{line}" for name, (file, line) in siblings.items()}


_IMPORT_CHECK_UNDER = """
def under(path, parent):
    path, parent = os.path.normcase(path), os.path.normcase(parent)
    try:
        return os.path.commonpath([path, parent]) == parent
    except ValueError:  # different drives on Windows
        return False
"""

_DISTRIBUTIONS_MARKER = "modulith-gate-distributions:"

_IMPORT_CHECK = (
    """
import importlib, importlib.metadata, json, os, re, site, sys, sysconfig
root, dotted, source = sys.argv[1:4]
declared = set(json.loads(sys.argv[4]))
loaded_before = set(sys.modules)
sys.path.insert(0, root)
importlib.import_module(dotted)
# The worker imports these two at start-up and tolerates only their absence.
for name in sys.argv[5:]:
    try:
        importlib.import_module(name)
    except ModuleNotFoundError as exc:
        if exc.name != name:
            raise
"""
    + _IMPORT_CHECK_UNDER
    + """
root, source = os.path.realpath(root), os.path.realpath(source)
# A directory holding the project would exempt its first-party code too. That
# covers a prefix and, on Windows, site.getsitepackages(), which lists each
# prefix itself; the library directories inside such a prefix stay exempt.
prefixes = {os.path.realpath(p) for p in (sys.prefix, sys.base_prefix, sys.exec_prefix)}
libraries = [sysconfig.get_path(n) for n in ("stdlib", "platstdlib", "purelib", "platlib")]
libraries += site.getsitepackages() + [site.getusersitepackages()]
libraries = {os.path.realpath(p) for p in libraries} - prefixes
# Extension modules (Windows DLLs) and pip VCS checkouts sit beside a prefix's
# library directories. They stay exempt, but unlike a library directory they do
# not narrow an app installed there to its top-level package.
extras = {os.path.join(p, sub) for p in prefixes for sub in ("DLLs", "src")}
# An installed copy of the app shares its library directory with every other
# distribution, so only the app's own top-level package counts as first-party.
if any(under(source, p) for p in libraries):
    source = os.path.join(source, dotted.split(".")[0])
exempt = {p for p in prefixes | libraries | extras if not under(source, p)}
by_top_level = importlib.metadata.packages_distributions()


def is_declared(name):
    owners = by_top_level.get(name.split(".")[0], ())
    return any(re.sub(r"[-_.]+", "-", d).lower() in declared for d in owners)


leaked = sorted(
    name
    for name, mod in list(sys.modules.items())
    if isinstance(getattr(mod, "__file__", None), str)
    and under(os.path.realpath(mod.__file__), source)
    and not under(os.path.realpath(mod.__file__), root)
    and not any(under(os.path.realpath(mod.__file__), p) for p in exempt)
    and not is_declared(name)
)
if leaked:
    sys.exit(
        "ImportError: imported " + ", ".join(leaked) + " from the source tree " + source
        + ", outside the extracted service; move that code into the module or a helper "
        "under the package, or declare it as a dependency"
    )
new_top_levels = {n.split(".")[0] for n in sys.modules if n not in loaded_before}
new_top_levels.discard(dotted.split(".")[0])
distributions = sorted({d for top in new_top_levels for d in by_top_level.get(top, ())})
print("""
    + repr(_DISTRIBUTIONS_MARKER)
    + """, json.dumps(distributions), file=sys.stderr)
"""
)

_IMPORT_CHECK_TIMEOUT = 120
_TRACEBACK_FRAME = re.compile(r'^\s*File "(.+)", line (\d+)')


def _last_frame(stderr_lines: list[str], gate_root: Path) -> str | None:
    """``path:line`` of the last traceback frame in a file, or None when there is none.

    A path inside the throw-away *gate_root* is shown relative to it, which is
    also its path in the source package. The gate script's own frame
    (``<string>``) and frozen modules have no file to point at.
    """
    for line in reversed(stderr_lines):
        match = _TRACEBACK_FRAME.match(line)
        if match is None or match[1].startswith("<"):
            continue
        path = Path(match[1])
        if path.is_relative_to(gate_root):
            path = path.relative_to(gate_root)
        return f"{path.as_posix()}:{match[2]}"
    return None


def _check_imports(
    root: Path,
    dotted: str,
    source: Path,
    also: Sequence[str] = (),
    declared: Collection[str] = (),
) -> list[str]:
    """Import *dotted* in a fresh interpreter from the extracted tree at *root*; raise if it fails.

    Each module of *also* is imported too, and only that exact module being
    absent is tolerated, as ``_worker._import_contracts`` and
    ``_import_manifest`` do. Returns the third-party distributions the imports
    loaded, so the caller can compare them with what the service declares.

    *root* goes first on the child's ``sys.path`` explicitly, so neither
    ``PYTHONSAFEPATH`` nor an installed copy of the monolith can shadow it.
    Any module the child then loads from *source* (the monolith's source
    directory, reachable through ``PYTHONPATH`` or an editable install) is a
    failure: the deployed service will not have it. When *source* lies in a
    library directory (the app is an installed copy), only the app's top-level
    package counts, so other installed distributions stay exempt.

    A leaked module whose top-level package belongs to an installed
    distribution named in *declared* (canonical names) is exempt: the service
    installs that distribution instead of carrying a copy.

    Runs the extracted module's code, which is acceptable because extract is
    a trusted-source tool that already imports the app to discover modules.
    The child runs on a throw-away copy of *root*, so files that import writes
    never reach the published output. Its stderr goes to a temporary file and
    its stdout nowhere: ``subprocess.run`` waits for a pipe's EOF, which a
    descendant the import leaves running would hold open past the child's exit.
    """
    with tempfile.TemporaryDirectory(prefix=f".{root.name}.gate.", dir=root.parent) as scratch:
        gate_root = Path(scratch) / root.name
        shutil.copytree(root, gate_root)
        with tempfile.TemporaryFile() as stderr_file:
            try:
                returncode = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        "-c",
                        _IMPORT_CHECK,
                        str(gate_root),
                        dotted,
                        str(source),
                        json.dumps(sorted(declared)),
                        *also,
                    ],
                    cwd=gate_root,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr_file,
                    timeout=_IMPORT_CHECK_TIMEOUT,
                ).returncode
            except subprocess.TimeoutExpired:
                raise ValueError(f"importing {dotted} from the extracted tree timed out") from None
            stderr_file.seek(0)
            lines = stderr_file.read().decode(errors="replace").strip().splitlines()
    if returncode != 0:
        reason = lines[-1] if lines else f"exit code {returncode}"
        frame = _last_frame(lines, gate_root)
        where = f" (at {frame})" if frame else ""
        raise ValueError(f"the extracted service cannot import {dotted}: {reason}{where}")
    for line in reversed(lines):
        if line.startswith(_DISTRIBUTIONS_MARKER):
            distributions: list[str] = json.loads(line.removeprefix(_DISTRIBUTIONS_MARKER))
            return distributions
    return []


def _canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


_EXTRA_MARKER = re.compile(r"""extra\s*==\s*['"]([^'"]+)['"]""")


def _declared_distributions(dependencies: list[str]) -> set[str]:
    """Canonical names of the distributions installing *dependencies* brings in, transitively.

    Reads the extracting environment's metadata. A requirement gated on an
    extra counts only when that extra is requested; other environment
    markers are ignored, so the result can overstate what a platform installs.
    """
    declared: set[str] = set()
    seen: set[tuple[str, frozenset[str]]] = set()
    pending = list(dependencies)
    while pending:
        match = _REQUIREMENT_HEAD.match(pending.pop())
        if match is None:
            continue
        name = _canonical_name(match.group(1))
        extras = frozenset(
            _canonical_name(extra) for extra in (match.group(2) or "").split(",") if extra.strip()
        )
        if (name, extras) in seen:
            continue
        seen.add((name, extras))
        declared.add(name)
        try:
            requires = importlib.metadata.requires(name) or []
        except importlib.metadata.PackageNotFoundError:
            continue
        for requirement in requires:
            gates = {_canonical_name(extra) for extra in _EXTRA_MARKER.findall(requirement)}
            if not gates or gates & extras:
                pending.append(requirement)
    return declared


def _validate_module_name(value: str, *, what: str) -> None:
    """Reject a module/helper name that is not a safe dotted Python identifier.

    ``module`` (and each ``helpers`` entry) comes from ``ModuleInfo.name``/
    import records sourced from the pluggable ``modulith_discover_modules``
    hook — untrusted, plugin-supplied input — and is turned into a filesystem
    path via ``Path(*value.split("."))`` before any write. An absolute-looking
    or ``..``-carrying value would escape the intended output tree (mirrors
    ``config._validate_contracts_module``'s treatment of the sibling field).
    """
    parts = value.split(".")
    if not value or any(not part.isidentifier() or keyword.iskeyword(part) for part in parts):
        raise ValueError(
            f"{what} must be a non-empty dot-separated Python identifier, got {value!r}"
        )


def _toml_scalar(value: Any) -> str:
    """Render a Python value as a TOML scalar/array/inline-table literal.

    JSON and TOML agree on string/number/bool/array syntax, so json.dumps
    covers those directly. A dict needs its own inline-table form since
    TOML uses ``key = value`` pairs inside ``{ }`` where JSON uses
    ``"key": value``.
    """
    if value is None:
        raise ValueError("None has no TOML scalar representation")
    if isinstance(value, dict):
        pairs = ", ".join(
            f"{json.dumps(str(k))} = {_toml_scalar(v)}" for k, v in value.items() if v is not None
        )
        return "{ " + pairs + " }" if pairs else "{}"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_scalar(item) for item in value) + "]"
    return json.dumps(value)


_REQUIREMENT_HEAD = re.compile(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[([^\]]*)\])?")


def _is_modupy_requirement(dep: str) -> bool:
    match = _REQUIREMENT_HEAD.match(dep)
    return match is not None and _canonical_name(match.group(1)) == "modupy"


def _extract_modupy_extras(source_deps: list[str]) -> set[str]:
    for dep in source_deps:
        match = _REQUIREMENT_HEAD.match(dep)
        if match and _is_modupy_requirement(dep):
            return {extra.strip() for extra in (match.group(2) or "").split(",") if extra.strip()}
    return set()


def _dependencies(cfg: Configuration, source_deps: list[str]) -> list[str]:
    extras = ["fastapi", "cli"]
    if cfg.broker == "redis-streams":
        extras.append("redis")
    elif cfg.broker == "database":
        extras.append("database")
    if cfg.outbox == "postgres" and "database" not in extras:
        extras.append("database")
    if cfg.outbox == "postgres":
        extras.append("postgres")

    source_extras = _extract_modupy_extras(source_deps)
    merged_extras = sorted(set(extras) | source_extras)

    from . import __version__

    dependencies = [f"modupy[{','.join(merged_extras)}]=={__version__}"]
    dependencies.extend(dep for dep in source_deps if not _is_modupy_requirement(dep))
    broker_url = _configured_broker_url(cfg.broker_options)
    if (
        cfg.broker == "database"
        and isinstance(broker_url, str)
        and broker_url.startswith(("postgres://", "postgresql"))
        and not any(dep.lower().startswith("psycopg") for dep in dependencies)
    ):
        dependencies.append("psycopg[binary]>=3.1,<4.0")
    return dependencies


def _render_pyproject(*, cfg: Configuration, module: str, source_deps: list[str]) -> str:
    dependencies = _dependencies(cfg, source_deps)
    assert cfg.package is not None
    root_package = cfg.package.split(".", 1)[0]
    lines = [
        "[build-system]",
        'requires = ["hatchling>=1.27"]',
        'build-backend = "hatchling.build"',
        "",
        "[project]",
        f"name = {_toml_scalar(f'{module}-service')}",
        'version = "0.1.0"',
        f'requires-python = ">={_PY_VERSION}"',
        f"dependencies = {_toml_scalar(dependencies)}",
        "",
        "[tool.hatch.build.targets.wheel]",
        f"packages = {_toml_scalar([root_package])}",
        "",
        "[tool.modulith]",
        f"package = {_toml_scalar(cfg.package)}",
    ]
    for key in _CONFIG_TABLE_KEYS:
        value = getattr(cfg, key)
        if cfg.is_explicit(key) and value is not None:
            lines.append(f"{json.dumps(key)} = {_toml_scalar(value)}")

    if cfg.outbox_options:
        lines += ["", "[tool.modulith.outbox_options]"]
        lines += [
            f"{json.dumps(str(k))} = {_toml_scalar(v)}"
            for k, v in cfg.outbox_options.items()
            if v is not None
        ]

    broker_options = {k: v for k, v in cfg.broker_options.items() if k not in ("url", "dsn")}
    if broker_options:
        lines += ["", "[tool.modulith.broker_options]"]
        lines += [
            f"{json.dumps(str(k))} = {_toml_scalar(v)}"
            for k, v in broker_options.items()
            if v is not None
        ]

    module_subscriptions = cfg.subscriptions.get(module)
    if module_subscriptions:
        lines += ["", "[tool.modulith.subscriptions]"]
        lines.append(f"{json.dumps(module)} = {_toml_scalar(module_subscriptions)}")

    if cfg.is_explicit("verify_disabled_rules") and cfg.verify_disabled_rules:
        lines += ["", "[tool.modulith.verify]"]
        lines.append(f"disabled_rules = {_toml_scalar(list(cfg.verify_disabled_rules))}")

    return "\n".join(lines) + "\n"


def _render_dockerfile(*, module: str, package: str) -> str:
    root_package = package.split(".", 1)[0]
    return (
        f"FROM python:{_PY_VERSION}-slim\n"
        "WORKDIR /app\n"
        "COPY pyproject.toml ./\n"
        f"COPY {root_package}/ ./{root_package}/\n"
        "RUN pip install --no-cache-dir .\n"
        f"ENV MODULITH_MODULE={module}\n"
        f"ENV MODULITH_APP_PACKAGE={package}\n"
        "EXPOSE 8000\n"
        'CMD ["uvicorn", "modulith._worker:create_app", "--factory", '
        '"--host", "0.0.0.0", "--port", "8000"]\n'
    )


def _render_env_example(*, cfg: Configuration, module: str, package: str) -> str:
    lines = [
        f"MODULITH_MODULE={module}",
        f"MODULITH_APP_PACKAGE={package}",
        f"MODULITH_BROKER={cfg.broker}",
        "MODULITH_BROKER_URL=",
    ]
    if cfg.broker == "redis-streams":
        lines.append("REDIS_URL=")
    lines += [
        "MODULITH_BROKER_SCHEMA=",
        "MODULITH_OUTBOX=",
        "MODULITH_OUTBOX_URL=",
        "# Alembic migrations only; the runtime outbox reads MODULITH_OUTBOX_URL.",
        "MODULITH_DB_URL=",
        "MODULITH_DB_SCHEMA=",
        "UVICORN_LOG_LEVEL=info",
    ]
    return "\n".join(lines) + "\n"


def _unresolved_dynamic_imports(output: Path, dest_pkg: Path, package: str) -> list[str]:
    """README bullets for each dynamic import in the copied sources the scan could not resolve."""
    from .builtin.verifier import _file_package, _ImportCollector, _parse_source

    entries: list[str] = []
    for path in sorted(dest_pkg.rglob("*.py")):
        try:
            tree = _parse_source(path)
        except SyntaxError:
            continue
        collector = _ImportCollector(path, _file_package(dest_pkg, package, path))
        collector.visit(tree)
        where = path.relative_to(output).as_posix()
        entries += [f"`{where}:{line}` `{call}`" for line, call in collector.unresolved]
    return entries


_UNDECLARED_NOTE = (
    "The import check loaded these third-party distributions, which the generated "
    "`pyproject.toml` dependencies do not install (directly or through `modupy`'s own "
    "requirements). Add the ones the service needs to `dependencies`:"
)


_DEPENDENCIES_OUTSIDE_PROJECT_NOTE = (
    "The source `pyproject.toml` declares dependencies outside `[project].dependencies` "
    "(`dynamic` lists `dependencies`, or `[tool.poetry]` is present). The generated "
    "`pyproject.toml` takes its dependencies from `[project].dependencies` only, so add "
    "the ones the service needs to `dependencies` by hand."
)


def _render_readme(
    *,
    cfg: Configuration,
    module: str,
    pkg_name: str,
    helpers: list[str],
    notes: list[str],
    dynamic_imports: Sequence[str] = (),
    undeclared: Sequence[str] = (),
    dependencies_outside_project: bool = False,
) -> str:
    lines = [
        f"# {module}-service",
        "",
        f"Standalone service extracted from the `{module}` module of `{pkg_name}`.",
        "",
        "## Run locally",
        "",
        "```",
        "pip install -e .",
        f"MODULITH_MODULE={module} MODULITH_APP_PACKAGE={pkg_name} "
        "uvicorn modulith._worker:create_app --factory",
        "```",
        "",
        "Or run it the way the whole application runs, one process per module:",
        "",
        "```",
        f"modulith run {pkg_name}:app --topology processes",
        "```",
        "",
        "## Run with Docker",
        "",
        "```",
        f"docker build -t {module}-service .",
        f"docker run --env-file .env -p 8000:8000 {module}-service",
        "```",
        "",
        "## Environment variables",
        "",
        "| Variable | Purpose |",
        "| --- | --- |",
        f"| `MODULITH_MODULE` | Module to serve (this image serves only `{module}`) |",
        "| `MODULITH_APP_PACKAGE` | Top-level application package |",
        "| `MODULITH_BROKER` | Broker adapter scheme |",
        "| `MODULITH_BROKER_URL` | Broker connection string |",
        "| `MODULITH_BROKER_SCHEMA` | Broker-side schema/prefix |",
        "| `MODULITH_OUTBOX` | Outbox adapter (`memory`, or a durable adapter such as `postgres`) |",
        "| `MODULITH_OUTBOX_URL` | Async SQLAlchemy URL of the database the outbox store binds to |",
        "| `MODULITH_DB_URL` | Connection string the Alembic migrations read |",
        "| `MODULITH_DB_SCHEMA` | Database schema for this module |",
        "| `UVICORN_LOG_LEVEL` | uvicorn log level |",
        "",
        "## Routes",
        "",
        f"`/health` is served at the root (no reverse proxy in this standalone "
        "service); the module's own routes are mounted under "
        f"`/{module}`.",
        "",
        "## Outbox",
        "",
        "The service binds its outbox store from `MODULITH_OUTBOX_URL` "
        "(`[tool.modulith] outbox_url`) at startup. When `MODULITH_OUTBOX` "
        "is not `memory`, the service refuses to start without one "
        "(unless module code it imports binds a store itself): `main.py` is "
        "not copied into an extracted service, so its lifespan wiring never "
        "runs.",
        "",
        "## Database migrations",
        "",
        "```",
        "MODULITH_DB_URL=... alembic -c "
        "\"$(python -c 'import modulith.adapters, pathlib; "
        'print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")\')" '
        f"-x schema={module} upgrade head",
        "```",
        "",
        "PostgreSQL requires `psycopg[binary]`; it is included when the "
        "configured database-broker URL uses PostgreSQL.",
        "",
        "## Next steps",
        "",
        "- `modulith k8s-manifest` generates a Deployment/Service for this image.",
        "- `modulith openapi` generates this service's OpenAPI schema.",
    ]

    if helpers:
        lines += [
            "",
            "## Copied helper modules — review these",
            "",
            "Package-level code this module or its contracts import, directly or "
            "transitively, outside any "
            "declared module and outside the contracts package. Copied so "
            "the extracted service still imports; review whether it belongs "
            "here or should become part of the contract:",
            "",
        ]
        lines += [f"- `{helper}`" for helper in helpers]

    if notes or dynamic_imports or undeclared or dependencies_outside_project:
        lines += ["", "## Extraction notes"]
    if dependencies_outside_project:
        lines += ["", _DEPENDENCIES_OUTSIDE_PROJECT_NOTE]
    if notes:
        lines += [
            "",
            "Extracted with `--force`, overriding the following blockers:",
            "",
        ]
        lines += [f"- {note}" for note in notes]
    if dynamic_imports:
        lines += [
            "",
            "The import scan is static and could not resolve these dynamic imports, so "
            "anything they load was not copied. Check that each target is in the service:",
            "",
        ]
        lines += [f"- {entry}" for entry in dynamic_imports]
    if undeclared:
        lines += ["", _UNDECLARED_NOTE, ""]
        lines += [f"- `{name}`" for name in undeclared]

    return "\n".join(lines) + "\n"


def _required_package_initializers(package_dir: Path, package: str) -> list[Path]:
    package_parts = package.split(".")
    source_root = package_dir
    for _ in package_parts:
        source_root = source_root.parent

    initializers: list[Path] = []
    current = source_root
    for part in package_parts:
        current /= part
        initializers.append(current / "__init__.py")
    return initializers


def _validate_initializers(initializers: list[Path]) -> None:
    from .builtin.verifier import _parse_source

    for initializer in initializers:
        if initializer.parent.is_symlink():
            raise ValueError(f"source package {initializer.parent} is a symlink")
        if initializer.is_symlink():
            raise ValueError(f"source package initializer {initializer} is a symlink")
        if not initializer.exists():
            continue
        try:
            tree = _parse_source(initializer)
        except (OSError, SyntaxError) as exc:
            raise ValueError(f"cannot validate package initializer {initializer}: {exc}") from None
        statements = [
            statement
            for statement in tree.body
            if not isinstance(statement, ast.Pass)
            and not (isinstance(statement, ast.ImportFrom) and statement.module == "__future__")
            and not (
                isinstance(statement, ast.Expr)
                and isinstance(statement.value, ast.Constant)
                and isinstance(statement.value.value, str)
            )
        ]
        if statements:
            raise ValueError(
                f"package initializer {initializer} contains imports or executable behavior "
                "that extraction cannot preserve safely; move that behavior into the selected "
                "module or an explicit startup hook"
            )


def _validate_package_initializers(package_dir: Path, package: str) -> None:
    _validate_initializers(_required_package_initializers(package_dir, package))


def _source_dependencies() -> tuple[list[str], bool]:
    """The source's ``[project].dependencies`` and whether it declares others elsewhere."""
    pyproject_path = _find_pyproject()
    if pyproject_path is None:
        return [], False
    with pyproject_path.open("rb") as file:
        data = tomllib.load(file)
    project = data.get("project", {})
    dependencies = [dep for dep in project.get("dependencies", []) if isinstance(dep, str)]
    outside = "dependencies" in project.get("dynamic", []) or "poetry" in data.get("tool", {})
    return dependencies, outside


def _write_generated_files(
    *,
    output: Path,
    cfg: Configuration,
    module: str,
    helpers: list[str],
    notes: list[str],
    dynamic_imports: list[str],
    source_deps: list[str],
    undeclared: list[str],
    dependencies_outside_project: bool,
) -> list[str]:
    assert cfg.package is not None
    generated = {
        "pyproject.toml": _render_pyproject(cfg=cfg, module=module, source_deps=source_deps),
        "Dockerfile": _render_dockerfile(module=module, package=cfg.package),
        ".env.example": _render_env_example(cfg=cfg, module=module, package=cfg.package),
        "README.md": _render_readme(
            cfg=cfg,
            module=module,
            pkg_name=cfg.package,
            helpers=helpers,
            notes=notes,
            dynamic_imports=dynamic_imports,
            undeclared=undeclared,
            dependencies_outside_project=dependencies_outside_project,
        ),
    }
    for name, content in generated.items():
        (output / name).write_text(content, encoding="utf-8")
    return list(generated)


def refuse_split_package(package: str) -> None:
    """Raise ``ValueError`` naming every directory when *package* spans more than one."""
    from .builtin.verifier import _package_portions

    portions = _package_portions(package)
    if len(portions) > 1:
        raise ValueError(
            f"package {package!r} spans {len(portions)} directories "
            f"({', '.join(str(portion) for portion in portions)}); extraction copies one "
            "directory and would leave out the others, so merge them into one directory first"
        )


def installed_copy_location(package_dir: Path) -> Path | None:
    """The interpreter library directory holding *package_dir*, or None for a source tree.

    Windows lists each prefix itself among the site-packages directories; a
    prefix is not a library directory, as in the import check.
    """
    resolved = package_dir.resolve()
    prefixes = {Path(p).resolve() for p in (sys.prefix, sys.base_prefix, sys.exec_prefix)}
    for directory in (*site.getsitepackages(), site.getusersitepackages()):
        library = Path(directory)
        if library.resolve() not in prefixes and resolved.is_relative_to(library.resolve()):
            return library
    return None


def write_extraction(
    *,
    cfg: Configuration,
    module: str,
    package_dir: Path,
    output: Path,
    helpers: list[str],
    notes: list[str],
) -> list[str]:
    """Build the extraction in staging, then publish it atomically to *output*.

    Returns the paths written, relative to *output*.
    """
    assert cfg.package is not None
    refuse_split_package(cfg.package)
    _validate_module_name(module, what="module")
    module_path = package_dir / Path(*module.split("."))
    if not (module_path.is_dir() and (module_path / "__init__.py").is_file()):
        raise ValueError(f"module {module!r} does not exist as a valid Python package")
    for helper in helpers:
        _validate_module_name(helper, what="helper")
        if _source_path(package_dir, cfg.package, helper) is None:
            raise ValueError(
                f"helper {helper!r} does not exist as a module or package under {cfg.package!r}"
            )
    if package_dir.is_symlink():
        raise ValueError(f"source package {package_dir} is a symlink")
    _validate_package_initializers(package_dir, cfg.package)
    source_root = package_dir.resolve(strict=True)
    output = output.absolute()
    resolved_output = output.resolve(strict=False)
    if output.is_symlink():
        raise ValueError(f"--output {output} is a symlink")
    output_was_empty = output.is_dir() and not any(output.iterdir())
    if output.exists() and not output_was_empty:
        raise FileExistsError(f"--output {output} already exists and is not empty")
    if resolved_output == source_root or resolved_output.is_relative_to(source_root):
        raise ValueError(f"--output {output} is inside source package {source_root}")

    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        written = _populate_extraction(
            cfg=cfg,
            module=module,
            package_dir=source_root,
            output=staging,
            helpers=helpers,
        )
        source_deps, dependencies_outside_project = _source_dependencies()
        loaded = _check_imports(
            staging,
            f"{cfg.package}.{module}",
            source_root.parents[len(cfg.package.split(".")) - 1],
            also=(f"{cfg.package}.{cfg.contracts_module}", f"{cfg.package}.{module}._manifest"),
            declared=_declared_distributions(source_deps),
        )
        covered = _declared_distributions(_dependencies(cfg, source_deps))
        undeclared = sorted(name for name in loaded if _canonical_name(name) not in covered)
        if undeclared:
            print(
                "warning: the import check loaded third-party distribution(s) the generated "
                f"dependencies do not install: {', '.join(undeclared)}; see Extraction notes "
                "in the README",
                file=sys.stderr,
            )
        written += _write_generated_files(
            output=staging,
            cfg=cfg,
            module=module,
            helpers=helpers,
            notes=notes,
            dynamic_imports=_unresolved_dynamic_imports(
                staging, staging.joinpath(*cfg.package.split(".")), cfg.package
            ),
            source_deps=source_deps,
            undeclared=undeclared,
            dependencies_outside_project=dependencies_outside_project,
        )
        if output.is_symlink():
            raise FileExistsError(f"--output {output} appeared during extraction")
        if output.exists():
            if not output_was_empty or not output.is_dir() or any(output.iterdir()):
                raise FileExistsError(f"--output {output} changed during extraction")
            output.rmdir()
        staging.replace(output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return written


def _populate_extraction(
    *,
    cfg: Configuration,
    module: str,
    package_dir: Path,
    output: Path,
    helpers: list[str],
) -> list[str]:
    assert cfg.package is not None
    dest_pkg = output.joinpath(*cfg.package.split("."))
    dest_pkg.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    written: list[str] = []

    # A PEP 420 namespace root keeps no initializer, or it would hide the namespace's
    # other portions (an installed company.common beside company.shop).
    current = output
    for part, source_init in zip(
        cfg.package.split("."),
        _required_package_initializers(package_dir, cfg.package),
        strict=True,
    ):
        current /= part
        if source_init.exists():
            init = current / "__init__.py"
            init.write_text("", encoding="utf-8")
            written.append(str(init.relative_to(output)))

    def validate_source(src: Path) -> None:
        for path in (src, *src.rglob("*")):
            if path.is_symlink():
                raise ValueError(
                    f"source symlink {path} is not supported; extracted source must be "
                    "self-contained"
                )

    def ensure_parent_packages(rel: Path) -> None:
        # A namespace folder stays one: an initializer would make module discovery
        # see it as an extra module in the extracted service.
        parent = dest_pkg
        for depth, part in enumerate(rel.parent.parts, start=1):
            parent /= part
            parent.mkdir(exist_ok=True)
            init = parent / "__init__.py"
            source_init = package_dir.joinpath(*rel.parent.parts[:depth], "__init__.py")
            if source_init.exists() and not init.exists():
                init.write_text("", encoding="utf-8")
                written.append(str(init.relative_to(output)))

    def copy_rel(rel: Path) -> None:
        src = package_dir / rel
        dst = dest_pkg / rel
        validate_source(src)
        _validate_initializers(
            [
                package_dir.joinpath(*rel.parent.parts[:depth], "__init__.py")
                for depth in range(1, len(rel.parent.parts) + 1)
            ]
        )
        ensure_parent_packages(rel)
        if src.is_dir():
            shutil.copytree(src, dst, ignore=ignore, symlinks=True, dirs_exist_ok=True)
            for path in sorted(dst.rglob("*")):
                if path.is_file():
                    written.append(str(path.relative_to(output)))
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst, follow_symlinks=False)
            written.append(str(dst.relative_to(output)))

    copy_rel(Path(*module.split(".")))

    helper_set = set(helpers)

    def inside_helper(dotted: str) -> bool:
        parts = dotted.split(".")
        return any(".".join(parts[:depth]) in helper_set for depth in range(1, len(parts)))

    for helper in helpers:
        if inside_helper(helper):
            continue  # already copied inside its ancestor package
        source = _source_path(package_dir, cfg.package, helper)
        if source is not None:
            copy_rel(source.relative_to(package_dir))

    contracts = f"{cfg.package}.{cfg.contracts_module}"
    contracts_src = _contracts_source(package_dir, cfg.package, cfg.contracts_module)
    if contracts_src is not None and not inside_helper(contracts):
        copy_rel(contracts_src.relative_to(package_dir))

    return written
