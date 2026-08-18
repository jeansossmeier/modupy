"""Scaffold a standalone deployable service from one module of a modulith app.

Given one already-verified module, produces a self-contained package tree
(the module itself, its contracts, and the shared package entrypoint)
alongside a ``pyproject.toml``, ``Dockerfile``, ``README.md``, and
``.env.example`` so the module can run as its own process against
``modulith._worker:create_app``.
"""

from __future__ import annotations

import json
import shutil
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import _find_pyproject

if TYPE_CHECKING:
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
    return blockers


def _toml_scalar(value: Any) -> str:
    """Render a Python value as a TOML scalar/array/inline-table literal.

    JSON and TOML agree on string/number/bool/array syntax, so json.dumps
    covers those directly. A dict needs its own inline-table form since
    TOML uses ``key = value`` pairs inside ``{ }`` where JSON uses
    ``"key": value``.
    """
    if isinstance(value, dict):
        pairs = ", ".join(f"{k} = {_toml_scalar(v)}" for k, v in value.items())
        return "{ " + pairs + " }" if pairs else "{}"
    return json.dumps(value)


def _render_pyproject(*, cfg: Configuration, module: str, source_deps: list[str]) -> str:
    extras = ["fastapi", "cli"]
    if cfg.broker == "redis-streams":
        extras.append("redis")
    elif cfg.broker == "database":
        extras.append("database")
    if cfg.outbox == "postgres" and "database" not in extras:
        extras.append("database")
    if cfg.outbox == "postgres":
        extras.append("postgres")

    from . import __version__

    dependencies = [f"modupy[{','.join(extras)}]=={__version__}"]
    dependencies.extend(dep for dep in source_deps if not dep.startswith("modupy"))

    lines = [
        "[project]",
        f"name = {_toml_scalar(f'{module}-service')}",
        f'requires-python = ">={_PY_VERSION}"',
        f"dependencies = {_toml_scalar(dependencies)}",
        "",
        "[tool.modulith]",
        f"package = {_toml_scalar(cfg.package)}",
    ]
    for key in _CONFIG_TABLE_KEYS:
        if cfg.is_explicit(key):
            lines.append(f"{key} = {_toml_scalar(getattr(cfg, key))}")

    if cfg.outbox_options:
        lines += ["", "[tool.modulith.outbox_options]"]
        lines += [f"{k} = {_toml_scalar(v)}" for k, v in cfg.outbox_options.items()]

    broker_options = {k: v for k, v in cfg.broker_options.items() if k not in ("url", "dsn")}
    if broker_options:
        lines += ["", "[tool.modulith.broker_options]"]
        lines += [f"{k} = {_toml_scalar(v)}" for k, v in broker_options.items()]

    module_subscriptions = cfg.subscriptions.get(module)
    if module_subscriptions:
        lines += ["", "[tool.modulith.subscriptions]"]
        lines.append(f"{module} = {_toml_scalar(module_subscriptions)}")

    return "\n".join(lines) + "\n"


def _render_dockerfile(*, module: str, pkg_name: str) -> str:
    return (
        f"FROM python:{_PY_VERSION}-slim\n"
        "WORKDIR /app\n"
        "COPY pyproject.toml ./\n"
        f"COPY {pkg_name}/ ./{pkg_name}/\n"
        "RUN pip install --no-cache-dir .\n"
        f"ENV MODULITH_MODULE={module}\n"
        f"ENV MODULITH_APP_PACKAGE={pkg_name}\n"
        "EXPOSE 8000\n"
        'CMD ["uvicorn", "modulith._worker:create_app", "--factory", '
        '"--host", "0.0.0.0", "--port", "8000"]\n'
    )


def _render_env_example(*, cfg: Configuration, module: str, pkg_name: str) -> str:
    lines = [
        f"MODULITH_MODULE={module}",
        f"MODULITH_APP_PACKAGE={pkg_name}",
        f"MODULITH_BROKER={cfg.broker}",
        "MODULITH_BROKER_URL=",
    ]
    if cfg.broker == "redis-streams":
        lines.append("REDIS_URL=")
    lines += [
        "MODULITH_BROKER_SCHEMA=",
        "MODULITH_DB_URL=",
        "MODULITH_DB_SCHEMA=",
        "UVICORN_LOG_LEVEL=info",
    ]
    return "\n".join(lines) + "\n"


def _render_readme(
    *, cfg: Configuration, module: str, pkg_name: str, helpers: list[str], notes: list[str]
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
        "| `MODULITH_DB_URL` | Database connection string |",
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
        "The outbox is not auto-wired here: `main.py` is not copied into an "
        "extracted service, so code the worker imports must call "
        "`outbox.configure(store, serializer)` itself before the app starts "
        "handling requests.",
        "",
        "## Database migrations",
        "",
        "```",
        f"MODULITH_DB_URL=... alembic -c alembic.ini upgrade head -x schema={module}",
        "```",
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
            "Package-level code this module imports directly, outside any "
            "declared module and outside the contracts package. Copied so "
            "the extracted service still imports; review whether it belongs "
            "here or should become part of the contract:",
            "",
        ]
        lines += [f"- `{helper}`" for helper in helpers]

    if notes:
        lines += [
            "",
            "## Extraction notes",
            "",
            "Extracted with `--force`, overriding the following blockers:",
            "",
        ]
        lines += [f"- {note}" for note in notes]

    return "\n".join(lines) + "\n"


def write_extraction(
    *,
    cfg: Configuration,
    module: str,
    package_dir: Path,
    output: Path,
    helpers: list[str],
    notes: list[str],
) -> list[str]:
    """Copy *module* plus shared machinery into *output*, then render scaffolding.

    Returns the paths written, relative to *output*.
    """
    pkg_name = package_dir.name
    dest_pkg = output / pkg_name
    dest_pkg.mkdir(parents=True, exist_ok=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    written: list[str] = []

    def copy_rel(rel: Path) -> None:
        src = package_dir / rel
        dst = dest_pkg / rel
        if src.is_dir():
            shutil.copytree(src, dst, ignore=ignore, dirs_exist_ok=True)
            for path in sorted(dst.rglob("*")):
                if path.is_file():
                    written.append(str(path.relative_to(output)))
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            written.append(str(dst.relative_to(output)))

    copy_rel(Path("__init__.py"))
    copy_rel(Path(*module.split(".")))

    contracts_rel = Path(*cfg.contracts_module.split("."))
    if (package_dir / contracts_rel).is_dir():
        copy_rel(contracts_rel)
    elif (package_dir / f"{contracts_rel}.py").is_file():
        copy_rel(Path(f"{contracts_rel}.py"))

    for helper in helpers:
        parts = helper.split(".")[1:]  # drop the leading top-level package name
        if not parts:
            continue
        rel = Path(*parts)
        if (package_dir / rel).is_dir():
            copy_rel(rel)
        elif (package_dir / f"{rel}.py").is_file():
            copy_rel(Path(f"{rel}.py"))

    source_deps: list[str] = []
    pyproject_path = _find_pyproject()
    if pyproject_path is not None:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
        raw_deps = data.get("project", {}).get("dependencies", [])
        source_deps = [dep for dep in raw_deps if isinstance(dep, str)]

    generated = {
        "pyproject.toml": _render_pyproject(cfg=cfg, module=module, source_deps=source_deps),
        "Dockerfile": _render_dockerfile(module=module, pkg_name=pkg_name),
        ".env.example": _render_env_example(cfg=cfg, module=module, pkg_name=pkg_name),
        "README.md": _render_readme(
            cfg=cfg, module=module, pkg_name=pkg_name, helpers=helpers, notes=notes
        ),
    }
    for name, content in generated.items():
        (output / name).write_text(content, encoding="utf-8")
        written.append(name)

    return written
