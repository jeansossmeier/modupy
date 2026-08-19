"""Scaffold a standalone deployable service from one module of a modulith app.

Given one already-verified module, produces a self-contained package tree
(the module itself, its contracts, and the shared package entrypoint)
alongside a ``pyproject.toml``, ``Dockerfile``, ``README.md``, and
``.env.example`` so the module can run as its own process against
``modulith._worker:create_app``.
"""

from __future__ import annotations

import ast
import json
import shutil
import tempfile
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import _configured_broker_url, _find_pyproject

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
    broker_url = _configured_broker_url(cfg.broker_options)
    if (
        cfg.broker == "database"
        and isinstance(broker_url, str)
        and broker_url.startswith(("postgres://", "postgresql"))
        and not any(dep.lower().startswith("psycopg") for dep in dependencies)
    ):
        dependencies.append("psycopg[binary]>=3.1,<4.0")

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
    for initializer in initializers:
        if initializer.parent.is_symlink():
            raise ValueError(f"source package {initializer.parent} is a symlink")
        if initializer.is_symlink():
            raise ValueError(f"source package initializer {initializer} is a symlink")
        if not initializer.exists():
            continue
        try:
            tree = ast.parse(initializer.read_text(encoding="utf-8"), filename=str(initializer))
        except (OSError, SyntaxError, UnicodeDecodeError) as exc:
            raise ValueError(f"cannot validate package initializer {initializer}: {exc}") from None
        statements = [
            statement
            for statement in tree.body
            if not isinstance(statement, ast.Pass)
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


def _source_dependencies() -> list[str]:
    pyproject_path = _find_pyproject()
    if pyproject_path is None:
        return []
    with pyproject_path.open("rb") as file:
        data = tomllib.load(file)
    return [dep for dep in data.get("project", {}).get("dependencies", []) if isinstance(dep, str)]


def _write_generated_files(
    *,
    output: Path,
    cfg: Configuration,
    module: str,
    helpers: list[str],
    notes: list[str],
) -> list[str]:
    assert cfg.package is not None
    generated = {
        "pyproject.toml": _render_pyproject(
            cfg=cfg,
            module=module,
            source_deps=_source_dependencies(),
        ),
        "Dockerfile": _render_dockerfile(module=module, package=cfg.package),
        ".env.example": _render_env_example(cfg=cfg, module=module, package=cfg.package),
        "README.md": _render_readme(
            cfg=cfg,
            module=module,
            pkg_name=cfg.package,
            helpers=helpers,
            notes=notes,
        ),
    }
    for name, content in generated.items():
        (output / name).write_text(content, encoding="utf-8")
    return list(generated)


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
            notes=notes,
        )
        if output.is_symlink():
            raise FileExistsError(f"--output {output} appeared during extraction")
        if output.exists():
            if not output_was_empty or not output.is_dir() or any(output.iterdir()):
                raise FileExistsError(f"--output {output} changed during extraction")
            output.rmdir()
        staging.replace(output)
    except Exception:
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
    notes: list[str],
) -> list[str]:
    assert cfg.package is not None
    dest_pkg = output.joinpath(*cfg.package.split("."))
    dest_pkg.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    written: list[str] = []

    current = output
    for part in cfg.package.split("."):
        current /= part
        current.mkdir(exist_ok=True)
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
        parent = dest_pkg
        for part in rel.parent.parts:
            parent /= part
            parent.mkdir(exist_ok=True)
            init = parent / "__init__.py"
            if not init.exists():
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

    contracts_rel = Path(*cfg.contracts_module.split("."))
    if (package_dir / contracts_rel).is_dir():
        copy_rel(contracts_rel)
    elif (package_dir / f"{contracts_rel}.py").is_file():
        copy_rel(Path(f"{contracts_rel}.py"))

    for helper in helpers:
        prefix = f"{cfg.package}."
        if not helper.startswith(prefix):
            continue
        rel = Path(*helper.removeprefix(prefix).split("."))
        if (package_dir / rel).is_dir():
            copy_rel(rel)
        elif (package_dir / f"{rel}.py").is_file():
            copy_rel(Path(f"{rel}.py"))

    written.extend(
        _write_generated_files(
            output=output,
            cfg=cfg,
            module=module,
            helpers=helpers,
            notes=notes,
        )
    )

    return written
