"""The starter project `modulith init` writes, and the module `modulith add module` adds.

The starter is the quickstart example (``examples/quickstart``) under the
package name the user picks. ``tests/test_init.py`` holds it to that example
file for file, so the project the README walks through, the one CI runs and
the one ``init`` hands out cannot drift apart. ``init --empty`` writes only
its configuration, an app with no modules and one test.
"""

from __future__ import annotations

import ast
import importlib.util
import io
import keyword
import os
import re
import shutil
import tempfile
import tomllib
from pathlib import Path
from string import Template

# The package name the files below are written under, replaced by the user's.
_PACKAGE = "myapp"

_FILES: dict[str, str] = {
    "pyproject.toml": """\
[project]
name = "myapp"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["modupy[fastapi,cli]"]

[project.optional-dependencies]
test = ["pytest", "httpx2"]

[tool.modulith]
package = "myapp"

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "W", "F", "I", "B", "UP", "RUF"]

[tool.ruff.lint.isort]
known-first-party = ["myapp"]
known-third-party = ["modulith"]
""",
    "myapp/__init__.py": "",
    "myapp/main.py": """\
import logging

from fastapi import FastAPI

from myapp.inventory import router as inventory_router
from myapp.orders import router as orders_router

logging.basicConfig(level=logging.INFO)  # so the banner below is visible

app = FastAPI()
app.include_router(orders_router, prefix="/orders")
app.include_router(inventory_router, prefix="/inventory")

# That's it. Modules auto-discovered. Listeners auto-registered.
""",
    "myapp/contracts/__init__.py": "",
    "myapp/contracts/events.py": """\
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str


@event
@dataclass(frozen=True)
class PaymentReceived:
    order_id: str
""",
    "myapp/orders/__init__.py": '''\
from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived

_orders: dict[str, str] = {}  # order_id -> customer_id
_fulfilled: set[str] = set()


async def create_order(customer_id: str) -> str:
    order_id = f"ord-{len(_orders) + 1}"
    _orders[order_id] = customer_id  # your real persistence goes here
    await publish(OrderCreated(order_id=order_id))
    return order_id


def is_fulfilled(order_id: str) -> bool:
    return order_id in _fulfilled


@listener
async def on_payment(event: PaymentReceived) -> None:
    """Cross-module communication via events, not direct calls."""
    _fulfilled.add(event.order_id)  # your real fulfilment goes here


# Re-export the router onto the module package. Process-per-module mode serves
# HTTP by mounting each module package's `router` attribute under /<module>.
from myapp.orders.api import router as router  # noqa: E402
''',
    "myapp/orders/api.py": """\
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from myapp.orders import create_order, is_fulfilled

router = APIRouter()


class NewOrder(BaseModel):
    customer_id: str


@router.post("")
async def post_order(body: NewOrder) -> dict[str, str]:
    return {"order_id": await create_order(body.customer_id)}


@router.get("/{order_id}/fulfilment")
async def get_fulfilment(order_id: str) -> dict[str, str | bool]:
    if not is_fulfilled(order_id):
        raise HTTPException(status_code=404, detail="order not fulfilled")
    return {"order_id": order_id, "fulfilled": True}
""",
    "myapp/payments/__init__.py": """\
from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived


@listener
async def charge(event: OrderCreated) -> None:
    await publish(PaymentReceived(order_id=event.order_id))  # your real charge goes here
""",
    "myapp/inventory/__init__.py": """\
from fastapi import APIRouter, HTTPException
from modulith import listener

from myapp.contracts.events import OrderCreated

_reserved: set[str] = set()

router = APIRouter()


@listener
async def reserve(event: OrderCreated) -> None:
    _reserved.add(event.order_id)  # your real stock reservation goes here


@router.get("/{order_id}")
async def get_reservation(order_id: str) -> dict[str, str | bool]:
    if order_id not in _reserved:
        raise HTTPException(status_code=404, detail="order not reserved")
    return {"order_id": order_id, "reserved": True}
""",
    "tests/test_quickstart_flow.py": """\
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp, Scenario


def test_creating_an_order_fans_out_to_payments(
    scenario: Scenario, modulith_app: ModulithTestApp
) -> None:
    from myapp.contracts.events import OrderCreated, PaymentReceived
    from myapp.orders import create_order

    payment = scenario.call(create_order, "alice").expect_event(PaymentReceived).within(seconds=2)

    assert payment.order_id == "ord-1"
    assert OrderCreated(order_id="ord-1") in modulith_app.published_events


def test_payment_marks_the_order_fulfilled(scenario: Scenario) -> None:
    from myapp.contracts.events import PaymentReceived
    from myapp.orders import create_order, is_fulfilled

    assert not is_fulfilled("ord-1")

    scenario.call(create_order, "alice").expect_event(PaymentReceived).within(seconds=2)

    assert is_fulfilled("ord-1")


def test_http_flow_reaches_orders_and_inventory(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)

    created = client.post("/orders", json={"customer_id": "alice"})
    fulfilment = client.get("/orders/ord-1/fulfilment")
    reservation = client.get("/inventory/ord-1")

    assert created.status_code == 200
    assert created.json() == {"order_id": "ord-1"}
    assert fulfilment.status_code == 200
    assert fulfilment.json() == {"order_id": "ord-1", "fulfilled": True}
    assert reservation.status_code == 200
    assert reservation.json() == {"order_id": "ord-1", "reserved": True}


def test_unknown_order_is_404_until_its_events_arrive(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)
    fulfilment, reservation = "/orders/ord-1/fulfilment", "/inventory/ord-1"

    assert client.get(fulfilment).status_code == 404
    assert client.get(reservation).status_code == 404

    client.post("/orders", json={"customer_id": "alice"})

    assert client.get(fulfilment).status_code == 200
    assert client.get(reservation).status_code == 200


def test_an_orders_worker_serves_orders_and_not_inventory(
    modulith_app: ModulithTestApp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from modulith._worker import create_app

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    monkeypatch.setenv("MODULITH_MODULE", "orders")
    monkeypatch.setenv("MODULITH_APP_PACKAGE", "myapp")

    client = TestClient(create_app())

    paths = client.get("/orders/openapi.json").json()["paths"]

    assert "/orders" in paths
    assert "/orders/{order_id}/fulfilment" in paths
    assert not any(path.startswith("/inventory") for path in paths)
    assert client.get("/inventory/ord-1").status_code == 404
""",
}


_EMPTY_FILES: dict[str, str] = {
    "pyproject.toml": _FILES["pyproject.toml"],
    "myapp/__init__.py": "",
    "myapp/main.py": """\
import logging

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)  # so modupy's startup banner is visible

app = FastAPI()

# Modules are auto-discovered and their listeners auto-registered.
# `modulith add module NAME` adds one and mounts its router here.
""",
    "myapp/contracts/__init__.py": "",
    "myapp/contracts/events.py": '''\
"""The events modules publish and listen to, shared by all of them.

Declare each one as a frozen dataclass marked @event:

    from dataclasses import dataclass

    from modulith import event


    @event
    @dataclass(frozen=True)
    class OrderCreated:
        order_id: str
"""
''',
    "tests/test_app.py": """\
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp


def test_the_app_serves_its_openapi_document(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    response = TestClient(app).get("/openapi.json")

    assert response.status_code == 200
""",
}

_GITIGNORE = """\
__pycache__/
*.py[cod]
.venv/
.pytest_cache/
.ruff_cache/
.mypy_cache/

# modulith doctor's local cache of event schemas
.modulith-schemas.json
"""

# Actions at the versions modupy's own CI runs.
_GITHUB_CI = """\
name: CI

on: [push, pull_request]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: astral-sh/setup-uv@v7
      - run: uv sync --extra test
      - run: uv run modulith verify
      - run: uv run pytest
      - run: uvx ruff check myapp tests
      - run: uvx ruff format --check myapp tests
"""

GITHUB_CI_PATH = ".github/workflows/ci.yml"

# `modulith add module`: keys and contents are templates of $package and $module.
_MODULE_FILES: dict[str, str] = {
    "$module/__init__.py": '''\
"""The $module module.

Other modules may use only the public names defined here.
"""


def status() -> str:
    return "ok"  # replace with this module's real public functions


# `router` is what a worker process serves under /$module. It stays at the
# bottom because api.py imports this module's public functions from here.
from $package.$module.api import router as router  # noqa: E402
''',
    "$module/api.py": """\
from fastapi import APIRouter

from $package.$module import status

router = APIRouter()


@router.get("")
async def get_status() -> dict[str, str]:
    return {"module": "$module", "status": status()}
""",
    "test_$module.py": """\
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp


def test_the_module_serves_its_routes(modulith_app: ModulithTestApp) -> None:
$app
    response = TestClient(app).get("/$module")

    assert response.status_code == 200
    assert response.json() == {"module": "$module", "status": "ok"}
""",
}

# The app the module's test requests: main.py's, where the router was mounted,
# else an app of the test's own.
_TEST_APP_FROM_MAIN = "    from $package.main import app\n"
_TEST_APP_OF_ITS_OWN = """\
    from fastapi import FastAPI

    from $package.$module import router

    app = FastAPI()
    app.include_router(router, prefix="/$module")
"""

# ruff's default line length. Longer lines are split with a trailing comma,
# which keeps ruff from joining them again under any longer limit.
_LINE_LENGTH = 88


def _check_identifier(name: str, what: str, example: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or keyword.iskeyword(name):
        raise ValueError(f"{name!r} is not a lowercase Python {what} name, such as {example!r}")


def _refuse_existing(*paths: Path) -> None:
    """Raise ``FileExistsError`` naming the first path in the way of writing ``paths``.

    That is one of ``paths``, or a file or dangling symlink where a directory
    above one of them has to be.
    """
    for path in paths:
        if path.exists() or path.is_symlink():
            raise FileExistsError(path)
        parent = path.parent
        while not parent.is_dir():
            if parent.exists() or parent.is_symlink():
                raise FileExistsError(parent)
            parent = parent.parent


def requirements() -> list[str]:
    """The starter's dependencies and its ``test`` extra, as pip arguments."""
    project = tomllib.loads(_FILES["pyproject.toml"])["project"]
    return [*project["dependencies"], *project["optional-dependencies"]["test"]]


def write_starter(
    name: str,
    root: Path,
    *,
    existing_ok: bool = False,
    empty: bool = False,
    github_ci: bool = False,
) -> list[str]:
    """Write the starter project for package ``name`` into the directory ``root``.

    ``empty`` writes the skeleton without the sample modules, and
    ``github_ci`` adds a GitHub Actions workflow. A ``.gitignore`` is written
    unless ``root`` already has one. Returns the paths written, relative to
    ``root``. Raises ``ValueError`` for a name that cannot be the package and
    ``FileExistsError`` naming the path in the way: ``root`` itself, or with
    ``existing_ok`` (a fresh repository or an empty workspace) a
    ``pyproject.toml``, ``NAME``, ``tests`` or the workflow already in it, or a
    file where a directory has to be. Nothing is overwritten, and nothing is
    written when it raises.
    """
    _check_identifier(name, "package", "myapp")
    files = dict(_EMPTY_FILES if empty else _FILES)
    if github_ci:
        files[GITHUB_CI_PATH] = _GITHUB_CI
    if not (root / ".gitignore").exists() and not (root / ".gitignore").is_symlink():
        files = {".gitignore": _GITIGNORE, **files}
    targets = {
        template.replace(_PACKAGE, name): content.replace(_PACKAGE, name)
        for template, content in files.items()
    }
    # Checked before importability: under `python -m modulith` the cwd is on
    # sys.path, so an existing ./NAME would otherwise be reported as importable.
    if existing_ok and root.is_dir():
        _refuse_existing(*(root / rel for rel in ("pyproject.toml", name, "tests", *targets)))
    else:
        _refuse_existing(root)
    if name == "tests":
        raise ValueError("'tests' is the project's test directory; pick another package name")
    if importlib.util.find_spec(name) is not None:
        raise ValueError(f"{name!r} is already an importable module, which the project would hide")
    if name in {re.split(r"[^\w.-]", requirement)[0] for requirement in requirements()}:
        raise ValueError(f"{name!r} is a dependency of the project, so it cannot be its name")
    root.mkdir(parents=True, exist_ok=existing_ok)
    for rel, content in targets.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(content, encoding="utf-8")
    return list(targets)


def mount_lines(package: str, module: str) -> tuple[str, str]:
    """The import and the ``include_router`` call that serve ``module``'s router."""
    return (
        f"from {package}.{module} import router as {module}_router",
        f'app.include_router({module}_router, prefix="/{module}")',
    )


def _natural(name: str) -> list[str | int]:
    """``name`` as ruff's isort orders modules: ignoring case, with numbers by value."""
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", name.lower())]


def _mount_router(source: str, package: str, module: str) -> str | None:
    """``source`` with ``module``'s router imported and mounted on its ``app``.

    The import goes where ruff's isort puts it among the package's own
    imports, and the mount after the last ``app.include_router(...)``, else
    right after the app is built. None when ``source`` does not parse, or has
    no top-level imports and ``app = FastAPI(...)`` to place them by: an app
    that a factory builds is left for its author to mount the router in.
    """
    try:
        body = ast.parse(source).body
    except SyntaxError:
        return None
    apps = [
        node
        for node in body
        if isinstance(node, ast.Assign | ast.AnnAssign)
        and any(
            isinstance(target, ast.Name) and target.id == "app"
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        )
    ]
    if not apps:
        return None
    built = apps[0].value
    if not (
        isinstance(built, ast.Call) and ast.unparse(built.func) in ("FastAPI", "fastapi.FastAPI")
    ):
        return None
    imports = [
        node
        for node in body
        if isinstance(node, ast.Import | ast.ImportFrom) and node.lineno < apps[0].lineno
    ]
    if not imports:
        return None
    mounts = [
        node
        for node in body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and ast.unparse(node.value.func) == "app.include_router"
    ]

    def first_party(name: str) -> bool:
        return name == package or name.startswith(f"{package}.")

    own_from = [
        node
        for node in imports
        if isinstance(node, ast.ImportFrom) and node.level == 0 and first_party(node.module or "")
    ]
    own_plain = [
        node
        for node in imports
        if isinstance(node, ast.Import) and any(first_party(alias.name) for alias in node.names)
    ]
    relative = [node for node in imports if isinstance(node, ast.ImportFrom) and node.level]
    # Lines as the parser counts them: str.splitlines also splits at form feeds and the like.
    lines = io.StringIO(source, newline="").readlines()
    eol = "\r\n" if lines[0].endswith("\r\n") else "\n"
    if not lines[-1].endswith(("\n", "\r")):
        lines[-1] += eol
    import_line, mount_line = mount_lines(package, module)
    if len(import_line) > _LINE_LENGTH:
        import_line = f"from {package}.{module} import ({eol}    router as {module}_router,{eol})"
    if len(mount_line) > _LINE_LENGTH:
        mount_line = (
            f'app.include_router({eol}    {module}_router,{eol}    prefix="/{module}",{eol})'
        )
    # The mount comes after every import, so inserting it first keeps their line numbers.
    lines.insert(
        max(node.end_lineno or node.lineno for node in [apps[0], *mounts]), mount_line + eol
    )
    # ruff's isort sorts `import` statements before `from` ones, and puts the
    # relative imports in a section of their own after the package's.
    later = [
        node for node in own_from if _natural(node.module or "") > _natural(f"{package}.{module}")
    ]
    if later:
        lines.insert(later[0].lineno - 1, import_line + eol)
    elif own_from or own_plain:
        last = (own_from or own_plain)[-1]
        lines.insert(last.end_lineno or last.lineno, import_line + eol)
    elif relative:
        lines.insert(relative[0].lineno - 1, import_line + eol + eol)
    else:
        lines.insert(imports[-1].end_lineno or imports[-1].lineno, eol + import_line + eol)
    return "".join(lines)


def _replace_text(path: Path, text: str) -> None:
    """Write ``text`` over ``path`` by renaming a new file onto it, so a failed write leaves it whole."""
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        shutil.copymode(path, temporary)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def write_module(
    root: Path, package: str, module: str, *, contracts: str = "contracts"
) -> tuple[list[str], str | None]:
    """Write module ``module`` of ``package``, with its test, into the project at ``root``.

    The package is ``root/package`` or ``root/src/package``. The module gets a
    router, and its test goes in ``root/tests``. When the package's
    ``main.py`` can be written and builds ``app = FastAPI(...)`` at top level,
    the router is mounted there at ``/module``; otherwise the test serves the
    router on an app of its own. Returns the paths created and the ``main.py``
    changed, or None, relative to ``root``. Raises ``ValueError`` for a name
    that cannot be a module or a package not found, and ``FileExistsError``
    naming a path in the way. Nothing is overwritten, and nothing is written
    when it raises.
    """
    _check_identifier(module, "module", "billing")
    if module == contracts:
        raise ValueError(f"{module!r} is the contracts package; pick another module name")
    package_dir = next(
        (
            directory
            for directory in (root / package, root / "src" / package)
            if (directory / "__init__.py").is_file()
        ),
        None,
    )
    if package_dir is None:
        raise ValueError(f"no package {package!r} in {root} or {root / 'src'}")
    main = package_dir / "main.py"
    try:
        # Read undecoded line endings, so a CRLF file stays one; a symlink is not replaced.
        writable = main.is_file() and not main.is_symlink() and os.access(main, os.W_OK)
        source = main.read_bytes().decode("utf-8") if writable else None
    except (OSError, UnicodeDecodeError):
        source = None
    mounted = None if source is None else _mount_router(source, package, module)
    names = {"package": package, "module": module}
    names["app"] = Template(
        _TEST_APP_OF_ITS_OWN if mounted is None else _TEST_APP_FROM_MAIN
    ).substitute(names)
    targets = {
        (root / "tests" if template.startswith("test_") else package_dir)
        / Template(template).substitute(names): Template(content).substitute(names)
        for template, content in _MODULE_FILES.items()
    }
    _refuse_existing(package_dir / module, package_dir / f"{module}.py", *targets)
    for path, content in targets.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    created = [path.relative_to(root).as_posix() for path in targets]
    if mounted is None:
        return created, None
    try:
        _replace_text(main, mounted)
    except OSError:
        return created, None
    return created, main.relative_to(root).as_posix()
