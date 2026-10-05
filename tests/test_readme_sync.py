"""Guard tests: the README's example project must hold together on its own.

A reader building from the README types only what the README shows, so every
python block in it is a claim with no other checker behind it:

- the block parses as Python — nothing imports these blocks (they reference an
  example package that does not exist at test time), so a typo in them ships;
- every module the blocks import from the example package is a module some
  block also shows how to write. A block importing ``myapp.orders.api`` while
  no block defines ``# myapp/orders/api.py`` leaves the reader with an
  ``ImportError`` and no file to create;
- the prescribed directory tree lists ``pyproject.toml``. Every CLI command
  except ``audit`` resolves the application package from it, so a tree that
  omits it produces a project where the CLI exits 1;
- the 30-second pitch, rebuilt from its own blocks plus the empty package
  files its prose names, makes modupy discover exactly the modules its banner
  lists. A directory without an ``__init__.py`` is not a module, so a pitch
  that never mentions one shows a banner no reader can reproduce.

Every expectation is derived from README.md itself — including the name of the
example package, taken from the path comments that introduce the blocks — so
renaming the package cannot quietly disable the guard.
"""

from __future__ import annotations

import ast
import difflib
import re
import sys
from collections.abc import Callable
from pathlib import Path

from modulith.builtin.discovery import modulith_discover_modules

from conftest import Block, fenced_blocks

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"
QUICKSTART = REPO / "examples" / "quickstart"
QUICKSTART_README = QUICKSTART / "README.md"
PITCH = "The 30-second pitch"
QUICKSTART_SECTIONS = (PITCH, "Quickstart")
PATH_COMMENT = re.compile(r"^#\s*([\w./-]+\.py)\s*$")
BANNER = re.compile(r"discovered (\d+) module\(s\): (.+)")
EMPTY_INIT = re.compile(r"`((?:[\w.-]+/)+__init__\.py)`")
TREE_BRANCHES = ("├──", "└──")


def _python_blocks(blocks: list[Block] | None = None) -> list[tuple[Block, str | None]]:
    """Python blocks paired with the file path their first line declares, if any.

    ``blocks`` defaults to every fenced block in the README; pass one section's
    blocks to look at only those.

    A block without a path comment is a fragment, not a file: it contributes
    nothing to the set of modules the README defines, but its imports are still
    checked.
    """
    paired: list[tuple[Block, str | None]] = []
    for block in blocks if blocks is not None else fenced_blocks(README):
        if block.lang != "python":
            continue
        body = block.body.strip()
        match = PATH_COMMENT.match(body.splitlines()[0].strip()) if body else None
        paired.append((block, match.group(1) if match else None))
    return paired


def _example_package(blocks: list[tuple[Block, str | None]]) -> str:
    """The example application package, read off the blocks' path comments."""
    roots: dict[str, list[int]] = {}
    for block, path in blocks:
        if path is not None:
            roots.setdefault(path.split("/")[0], []).append(block.line)
    assert roots, (
        "README.md has no python block introduced by a path comment (e.g. "
        "'# myapp/main.py'), so nothing states which files the example project "
        "is made of and the example cannot be checked for consistency"
    )
    assert len(roots) == 1, (
        f"README.md path comments name more than one top-level package: {sorted(roots)} "
        f"(first seen at lines {sorted(line for lines in roots.values() for line in lines)}); "
        "the guard cannot tell which one is the example application package"
    )
    return next(iter(roots))


def _module_name(path: str) -> str:
    """``myapp/orders/__init__.py`` -> ``myapp.orders``."""
    parts = path[: -len(".py")].split("/")
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _containing_package(path: str) -> str:
    """Package a relative import inside ``path`` resolves against.

    ``myapp/main.py`` and ``myapp/orders/__init__.py`` both drop their last path
    segment: a module resolves against its parent package, and ``__init__.py``
    resolves against the package it *is*.
    """
    return ".".join(path[: -len(".py")].split("/")[:-1])


def _imported_modules(block: Block, path: str | None) -> list[tuple[str, int]]:
    """Modules imported by a block, each with its 1-based README line.

    Blocks that do not parse are skipped; their syntax is the claim of
    ``test_readme_python_blocks_are_valid_python``, and reporting it twice
    would bury the real cause.
    """
    try:
        tree = ast.parse(block.body)
    except SyntaxError:
        return []

    package = _containing_package(path) if path is not None else ""
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        line = block.line + node.lineno - 1 if isinstance(node, ast.stmt) else block.line
        if isinstance(node, ast.Import):
            found.extend((alias.name, line) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    found.append((node.module, line))
                continue
            if path is None:
                continue  # no file path: nothing to resolve the dots against
            parts = package.split(".") if package else []
            if node.level - 1 > len(parts):
                continue
            base = ".".join(parts[: len(parts) - (node.level - 1)])
            if node.module:
                found.append((f"{base}.{node.module}" if base else node.module, line))
            else:
                found.extend(
                    (f"{base}.{alias.name}" if base else alias.name, line) for alias in node.names
                )
    return found


def test_readme_python_blocks_are_valid_python() -> None:
    """Every python block must compile.

    Compiled, not imported: the blocks reference an example package that does
    not exist here, so importing them would fail for a reason that says nothing
    about whether the reader can copy them.
    """
    broken: list[str] = []
    for block, _path in _python_blocks():
        try:
            compile(block.body, f"README.md:{block.line}", "exec")
        except SyntaxError as exc:
            line = block.line + (exc.lineno or 1) - 1
            broken.append(f"README.md:{line}: {exc.msg}")

    assert not broken, (
        "README.md has python blocks that do not parse, so a reader copying "
        "them gets a SyntaxError: " + "; ".join(broken)
    )


def test_readme_examples_define_every_module_they_import() -> None:
    """Imports of the example package must resolve to a block that defines them.

    A reader has no source for a module the README never shows, so an import of
    one is a dead end. Both sides are derived from the README: what it defines
    comes from the path comments, what it imports from the blocks' ASTs.
    """
    blocks = _python_blocks()
    package = _example_package(blocks)
    defined = {_module_name(path) for _block, path in blocks if path is not None}

    dangling: list[str] = []
    for block, path in blocks:
        for module, line in _imported_modules(block, path):
            if module != package and not module.startswith(f"{package}."):
                continue  # third-party or stdlib: not the README's to define
            if module in defined:
                continue
            expected = module.replace(".", "/")
            dangling.append(
                f"README.md:{line} imports '{module}', but no README block defines it "
                f"— add a block opening with '# {expected}.py' or '# {expected}/__init__.py'"
            )

    assert not dangling, (
        f"README.md example blocks import modules of the '{package}' example package that "
        f"the README never shows how to write (defined: {sorted(defined)}): " + "; ".join(dangling)
    )


def test_readme_project_tree_lists_pyproject_toml() -> None:
    """The prescribed tree must show the ``pyproject.toml`` the CLI needs.

    Every CLI command except ``audit`` resolves the application package from
    ``[tool.modulith].package`` / ``[project].name``; a reader who builds the
    tree exactly as drawn and then runs any of them gets exit 1 and
    ``could not determine the application package``.
    """
    package = _example_package(_python_blocks())
    trees = [
        block
        for block in fenced_blocks(README)
        if any(branch in block.body for branch in TREE_BRANCHES) and f"{package}/" in block.body
    ]

    assert trees, (
        f"README.md no longer draws a directory tree containing '{package}/', so the layout "
        "the CLI requires — including its pyproject.toml — is not shown to a reader anywhere"
    )
    missing = [f"README.md:{block.line}" for block in trees if "pyproject.toml" not in block.body]
    assert not missing, (
        "README.md draws a project tree that omits 'pyproject.toml', which every CLI command "
        f"except 'audit' needs to resolve the application package: {'; '.join(missing)}"
    )


def _section_span(path: Path, title: str) -> tuple[int, int]:
    """Line numbers of the ``## <title>`` heading and of the heading after it."""
    lines = path.read_text(encoding="utf-8").splitlines()
    fenced_lines = {
        number
        for block in fenced_blocks(path)
        for number in range(block.line - 1, block.line + len(block.body.splitlines()) + 1)
    }
    headings = [
        (number, text[3:].strip())
        for number, text in enumerate(lines, start=1)
        if text.startswith("## ") and number not in fenced_lines
    ]
    starts = [number for number, name in headings if name == title]
    assert starts, f"{path.name} has no '## {title}' section"
    following = [number for number, _name in headings if number > starts[0]]
    return starts[0], following[0] if following else len(lines) + 1


def _section_blocks(path: Path, title: str) -> list[Block]:
    """Fenced blocks under the ``## <title>`` heading, sub-headings included."""
    start, end = _section_span(path, title)
    return [block for block in fenced_blocks(path) if start < block.line < end]


def _section_text(path: Path, title: str) -> str:
    """Everything under the ``## <title>`` heading, code blocks included."""
    start, end = _section_span(path, title)
    return "\n".join(path.read_text(encoding="utf-8").splitlines()[start : end - 1])


def _diff(expected: str, actual: str, expected_name: str, actual_name: str) -> str:
    return "".join(
        difflib.unified_diff(
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=expected_name,
            tofile=actual_name,
        )
    )


def test_root_readme_quickstart_blocks_run_in_the_quickstart_example() -> None:
    """The root README's example is the quickstart project, not a lookalike.

    What a reader copies from the root README is what CI executes in
    ``examples/quickstart``: each ``# myapp/...`` python block equals that file
    without its path comment, no non-empty module of the example goes unshown,
    and every bash block under the pitch and the Quickstart reappears verbatim
    in the example's README, whose commands the runbook test runs.
    """
    problems: list[str] = []
    blocks = _python_blocks()
    package = _example_package(blocks)

    shown: set[str] = set()
    for block, path in blocks:
        if path is None:
            continue
        shown.add(path)
        source = QUICKSTART / path
        if not source.is_file():
            problems.append(f"README.md:{block.line} shows {path}, which the example lacks")
            continue
        code = block.body.partition("\n")[2] + "\n"
        if code != source.read_text(encoding="utf-8"):
            problems.append(
                f"README.md:{block.line} differs from examples/quickstart/{path}:\n"
                + _diff(source.read_text(encoding="utf-8"), code, path, f"README.md:{block.line}")
            )

    on_disk = {
        module.relative_to(QUICKSTART).as_posix()
        for module in (QUICKSTART / package).rglob("*.py")
        if module.read_text(encoding="utf-8").strip()
    }
    problems.extend(
        f"examples/quickstart/{path} is not shown by any README python block"
        for path in sorted(on_disk - shown)
    )

    if QUICKSTART_README.is_file():
        runnable = [
            block.body for block in fenced_blocks(QUICKSTART_README) if block.lang == "bash"
        ]
    else:
        problems.append("examples/quickstart/README.md does not exist")
        runnable = []
    for title in QUICKSTART_SECTIONS:
        commands = [block for block in _section_blocks(README, title) if block.lang == "bash"]
        assert commands, f"README.md '{title}' shows no bash block, so nothing is checked there"
        problems.extend(
            f"README.md:{block.line} bash block is not in examples/quickstart/README.md:\n"
            + block.body
            for block in commands
            if not any(block.body in body for body in runnable)
        )

    assert not problems, "\n\n".join(problems)


def _discover(package: str) -> list[str]:
    """Module names modupy's built-in discovery returns for ``package``.

    The package is imported afresh and forgotten again: the pitch's modules
    register listeners, and other tests build packages under the same name.
    """

    def forget() -> None:
        for name in [n for n in sys.modules if n == package or n.startswith(f"{package}.")]:
            del sys.modules[name]

    forget()
    try:
        return [module.name for module in modulith_discover_modules(package)]
    finally:
        forget()


def test_readme_pitch_discovers_the_modules_its_banner_lists(
    make_fake_app: Callable[..., str], tmp_path: Path
) -> None:
    """The pitch, rebuilt the way a reader would, discovers the modules its banner lists.

    The tree is the pitch's own ``# myapp/...`` blocks plus every
    ``dir/__init__.py`` its prose names as an empty file, and nothing else.
    modupy counts a directory as a module only when it is a package, so a pitch
    that never mentions ``myapp/contracts/__init__.py`` yields 3 modules where
    its banner promises 4.
    """
    make_fake_app({})  # puts tmp_path on sys.path; its teardown resets the runtime
    package = _example_package(_python_blocks())
    pitch = _section_blocks(README, PITCH)

    files = {
        path: block.body.partition("\n")[2] + "\n" for block, path in _python_blocks(pitch) if path
    }
    for path in EMPTY_INIT.findall(_section_text(README, PITCH)):
        files.setdefault(path, "")
    for path, code in files.items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(code, encoding="utf-8")

    banners = [
        (block.line + block.body.count("\n", 0, match.start()), match)
        for block in pitch
        if block.lang == "bash"
        for match in BANNER.finditer(block.body)
    ]
    assert len(banners) == 1, (
        f"README.md '{PITCH}' must show one 'discovered N module(s): ...' banner line, "
        f"found {len(banners)}"
    )
    line, banner = banners[0]
    count, listed = int(banner[1]), banner[2].split(", ")

    found = _discover(package)

    assert (len(found), found) == (count, listed), (
        f"README.md:{line} says the pitch discovers {count} module(s): {', '.join(listed)}, but "
        f"a project built from the pitch's own files ({sorted(files)}) discovers "
        f"{len(found)}: {', '.join(found)}. A directory is a module only when it is a package, "
        "so the pitch must tell the reader about every __init__.py its blocks do not show"
    )
