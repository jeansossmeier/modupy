"""Tests for `modulith skill install`, and guards on the skill it ships.

The installer tests drive the command against a stand-in skill tree, so they
hold whatever the packaged skill contains. The guards read the packaged skill
(``modulith/skill``) and check each thing it tells a coding assistant against
the code: an assistant has no other source, so a command, a setting or an
import the skill gets wrong is one the assistant then writes.
"""

from __future__ import annotations

import ast
import importlib
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterator
from dataclasses import fields
from functools import partial
from pathlib import Path
from typing import Any

import pytest
import typer
import yaml
from typer.testing import CliRunner

from modulith import _skill, config
from modulith.cli import app

from conftest import Block

runner = CliRunner()

REPO = Path(__file__).resolve().parent.parent
SKILL = REPO / "modulith" / "skill"

# ---------------------------------------------------------------------------
# modulith skill install
# ---------------------------------------------------------------------------

SHIPPED = {
    "SKILL.md": "---\nname: modupy\ndescription: Use when testing the installer.\n---\n\n# modupy\n",
    "references/cli.md": "# CLI\n",
    "references/outbox.md": "# Outbox\n",
}
DEFAULT_DIRS = (".claude/skills", ".agents/skills")


def _write(root: Path, files: dict[str, str]) -> None:
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text, encoding="utf-8")


def _files(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in root.rglob("*")
        if path.is_file()
    }


def _snapshot(root: Path) -> dict[str, str]:
    """Every path under ``root``: a file's text, a symlink's target, or ``<dir>``."""
    return {
        path.relative_to(root).as_posix(): (
            f"-> {os.readlink(path)}"
            if path.is_symlink()
            else path.read_text(encoding="utf-8")
            if path.is_file()
            else "<dir>"
        )
        for path in root.rglob("*")
    }


def _skill_in(*skills_dirs: str, files: dict[str, str] = SHIPPED) -> dict[str, str]:
    """What ``_files`` reads back once the skill is installed in each skills directory."""
    return {f"{d}/modupy/{rel}": text for d in skills_dirs for rel, text in files.items()}


@pytest.fixture
def shipped(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A stand-in for the packaged skill, which `skill install` then copies instead.

    The home directory is a temporary one too, so no test can write the real one.
    """
    source = tmp_path_factory.mktemp("shipped")
    _write(source, SHIPPED)
    monkeypatch.setattr(_skill, "install_skill", partial(_skill.install_skill, source=source))
    home = tmp_path_factory.mktemp("home")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    return source


def test_install_lands_beside_the_nearest_pyproject(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(tmp_path, {"pyproject.toml": '[project]\nname = "shop"\n'})
    nested = tmp_path / "shop" / "orders"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        f"installed {tmp_path / skills_dir / 'modupy'}" for skills_dir in DEFAULT_DIRS
    ]
    assert _files(tmp_path) == {
        "pyproject.toml": '[project]\nname = "shop"\n',
        **_skill_in(*DEFAULT_DIRS),
    }


def test_install_without_a_pyproject_lands_in_the_current_directory(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert config._find_pyproject() is None, "this test needs a tmp_path outside any project"

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        f"installed {tmp_path / skills_dir / 'modupy'}" for skills_dir in DEFAULT_DIRS
    ]
    assert _files(tmp_path) == _skill_in(*DEFAULT_DIRS)


def test_user_installs_under_the_home_directory(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(tmp_path, {"pyproject.toml": ""})
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["skill", "install", "--user"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        f"installed {Path.home() / skills_dir / 'modupy'}" for skills_dir in DEFAULT_DIRS
    ]
    assert _files(Path.home()) == _skill_in(*DEFAULT_DIRS)
    assert _files(tmp_path) == {"pyproject.toml": ""}


def test_dir_replaces_the_default_locations(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(tmp_path, {"pyproject.toml": ""})
    monkeypatch.chdir(tmp_path)
    elsewhere = tmp_path / "elsewhere"

    result = runner.invoke(
        app, ["skill", "install", "--dir", "tools/skills", "--dir", str(elsewhere)]
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "installed tools/skills/modupy",
        f"installed {elsewhere / 'modupy'}",
    ]
    assert _files(tmp_path) == {"pyproject.toml": "", **_skill_in("tools/skills", "elsewhere")}
    assert _files(Path.home()) == {}


def test_reinstall_is_idempotent(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["skill", "install"]).exit_code == 0

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        f"updated {tmp_path / skills_dir / 'modupy'}" for skills_dir in DEFAULT_DIRS
    ]
    assert _files(tmp_path) == _skill_in(*DEFAULT_DIRS)


def test_reinstall_leaves_exactly_what_this_version_ships(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["skill", "install"]).exit_code == 0
    _write(tmp_path / ".claude/skills/modupy", {"SKILL.md": SHIPPED["SKILL.md"] + "edited\n"})
    # The next version drops one reference file and adds another.
    (shipped / "references" / "outbox.md").unlink()
    _write(shipped, {"references/recipes.md": "# Recipes\n"})

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 0, result.output
    now_shipped = _files(shipped)
    assert sorted(now_shipped) == ["SKILL.md", "references/cli.md", "references/recipes.md"]
    assert _files(tmp_path) == _skill_in(*DEFAULT_DIRS, files=now_shipped)


def test_install_copes_with_one_skills_directory_linked_to_the_other(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A project that links `.claude/skills` to `.agents/skills` keeps one copy under both."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".agents" / "skills").mkdir(parents=True)
    (tmp_path / ".claude").mkdir()
    (tmp_path / ".claude" / "skills").symlink_to(
        Path("..") / ".agents" / "skills", target_is_directory=True
    )

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        f"installed {tmp_path / '.claude/skills/modupy'}",
        f"updated {tmp_path / '.agents/skills/modupy'}",
    ]
    assert _files(tmp_path / ".agents") == _skill_in("skills")


def _a_skill_md(text: str) -> Callable[[Path], None]:
    return lambda path: _write(path, {"SKILL.md": text, "notes.md": "mine\n"})


def _a_file(path: Path) -> None:
    path.write_text("mine\n", encoding="utf-8")


def _a_symlink_to_the_skill(path: Path) -> None:
    real = path.parent / "kept-elsewhere"
    _write(real, {**SHIPPED, "SKILL.md": SHIPPED["SKILL.md"] + "kept elsewhere\n"})
    path.symlink_to(real, target_is_directory=True)


NOT_THE_SKILL = {
    "another skill": _a_skill_md("---\nname: house-style\ndescription: Ours.\n---\n"),
    "front matter without a name": _a_skill_md("---\ndescription: name: modupy\n---\n"),
    "no front matter": _a_skill_md("# Notes\n\nname: modupy\n"),
    "directory without a SKILL.md": lambda path: _write(path, {"notes.md": "mine\n"}),
    "file": _a_file,
    "symlink": _a_symlink_to_the_skill,
}


@pytest.mark.parametrize("occupy", NOT_THE_SKILL.values(), ids=NOT_THE_SKILL.keys())
def test_install_refuses_a_modupy_that_is_not_the_skill_and_writes_nothing(
    occupy: Callable[[Path], None], shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first location holds an earlier install, edited since; the second is taken.

    Nothing may change in either: the edit staying is what shows the first
    location was not written before the second was found to be in the way.
    """
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / ".claude/skills/modupy",
        {**SHIPPED, "SKILL.md": SHIPPED["SKILL.md"] + "edited\n"},
    )
    in_the_way = tmp_path / ".agents/skills/modupy"
    in_the_way.parent.mkdir(parents=True)
    occupy(in_the_way)
    before = _snapshot(tmp_path)

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert f"error: {in_the_way} exists and is not the modupy skill" in result.stderr
    assert _snapshot(tmp_path) == before


BLOCKED_SKILLS_DIR = {
    "file": _a_file,
    "dangling symlink": lambda path: path.symlink_to(path.parent / "gone"),
}


@pytest.mark.parametrize("block", BLOCKED_SKILLS_DIR.values(), ids=BLOCKED_SKILLS_DIR.keys())
def test_install_refuses_a_skills_directory_it_cannot_create_and_writes_nothing(
    block: Callable[[Path], None], shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / ".claude/skills/modupy",
        {**SHIPPED, "SKILL.md": SHIPPED["SKILL.md"] + "edited\n"},
    )
    (tmp_path / ".agents").mkdir()
    block(tmp_path / ".agents" / "skills")
    before = _snapshot(tmp_path)

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert f"error: {tmp_path / '.agents' / 'skills'} is not a directory" in result.stderr
    assert _snapshot(tmp_path) == before


def test_reinstall_removes_a_symlink_inside_the_skill_without_following_it(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["skill", "install", "--dir", "skills"]).exit_code == 0
    _write(tmp_path, {"keep/notes.md": "mine\n"})
    (tmp_path / "skills/modupy/linked").symlink_to(tmp_path / "keep", target_is_directory=True)

    result = runner.invoke(app, ["skill", "install", "--dir", "skills"])

    assert result.exit_code == 0, result.output
    assert _snapshot(tmp_path) == {
        "keep": "<dir>",
        "keep/notes.md": "mine\n",
        "skills": "<dir>",
        "skills/modupy": "<dir>",
        "skills/modupy/references": "<dir>",
        **_skill_in("skills"),
    }


def test_user_and_dir_together_are_refused(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["skill", "install", "--user", "--dir", "skills"])

    assert result.exit_code == 1
    assert "error: --user and --dir are mutually exclusive" in result.stderr
    assert _snapshot(tmp_path) == _snapshot(Path.home()) == {}


def test_install_refuses_a_package_that_ships_no_skill(
    shipped: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (shipped / "SKILL.md").unlink()

    result = runner.invoke(app, ["skill", "install"])

    assert result.exit_code == 1
    assert f"error: no modupy skill at {shipped}" in result.stderr
    assert _snapshot(tmp_path) == {}


@pytest.mark.skipif(
    not (SKILL / "SKILL.md").is_file(), reason="modulith/skill/SKILL.md does not exist yet"
)
def test_the_packaged_skill_installs_from_the_command_line(tmp_path: Path) -> None:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("MODULITH_", "PYTEST_"))
    }

    done = subprocess.run(
        [sys.executable, "-m", "modulith", "skill", "install"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert done.returncode == 0, done.stderr
    assert _files(tmp_path) == _skill_in(*DEFAULT_DIRS, files=_files(SKILL))


# ---------------------------------------------------------------------------
# The packaged skill, checked against the code it describes
# ---------------------------------------------------------------------------

SHELL = {"bash", "sh", "shell", "console", "zsh"}
FENCE = re.compile(r"^( *)```(\S*)\s*$")
INLINE_CODE = re.compile(r"`([^`]+)`")
LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^)\s>]+)")
REFERENCE_LINK = re.compile(r"^ {0,3}\[[^\]]+\]:\s*<?([^\s>]+)")
URL_OR_ANCHOR = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:|#")
# In a shell block `modulith` is a command wherever it stands as a word (`uv run modulith`,
# `docker compose exec app modulith`), a Python import aside. Inline code also quotes
# messages and code, so there it has to open the span.
SHELL_INVOCATION = re.compile(r"(?:^|(?<=[\s/]))(?<!from )(?<!import )modulith(?=\s|$)")
INLINE_INVOCATION = re.compile(
    r"^(?:\$ )?(?:[A-Z_]+=\S+ )*(?:uv run |python3? -m )?modulith(?=\s|$)"
)
OPTION = re.compile(r"(?<![\w-])--[a-z][\w-]*")
PLACEHOLDER = re.compile(r"[<\[{.]|[A-Z_]+$")
MACHINE_LOCAL = re.compile(r"/(?:home|tmp)/")


def _documents() -> list[Path]:
    """The skill's Markdown files; fails when there is no skill to check."""
    assert (SKILL / "SKILL.md").is_file(), "modulith/skill/SKILL.md does not exist"
    return sorted(SKILL.rglob("*.md"))


def _where(path: Path, line: int) -> str:
    return f"{path.relative_to(REPO).as_posix()}:{line}"


def _read(path: Path) -> tuple[list[Block], list[tuple[int, str]]]:
    """A document's fenced blocks, and its other lines as ``(line number, text)``.

    ``conftest.fenced_blocks`` sees only fences at column 0, the rule for the
    example READMEs. A skill may nest a block in a list item, and a block that
    is not seen is not checked, so this reader takes an indented fence too and
    strips that indentation from the block's body.
    """
    blocks: list[Block] = []
    prose: list[tuple[int, str]] = []
    opened: tuple[str, str, int, list[str]] | None = None
    for number, text in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        fence = FENCE.match(text)
        if opened is None:
            if fence is None:
                prose.append((number, text))
            else:
                opened = (fence[1], fence[2], number + 1, [])
        elif fence is not None and not fence[2]:
            blocks.append(Block(opened[1], opened[2], "\n".join(opened[3])))
            opened = None
        else:
            opened[3].append(text.removeprefix(opened[0]))
    assert opened is None, f"{_where(path, opened[2] - 1)} opens a code fence that is never closed"
    return blocks, prose


def _blocks(path: Path) -> list[Block]:
    return _read(path)[0]


def _prose(path: Path) -> list[tuple[int, str]]:
    return _read(path)[1]


def test_skill_md_front_matter_is_what_agents_read() -> None:
    """The limits are the Agent Skills specification's (agentskills.io/specification)."""
    _documents()
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert text.startswith("---\n"), "SKILL.md must open with front matter on its first line"
    raw, closed, _ = text.removeprefix("---\n").partition("\n---\n")
    assert closed, "SKILL.md front matter is never closed by a '---' line"
    meta = yaml.safe_load(raw)
    assert isinstance(meta, dict), f"SKILL.md front matter is not a mapping: {meta!r}"

    assert meta.get("name") == _skill.NAME
    description = meta.get("description")
    assert isinstance(description, str) and description.strip(), "description is empty"
    assert len(description) <= 1024, f"description is {len(description)} characters"
    assert description.startswith("Use when"), f"description starts {description[:40]!r}"
    assert len(text.splitlines()) < 500, f"SKILL.md is {len(text.splitlines())} lines"


def _relative_links(path: Path) -> Iterator[tuple[int, str]]:
    """Each relative link a document writes outside code, as ``(line number, target)``."""
    for number, text in _prose(path):
        text = INLINE_CODE.sub("", text)
        for target in (*LINK.findall(text), *REFERENCE_LINK.findall(text)):
            if not URL_OR_ANCHOR.match(target):
                yield number, target


def test_skill_links_resolve_inside_the_skill_and_reach_every_reference() -> None:
    """An installed skill is a directory on its own: a link out of it leads nowhere."""
    root = SKILL.resolve()
    problems: list[str] = []
    linked: dict[Path, set[Path]] = {}
    for doc in _documents():
        for line, target in _relative_links(doc):
            resolved = (doc.parent / target.partition("#")[0]).resolve()
            linked.setdefault(doc, set()).add(resolved)
            if not resolved.is_relative_to(root):
                problems.append(f"{_where(doc, line)} links to {target}, outside the skill")
            elif not resolved.is_file():
                problems.append(f"{_where(doc, line)} links to {target}, which does not exist")

    from_skill_md = linked.get(SKILL / "SKILL.md", set())
    for path in sorted(SKILL.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(SKILL).as_posix()
        if path.suffix != ".md":
            problems.append(f"{rel} is not a Markdown file")
        if rel.startswith("references/") and path.resolve() not in from_skill_md:
            problems.append(f"{rel} is not linked from SKILL.md, so no assistant is led to it")

    assert not problems, "\n".join(problems)


def _cli() -> dict[tuple[str, ...], set[str]]:
    """Every command path of the CLI, groups included, with the options it takes."""
    paths: dict[tuple[str, ...], set[str]] = {}

    def walk(command: Any, path: tuple[str, ...]) -> None:
        paths[path] = {"--help"}
        for param in command.params:
            paths[path].update(param.opts, param.secondary_opts)
        for name, sub in getattr(command, "commands", {}).items():
            walk(sub, (*path, name))

    walk(typer.main.get_command(app), ())
    return paths


def _is_group(path: tuple[str, ...], cli: dict[tuple[str, ...], set[str]]) -> bool:
    return any(len(other) == len(path) + 1 and other[: len(path)] == path for other in cli)


def _command_lines(block: Block) -> Iterator[tuple[int, str]]:
    """The commands of a shell block, as ``(line number, command)``.

    A block with any ``$ `` line is a transcript: only those lines are
    commands, the rest is their output. A trailing backslash joins a line to
    the next, and comments are dropped.
    """
    lines = block.body.splitlines()
    transcript = any(line.startswith("$ ") for line in lines)
    command, start = "", 0
    for offset, raw in enumerate(lines):
        if not command:
            if transcript and not raw.startswith("$ "):
                continue
            start = block.line + offset
        text = re.sub(r"(?:^|\s)#.*", "", raw.strip().removeprefix("$ "))
        command = " ".join((command, text.rstrip("\\"))).strip()
        if not text.endswith("\\"):
            yield start, command
            command = ""


def _invocations(path: Path) -> Iterator[tuple[int, list[str]]]:
    """The words after each ``modulith`` a document runs, in shell blocks and inline code."""
    written = [
        (number, command, SHELL_INVOCATION)
        for block in _blocks(path)
        if block.lang in SHELL
        for number, command in _command_lines(block)
    ]
    written.extend(
        (number, span, INLINE_INVOCATION)
        for number, text in _prose(path)
        for span in INLINE_CODE.findall(text)
    )
    for number, command, invocation in written:
        for part in re.split(r"&&|\|\||;|\s\|\s", command):
            found = invocation.search(part.strip())
            if found:
                yield number, part.strip()[found.end() :].split()


def _invocation_problem(words: list[str], cli: dict[tuple[str, ...], set[str]]) -> str | None:
    path: tuple[str, ...] = ()
    rest = list(words)
    while rest and (*path, rest[0]) in cli:
        path = (*path, rest.pop(0))
    if _is_group(path, cli) and rest and not rest[0].startswith("-"):
        if PLACEHOLDER.match(rest[0]):
            return None  # `modulith outbox <command>` names no command to check
        return f"there is no `modulith {' '.join((*path, rest[0]))}` command"
    unknown = sorted(set(OPTION.findall(" ".join(rest))) - cli[path])
    if unknown:
        return f"`modulith {' '.join(path)}` takes no {', '.join(unknown)}"
    return None


def test_skill_invocations_name_real_commands_and_options() -> None:
    cli = _cli()
    problems = [
        f"{_where(doc, line)}: {problem}"
        for doc in _documents()
        for line, words in _invocations(doc)
        if (problem := _invocation_problem(words, cli))
    ]

    reference = SKILL / "references" / "cli.md"
    assert reference.is_file(), "modulith/skill/references/cli.md does not exist"
    text = reference.read_text(encoding="utf-8")
    problems.extend(
        f"references/cli.md never shows `modulith {' '.join(path)}`"
        for path in sorted(cli)
        if path
        and not _is_group(path, cli)
        and not re.search(rf"\bmodulith {re.escape(' '.join(path))}(?![\w-])", text)
    )

    assert not problems, "\n".join(problems)


def test_skill_toml_blocks_show_only_real_settings() -> None:
    """Keys are held to what ``modulith/config.py`` accepts.

    Broker options have no list there, since each broker reads its own, so a
    broker option only has to be a name the configuration or an adapter uses.
    """
    scalars = {f.name for f in fields(config.Configuration)} - config._DICT_FIELDS
    scalars.discard("explicit_keys")
    package = Path(config.__file__).parent
    sources = [package / "config.py", *sorted((package / "adapters").glob("*.py"))]
    broker_words = {
        word
        for source in sources
        for word in re.findall(r"[A-Za-z_]\w*", source.read_text(encoding="utf-8"))
    }
    # None: the keys are the application's own names (modules, event types).
    known: dict[str, set[str] | frozenset[str] | None] = {
        "outbox_options": config._OUTBOX_OPTION_KEYS,
        "broker_options": broker_words,
        "workers": None,
        "subscriptions": None,
        "verify": {"disabled_rules"},  # the one key _read_pyproject reads from that table
    }
    tables = {name: known[field] for name, field in config._SUBTABLE_FIELD.items()}
    tables.update({name: known[name] for name in config._RESERVED_SUBTABLES})

    problems: list[str] = []
    for doc in _documents():
        for block in _blocks(doc):
            if block.lang != "toml" or "tool.modulith" not in block.body:
                continue
            where = _where(doc, block.line)
            try:
                settings = tomllib.loads(block.body).get("tool", {}).get("modulith", {})
            except tomllib.TOMLDecodeError as exc:
                problems.append(f"{where}: the block is not valid TOML: {exc}")
                continue
            for key, value in settings.items():
                if not isinstance(value, dict):
                    if key not in scalars:
                        problems.append(f"{where}: [tool.modulith] has no setting {key!r}")
                elif key not in tables:
                    problems.append(f"{where}: there is no [tool.modulith.{key}] table")
                elif (allowed := tables[key]) is not None:
                    problems.extend(
                        f"{where}: [tool.modulith.{key}] has no setting {name!r}"
                        for name in value
                        if name not in allowed
                    )

    assert not problems, "\n".join(problems)


def _import_problem(module: str, name: str | None) -> str | None:
    """Why ``from module import name``, or ``import module``, fails; None when it works."""
    try:
        imported = importlib.import_module(module)
    except ImportError as exc:
        return f"cannot import {module}: {exc}"
    if name is None or name == "*" or hasattr(imported, name):
        return None
    try:
        importlib.import_module(f"{module}.{name}")
    except ImportError:
        return f"{module} has no {name!r}"
    return None


def test_skill_python_blocks_parse_and_import_real_names() -> None:
    """Parsed, not run: the blocks are fragments of an application that is not here."""
    problems: list[str] = []
    for doc in _documents():
        for block in _blocks(doc):
            if block.lang not in ("python", "py"):
                continue
            try:
                tree = ast.parse(block.body)
            except SyntaxError as exc:
                problems.append(f"{_where(doc, block.line + (exc.lineno or 1) - 1)}: {exc.msg}")
                continue
            for node in ast.walk(tree):
                imports: list[tuple[str, str | None]] = []
                if isinstance(node, ast.Import):
                    imports = [(alias.name, None) for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    imports = [(node.module, alias.name) for alias in node.names]
                for module, name in imports:
                    if module.split(".")[0] != "modulith":
                        continue
                    problem = _import_problem(module, name)
                    if problem:
                        line = block.line + getattr(node, "lineno", 1) - 1
                        problems.append(f"{_where(doc, line)}: {problem}")

    assert not problems, "\n".join(problems)


def test_skill_names_no_machine_local_paths() -> None:
    problems = [
        f"{_where(doc, number)}: {text.strip()}"
        for doc in _documents()
        for number, text in enumerate(doc.read_text(encoding="utf-8").splitlines(), start=1)
        if MACHINE_LOCAL.search(text)
    ]

    assert not problems, "\n".join(problems)
