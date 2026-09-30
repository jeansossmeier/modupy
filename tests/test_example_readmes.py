"""The README runbooks: what each example README promises, and what CI checks of it.

An example README is executable documentation. ``RUNBOOKS`` holds only what the
README's text cannot say (section grouping, ``repo_env``, and per-step ``serve``,
``eventually`` and ``exit_code``); the commands and the output they print are
read from the README itself. The tests here keep the two in step and refuse any
README shape the reader below would silently skip:

- the commands in the README's ``bash`` fences equal the runbook's, in order;
- no command sits where the reader cannot see it (other fence tags, indented
  fences, a bare ``cd``/``source``/``.``, an inline ``#`` on a serve step);
- the README's ``pip install`` lines name exactly the dependencies the example
  declares.

The reader (``readme_commands``) and the data model are module-level so the
executors that run a runbook import them from here.
"""

from __future__ import annotations

import difflib
import re
import shlex
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from conftest import Block, fenced_blocks

REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
MISPLACED_FENCE_TAGS = ("", "sh", "shell", "console", "zsh")
INDENTED_FENCE = re.compile(r"^[ \t]+```")


@dataclass(frozen=True)
class Step:
    command: str
    serve: bool = False
    eventually: bool = False
    exit_code: int = 0


@dataclass(frozen=True)
class Section:
    name: str
    steps: tuple[Step, ...]
    repo_env: bool = True


@dataclass(frozen=True)
class Command:
    text: str
    output: tuple[str, ...]
    line: int


CURL_POST = (
    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
    '-d \'{"customer_id": "alice"}\''
)

RUNBOOKS: dict[str, tuple[Section, ...]] = {
    "quickstart": (
        Section(
            "Install and look around",
            (
                Step("pip install 'modupy[fastapi,cli]'"),
                Step("modulith info"),
                Step("modulith verify"),
            ),
        ),
        Section(
            "Run it in one process",
            (
                Step("uvicorn myapp.main:app", serve=True),
                Step(CURL_POST),
                Step("curl -s localhost:8000/orders/ord-1/fulfilment"),
                Step("curl -s localhost:8000/inventory/ord-1"),
                Step("uvicorn myapp.main:app --reload", serve=True),
            ),
        ),
        Section(
            "Run the same code, one process per module",
            (
                Step("modulith run myapp.main:app --topology=processes", serve=True),
                Step(CURL_POST),
                Step("curl -s localhost:8000/orders/ord-1/fulfilment", eventually=True),
                Step("curl -s localhost:8000/inventory/ord-1", eventually=True),
            ),
        ),
        Section(
            "Run its tests",
            (Step("pip install pytest"), Step("pytest")),
        ),
    ),
}


@dataclass
class _Draft:
    line: int
    text: str = ""
    output: list[str] = field(default_factory=list)


def _block_commands(block: Block) -> list[Command]:
    lines = block.body.splitlines()
    transcript = any(text.startswith("$ ") for text in lines)
    drafts: list[_Draft] = []
    continued = False
    for offset, raw in enumerate(lines):
        text = raw.rstrip()
        number = block.line + offset
        if continued:
            piece = text.strip()
        elif text.startswith("#"):
            continue
        elif transcript and text.startswith("$ "):
            drafts.append(_Draft(number))
            piece = text[2:].strip()
        elif transcript:
            if drafts:
                drafts[-1].output.append(text)
            continue
        elif text.strip():
            drafts.append(_Draft(number))
            piece = text.strip()
        else:
            continue
        continued = piece.endswith("\\")
        if continued:
            piece = piece.removesuffix("\\").rstrip()
        drafts[-1].text = f"{drafts[-1].text} {piece}".strip()
    return [Command(d.text, tuple(d.output), d.line) for d in drafts]


def readme_commands(path: Path) -> list[Command]:
    """The commands of a README's ``bash`` fences, in order.

    A fence with any ``$ `` line is a transcript: each ``$ `` line is a command
    and the lines up to the next one are its expected output. Any other fence
    lists commands only. A trailing ``\\`` joins a line to the next, and a line
    starting with ``#`` is skipped; an inline ``# comment`` stays in the command.
    """
    return [
        command
        for block in fenced_blocks(path)
        if block.lang == "bash"
        for command in _block_commands(block)
    ]


def readme_headings(path: Path) -> list[tuple[str, int]]:
    """The ``## `` headings outside fences, as ``(title, 1-based line)``."""
    fenced = {
        number
        for block in fenced_blocks(path)
        for number in range(block.line - 1, block.line + len(block.body.splitlines()) + 1)
    }
    return [
        (text[3:].strip(), number)
        for number, text in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if text.startswith("## ") and number not in fenced
    ]


def commands_diff(readme: Path, sections: Sequence[Section]) -> str:
    """Unified diff from the runbook's commands to the README's; empty when equal."""
    runbook = [step.command for section in sections for step in section.steps]
    written = [command.text for command in readme_commands(readme)]
    return "\n".join(
        difflib.unified_diff(runbook, written, fromfile="RUNBOOKS", tofile=str(readme), lineterm="")
    )


def _has_inline_comment(command: str) -> bool:
    quote = ""
    escaped = False
    previous = " "
    for char in command:
        if escaped:
            escaped = False
        elif char == "\\" and quote != "'":
            escaped = True
        elif quote:
            quote = "" if char == quote else quote
        elif char in "'\"":
            quote = char
        elif char == "#" and previous.isspace():
            return True
        previous = char
    return False


def _is_bare_shell_state_change(command: str) -> bool:
    first = command.split(maxsplit=1)[0]
    return first in ("source", ".") or (first == "cd" and "&&" not in command)


def placement_problems(readme: Path, steps: Sequence[Step]) -> list[str]:
    """Everything in a README that could hide a command from ``readme_commands``.

    Each problem starts with ``<readme>:<line>:``. ``steps`` are the runbook's
    steps in order; step N is the README's command N.
    """
    problems: list[str] = []
    for block in fenced_blocks(readme):
        if block.lang in MISPLACED_FENCE_TAGS:
            tag = block.lang or "no tag"
            problems.append(f"{readme}:{block.line - 1}: fence with {tag} (only bash is read)")
    for number, text in enumerate(readme.read_text(encoding="utf-8").splitlines(), start=1):
        if INDENTED_FENCE.match(text):
            problems.append(f"{readme}:{number}: indented fence (fences must start at column 0)")
    commands = readme_commands(readme)
    for command in commands:
        if _is_bare_shell_state_change(command.text):
            problems.append(
                f"{readme}:{command.line}: bare {command.text.split(maxsplit=1)[0]!r} "
                "(write 'cd <dir> && <command>'; source and '.' are not allowed)"
            )
    for command, step in zip(commands, steps, strict=False):
        if step.serve and _has_inline_comment(command.text):
            problems.append(f"{readme}:{command.line}: inline '#' comment on a serve step")
    return problems


def _normalized(requirement: str) -> str:
    parsed = Requirement(requirement)
    extras = sorted(canonicalize_name(extra) for extra in parsed.extras)
    return canonicalize_name(parsed.name) + (f"[{','.join(extras)}]" if extras else "")


def requirements_problem(readme: Path, pyproject: Path) -> str:
    """Why the README's ``pip install`` lines differ from the declared dependencies.

    Empty when they match: each requirement compared as normalized name plus
    extras. An install of the project itself (``.`` or ``.[extra]``) stands for
    its ``[project].dependencies`` and the named extras' groups.
    """
    project = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]
    groups: dict[str, list[str]] = project.get("optional-dependencies", {})
    dependencies: list[str] = project.get("dependencies", [])
    declared = {_normalized(r) for r in [*dependencies, *chain.from_iterable(groups.values())]}
    installed: set[str] = set()
    for command in readme_commands(readme):
        words = shlex.split(command.text, comments=True)
        if words[:2] != ["pip", "install"]:
            continue
        for word in words[2:]:
            if word.startswith("."):
                named = word.partition("[")[2].rstrip("]").split(",")
                installed.update(_normalized(r) for r in dependencies)
                installed.update(_normalized(r) for extra in named for r in groups.get(extra, []))
            elif not word.startswith("-"):
                installed.add(_normalized(word))
    if installed == declared:
        return ""
    return (
        f"{readme} pip install lines differ from {pyproject}: "
        f"declared but not installed {sorted(declared - installed)}; "
        f"installed but not declared {sorted(installed - declared)}"
    )


def _readme(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "README.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_transcript_fence_reads_each_dollar_line_as_a_command_with_its_output(
    tmp_path: Path,
) -> None:
    readme = _readme(
        tmp_path, "intro", "```bash", "$ first", "out 1", "  out 2", "$ second", "out 3", "```"
    )

    assert readme_commands(readme) == [
        Command("first", ("out 1", "  out 2"), 3),
        Command("second", ("out 3",), 6),
    ]


def test_plain_fence_reads_every_line_as_a_command_without_output(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "pip install pytest", "", "pytest -q", "```")

    assert readme_commands(readme) == [
        Command("pip install pytest", (), 2),
        Command("pytest -q", (), 4),
    ]


def test_trailing_backslash_joins_a_transcript_command_with_the_next_line(
    tmp_path: Path,
) -> None:
    readme = _readme(
        tmp_path,
        "```bash",
        "$ curl -sX POST host \\",
        "      -H 'a: b' \\",
        "      -d '{}'",
        '{"ok":true}',
        "```",
    )

    assert readme_commands(readme) == [
        Command("curl -sX POST host -H 'a: b' -d '{}'", ('{"ok":true}',), 2)
    ]


def test_trailing_backslash_joins_a_plain_command_with_the_next_line(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "echo one \\", "  two", "echo three", "```")

    assert readme_commands(readme) == [
        Command("echo one two", (), 2),
        Command("echo three", (), 4),
    ]


def test_hash_lines_are_comments_in_a_plain_fence(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "# a comment", "make", "# another", "```")

    assert readme_commands(readme) == [Command("make", (), 3)]


def test_hash_lines_are_comments_in_a_transcript_fence(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "$ make", "# note", "done", "```")

    assert readme_commands(readme) == [Command("make", ("done",), 2)]


def test_inline_comment_stays_in_a_plain_command(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "make  # build it", "```")

    assert readme_commands(readme) == [Command("make  # build it", (), 2)]


def test_inline_comment_stays_in_a_transcript_command(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "$ make  # build it", "built", "```")

    assert readme_commands(readme) == [Command("make  # build it", ("built",), 2)]


def test_fences_other_than_bash_hold_no_commands(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```python", "$ x = 1", "```", "```text", "pytest", "```")

    assert readme_commands(readme) == []


def test_headings_skip_lines_inside_fences(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "# Title", "## One", "```bash", "## not a heading", "```", "## Two")

    assert readme_headings(readme) == [("One", 2), ("Two", 6)]


def test_commands_diff_is_empty_when_readme_and_runbook_agree(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "a", "a", "b", "```")
    sections = (Section("s", (Step("a"), Step("a"))), Section("t", (Step("b"),)))

    assert commands_diff(readme, sections) == ""


def test_commands_diff_shows_a_dropped_and_an_added_command(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "a", "c", "```")
    sections = (Section("s", (Step("a"), Step("b"))),)

    diff = commands_diff(readme, sections)

    assert diff.splitlines()[:2] == ["--- RUNBOOKS", f"+++ {readme}"]
    assert "-b" in diff.splitlines()
    assert "+c" in diff.splitlines()


def test_commands_diff_counts_duplicates(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "```bash", "a", "a", "```")

    assert commands_diff(readme, (Section("s", (Step("a"),)),)) != ""


def test_untagged_fence_is_refused_with_its_file_and_line(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "intro", "```", "pytest", "```")

    problems = placement_problems(readme, ())

    assert len(problems) == 1
    assert problems[0].startswith(f"{readme}:2:")


@pytest.mark.parametrize("tag", ["sh", "shell", "console", "zsh"])
def test_shell_tagged_fence_is_refused_with_its_file_and_line(tmp_path: Path, tag: str) -> None:
    readme = _readme(tmp_path, "intro", f"```{tag}", "pytest", "```")

    problems = placement_problems(readme, ())

    assert len(problems) == 1
    assert problems[0].startswith(f"{readme}:2:")
    assert tag in problems[0]


def test_indented_fence_is_refused_with_its_file_and_line(tmp_path: Path) -> None:
    readme = _readme(tmp_path, "intro", "  ```bash", "  pytest", "  ```")

    problems = placement_problems(readme, ())

    assert problems
    assert problems[0].startswith(f"{readme}:2:")


@pytest.mark.parametrize("command", ["cd sub", "source .venv/bin/activate", ". .venv/bin/activate"])
def test_bare_directory_or_environment_command_is_refused_with_its_file_and_line(
    tmp_path: Path, command: str
) -> None:
    readme = _readme(tmp_path, "```bash", command, "pytest", "```")

    problems = placement_problems(readme, ())

    assert len(problems) == 1
    assert problems[0].startswith(f"{readme}:2:")


def test_inline_comment_on_a_serve_step_is_refused_with_its_file_and_line(
    tmp_path: Path,
) -> None:
    command = "uvicorn myapp.main:app  # start it"
    readme = _readme(tmp_path, "```bash", "$ pip install x", f"$ {command}", "```")
    steps = (Step("pip install x"), Step(command, serve=True))

    problems = placement_problems(readme, steps)

    assert len(problems) == 1
    assert problems[0].startswith(f"{readme}:3:")


def test_placement_accepts_scoped_cd_and_comments_that_are_not_on_a_serve_step(
    tmp_path: Path,
) -> None:
    readme = _readme(
        tmp_path,
        "```bash",
        "cd sub && pytest",
        "make  # build it",
        "$ uvicorn app:app --header 'x: a #b'",
        "```",
    )
    steps = (
        Step("cd sub && pytest"),
        Step("make  # build it"),
        Step("uvicorn app:app --header 'x: a #b'", serve=True),
    )

    assert placement_problems(readme, steps) == []


def _project(tmp_path: Path, dependencies: str, groups: str = "") -> Path:
    path = tmp_path / "pyproject.toml"
    path.write_text(
        f"[project]\nname = 'ex'\nversion = '0'\ndependencies = {dependencies}\n"
        f"[project.optional-dependencies]\n{groups}\n",
        encoding="utf-8",
    )
    return path


def test_requirements_match_ignoring_case_extras_order_and_versions(tmp_path: Path) -> None:
    project = _project(tmp_path, "['modupy[fastapi,cli]>=0.1']", "test = ['pytest']")
    readme = _readme(
        tmp_path, "```bash", "pip install 'Modupy[CLI,FastAPI]'", "pip install pytest", "```"
    )

    assert requirements_problem(readme, project) == ""


def test_requirements_missing_from_the_readme_are_reported(tmp_path: Path) -> None:
    project = _project(tmp_path, "['modupy[cli]']", "test = ['pytest']")
    readme = _readme(tmp_path, "```bash", "pip install 'modupy[cli]'", "```")

    problem = requirements_problem(readme, project)

    assert str(readme) in problem
    assert "pytest" in problem


def test_requirements_the_project_does_not_declare_are_reported(tmp_path: Path) -> None:
    project = _project(tmp_path, "['modupy[cli]']")
    readme = _readme(tmp_path, "```bash", "pip install 'modupy[cli]' httpx", "```")

    problem = requirements_problem(readme, project)

    assert str(readme) in problem
    assert "httpx" in problem


def test_requirements_an_install_of_the_project_itself_stands_for_its_declared_ones(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path, "['modupy[cli]']", "test = ['pytest']")
    readme = _readme(tmp_path, "```bash", 'pip install -e ".[test]"', "```")

    assert requirements_problem(readme, project) == ""


def _readme_of(example: str) -> Path:
    return EXAMPLES / example / "README.md"


@pytest.mark.parametrize("example", sorted(RUNBOOKS))
def test_readme_lists_exactly_the_runbook_commands(example: str) -> None:
    diff = commands_diff(_readme_of(example), RUNBOOKS[example])

    assert not diff, (
        f"{_readme_of(example)} and RUNBOOKS[{example!r}] list different commands "
        f"(- runbook only, + README only):\n{diff}"
    )


@pytest.mark.parametrize("example", sorted(RUNBOOKS))
def test_readme_puts_every_command_where_the_runbook_reads_it(example: str) -> None:
    steps = [step for section in RUNBOOKS[example] for step in section.steps]

    problems = placement_problems(_readme_of(example), steps)

    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("example", sorted(RUNBOOKS))
def test_readme_installs_exactly_the_declared_dependencies(example: str) -> None:
    problem = requirements_problem(_readme_of(example), EXAMPLES / example / "pyproject.toml")

    assert not problem, problem


@pytest.mark.parametrize("example", sorted(RUNBOOKS))
def test_runbook_has_one_section_per_readme_heading_in_order(example: str) -> None:
    readme = _readme_of(example)
    headings = readme_headings(readme)
    commands = readme_commands(readme)
    ends = [line for _name, line in headings[1:]] + [10**9]
    from_readme = [
        (name, [c.text for c in commands if start < c.line < end])
        for (name, start), end in zip(headings, ends, strict=True)
    ]
    from_runbook = [(s.name, [step.command for step in s.steps]) for s in RUNBOOKS[example]]

    assert from_runbook == from_readme


def test_quickstart_runbook_flags_serves_and_eventual_reads() -> None:
    steps = [step for section in RUNBOOKS["quickstart"] for step in section.steps]

    assert [s.command for s in steps if s.serve] == [
        "uvicorn myapp.main:app",
        "uvicorn myapp.main:app --reload",
        "modulith run myapp.main:app --topology=processes",
    ]
    processes = next(s for s in RUNBOOKS["quickstart"] if s.name.startswith("Run the same code"))
    assert [s.command for s in processes.steps if s.eventually] == [
        "curl -s localhost:8000/orders/ord-1/fulfilment",
        "curl -s localhost:8000/inventory/ord-1",
    ]
    assert sum(s.eventually for s in steps) == 2
