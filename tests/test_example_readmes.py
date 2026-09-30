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
  declares;
- the examples index (``examples/README.md``) runs nothing, since no runbook
  reads it and a command there would go untested.

The reader (``readme_commands``) and the data model are module-level so the
executors that run a runbook import them from here.
"""

from __future__ import annotations

import difflib
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import Any, NoReturn

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


def index_problems(index: Path) -> list[str]:
    """Every fence in the examples index that a reader could run as shell."""
    problems = placement_problems(index, ())
    problems.extend(
        f"{index}:{block.line - 1}: bash fence (commands belong in an example's README)"
        for block in fenced_blocks(index)
        if block.lang == "bash"
    )
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


STEP_TIMEOUT = 300.0
SCRUBBED_PREFIXES = ("MODULITH_", "UVICORN_", "PYTEST_", "OTEL_", "COMPOSE_")
SCRUBBED_NAMES = ("PYTHONPATH", "REDIS_URL", "ENV")
COPY_IGNORE = shutil.ignore_patterns(
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "*.db",
    ".modulith",
    "build",
)
PYTEST_ELAPSED = re.compile(r" in \d+(?:\.\d+)?s")
PYTEST_COUNT = re.compile(r"(\d+) (\w+)")


class RunbookFailure(AssertionError):
    """A runbook step that broke its README's promise; the message carries the transcript."""


def child_env(base: Mapping[str, str], workdir: Path) -> dict[str, str]:
    """``base`` without the settings that would steer a README's commands, plus the workdir's own.

    ``PYTHONPATH`` is dropped and never set: uvicorn, ``python -m`` and the ``modulith`` CLI
    put the project directory on ``sys.path`` themselves, which is the behaviour under test.
    """
    env = {
        name: value
        for name, value in base.items()
        if not name.startswith(SCRUBBED_PREFIXES) and name not in SCRUBBED_NAMES
    }
    env["PATH"] = os.pathsep.join(filter(None, [str(Path(sys.executable).parent), env.get("PATH")]))
    env["XDG_STATE_HOME"] = str(workdir / ".state")
    env["COMPOSE_PROJECT_NAME"] = f"modupy-readme-{uuid.uuid4().hex[:12]}"
    return env


def copy_example(source: Path, workdir: Path) -> None:
    shutil.copytree(source, workdir, ignore=COPY_IGNORE)


def _run_shell(
    command: str, env: Mapping[str, str], cwd: Path, timeout: float, *, merge: bool = True
) -> tuple[int | None, str, str]:
    """``bash -c command`` in its own session: ``(exit code, stdout, stderr)``.

    stderr is folded into stdout when ``merge``. The exit code is ``None`` after
    ``timeout``, when the whole process group has been killed.
    """
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = subprocess.Popen(
            ["bash", "-c", command],
            cwd=cwd,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=out if merge else err,
            start_new_session=True,
        )
        code: int | None
        try:
            code = process.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            code = None
        texts = []
        for stream in (out, err):
            stream.seek(0)
            texts.append(stream.read().decode(errors="replace"))
        return code, texts[0], texts[1]


def _missing_line(expected: Sequence[str], output: str) -> str | None:
    """The first expected line that is not a whole output line after the previous match."""
    lines = [line.rstrip() for line in output.splitlines()]
    position = 0
    for line in (text.rstrip() for text in expected):
        if not line:
            continue
        try:
            position = lines.index(line, position) + 1
        except ValueError:
            return line
    return None


def _pytest_problem(output: str) -> str:
    """Why a pytest run does not count as a pass, judged by its summary line; empty when it does."""
    counts: dict[str, int] = {}
    for line in reversed(output.splitlines()):
        elapsed = PYTEST_ELAPSED.search(line)
        if elapsed:
            counts = {word: int(n) for n, word in PYTEST_COUNT.findall(line[: elapsed.start()])}
            break
    for word in ("failed", "error", "errors", "skipped"):
        if counts.get(word):
            return f"pytest: {counts[word]} {word}"
    return "" if counts.get("passed") else "pytest: no test passed"


def _shown(output: str) -> str:
    return output if not output or output.endswith("\n") else output + "\n"


class Runbook:
    """One README run in one temp copy of its example, the way a reader works down a terminal.

    Sections run in order in the copy, so files and ``export`` lines carry from one to the
    next. Step N is the README's command N; its expected output comes from the README.
    """

    def __init__(
        self,
        source: Path,
        sections: Sequence[Section],
        workdir: Path,
        *,
        timeout: float = STEP_TIMEOUT,
    ) -> None:
        self.sections = sections
        self.workdir = workdir
        self.timeout = timeout
        self.readme = source / "README.md"
        self.commands = readme_commands(self.readme)
        steps = sum(len(section.steps) for section in sections)
        if steps != len(self.commands):
            raise ValueError(
                f"{self.readme}: the runbook lists {steps} steps but the README has "
                f"{len(self.commands)} commands"
            )
        copy_example(source, workdir)
        self.env = child_env(os.environ, workdir)
        self._log: list[str] = []

    @property
    def transcript(self) -> str:
        return "".join(self._log)

    def run_section(self, position: int) -> None:
        first = sum(len(section.steps) for section in self.sections[:position])
        for number, step in enumerate(self.sections[position].steps, start=first):
            self._run_step(step, self.commands[number])

    def _fail(self, command: Command, reason: str) -> NoReturn:
        raise RunbookFailure(f"{self.readme}:{command.line}: {reason}\n\n{self.transcript}")

    def _run_step(self, step: Step, command: Command) -> None:
        if step.serve or step.eventually:
            raise NotImplementedError(f"{'serve' if step.serve else 'eventually'} steps")
        words = step.command.split()
        if words[:2] == ["pip", "install"]:
            self._log.append(f"$ {step.command}\n(skipped: pip install in the repo environment)\n")
        elif words[:1] == ["export"]:
            self._export(step, command)
        else:
            code, output, _ = _run_shell(step.command, self.env, self.workdir, self.timeout)
            self._log.append(f"$ {step.command}\n{_shown(output)}")
            self._check_exit(step, command, code)
            if words[:1] == ["pytest"] and (problem := _pytest_problem(output)):
                self._fail(command, problem)
            if (line := _missing_line(command.output, output)) is not None:
                self._fail(command, f"expected line {line!r} not found, in order, in the output")

    def _export(self, step: Step, command: Command) -> None:
        code, stdout, stderr = _run_shell(
            f"{step.command}; env -0", self.env, self.workdir, self.timeout, merge=False
        )
        self._log.append(f"$ {step.command}\n{_shown(stderr)}")
        self._check_exit(step, command, code)
        self.env = dict(item.split("=", 1) for item in stdout.split("\0") if "=" in item)

    def _check_exit(self, step: Step, command: Command, code: int | None) -> None:
        if code is None:
            self._fail(command, f"timed out after {self.timeout:g}s")
        if code != step.exit_code:
            self._fail(command, f"exit {code}, expected {step.exit_code}")


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


def test_index_refuses_a_bash_fence_with_its_file_and_line(tmp_path: Path) -> None:
    index = _readme(tmp_path, "intro", "```bash", "pytest", "```")

    problems = index_problems(index)

    assert len(problems) == 1
    assert problems[0].startswith(f"{index}:2:")


def test_examples_index_runs_nothing() -> None:
    problems = index_problems(EXAMPLES / "README.md")

    assert not problems, "\n".join(problems)


def _bash(*lines: str) -> list[str]:
    return ["```bash", *lines, "```"]


def _runbook(
    tmp_path: Path,
    *readme_lines: str,
    steps: Sequence[Step] | None = None,
    sections: Sequence[Section] | None = None,
    files: Mapping[str, str] | None = None,
    timeout: float = 30.0,
) -> Runbook:
    source = tmp_path / "example"
    source.mkdir(parents=True)
    (source / "README.md").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")
    for name, text in (files or {}).items():
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        (source / name).write_text(text, encoding="utf-8")
    if sections is None:
        listed = steps or tuple(Step(c.text) for c in readme_commands(source / "README.md"))
        sections = (Section("all", tuple(listed)),)
    return Runbook(source, sections, tmp_path / "work", timeout=timeout)


def _run(tmp_path: Path, *readme_lines: str, **kwargs: Any) -> Runbook:
    runbook = _runbook(tmp_path, *readme_lines, **kwargs)
    for position in range(len(runbook.sections)):
        runbook.run_section(position)
    return runbook


def _failure(tmp_path: Path, *readme_lines: str, **kwargs: Any) -> str:
    with pytest.raises(RunbookFailure) as caught:
        _run(tmp_path, *readme_lines, **kwargs)
    return str(caught.value)


def _reason(message: str) -> str:
    return message.splitlines()[0]


def test_a_step_runs_from_the_copy_root_and_leaves_the_example_untouched(tmp_path: Path) -> None:
    runbook = _run(
        tmp_path,
        *_bash("$ pwd", str(tmp_path / "work"), "$ echo made > made.txt"),
    )

    assert (tmp_path / "work" / "made.txt").read_text() == "made\n"
    assert not (tmp_path / "example" / "made.txt").exists()
    assert runbook.workdir == tmp_path / "work"


def test_the_copy_leaves_out_environments_caches_databases_and_build_output(
    tmp_path: Path,
) -> None:
    source = tmp_path / "example"
    for name in (".venv/bin/python", "pkg/__pycache__/x.pyc", ".pytest_cache/v", ".mypy_cache/m"):
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        (source / name).write_text("x")
    for name in (".ruff_cache/r", "build/lib/y.py", ".modulith/state", "shop.db"):
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        (source / name).write_text("x")
    for name in ("pkg/app.py", "builder.py", "README.md"):
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        (source / name).write_text("x")

    copy_example(source, tmp_path / "work")

    kept = sorted(
        p.relative_to(tmp_path / "work").as_posix()
        for p in (tmp_path / "work").rglob("*")
        if p.is_file()
    )
    assert kept == ["README.md", "builder.py", "pkg/app.py"]


def test_sections_share_one_copy_and_an_export_reaches_later_sections(tmp_path: Path) -> None:
    sections = (
        Section("first", (Step("export GREETING=hello"), Step("echo kept > note.txt"))),
        Section("second", (Step("echo $GREETING"), Step("cat note.txt"))),
    )
    readme = (
        "## first",
        *_bash("export GREETING=hello", "echo kept > note.txt"),
        "## second",
        *_bash("$ echo $GREETING", "hello", "$ cat note.txt", "kept"),
    )

    _run(tmp_path, *readme, sections=sections)

    assert "GREETING" not in os.environ


def test_an_export_evaluates_command_substitution_like_a_terminal(tmp_path: Path) -> None:
    _run(
        tmp_path,
        *_bash("$ export STAMP=$(echo computed)", "$ echo $STAMP", "computed"),
    )


def test_an_export_that_bash_cannot_parse_fails_the_step(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("export BAD=("))

    assert "export BAD=(" in message
    assert "syntax error" in message


def test_a_step_that_does_not_export_does_not_change_the_environment(tmp_path: Path) -> None:
    message = _failure(
        tmp_path,
        *_bash("$ FOO=bar true", '$ echo "[${FOO-}]"', "[bar]"),
    )

    assert "\n[]\n" in message


def test_pip_install_is_skipped_in_the_repo_environment(tmp_path: Path) -> None:
    runbook = _run(
        tmp_path,
        *_bash(
            "$ pip install modupy-package-that-does-not-exist-anywhere", "$ echo after", "after"
        ),
    )

    assert "skipped" in runbook.transcript


def test_a_command_that_only_mentions_pip_install_is_not_skipped(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("echo pip install nothing; false"))

    assert "exit 1" in message


def _pytest_example(*tests: str) -> dict[str, str]:
    return {"tests/test_it.py": "import pytest\n\n" + "\n\n".join(tests) + "\n"}


PASSING = "def test_ok():\n    assert True"
SKIPPED = "def test_skipped():\n    pytest.skip('no')"
FAILING = "def test_bad():\n    assert False"
XFAIL = "@pytest.mark.xfail\ndef test_expected_failure():\n    assert False"
BROKEN = "def test_error(missing_fixture):\n    pass"


def test_a_pytest_step_passes_when_something_passed_and_nothing_else_happened(
    tmp_path: Path,
) -> None:
    runbook = _run(tmp_path, *_bash("pytest"), files=_pytest_example(PASSING))

    assert "1 passed" in runbook.transcript


@pytest.mark.parametrize(
    ("tests", "command", "reason"),
    [
        ((PASSING, SKIPPED), "pytest", "skipped"),
        ((PASSING, FAILING), "pytest || true", "failed"),
        ((PASSING, BROKEN), "pytest || true", "error"),
        ((XFAIL,), "pytest", "no test passed"),
        ((PASSING,), "pytest -k nomatch || true", "no test passed"),
    ],
    ids=["skipped", "failed", "errored", "nothing-passed", "nothing-collected"],
)
def test_a_pytest_step_fails_on_skipped_failed_errored_or_no_passes(
    tmp_path: Path, tests: tuple[str, ...], command: str, reason: str
) -> None:
    message = _failure(tmp_path, *_bash(command), files=_pytest_example(*tests))

    assert reason in _reason(message)


def test_a_failing_pytest_step_fails_on_its_exit_code(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("pytest"), files=_pytest_example(FAILING))

    assert "exit 1" in _reason(message)


def test_expected_lines_may_have_unshown_lines_between_them(tmp_path: Path) -> None:
    _run(tmp_path, *_bash("$ printf 'a\\nb\\nc\\nd\\n'", "a", "c"))


def test_blank_expected_lines_and_trailing_spaces_are_ignored(tmp_path: Path) -> None:
    _run(tmp_path, *_bash("$ printf 'x   \\n\\ny\\n'", "x", "", "", "y"))


def test_a_final_line_without_a_newline_counts_as_a_line(tmp_path: Path) -> None:
    _run(tmp_path, *_bash("$ printf 'a\\nbody'", "a", "body"))


def test_stderr_is_merged_into_the_matched_output(tmp_path: Path) -> None:
    _run(tmp_path, *_bash("$ echo oops >&2", "oops"))


def test_expected_lines_out_of_order_fail(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("$ printf 'a\\nb\\n'", "b", "a"))

    assert "'a'" in _reason(message)


def test_a_prefix_of_an_output_line_is_not_a_match(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("$ echo abc", "ab"))

    assert "'ab'" in _reason(message)


def test_a_missing_expected_line_is_named_with_the_actual_output(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("$ printf 'a\\nb\\n'", "a", "gone", "b"))

    assert "'gone'" in _reason(message)
    assert "\na\nb\n" in message


def test_a_non_zero_exit_fails_the_step(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("echo before; exit 3"))

    assert "exit 3" in _reason(message)
    assert "before" in message


def test_a_declared_exit_code_passes_and_any_other_exit_fails(tmp_path: Path) -> None:
    _run(
        tmp_path,
        *_bash("$ echo shown; exit 3", "shown"),
        steps=(Step("echo shown; exit 3", exit_code=3),),
    )

    wrong = _failure(tmp_path / "wrong", *_bash("exit 4"), steps=(Step("exit 4", exit_code=3),))
    clean = _failure(tmp_path / "clean", *_bash("true"), steps=(Step("true", exit_code=3),))

    assert "exit 4" in _reason(wrong)
    assert "exit 0" in _reason(clean)


def test_a_step_that_outlives_its_timeout_fails_with_its_partial_output(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("echo started; sleep 60"), timeout=1.0)

    assert "timed out" in _reason(message)
    assert "started" in message


def test_a_timeout_kills_the_whole_process_group(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("sleep 60 & echo child=$!; wait"), timeout=1.0)

    child = int(re.search(r"child=(\d+)", message.split("\n$ ", 1)[1]).group(1))  # type: ignore[union-attr]
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"child {child} survived the timeout")


def test_a_step_leads_its_own_session(tmp_path: Path) -> None:
    _run(
        tmp_path,
        *_bash(
            "$ exec python -c 'import os; print(os.getsid(0) == os.getpid())'",
            "True",
        ),
    )


def test_the_failure_shows_every_command_as_executed_with_its_output(tmp_path: Path) -> None:
    message = _failure(tmp_path, *_bash("echo one", "echo two; false"))

    assert "$ echo one\none\n" in message
    assert "$ echo two; false\ntwo\n" in message
    assert "README.md:" in message


def test_the_runbook_and_the_readme_must_list_the_same_number_of_commands(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"2 steps.*1 command"):
        _runbook(tmp_path, *_bash("true"), steps=(Step("true"), Step("false")))


@pytest.mark.parametrize("flag", ["serve", "eventually"])
def test_a_serve_or_eventually_step_is_refused_not_run_as_a_plain_step(
    tmp_path: Path, flag: str
) -> None:
    with pytest.raises(NotImplementedError, match=flag):
        _run(tmp_path, *_bash("true"), steps=(Step("true", **{flag: True}),))


PARENT_ENV = {
    "MODULITH_BROKER": "redis",
    "UVICORN_PORT": "1",
    "PYTEST_ADDOPTS": "-x",
    "OTEL_SDK_DISABLED": "false",
    "COMPOSE_FILE": "elsewhere.yml",
    "COMPOSE_PROJECT_NAME": "parent",
    "PYTHONPATH": "/somewhere",
    "REDIS_URL": "redis://elsewhere",
    "ENV": "/etc/profile",
}


def test_the_child_environment_drops_the_parent_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in PARENT_ENV.items():
        monkeypatch.setenv(name, value)

    runbook = _run(tmp_path, *_bash("env"))

    names = {line.partition("=")[0] for line in runbook.transcript.splitlines()}
    assert not names & (set(PARENT_ENV) - {"COMPOSE_PROJECT_NAME"})
    assert "PYTEST_CURRENT_TEST" not in names
    assert "COMPOSE_PROJECT_NAME=parent" not in runbook.transcript
    assert "COMPOSE_PROJECT_NAME" in names


def test_the_child_environment_is_the_scrubbed_parent_plus_the_workdir_settings(
    tmp_path: Path,
) -> None:
    base = {**PARENT_ENV, "PATH": "/usr/bin", "HOME": "/home/x", "LANG": "C"}

    env = child_env(base, tmp_path / "work")

    assert set(env) == {
        "PATH",
        "HOME",
        "LANG",
        "XDG_STATE_HOME",
        "COMPOSE_PROJECT_NAME",
    }
    assert env["HOME"] == "/home/x"
    assert env["XDG_STATE_HOME"].startswith(str(tmp_path / "work"))
    assert env["COMPOSE_PROJECT_NAME"] != "parent"


def test_each_child_environment_gets_its_own_compose_project(tmp_path: Path) -> None:
    names = {child_env({}, tmp_path)["COMPOSE_PROJECT_NAME"] for _ in range(3)}

    assert len(names) == 3


def test_the_interpreters_bin_directory_leads_the_path(tmp_path: Path) -> None:
    env = child_env({"PATH": "/usr/bin"}, tmp_path)

    assert env["PATH"] == f"{Path(sys.executable).parent}{os.pathsep}/usr/bin"


def test_python_in_a_step_is_the_running_interpreters_python(tmp_path: Path) -> None:
    _run(
        tmp_path,
        *_bash("$ command -v python", str(Path(sys.executable).parent / "python")),
    )


def test_the_child_environment_never_sets_pythonpath(tmp_path: Path) -> None:
    assert "PYTHONPATH" not in child_env({"PYTHONPATH": "/x"}, tmp_path)
    assert "PYTHONPATH" not in child_env({}, tmp_path)
