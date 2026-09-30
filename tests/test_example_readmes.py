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

import contextlib
import difflib
import http.client
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from itertools import chain, takewhile
from pathlib import Path
from typing import Any, NoReturn

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from conftest import Block, _docker_available, _free_port_block, fenced_blocks

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
    "demo_app": (
        Section(
            "Look before you run",
            (
                Step("pip install 'modupy[fastapi,cli,postgres]' aiosqlite"),
                Step("modulith info"),
                Step("modulith verify"),
                Step("modulith docs"),
            ),
        ),
        Section(
            "Run its tests",
            (Step("pip install pytest pytest-asyncio"), Step("pytest")),
        ),
        Section(
            "Stage 1: in-memory events",
            (
                Step("python -m shop.schema"),
                Step("uvicorn shop.main:app", serve=True),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-1", "customer_id": "alice", "total": 19.99}\''
                ),
                Step("curl -s localhost:8000/orders/o-1"),
                Step("curl -s localhost:8000/inventory/reservations/o-1"),
                Step("curl -s localhost:8000/notifications/o-1"),
            ),
        ),
        Section(
            "Stage 2: a durable outbox on SQLite",
            (
                Step(
                    "export MODULITH_OUTBOX=postgres MODULITH_OUTBOX_URL=sqlite+aiosqlite:///shop.db"
                ),
                Step("python -m shop.schema"),
                Step("modulith migrate"),
                Step("uvicorn shop.main:app", serve=True),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-2", "customer_id": "alice", "total": 19.99}\''
                ),
                Step("curl -s localhost:8000/notifications/o-2", eventually=True),
                Step("modulith outbox status", eventually=True),
            ),
        ),
        Section(
            "Stage 2 afterwards: what the outbox kept",
            (Step("modulith outbox status"), Step("modulith doctor")),
        ),
        Section(
            "Stage 3: the same code, one process per module",
            (
                Step(
                    "export MODULITH_OUTBOX=postgres MODULITH_OUTBOX_URL=sqlite+aiosqlite:///shop.db"
                ),
                Step(
                    "modulith run shop.main:app --topology processes --host 127.0.0.1",
                    serve=True,
                ),
                Step("curl -s localhost:8000/_modulith/health"),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-3", "customer_id": "alice", "total": 19.99}\''
                ),
                Step("curl -s localhost:8000/orders/o-3", eventually=True),
                Step("curl -s localhost:8000/inventory/reservations/o-3", eventually=True),
                Step("curl -s localhost:8000/notifications/o-3", eventually=True),
                Step("modulith outbox status", eventually=True),
            ),
        ),
        Section(
            "Stage 3 afterwards: the outbox after the drain",
            (Step("modulith outbox status"),),
        ),
        Section(
            "Stage 4a: the outbox on Postgres",
            (
                Step("docker compose up -d --wait postgres"),
                Step(
                    "export MODULITH_OUTBOX=postgres "
                    "MODULITH_OUTBOX_URL=postgresql+asyncpg://modulith:modulith@localhost:55433/modulith"
                ),
                Step("python -m shop.schema"),
                Step("modulith migrate"),
                Step("uvicorn shop.main:app", serve=True),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-4", "customer_id": "alice", "total": 19.99}\''
                ),
                Step("curl -s localhost:8000/notifications/o-4", eventually=True),
                Step("modulith outbox status", eventually=True),
            ),
        ),
        Section(
            "Stage 4b: processes over Redis Streams",
            (
                Step("pip install 'modupy[redis]'"),
                Step("docker compose up -d --wait redis"),
                Step(
                    "export MODULITH_OUTBOX=postgres "
                    "MODULITH_OUTBOX_URL=sqlite+aiosqlite:///shop.db "
                    "MODULITH_BROKER=redis-streams REDIS_URL=redis://:modulith@localhost:56379"
                ),
                Step(
                    "modulith run shop.main:app --topology processes --host 127.0.0.1",
                    serve=True,
                ),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-5", "customer_id": "alice", "total": 19.99}\''
                ),
                Step("curl -s localhost:8000/orders/o-5", eventually=True),
                Step("curl -s localhost:8000/inventory/reservations/o-5", eventually=True),
                Step("curl -s localhost:8000/notifications/o-5", eventually=True),
            ),
        ),
        Section("Clean up", (Step("docker compose down -v"),)),
    ),
    "marketplace": (
        Section(
            "Check the architecture",
            (
                Step('pip install -e ".[test]"'),
                Step("pytest"),
                Step("modulith verify"),
                Step("modulith docs --output-dir build/docs"),
                Step("modulith openapi --output build/openapi.json"),
                Step("modulith k8s-manifest --image marketplace:1.0.0 --output build/k8s.yaml"),
            ),
            repo_env=False,
        ),
        Section(
            "Run the platform, then extract a service",
            (
                Step("docker compose up -d --wait postgres"),
                Step(
                    "export MODULITH_OUTBOX_URL=postgresql+asyncpg://marketplace:marketplace"
                    "@localhost:55432/marketplace "
                    "MODULITH_BROKER_URL=postgresql+asyncpg://marketplace:marketplace"
                    "@localhost:55432/marketplace "
                    "MODULITH_ACTUATOR_TOKEN=local-demo-token"
                ),
                Step("modulith migrate"),
                Step("python -m marketplace.schema"),
                Step('python -m marketplace.catalog SKU-MUG "Stoneware mug" 1200 10'),
                Step("modulith doctor"),
                Step("modulith run marketplace.main:app --topology processes", serve=True),
                Step(
                    "curl -s localhost:8000/_modulith/health "
                    '-H "authorization: Bearer $MODULITH_ACTUATOR_TOKEN"'
                ),
                Step("curl -s -o /dev/null -w '%{http_code}\\n' localhost:8000/_modulith/health"),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-100", "customer_id": "alice", "sku": "SKU-MUG", '
                    '"quantity": 2, "card_token": "tok_visa", "country": "US"}\''
                ),
                Step("curl -s localhost:8000/notifications/o-100", eventually=True),
                Step("curl -s localhost:8000/orders/o-100"),
                Step("curl -s localhost:8000/shipping/o-100"),
                Step("curl -s localhost:8000/inventory/SKU-MUG"),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-200", "customer_id": "bob", "sku": "SKU-MUG", '
                    '"quantity": 3, "card_token": "tok_declined", "country": "DE"}\''
                ),
                Step("curl -s localhost:8000/notifications/o-200", eventually=True),
                Step("curl -s localhost:8000/orders/o-200"),
                Step("curl -s localhost:8000/inventory/SKU-MUG"),
                Step("curl -s localhost:8000/shipping/o-200"),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-300", "customer_id": "carol", "sku": "SKU-MUG", '
                    '"quantity": 1, "card_token": "tok_visa", "country": "NZ"}\''
                ),
                Step("modulith outbox dead-letter --list", eventually=True),
                Step("curl -s localhost:8000/shipping/o-300"),
                Step(
                    "curl -sX PUT localhost:8000/shipping/zones/NZ "
                    "-H 'content-type: application/json' -d '{\"carrier\": \"NZPost\"}'"
                ),
                Step("modulith outbox dead-letter --retry-all"),
                Step("curl -s localhost:8000/notifications/o-300", eventually=True),
                Step("curl -s localhost:8000/reporting/summary", eventually=True),
                Step("modulith outbox status", eventually=True),
                Step("modulith extract notifications --output build/notifications-service"),
                Step(
                    "cd build/notifications-service && OTEL_TRACES_EXPORTER=console "
                    "modulith run marketplace:app --topology processes "
                    "--port 8100 --worker-port-base 9101",
                    serve=True,
                ),
                Step("modulith dev marketplace.main:app --isolate orders", serve=True),
                Step(
                    "curl -sX POST localhost:8000/orders -H 'content-type: application/json' "
                    '-d \'{"order_id": "o-400", "customer_id": "dana", "sku": "SKU-MUG", '
                    '"quantity": 1, "card_token": "tok_visa", "country": "US"}\''
                ),
                Step(
                    "curl -s -o /dev/null -w '%{http_code}\\n' localhost:8000/notifications/o-400"
                ),
                Step("curl -s localhost:8100/notifications/o-400", eventually=True),
                Step("docker compose down -v"),
            ),
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
STOP_WAIT = 30.0
READY_TIMEOUT = 60.0
EVENTUALLY_TIMEOUT = 60.0
EVENTUALLY_INTERVAL = 0.5
PORT_BLOCK = 12
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
PORT_POSITION = re.compile(
    r"((?<![\w-])--port[= ]|(?<![\w-])--worker-port-base[= ]|localhost:|127\.0\.0\.1:)(\d+)"
)
PORT_NUMBER = re.compile(r"(?<!\d)\d{2,5}(?!\d)")
WORKER_BANNER = re.compile(r"worker\(s\) \[([^\]]*)\]")
NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))
PYTEST_ELAPSED = re.compile(r" in \d+(?:\.\d+)?s")
PYTEST_COUNT = re.compile(r"(\d+) (\w+)")


class RunbookFailure(AssertionError):
    """A runbook step that broke its README's promise; the message carries the transcript."""


def _without_interpreter_bin(path: str) -> list[str]:
    """The ``PATH`` entries minus the running interpreter's ``bin``, unless that is a system one."""
    interpreter_bin = Path(sys.executable).parent
    entries = path.split(os.pathsep)
    if (interpreter_bin / "bash").exists():
        return entries
    return [
        entry
        for entry in entries
        if not entry or Path(entry).resolve() != interpreter_bin.resolve()
    ]


def child_env(base: Mapping[str, str], workdir: Path, venv: Path | None = None) -> dict[str, str]:
    """``base`` without the settings that would steer a README's commands, plus the workdir's own.

    ``PYTHONPATH`` is dropped and never set: uvicorn, ``python -m`` and the ``modulith`` CLI
    put the project directory on ``sys.path`` themselves, which is the behaviour under test.
    With ``venv``, that environment's ``bin`` leads ``PATH`` and the running interpreter's
    ``bin`` (found by resolved path, so links and duplicates go too) is dropped, so nothing
    installed for the tests, in a venv or system-wide, answers for a missing install. A
    directory that also holds ``bash`` is a system ``bin`` and stays, since the README's
    own ``bash``, ``curl`` and ``docker`` live there.
    """
    env = {
        name: value
        for name, value in base.items()
        if not name.startswith(SCRUBBED_PREFIXES) and name not in SCRUBBED_NAMES
    }
    if venv:
        env["PATH"] = os.pathsep.join(
            filter(None, [str(venv / "bin"), *_without_interpreter_bin(env.get("PATH", ""))])
        )
        env["VIRTUAL_ENV"] = str(venv)
    else:
        env["PATH"] = os.pathsep.join(
            filter(None, [str(Path(sys.executable).parent), env.get("PATH")])
        )
    env["XDG_STATE_HOME"] = str(workdir / ".state")
    env["COMPOSE_PROJECT_NAME"] = f"modupy-readme-{uuid.uuid4().hex[:12]}"
    return env


def copy_example(source: Path, workdir: Path) -> None:
    shutil.copytree(source, workdir, ignore=COPY_IGNORE)


def create_wheel_venv(venv: Path, wheel: Path) -> None:
    """A fresh ``python -m venv`` at ``venv`` holding ``wheel`` and its dependencies, no extras."""
    for command in (
        [sys.executable, "-m", "venv", str(venv)],
        [str(venv / "bin" / "python"), "-m", "pip", "install", str(wheel)],
    ):
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        assert result.returncode == 0, f"{' '.join(command)} exited {result.returncode}\n" + (
            result.stdout + result.stderr
        )


def wheel_provenance_problem(venv: Path, wheel: Path) -> str:
    """Why ``venv``'s modupy is not ``wheel``, judged by pip's ``direct_url.json``; empty when it is."""
    pattern = "lib/python*/site-packages/modupy-*.dist-info/direct_url.json"
    records = sorted(venv.glob(pattern))
    if not records:
        return f"{venv / pattern} does not exist: modupy was not installed from a local file"
    url: str = json.loads(records[0].read_text(encoding="utf-8"))["url"]
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "file":
        installed = Path(urllib.request.url2pathname(parsed.path)).resolve()
        if installed == wheel.resolve():
            return ""
    return f"{records[0]} names {url}, not the session-built wheel {wheel}"


def _exit_status(process: subprocess.Popen[bytes]) -> int | None:
    """The exit status of a finished process, without reaping it; ``None`` while it runs.

    An unreaped process keeps its pid, so the process group it leads can still be
    signalled without the pid having been handed to anything else.
    """
    result = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    return None if result is None else result.si_status


def _finish(process: subprocess.Popen[bytes], timeout: float) -> int | None:
    """Wait up to ``timeout`` for the group leader, then kill its whole group and reap it.

    Returns the leader's exit status, or ``None`` when it outlived ``timeout``.
    """
    deadline = time.monotonic() + timeout
    status = _exit_status(process)
    while status is None and time.monotonic() < deadline:
        time.sleep(0.01)
        status = _exit_status(process)
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    process.wait()
    return status


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def _stop_group(process: subprocess.Popen[bytes], wait: float) -> None:
    """Give the group ``wait`` seconds to end after SIGINT, then SIGKILL what is left.

    The leader is reaped as soon as it exits, so only live members keep the group
    alive. The kernel does not hand out a process id that still names a process
    group, so a group that is still alive cannot belong to anything else.
    """
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        process.poll()
        if not _group_alive(process.pid):
            break
        time.sleep(0.05)
    if _group_alive(process.pid):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    process.wait()


def _run_shell(
    command: str, env: Mapping[str, str], cwd: Path, timeout: float, *, merge: bool = True
) -> tuple[int | None, str, str]:
    """``bash -c command`` in its own session: ``(exit code, stdout, stderr)``.

    stderr is folded into stdout when ``merge``. Whatever the step left running in its
    process group is killed when it ends. The exit code is ``None`` after ``timeout``.
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
        code = _finish(process, timeout)
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


def _rewrite(command: str, ports: Mapping[int, int]) -> str:
    """``command`` with each documented port in a port position swapped for its allocated one."""
    return PORT_POSITION.sub(
        lambda found: found.group(1) + str(ports.get(int(found.group(2)), found.group(2))),
        command,
    )


def _map_back(text: str, ports: Mapping[int, int]) -> str:
    """``text`` with every allocated port number swapped back for its documented one."""
    documented = {allocated: port for port, allocated in ports.items()}
    return PORT_NUMBER.sub(
        lambda found: str(documented.get(int(found.group()), found.group())), text
    )


def _option(command: str, flag: str, default: int) -> int:
    found = re.search(rf"(?<![\w-]){flag}[= ](\d+)", command)
    return int(found.group(1)) if found else default


def _has_docker_step(steps: Sequence[Step]) -> bool:
    return any(step.command.split()[:1] == ["docker"] for step in steps)


def _repo_sections(sections: Sequence[Section]) -> list[Section]:
    """The leading sections a runbook can run in the repo environment."""
    return list(
        takewhile(
            lambda section: section.repo_env and not _has_docker_step(section.steps),
            sections,
        )
    )


def _http_status(port: int, path: str) -> int | None:
    """The status of ``GET path`` on a local port; ``None`` when nothing answers."""
    try:
        with NO_PROXY.open(f"http://127.0.0.1:{port}{path}", timeout=2) as response:
            status: int = response.status
            return status
    except urllib.error.HTTPError as error:
        return error.code
    except (OSError, http.client.HTTPException):
        return None


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
        venv: Path | None = None,
        timeout: float = STEP_TIMEOUT,
        stop_wait: float = STOP_WAIT,
        ready_timeout: float = READY_TIMEOUT,
        eventually_timeout: float = EVENTUALLY_TIMEOUT,
        eventually_interval: float = EVENTUALLY_INTERVAL,
    ) -> None:
        self.sections = sections
        self.workdir = workdir
        self.timeout = timeout
        self.stop_wait = stop_wait
        self.ready_timeout = ready_timeout
        self.eventually_timeout = eventually_timeout
        self.eventually_interval = eventually_interval
        self.pgids: list[int] = []
        self.readme = source / "README.md"
        self.commands = readme_commands(self.readme)
        steps = sum(len(section.steps) for section in sections)
        if steps != len(self.commands):
            raise ValueError(
                f"{self.readme}: the runbook lists {steps} steps but the README has "
                f"{len(self.commands)} commands"
            )
        copy_example(source, workdir)
        self.venv = venv
        self.env = child_env(os.environ, workdir, venv)
        self._log: list[str] = []
        self._serves: list[_Serve] = []

    @property
    def transcript(self) -> str:
        running = "".join(
            f"[log so far of {serve.executed}]\n{_shown(serve.text())}" for serve in self._serves
        )
        return "".join(self._log) + running

    def run_repo_environment(self) -> None:
        """Run the leading sections that need neither Docker nor the built wheel."""
        for position in range(len(_repo_sections(self.sections))):
            self.run_section(position)

    def run_built_wheel(self) -> None:
        """Run every section, Docker ones included, in the venv this runbook was given."""
        for position in range(len(self.sections)):
            self.run_section(position)

    def run_section(self, position: int) -> None:
        section = self.sections[position]
        first = sum(len(earlier.steps) for earlier in self.sections[:position])
        completed = False
        try:
            for number, step in enumerate(section.steps, start=first):
                self._run_step(step, self.commands[number])
            completed = True
        finally:
            try:
                self._stop_serves(check=completed)
            finally:
                if _has_docker_step(section.steps):
                    self._compose_down()

    def _fail(self, command: Command, reason: str) -> NoReturn:
        raise RunbookFailure(f"{self.readme}:{command.line}: {reason}\n\n{self.transcript}")

    def _ports(self) -> dict[int, int]:
        return {port: at for serve in self._serves for port, at in serve.mapped().items()}

    def _run_step(self, step: Step, command: Command) -> None:
        words = step.command.split()
        if step.serve:
            self._serve(step, command)
        elif words[:2] == ["pip", "install"] and self.venv is None:
            self._log.append(f"$ {step.command}\n(skipped: pip install in the repo environment)\n")
        elif words[:1] == ["export"]:
            self._export(step, command)
        else:
            self._plain(step, command)

    def _plain(self, step: Step, command: Command) -> None:
        executed = _rewrite(step.command, self._ports())
        deadline = time.monotonic() + self.eventually_timeout
        while True:
            code, output, _ = _run_shell(executed, self.env, self.workdir, self.timeout)
            problem = self._problem(step, command, code, output)
            if not problem or not step.eventually or time.monotonic() >= deadline:
                break
            time.sleep(self.eventually_interval)
        self._log.append(f"$ {executed}\n{_shown(output)}")
        if problem:
            retried = f" (still, after {self.eventually_timeout:g}s of retries)"
            self._fail(command, problem + (retried if step.eventually else ""))

    def _problem(self, step: Step, command: Command, code: int | None, output: str) -> str:
        if problem := _exit_problem(step, code, self.timeout):
            return problem
        if step.command.split()[:1] == ["pytest"] and (problem := _pytest_problem(output)):
            return problem
        line = _missing_line(command.output, _map_back(output, self._ports()))
        return "" if line is None else f"expected line {line!r} not found, in order, in the output"

    def _export(self, step: Step, command: Command) -> None:
        executed = _rewrite(step.command, self._ports())
        code, stdout, stderr = _run_shell(
            f"{executed}; env -0", self.env, self.workdir, self.timeout, merge=False
        )
        self._log.append(f"$ {executed}\n{_shown(stderr)}")
        if problem := _exit_problem(step, code, self.timeout):
            self._fail(command, problem)
        self.env = dict(item.split("=", 1) for item in stdout.split("\0") if "=" in item)

    def _serve(self, step: Step, command: Command) -> None:
        text = step.command
        base = _free_port_block(PORT_BLOCK)
        ports = {_option(text, "--port", 8000): base}
        first = _option(text, "--worker-port-base", 9001)
        workers = {first + offset: base + 1 + offset for offset in range(PORT_BLOCK - 1)}
        for holder in [serve for serve in self._serves if serve.ports.keys() & ports.keys()]:
            self._stop_serve(holder, check=True)
        executed = _rewrite(text, {**self._ports(), **ports, **workers})
        if not re.search(r"(?<![\w-])--port[= ]", text):
            executed += f" --port {base}"
        self._log.append(f"$ {executed}\n(serve)\n")
        log = self.workdir / f".serve-{len(self.pgids)}.log"
        with log.open("wb") as out:
            process = subprocess.Popen(
                ["bash", "-c", executed],
                cwd=self.workdir,
                env={**self.env, "MODULITH_WORKER_PORT_BASE": str(base + 1)},
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        serve = _Serve(executed, command, process, log, ports, workers)
        self._serves.append(serve)
        self.pgids.append(process.pid)
        self._await_ready(serve, base)

    def _await_ready(self, serve: _Serve, base: int) -> None:
        deadline = time.monotonic() + self.ready_timeout
        while True:
            status = _exit_status(serve.process)
            if status is not None:
                self._fail(serve.command, f"serve exited with code {status} before it was ready")
            problem = self._not_ready(serve, base)
            if not problem:
                return
            if time.monotonic() >= deadline:
                self._fail(
                    serve.command, f"serve not ready after {self.ready_timeout:g}s: {problem}"
                )
            time.sleep(0.1)

    def _not_ready(self, serve: _Serve, base: int) -> str:
        if _http_status(base, "/") is None:
            return f"no HTTP answer on port {base}"
        banner = WORKER_BANNER.search(serve.text())
        if banner is None:
            return ""
        for port in re.findall(r":(\d+)", banner.group(1)):
            if _http_status(int(port), "/health") != 200:
                return f"GET /health on worker port {port} did not answer 200"
        return ""

    def _stop_serves(self, *, check: bool) -> None:
        failure: RunbookFailure | None = None
        for serve in list(self._serves):
            try:
                self._stop_serve(serve, check=check)
            except RunbookFailure as error:
                failure = failure or error
        if failure:
            raise failure

    def _stop_serve(self, serve: _Serve, *, check: bool) -> None:
        documented = self._ports()
        self._serves.remove(serve)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(serve.process.pid, signal.SIGINT)
        _stop_group(serve.process, self.stop_wait)
        log = serve.text()
        self._log.append(f"[log of {serve.executed}]\n{_shown(log)}")
        line = _missing_line(serve.command.output, _map_back(log, documented))
        if check and line is not None:
            self._fail(
                serve.command,
                f"serve output: expected line {line!r} not found, in order, in its log",
            )

    def _compose_down(self) -> None:
        command = "docker compose down -v --remove-orphans"
        _code, output, _ = _run_shell(command, self.env, self.workdir, self.timeout)
        self._log.append(f"$ {command}\n{_shown(output)}")


def _exit_problem(step: Step, code: int | None, timeout: float) -> str:
    if code is None:
        return f"timed out after {timeout:g}s"
    return "" if code == step.exit_code else f"exit {code}, expected {step.exit_code}"


@dataclass
class _Serve:
    executed: str
    command: Command
    process: subprocess.Popen[bytes]
    log: Path
    ports: dict[int, int]
    workers: dict[int, int]

    def text(self) -> str:
        return self.log.read_text(encoding="utf-8", errors="replace")

    def mapped(self) -> dict[int, int]:
        """The ports this serve is known to hold; its workers count once its banner names them."""
        return {**self.ports, **self.workers} if WORKER_BANNER.search(self.text()) else self.ports


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


@pytest.mark.parametrize(
    "project", sorted(path.parent.name for path in EXAMPLES.glob("*/pyproject.toml"))
)
def test_every_example_project_has_a_runbook_that_installs_and_tests_it(project: str) -> None:
    leads = [
        step.command.split()[:2] for section in RUNBOOKS.get(project, ()) for step in section.steps
    ]

    assert ["pip", "install"] in leads, f"RUNBOOKS has no step installing examples/{project}"
    if (EXAMPLES / project / "tests").is_dir():
        assert ["pytest"] in (lead[:1] for lead in leads), (
            f"RUNBOOKS has no pytest step for examples/{project}/tests"
        )


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
    venv: Path | None = None,
    **options: float,
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
    return Runbook(source, sections, tmp_path / "work", timeout=timeout, venv=venv, **options)


def _run(tmp_path: Path, *readme_lines: str, **kwargs: Any) -> Runbook:
    runbook = _runbook(tmp_path, *readme_lines, **kwargs)
    for position in range(len(runbook.sections)):
        runbook.run_section(position)
    return runbook


def _broken(tmp_path: Path, *readme_lines: str, **kwargs: Any) -> tuple[Runbook, str]:
    runbook = _runbook(tmp_path, *readme_lines, **kwargs)
    with pytest.raises(RunbookFailure) as caught:
        for position in range(len(runbook.sections)):
            runbook.run_section(position)
    return runbook, str(caught.value)


def _failure(tmp_path: Path, *readme_lines: str, **kwargs: Any) -> str:
    return _broken(tmp_path, *readme_lines, **kwargs)[1]


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


PORTS = {8000: 41000, 9001: 41001, 9002: 41002, 9003: 41003}


@pytest.mark.parametrize(
    "command",
    [
        "curl localhost:8000/orders",
        "curl -s 127.0.0.1:9002/health",
        "python server.py --port 8000 --worker-port-base 9001",
        "python server.py --port=8000",
        "curl localhost:9001",
    ],
)
def test_rewriting_a_command_and_mapping_it_back_is_symmetric(command: str) -> None:
    rewritten = _rewrite(command, PORTS)

    assert rewritten != command
    assert "8000" not in rewritten and "9001" not in rewritten and "9002" not in rewritten
    assert _map_back(rewritten, PORTS) == command


def test_only_ports_in_a_port_position_are_rewritten() -> None:
    command = 'curl localhost:1234 -d \'{"port": 8000, "n": 9001}\' https://example.com:8000/x'

    assert _rewrite(command, PORTS) == command


def test_a_worker_banner_maps_back_to_its_documented_ports() -> None:
    banner = (
        "3 worker(s) [inventory:41001, orders:41002, payments:41003], "
        "reverse proxy on http://0.0.0.0:41000"
    )

    assert _map_back(banner, PORTS) == (
        "3 worker(s) [inventory:9001, orders:9002, payments:9003], "
        "reverse proxy on http://0.0.0.0:8000"
    )


def test_mapping_back_leaves_longer_numbers_alone() -> None:
    assert _map_back("pid 410010 at 41000 after 4100", PORTS) == "pid 410010 at 8000 after 4100"


def test_a_free_port_block_is_bindable_all_at_once() -> None:
    base = _free_port_block(PORT_BLOCK)
    held: list[socket.socket] = []
    try:
        for port in range(base, base + PORT_BLOCK):
            held.append(socket.socket())
            held[-1].bind(("127.0.0.1", port))
    finally:
        for sock in held:
            sock.close()

    assert len(held) == PORT_BLOCK


SERVER = """
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

SLOW_CHILD = (
    "import pathlib, signal, time, os; "
    "signal.signal(signal.SIGINT, lambda *_: (time.sleep(1), "
    "pathlib.Path('child.done').write_text('done'), os._exit(0))); "
    "[time.sleep(0.1) for _ in iter(int, 1)]"
)


def option(flag, default):
    return int(sys.argv[sys.argv.index(flag) + 1]) if flag in sys.argv else default


port = option("--port", 8000)
workers = option("--worker-port-base", int(os.environ.get("MODULITH_WORKER_PORT_BASE", "9001")))
if "--exit" in sys.argv:
    sys.exit(3)
if "--ignore-sigint" in sys.argv:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = f"hello from {self.server.server_port}".encode()
        self.send_response(503 if self.path == "/health" and self.server.broken else 200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def serve(number, broken=False):
    server = HTTPServer(("127.0.0.1", number), Handler)
    server.broken = broken
    threading.Thread(target=server.serve_forever, daemon=True).start()


if "--slow-child" in sys.argv:
    subprocess.Popen([sys.executable, "-c", SLOW_CHILD])
if "--topology=processes" in sys.argv or "--with-workers" in sys.argv:
    for offset in (0, 1, 2):
        serve(workers + offset, broken="--broken-worker" in sys.argv)
    print(
        f"3 worker(s) [inventory:{workers}, orders:{workers + 1}, payments:{workers + 2}], "
        f"reverse proxy on http://0.0.0.0:{port}",
        flush=True,
    )
if "--no-listen" not in sys.argv:
    serve(port)
print(f"listening on {port}", flush=True)
try:
    while True:
        time.sleep(0.1)
except KeyboardInterrupt:
    pass
"""

SERVED = {"server.py": SERVER}
SERVE = Step("python server.py", serve=True)
HELLO = "curl -s localhost:8000/hello"


def _group_gone(pgid: int, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def _all_gone(runbook: Runbook) -> bool:
    return bool(runbook.pgids) and all(_group_gone(pgid) for pgid in runbook.pgids)


def test_a_serve_is_reached_at_its_documented_port_and_its_log_matches_at_stop(
    tmp_path: Path,
) -> None:
    runbook = _run(
        tmp_path,
        *_bash("$ python server.py", "listening on 8000", f"$ {HELLO}", "hello from 8000"),
        steps=(SERVE, Step(HELLO)),
        files=SERVED,
    )

    assert re.search(r"\$ python server\.py --port \d+\n", runbook.transcript)
    assert "--port 8000" not in runbook.transcript
    assert _all_gone(runbook)


def test_a_processes_serve_gets_a_worker_port_base_and_its_banner_maps_back(
    tmp_path: Path,
) -> None:
    command = "python server.py --topology=processes"
    runbook = _run(
        tmp_path,
        *_bash(
            f"$ {command}",
            "3 worker(s) [inventory:9001, orders:9002, payments:9003], "
            "reverse proxy on http://0.0.0.0:8000",
            "listening on 8000",
            "$ curl -s localhost:9002/health",
            "hello from 9002",
        ),
        steps=(Step(command, serve=True), Step("curl -s localhost:9002/health")),
        files=SERVED,
    )

    assert "--worker-port-base" not in runbook.transcript.split("\n[log of")[0]
    assert _all_gone(runbook)


def test_a_serve_with_no_port_flag_puts_its_workers_on_the_allocated_block_through_the_environment(
    tmp_path: Path,
) -> None:
    command = "python server.py --with-workers"
    runbook = _run(
        tmp_path,
        *_bash(
            f"$ {command}",
            "3 worker(s) [inventory:9001, orders:9002, payments:9003], "
            "reverse proxy on http://0.0.0.0:8000",
            "$ curl -s localhost:9003/health",
            "hello from 9003",
        ),
        steps=(Step(command, serve=True), Step("curl -s localhost:9003/health")),
        files=SERVED,
    )

    executed = re.search(r"\$ python server\.py --with-workers --port (\d+)\n", runbook.transcript)
    assert executed is not None
    base = int(executed.group(1))
    assert f"[inventory:{base + 1}, orders:{base + 2}, payments:{base + 3}]" in runbook.transcript
    assert "--worker-port-base" not in runbook.transcript.split("\n[log of")[0]
    assert _all_gone(runbook)


def test_the_worker_port_variable_belongs_to_its_serve_step_only(tmp_path: Path) -> None:
    command = "python server.py --with-workers"
    runbook = _run(
        tmp_path,
        *_bash(f"$ {command}", "$ printenv MODULITH_WORKER_PORT_BASE"),
        steps=(
            Step(command, serve=True),
            Step("printenv MODULITH_WORKER_PORT_BASE", exit_code=1),
        ),
        files=SERVED,
    )

    assert "MODULITH_WORKER_PORT_BASE" not in runbook.env


def test_an_explicit_worker_port_base_flag_wins_over_the_variable(tmp_path: Path) -> None:
    command = "python server.py --with-workers --worker-port-base 9001"
    runbook = _run(
        tmp_path,
        *_bash(
            f"$ {command}",
            "3 worker(s) [inventory:9001, orders:9002, payments:9003], "
            "reverse proxy on http://0.0.0.0:8000",
        ),
        steps=(Step(command, serve=True),),
        files=SERVED,
    )

    executed = next(line for line in runbook.transcript.splitlines() if line.startswith("$ python"))
    assert executed.count("--worker-port-base") == 1
    assert "9001" not in executed
    assert _all_gone(runbook)


def test_a_group_member_finishing_its_graceful_shutdown_is_not_killed(tmp_path: Path) -> None:
    command = "python server.py --slow-child"

    runbook = _run(
        tmp_path,
        *_bash(f"$ {command}"),
        steps=(Step(command, serve=True),),
        files=SERVED,
        stop_wait=20.0,
    )

    assert (runbook.workdir / "child.done").read_text() == "done"
    assert _all_gone(runbook)


def test_explicit_port_flags_are_rewritten_not_appended(tmp_path: Path) -> None:
    command = "python server.py --topology=processes --port 8000 --worker-port-base 9001"
    runbook = _run(
        tmp_path,
        *_bash(
            f"$ {command}",
            "3 worker(s) [inventory:9001, orders:9002, payments:9003], "
            "reverse proxy on http://0.0.0.0:8000",
        ),
        steps=(Step(command, serve=True),),
        files=SERVED,
    )

    executed = next(line for line in runbook.transcript.splitlines() if line.startswith("$ python"))
    assert executed.count("--port") == 1
    assert executed.count("--worker-port-base") == 1
    assert "8000" not in executed and "9001" not in executed


def test_a_serve_whose_log_lacks_an_expected_line_fails_when_it_stops(tmp_path: Path) -> None:
    runbook, message = _broken(
        tmp_path,
        *_bash("$ python server.py", "never printed"),
        steps=(SERVE,),
        files=SERVED,
    )

    assert "'never printed'" in _reason(message)
    assert "listening on" in message
    assert _all_gone(runbook)


@pytest.mark.parametrize(
    ("flags", "reason"),
    [
        ("--no-listen", "no HTTP answer"),
        ("--topology=processes --broken-worker", "/health"),
        ("--exit", "exited with code 3"),
    ],
    ids=["app-port-silent", "worker-unhealthy", "exits-early"],
)
def test_a_serve_that_never_becomes_ready_fails_and_is_stopped(
    tmp_path: Path, flags: str, reason: str
) -> None:
    command = f"python server.py {flags}"

    runbook, message = _broken(
        tmp_path,
        *_bash(f"$ {command}"),
        steps=(Step(command, serve=True),),
        files=SERVED,
        ready_timeout=1.0,
    )

    assert reason in _reason(message)
    assert _all_gone(runbook)


def test_a_serve_that_stops_on_sigint_is_stopped_without_waiting_out_the_timeout(
    tmp_path: Path,
) -> None:
    started = time.monotonic()

    runbook = _run(
        tmp_path, *_bash("$ python server.py"), steps=(SERVE,), files=SERVED, stop_wait=20.0
    )

    assert time.monotonic() - started < 10.0
    assert _all_gone(runbook)


def test_a_serve_that_ignores_sigint_is_killed_after_the_wait(tmp_path: Path) -> None:
    command = "python server.py --ignore-sigint"
    started = time.monotonic()

    runbook = _run(
        tmp_path,
        *_bash(f"$ {command}"),
        steps=(Step(command, serve=True),),
        files=SERVED,
        stop_wait=1.0,
    )

    assert time.monotonic() - started >= 1.0
    assert _all_gone(runbook)


def test_a_step_that_fails_mid_section_still_stops_the_serve_and_keeps_its_own_reason(
    tmp_path: Path,
) -> None:
    runbook, message = _broken(
        tmp_path,
        *_bash("$ python server.py", "never printed", "$ false"),
        steps=(SERVE, Step("false")),
        files=SERVED,
    )

    assert "exit 1" in _reason(message)
    assert "listening on" in message
    assert _all_gone(runbook)


def test_serves_on_different_documented_ports_run_together_and_a_repeat_stops_only_its_holder(
    tmp_path: Path,
) -> None:
    other = "python server.py --port 8100"
    runbook = _run(
        tmp_path,
        *_bash(
            "$ python server.py",
            f"$ {other}",
            f"$ {HELLO}",
            "hello from 8000",
            "$ curl -s localhost:8100/hello",
            "hello from 8100",
            f"$ {other}",
            f"$ {HELLO}",
            "hello from 8000",
        ),
        steps=(
            SERVE,
            Step(other, serve=True),
            Step(HELLO),
            Step("curl -s localhost:8100/hello"),
            Step(other, serve=True),
            Step(HELLO),
        ),
        files=SERVED,
    )

    first, second, third = re.findall(r"\$ python server\.py --port (\d+)\n", runbook.transcript)
    text = runbook.transcript
    assert text.index(f"listening on {second}\n") < text.index(
        f"$ python server.py --port {third}\n"
    )
    assert text.index(f"listening on {first}\n") > text.index(
        f"$ python server.py --port {third}\n"
    )
    assert _all_gone(runbook)


def test_a_background_child_of_a_finished_step_is_killed(tmp_path: Path) -> None:
    runbook = _run(tmp_path, *_bash("sleep 60 & echo child=$!"))

    child = int(re.search(r"child=(\d+)", runbook.transcript.split("\n$ ", 1)[-1]).group(1))  # type: ignore[union-attr]
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    pytest.fail(f"child {child} outlived its step")


COUNTED = "n=$(cat n 2>/dev/null || echo 0); n=$((n+1)); echo $n > n; "


def _attempts(runbook: Runbook) -> int:
    return int((runbook.workdir / "n").read_text())


def test_an_eventually_step_reruns_until_its_output_matches(tmp_path: Path) -> None:
    command = COUNTED + "if [ $n -ge 3 ]; then echo ready; else echo waiting; fi"

    runbook = _run(
        tmp_path,
        *_bash(f"$ {command}", "ready"),
        steps=(Step(command, eventually=True),),
        eventually_interval=0.05,
    )

    assert _attempts(runbook) == 3


def test_a_step_that_is_not_eventually_runs_once(tmp_path: Path) -> None:
    command = COUNTED + "echo waiting"
    runbook, _message = _broken(tmp_path, *_bash(f"$ {command}", "ready"))

    assert _attempts(runbook) == 1


def test_an_eventually_step_that_never_matches_fails_showing_only_the_last_run(
    tmp_path: Path,
) -> None:
    command = COUNTED + 'echo "run $n"'

    runbook, message = _broken(
        tmp_path,
        *_bash(f"$ {command}", "ready"),
        steps=(Step(command, eventually=True),),
        eventually_timeout=1.0,
        eventually_interval=0.1,
    )

    attempts = _attempts(runbook)
    assert attempts > 2
    assert "retries" in _reason(message)
    assert f"run {attempts}\n" in message
    assert "run 1\n" not in message


def _fake_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.log"
    script = bin_dir / "docker"
    script.write_text(
        '#!/bin/sh\necho "$* foo=$FOO project=$COMPOSE_PROJECT_NAME" >> "$DOCKER_LOG"\n'
    )
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("DOCKER_LOG", str(log))
    return log


@pytest.mark.parametrize("tail", [(), ("false",)], ids=["passes", "fails"])
def test_a_docker_section_ends_with_compose_down_in_the_section_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tail: tuple[str, ...]
) -> None:
    log = _fake_docker(tmp_path, monkeypatch)
    commands = ["export FOO=bar", "docker compose up -d", *tail]
    section = Section("db", tuple(Step(command) for command in commands))

    with contextlib.suppress(RunbookFailure):
        runbook = _runbook(tmp_path / "run", *_bash(*commands), sections=(section,))
        runbook.run_section(0)
    lines = log.read_text().splitlines()

    assert lines[0].startswith("compose up -d")
    assert lines[-1] == (
        f"compose down -v --remove-orphans foo=bar project={runbook.env['COMPOSE_PROJECT_NAME']}"
    )


def test_a_section_without_a_docker_step_never_runs_compose_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_docker(tmp_path, monkeypatch)

    _run(tmp_path / "run", *_bash("echo hi"))

    assert not log.exists()


def test_the_repo_environment_stops_before_a_docker_section(tmp_path: Path) -> None:
    sections = (
        Section("one", (Step("echo x > one"),)),
        Section("two", (Step("docker compose up"),)),
        Section("three", (Step("echo x > three"),)),
    )
    runbook = _runbook(
        tmp_path,
        *_bash("echo x > one", "docker compose up", "echo x > three"),
        sections=sections,
    )

    runbook.run_repo_environment()

    assert (runbook.workdir / "one").exists()
    assert not (runbook.workdir / "three").exists()


def test_the_repo_environment_stops_before_a_section_that_needs_the_wheel(tmp_path: Path) -> None:
    sections = (
        Section("one", (Step("echo x > one"),)),
        Section("wheel", (Step("echo x > wheel"),), repo_env=False),
    )
    runbook = _runbook(tmp_path, *_bash("echo x > one", "echo x > wheel"), sections=sections)

    runbook.run_repo_environment()

    assert (runbook.workdir / "one").exists()
    assert not (runbook.workdir / "wheel").exists()


@pytest.mark.real_process
@pytest.mark.timeout(1800)
@pytest.mark.parametrize(
    "example",
    sorted(
        name
        for name, sections in RUNBOOKS.items()
        if any(section.steps for section in _repo_sections(sections))
    ),
)
def test_readme_runbook_passes_in_the_repo_environment(example: str, tmp_path: Path) -> None:
    runbook = Runbook(EXAMPLES / example, RUNBOOKS[example], tmp_path / example)

    runbook.run_repo_environment()

    serves = sum(
        step.serve for section in _repo_sections(RUNBOOKS[example]) for step in section.steps
    )
    assert len(runbook.pgids) == serves
    assert all(_group_gone(pgid) for pgid in runbook.pgids)


def _fake_venv(tmp_path: Path) -> tuple[Path, Path]:
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    log = tmp_path / "pip.log"
    pip = venv / "bin" / "pip"
    pip.write_text(f'#!/bin/sh\necho "$* in $VIRTUAL_ENV" >> "{log}"\n')
    pip.chmod(0o755)
    return venv, log


def test_pip_install_runs_literally_in_the_wheel_environment(tmp_path: Path) -> None:
    venv, log = _fake_venv(tmp_path)
    readme = _bash("pip install 'some-package[extra]'")

    repo = _run(tmp_path / "repo", *readme)
    wheel = _runbook(tmp_path / "wheel", *readme, venv=venv)
    wheel.run_built_wheel()

    assert "skipped" in repo.transcript
    assert "skipped" not in wheel.transcript
    assert log.read_text() == f"install some-package[extra] in {venv}\n"


def test_a_wheel_step_resolves_its_tools_inside_the_venv(tmp_path: Path) -> None:
    venv, _log = _fake_venv(tmp_path)

    _runbook(
        tmp_path / "wheel",
        *_bash("$ command -v pip", f"{venv}/bin/pip", "$ printenv VIRTUAL_ENV", str(venv)),
        venv=venv,
    ).run_built_wheel()


def test_the_wheel_environment_leads_the_path_with_the_venv_and_not_the_running_interpreter(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"

    env = child_env(
        {"PATH": "/opt/base/bin", "VIRTUAL_ENV": "/elsewhere"}, tmp_path / "work", venv=venv
    )

    assert env["PATH"] == f"{venv}/bin{os.pathsep}/opt/base/bin"
    assert env["VIRTUAL_ENV"] == str(venv)


def _interpreter_bin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, with_bash: bool) -> Path:
    """A bin directory that holds the running interpreter and ``fake-tool``, and leads ``PATH``."""
    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    tool = bin_dir / "fake-tool"
    tool.write_text("#!/bin/sh\necho fake-tool ran\n")
    tool.chmod(0o755)
    if with_bash:
        (bin_dir / "bash").symlink_to(shutil.which("bash") or "/bin/bash")
    linked = tmp_path / "linked"
    linked.symlink_to(bin_dir)
    monkeypatch.setattr(sys, "executable", str(bin_dir / "python"))
    monkeypatch.setenv("PATH", os.pathsep.join([str(bin_dir), str(linked), os.environ["PATH"]]))
    return bin_dir


def test_a_tool_only_the_running_interpreter_provides_is_missing_in_the_wheel_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _interpreter_bin(tmp_path, monkeypatch, with_bash=False)
    venv, _log = _fake_venv(tmp_path)
    readme = _bash("$ fake-tool", "fake-tool ran")

    _run(tmp_path / "repo", *readme)
    message = _failure(
        tmp_path / "wheel",
        *_bash("$ command -v fake-tool"),
        steps=(Step("command -v fake-tool"),),
        venv=venv,
    )

    assert "exit 1" in _reason(message)


def test_a_bin_directory_that_also_holds_bash_stays_on_the_wheel_lane_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _interpreter_bin(tmp_path, monkeypatch, with_bash=True)
    venv, _log = _fake_venv(tmp_path)

    _run(tmp_path / "wheel", *_bash("$ fake-tool", "fake-tool ran"), venv=venv)


def test_the_wheel_lane_runs_every_section_where_the_repo_lane_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    docker_log = _fake_docker(tmp_path, monkeypatch)
    venv, _log = _fake_venv(tmp_path)
    sections = (
        Section("one", (Step("touch one"),)),
        Section("two", (Step("docker compose up"),)),
        Section("three", (Step("touch three"),), repo_env=False),
    )
    readme = _bash("touch one", "docker compose up", "touch three")

    repo = _runbook(tmp_path / "repo", *readme, sections=sections)
    repo.run_repo_environment()
    assert (repo.workdir / "one").exists()
    assert not (repo.workdir / "three").exists()
    assert not docker_log.exists()

    wheel = _runbook(tmp_path / "wheel", *readme, sections=sections, venv=venv)
    wheel.run_built_wheel()

    assert (wheel.workdir / "one").exists()
    assert (wheel.workdir / "three").exists()
    assert [line.split()[:2] for line in docker_log.read_text().splitlines()] == [
        ["compose", "up"],
        ["compose", "down"],
    ]


def _record(venv: Path, url: str | None) -> None:
    info = venv / "lib" / "python3.11" / "site-packages" / "modupy-0.10.0.dist-info"
    info.mkdir(parents=True)
    if url is not None:
        (info / "direct_url.json").write_text(json.dumps({"url": url, "archive_info": {}}))


def test_provenance_accepts_the_session_built_wheel(tmp_path: Path) -> None:
    wheel = tmp_path / "dist" / "modupy-0.10.0-py3-none-any.whl"
    _record(tmp_path / "venv", wheel.as_uri())

    assert wheel_provenance_problem(tmp_path / "venv", wheel) == ""


@pytest.mark.parametrize(
    "url",
    [
        "https://files.pythonhosted.org/packages/ab/modupy-0.10.0-py3-none-any.whl",
        "file:///elsewhere/dist/modupy-0.10.0-py3-none-any.whl",
        "file:///home/somebody/modupy",
    ],
    ids=["pypi", "another-wheel", "source-tree"],
)
def test_provenance_names_the_url_of_anything_but_the_session_built_wheel(
    tmp_path: Path, url: str
) -> None:
    wheel = tmp_path / "dist" / "modupy-0.10.0-py3-none-any.whl"
    _record(tmp_path / "venv", url)

    problem = wheel_provenance_problem(tmp_path / "venv", wheel)

    assert url in problem
    assert str(wheel) in problem


@pytest.mark.parametrize("dist_info", [False, True], ids=["no-dist-info", "no-direct-url-file"])
def test_provenance_names_the_missing_file(tmp_path: Path, dist_info: bool) -> None:
    venv = tmp_path / "venv"
    (venv / "lib" / "python3.11" / "site-packages").mkdir(parents=True)
    if dist_info:
        _record(venv, None)

    problem = wheel_provenance_problem(venv, tmp_path / "modupy-0.10.0-py3-none-any.whl")

    assert "direct_url.json" in problem
    assert str(venv) in problem


def _stub_wheel(directory: Path) -> Path:
    directory.mkdir(parents=True)
    wheel = directory / "modupy-0.0.1-py3-none-any.whl"
    info = "modupy-0.0.1.dist-info"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            f"{info}/METADATA", "Metadata-Version: 2.1\nName: modupy\nVersion: 0.0.1\n"
        )
        archive.writestr(
            f"{info}/WHEEL",
            "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr(f"{info}/RECORD", f"{info}/METADATA,,\n{info}/WHEEL,,\n{info}/RECORD,,\n")
    return wheel


def test_a_wheel_venv_holds_the_wheel_and_pips_record_of_it_passes_the_provenance_check(
    tmp_path: Path,
) -> None:
    wheel = _stub_wheel(tmp_path / "dist")
    other = _stub_wheel(tmp_path / "other")
    venv = tmp_path / "venv"

    create_wheel_venv(venv, wheel)

    assert wheel_provenance_problem(venv, wheel) == ""
    assert wheel.as_uri() in wheel_provenance_problem(venv, other)


def test_a_wheel_venv_that_cannot_install_its_wheel_fails_naming_it(tmp_path: Path) -> None:
    missing = tmp_path / "dist" / "modupy-0.0.1-py3-none-any.whl"

    with pytest.raises(AssertionError, match=re.escape(missing.name)):
        create_wheel_venv(tmp_path / "venv", missing)


@pytest.fixture(scope="session")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    dist = tmp_path_factory.mktemp("dist")
    build = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(dist),
            str(REPO),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    (wheel,) = dist.glob("*.whl")
    return wheel


@pytest.mark.integration
@pytest.mark.timeout(1800)
@pytest.mark.parametrize("example", sorted(RUNBOOKS))
def test_readme_runbook_passes_from_the_built_wheel(
    example: str, tmp_path: Path, built_wheel: Path
) -> None:
    sections = RUNBOOKS[example]
    if any(_has_docker_step(section.steps) for section in sections) and not _docker_available():
        pytest.skip(f"the {example} runbook has a docker step and no Docker daemon is reachable")
    venv = tmp_path / "venv"
    create_wheel_venv(venv, built_wheel)
    runbook = Runbook(EXAMPLES / example, sections, tmp_path / example, venv=venv)

    runbook.run_built_wheel()

    problem = wheel_provenance_problem(venv, built_wheel)
    assert not problem, problem
    serves = sum(step.serve for section in sections for step in section.steps)
    assert len(runbook.pgids) == serves
    assert all(_group_gone(pgid) for pgid in runbook.pgids)
