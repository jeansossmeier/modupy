"""How ``modulith dev``/``run`` resolve the uvicorn they exec.

Separate from tests/test_cli.py because these tests are about *which* uvicorn
``os.execvp`` would find, not about the CLI surface: they build a throwaway
"virtualenv" on disk and check the PATH lookup the real exec would perform.
"""

from __future__ import annotations

import os
import shutil
import sys

import modulith.cli as cli

# shutil.which needs the platform's executable spelling: PATHEXT on Windows,
# the executable bit on POSIX.
_EXE = ".exe" if os.name == "nt" else ""


def _make_bin(directory, *names: str):
    """Create ``directory`` with an executable stub per name; return the dir."""
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        script = directory / f"{name}{_EXE}"
        script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        script.chmod(0o755)
    return directory


def _capture_resolution(monkeypatch) -> dict[str, object]:
    """Patch os.execvp to record its arguments and what PATH resolves them to."""
    captured: dict[str, object] = {}

    def fake_execvp(file: str, args: list[str]) -> None:
        captured["file"] = file
        captured["args"] = args
        captured["resolved"] = shutil.which(file)

    monkeypatch.setattr(os, "execvp", fake_execvp)
    return captured


def test_exec_uvicorn_prefers_the_uvicorn_beside_the_interpreter(tmp_path, monkeypatch) -> None:
    """``os.execvp("uvicorn", ...)`` searches PATH, and PATH is not the
    environment modulith was imported from when the CLI is invoked by absolute
    path into a non-activated virtualenv (systemd ExecStart, a container CMD, a
    Makefile) — the shapes docs/DEPLOYMENT.md documents. It used to exec a
    foreign uvicorn bound to a different interpreter, which cannot import the
    app. The uvicorn installed alongside ``sys.executable`` must win."""
    venv_bin = _make_bin(tmp_path / "venv" / "bin", "python", "uvicorn")
    foreign_bin = _make_bin(tmp_path / "foreign" / "bin", "uvicorn")

    monkeypatch.setattr(sys, "executable", str(venv_bin / f"python{_EXE}"))
    monkeypatch.setenv("PATH", str(foreign_bin))
    captured = _capture_resolution(monkeypatch)

    cli._exec_uvicorn(["uvicorn", "myapp:app"])

    assert captured["resolved"] == str(venv_bin / f"uvicorn{_EXE}")
    # The exec call shape is unchanged — still a PATH lookup of the bare name.
    assert captured["file"] == "uvicorn"
    assert captured["args"] == ["uvicorn", "myapp:app"]


def test_exec_uvicorn_falls_back_to_path_without_a_sibling(tmp_path, monkeypatch) -> None:
    """An interpreter with no co-installed uvicorn (a system python, a
    deliberately shadowed one) must keep resolving through PATH untouched."""
    bare_bin = _make_bin(tmp_path / "bare" / "bin", "python")
    other_bin = _make_bin(tmp_path / "other" / "bin", "uvicorn")

    monkeypatch.setattr(sys, "executable", str(bare_bin / f"python{_EXE}"))
    monkeypatch.setenv("PATH", str(other_bin))
    captured = _capture_resolution(monkeypatch)

    cli._exec_uvicorn(["uvicorn", "myapp:app"])

    assert captured["resolved"] == str(other_bin / f"uvicorn{_EXE}")
    assert os.environ["PATH"] == str(other_bin)
