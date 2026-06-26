"""Tests for application package auto-detection.

The call-stack detection strategy can't be unit-tested cleanly — pytest's
own frames are in the stack, and detection picks them up before reaching
any fallback. Real applications don't have this problem because their
own code is in the stack. To test the fallback path, we mock the stack
walker out and exercise pyproject.toml resolution in isolation.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from modulith import ConfigurationError
from modulith.discovery import detect_application_package


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """Each test gets a fresh cwd with no inherited pyproject.toml."""
    monkeypatch.chdir(tmp_path)
    yield


@pytest.fixture
def _no_caller_detection():
    """Force the call-stack strategy to return None for the duration.

    Lets us test the pyproject.toml fallback in isolation. Without this,
    pytest frames are picked up first and the fallback never runs.
    """
    with patch(
        "modulith.discovery._detect_from_caller_stack",
        return_value=None,
    ):
        yield


# ----- pyproject.toml fallback ----------------------------------------------


def test_detects_from_pyproject_project_name(tmp_path: Path, _no_caller_detection) -> None:
    """Falls back to [project].name in pyproject.toml."""
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "my-cool-app"\n')
    pkg = detect_application_package()
    # Hyphens normalize to underscores per PEP 503.
    assert pkg == "my_cool_app"


def test_normalizes_hyphens_to_underscores(tmp_path: Path, _no_caller_detection) -> None:
    """PyPI names use hyphens, Python packages use underscores."""
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "some-multi-word-name"\n')
    assert detect_application_package() == "some_multi_word_name"


def test_walks_up_to_find_pyproject(tmp_path: Path, monkeypatch, _no_caller_detection) -> None:
    """Detection looks in cwd and all parents for pyproject.toml."""
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "rootapp"\n')
    nested = tmp_path / "deep" / "nested" / "dir"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert detect_application_package() == "rootapp"


# ----- Failure case ---------------------------------------------------------


def test_raises_actionable_error_when_detection_fails(tmp_path: Path, _no_caller_detection) -> None:
    """The error message tells users exactly how to fix the problem."""
    with pytest.raises(ConfigurationError) as exc_info:
        detect_application_package()

    # The error message must name all three escape hatches.
    msg = str(exc_info.value)
    assert "configure(package=" in msg
    assert "pyproject.toml" in msg
    assert "MODULITH_PACKAGE" in msg


# ----- Edge cases -----------------------------------------------------------


def test_pyproject_without_project_section_falls_through(
    tmp_path: Path, _no_caller_detection
) -> None:
    """A pyproject without [project].name doesn't help — error raises."""
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\noutbox = "postgres"\n')
    with pytest.raises(ConfigurationError):
        detect_application_package()


# ----- Call-stack strategy (smoke test only) --------------------------------


def test_call_stack_strategy_returns_a_package_when_called_from_a_module() -> None:
    """The stack walker returns *something* when called from real code.

    We can't assert the exact value (pytest internals vary across
    versions), but it should return a non-empty string and never raise.
    """
    pkg = detect_application_package()
    assert isinstance(pkg, str)
    assert pkg  # non-empty
