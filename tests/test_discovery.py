"""Tests for application package auto-detection.

The call-stack detection strategy can't be unit-tested cleanly — pytest's
own frames are in the stack, and detection picks them up before reaching
any fallback. Real applications don't have this problem because their
own code is in the stack. To test the fallback path, we mock the stack
walker out and exercise pyproject.toml resolution in isolation.
"""

from __future__ import annotations

import sys
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


# ----- _is_stdlib_frame (installed-app detectability) -----------------------


def test_is_stdlib_frame_excludes_site_and_dist_packages() -> None:
    """An installed app under site-/dist-packages must NOT be treated as stdlib.

    Regression: classifying site-packages as third-party made every
    ``pip install``ed application undetectable via the stack walk.
    """
    from modulith.discovery import _is_stdlib_frame

    assert _is_stdlib_frame(Path("/usr/lib/python3.11/site-packages/myapp/orders.py")) is False
    assert _is_stdlib_frame(Path("/usr/lib/python3/dist-packages/myapp/orders.py")) is False


def test_is_stdlib_frame_flags_real_stdlib_path() -> None:
    """A file under <prefix>/lib (but not site-packages) is stdlib."""
    from modulith.discovery import _is_stdlib_frame

    stdlib_file = Path(sys.prefix) / "lib" / "python3.11" / "json" / "__init__.py"
    assert _is_stdlib_frame(stdlib_file) is True


def test_is_stdlib_frame_excludes_project_paths() -> None:
    """A plain project file is neither stdlib nor skipped."""
    from modulith.discovery import _is_stdlib_frame

    assert _is_stdlib_frame(Path("/home/dev/myapp/orders/service.py")) is False


# ----- builtin discovery resilience (broken module / manifest) --------------


def test_builtin_discovery_survives_a_broken_module(make_fake_app) -> None:
    """One module failing to import must not crash discovery of the others.

    The broken module also ships a ``_manifest.py``: probing for it via
    ``find_spec`` re-imports the (broken) parent, so without the
    import-ok guard the whole hook would raise and discover *nothing*.
    """
    import sys as _sys

    from modulith.builtin.discovery import modulith_discover_modules

    pkg = make_fake_app(
        {
            "good": "value = 1\n",
            "broken": "raise RuntimeError('boom at import time')\n",
        },
        extra_files={"broken/_manifest.py": "x = 1\n"},
    )

    modules = modulith_discover_modules(pkg)

    names = {m.name for m in modules}
    # Both modules are *recorded* (discovery is structural); the broken one
    # simply fails to import — it doesn't abort the walk.
    assert names == {"good", "broken"}
    # The good module imported successfully despite its broken sibling.
    assert f"{pkg}.good" in _sys.modules


# ----- builtin discovery skip/guard branches (A4-r1-12) ----------------------


def test_builtin_discovery_skips_underscore_and_single_file_submodules(make_fake_app) -> None:
    """A4-r1-12: underscore-prefixed subpackages are private and skipped, and
    a top-level single ``.py`` file is not a module (modules are packages) —
    neither may appear in the returned ModuleInfo list."""
    from modulith.builtin.discovery import modulith_discover_modules

    pkg = make_fake_app(
        {"good": "value = 1\n", "_private": "value = 2\n"},
        extra_files={"single_file_module.py": "value = 3\n"},
    )

    modules = modulith_discover_modules(pkg)

    assert {m.name for m in modules} == {"good"}


def test_builtin_discovery_fails_loud_for_unimportable_app_package() -> None:
    """W3 R3-F1 (supersedes A4-r1-12's return-[] contract): a missing or
    unimportable app package raises ConfigurationError so verify/doctor
    cannot go CI-green on an app that doesn't import."""
    from modulith import ConfigurationError
    from modulith.builtin.discovery import modulith_discover_modules

    with pytest.raises(ConfigurationError, match="definitely_not_installed_xyz_123"):
        modulith_discover_modules(app_package="definitely_not_installed_xyz_123")


def test_builtin_discovery_returns_empty_for_single_file_app_package(
    make_fake_app, tmp_path: Path
) -> None:
    """A4-r1-12: an app package that is itself a plain module (no ``__path__``)
    has no subpackages to discover — returns [] without raising."""
    import sys as _sys

    from modulith.builtin.discovery import modulith_discover_modules

    make_fake_app({})  # prepends tmp_path to sys.path
    (tmp_path / "solo_file_app.py").write_text("value = 1\n")
    try:
        assert modulith_discover_modules(app_package="solo_file_app") == []
    finally:
        _sys.modules.pop("solo_file_app", None)
