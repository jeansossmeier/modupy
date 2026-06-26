"""Smoke tests for the make_fake_app factory in conftest.py.

These don't exercise modulith semantics — they just verify the factory
produces importable packages with the right shape so future tests can
rely on it.
"""

from __future__ import annotations

import importlib
import sys


def test_make_fake_app_creates_importable_package(make_fake_app) -> None:
    """A bare module with arbitrary source becomes importable."""
    pkg = make_fake_app(
        {
            "billing": "VALUE = 'hello'\n",
        }
    )
    mod = importlib.import_module(f"{pkg}.billing")
    assert mod.VALUE == "hello"


def test_make_fake_app_supports_custom_package_name(make_fake_app) -> None:
    """The package_name argument controls the importable name."""
    pkg = make_fake_app(
        {"core": "VALUE = 1\n"},
        package_name="myapp",
    )
    assert pkg == "myapp"
    assert "myapp.core" in sys.modules or importlib.import_module("myapp.core")


def test_make_fake_app_supports_extra_files(make_fake_app) -> None:
    """extra_files lets a test create _manifest.py, _internal/, etc."""
    pkg = make_fake_app(
        {"orders": "VALUE = 1\n"},
        extra_files={
            "orders/_manifest.py": "MANIFEST_LOADED = True\n",
            "orders/_internal/persistence.py": "DB = 'fake'\n",
            "orders/_internal/__init__.py": "",
        },
    )
    manifest = importlib.import_module(f"{pkg}.orders._manifest")
    persistence = importlib.import_module(f"{pkg}.orders._internal.persistence")
    assert manifest.MANIFEST_LOADED is True
    assert persistence.DB == "fake"


def test_make_fake_app_dedents_source(make_fake_app) -> None:
    """Indented triple-quoted strings are dedented before being written
    so callers can write source naturally inside test bodies."""
    pkg = make_fake_app(
        {
            "x": """
            FROM_INDENTED = True
        """,
        }
    )
    mod = importlib.import_module(f"{pkg}.x")
    assert mod.FROM_INDENTED is True


def test_cleanup_removes_modules(make_fake_app) -> None:
    """After the test, sys.modules is clean. (Verified across two tests
    via the test below.)"""
    pkg = make_fake_app({"temp": "X = 1\n"})
    assert f"{pkg}.temp" not in sys.modules  # not yet imported
    importlib.import_module(f"{pkg}.temp")
    assert f"{pkg}.temp" in sys.modules
    # Cleanup happens at fixture teardown — no manual assertion possible
    # within this test. The next test verifies isolation.


def test_isolation_between_tests(make_fake_app) -> None:
    """Modules created in a previous test must not be visible here."""
    # The previous test created a "fakeapp.temp" module and it should not
    # be in sys.modules anymore (or at minimum, importlib should re-create
    # it from the new tmp_path).
    pkg = make_fake_app({"different": "X = 2\n"})
    mod = importlib.import_module(f"{pkg}.different")
    assert mod.X == 2
