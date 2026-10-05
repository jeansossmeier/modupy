"""Guard test: docs/API_REFERENCE.md must be regenerated when the public API changes.

The reference is generated from ``modulith.__all__`` docstrings + signatures by
``scripts/gen_api_reference.py``. This test runs that script's ``--check`` mode,
which re-renders in memory and diffs against the committed file — so a public
docstring or signature change that wasn't regenerated fails here instead of
shipping a stale reference. When it fails, run:

    python scripts/gen_api_reference.py
"""

from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path

import pluggy

import modulith

REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATOR = REPO_ROOT / "scripts" / "gen_api_reference.py"


def test_api_reference_is_in_sync():
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "docs/API_REFERENCE.md is out of date with the public API. "
        "Regenerate it with `python scripts/gen_api_reference.py`.\n\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_hookimpl_has_a_docstring_of_its_own():
    """The reference renders ``hookimpl`` through ``inspect.getdoc``, which falls
    back to the class docstring for an instance. pluggy's says to instantiate the
    marker with a ``project_name``, but ``modulith.hookimpl`` already is one.
    """
    doc = inspect.getdoc(modulith.hookimpl)
    assert doc, "hookimpl would render with no docstring in the API reference"
    assert doc != inspect.getdoc(pluggy.HookimplMarker), (
        "hookimpl renders pluggy's class docstring in the API reference"
    )
