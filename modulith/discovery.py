"""Auto-detection of the application's root package.

Strategies, in priority order:
  1. Walk the call stack for the first non-modulith, non-stdlib frame; take
     its top-level package name (skipping stdlib and __main__).
  2. Read [project].name from pyproject.toml in cwd or parents.

Auto-detection is best-effort and dev-oriented. It deliberately does NOT
treat ``site-packages`` as third-party: an installed application (``pip
install``, including many ``-e`` layouts) lives there, and skipping it would
misclassify the app's own frames. The trade-off is that a third-party library
that calls modulith on the user's behalf as the *immediate* caller could be
mis-detected — embedded/installed deployments should set the package
explicitly (``configure(package=...)``, ``[tool.modulith].package``, or
``MODULITH_PACKAGE``) rather than rely on the stack walk.

Detection runs once during bootstrap. If both strategies fail, raise a
ConfigurationError with explicit instructions on how to set the package
manually — the error itself documents the API.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from types import FrameType

from .config import ConfigurationError

# Resolved at module load so detection doesn't pay the cost on every call.
# Used to identify and skip frames inside the modulith package itself.
_MODULITH_ROOT = Path(__file__).parent.resolve()


def detect_application_package() -> str:
    """Detect the application's root package.

    Tries the call stack first (works for normal imports), then falls
    back to pyproject.toml. Raises ConfigurationError with actionable
    guidance if both fail.
    """
    pkg = _detect_from_caller_stack()
    if pkg:
        return pkg

    pkg = _detect_from_pyproject_name()
    if pkg:
        return pkg

    # Both strategies failed. The error message IS the API documentation:
    # users should be able to fix this without leaving the terminal.
    raise ConfigurationError(
        "Could not auto-detect the application package. Set it explicitly "
        "via one of:\n"
        "  - configure(package='myapp') before any @listener or publish() call\n"
        "  - [tool.modulith].package = 'myapp' in pyproject.toml\n"
        "  - MODULITH_PACKAGE environment variable"
    )


def _detect_from_caller_stack() -> str | None:
    """Walk the call stack for the first frame outside modulith and stdlib.

    Uses sys._getframe() rather than inspect.stack() — we walk one frame
    at a time and stop on the first match, avoiding the cost of building
    a full FrameInfo list.
    """
    # frame.f_back narrows back to Optional, so annotate from the start.
    frame: FrameType | None = sys._getframe()
    while frame is not None:
        # Determine the file this frame is executing in.
        try:
            frame_file = Path(frame.f_code.co_filename).resolve()
        except (OSError, ValueError):
            # Some frames (e.g. exec'd code) have no real path. Skip them.
            frame = frame.f_back
            continue

        # Skip frames inside the modulith package — we want the caller.
        if _is_inside(frame_file, _MODULITH_ROOT):
            frame = frame.f_back
            continue

        # Read the module name from the frame's globals. This is what
        # Python sets as __name__ when the module is imported normally.
        # Cast to str since f_globals is dict[str, Any].
        module_name = str(frame.f_globals.get("__name__", ""))
        top = module_name.split(".")[0] if module_name else ""

        # Skip stdlib frames — by module name (robust across install layouts)
        # and by path. NOT site-packages: an installed app lives there and must
        # remain detectable (see module docstring for the trade-off).
        if not top or top in sys.stdlib_module_names or _is_stdlib_frame(frame_file):
            frame = frame.f_back
            continue

        # __main__ frames don't tell us anything useful — fall through.
        if module_name != "__main__":
            return top

        frame = frame.f_back

    return None


def _detect_from_pyproject_name() -> str | None:
    """Read [project].name from pyproject.toml. Returns None if absent."""
    current = Path.cwd()
    for parent in (current, *current.parents):
        candidate = parent / "pyproject.toml"
        if not candidate.is_file():
            continue
        try:
            with candidate.open("rb") as f:
                data = tomllib.load(f)
        except (OSError, tomllib.TOMLDecodeError):
            # Unreadable/malformed pyproject — keep walking up the parent chain
            # rather than abandoning detection at the first bad file.
            continue

        name = data.get("project", {}).get("name")
        if name:
            # Distribution names may contain hyphens, but import package
            # names must be valid Python identifiers — the conventional
            # packaging mapping replaces '-' with '_'. (NOT PEP 503, which
            # governs package-index name normalization — hyphens/dots/
            # underscores collapse to '-', the opposite direction.)
            return str(name).replace("-", "_")
        # A pyproject without [project].name isn't a package declaration
        # (e.g. a tooling-only or monorepo-root file); try the next parent.
        continue
    return None


def _is_inside(path: Path, root: Path) -> bool:
    """True if `path` is `root` or a descendant of it."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _is_stdlib_frame(path: Path) -> bool:
    """True if ``path`` is a stdlib file — NOT site-packages/dist-packages.

    Complements the name-based ``sys.stdlib_module_names`` check for stdlib
    modules whose frame name is unusual (frozen bootstrap, runpy). Crucially it
    excludes site-packages/dist-packages so an *installed application* there is
    still detected as the app, not skipped as third-party.
    """
    parts = path.parts
    if "site-packages" in parts or "dist-packages" in parts:
        return False
    for prefix in {sys.prefix, sys.base_prefix}:
        if path.is_relative_to(Path(prefix) / "lib"):
            return True
    return False
