"""Private, per-user filesystem paths for local broker state."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import stat
import sys
from os import PathLike
from pathlib import Path

from ..config import ConfigurationError

_SAFE_NAMESPACE = re.compile(r"[^A-Za-z0-9._-]+")


def _absolute_unresolved(path: str | PathLike[str]) -> Path:
    return Path(os.path.abspath(os.path.expanduser(os.fspath(path))))


def _namespace(package: str | None) -> str:
    source = package or Path.cwd().name or "default"
    readable = _SAFE_NAMESPACE.sub("_", source).strip("._") or "default"
    origin = _canonical_package_origin(package)
    digest = hashlib.sha256(os.fsencode(origin)).hexdigest()[:12]
    return f"{readable}-{digest}"


def _canonical_package_origin(package: str | None) -> Path:
    """Return the resolved package location without importing application code.

    Symlinks are resolved, so a ``current -> releases/<ts>`` switch or a new
    venv yields a different location and therefore a different default store.
    """
    if package:
        root_package = package.partition(".")[0]
        try:
            # Looking up a dotted name imports its parent package. The top-level
            # package has the same project origin and can be located without
            # executing application code during broker bootstrap.
            spec = importlib.util.find_spec(root_package)
        except (AttributeError, ImportError, ModuleNotFoundError, ValueError):
            spec = None
        if spec is not None:
            locations = spec.submodule_search_locations
            if locations:
                return min(Path(location).resolve() for location in locations)
            if spec.origin and spec.origin not in {"built-in", "frozen"}:
                return Path(spec.origin).resolve()
    return Path.cwd().resolve()


def _default_state_home() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support"
    if sys.platform.startswith("win"):
        local_app_data = os.environ.get("LOCALAPPDATA")
        return Path(local_app_data) if local_app_data else Path.home() / "AppData" / "Local"
    xdg_state_home = os.environ.get("XDG_STATE_HOME")
    return Path(xdg_state_home) if xdg_state_home else Path.home() / ".local" / "state"


def _reject_linked_components(path: Path, label: str) -> None:
    for index, component in enumerate((path, *path.parents)):
        if component == component.parent:
            continue
        try:
            mode = component.lstat().st_mode
        except FileNotFoundError:
            continue
        except NotADirectoryError as exc:
            raise ConfigurationError(
                f"{label} path {path} has parent component {component.parent} "
                "which is not a directory; choose another path."
            ) from exc
        if stat.S_ISLNK(mode):
            raise ConfigurationError(
                f"{label} path {path} traverses symlink {component}; "
                "choose a real private directory instead."
            )
        if index > 0 and not stat.S_ISDIR(mode):
            raise ConfigurationError(
                f"{label} path {path} has parent component {component} "
                "which is not a directory; choose another path."
            )


def _reject_foreign_owner(path: Path, info: os.stat_result, label: str) -> None:
    if os.name != "posix" or info.st_uid == os.geteuid():
        return
    try:
        import pwd

        owner = f"{pwd.getpwuid(info.st_uid).pw_name} (uid {info.st_uid})"
    except (ImportError, KeyError):
        owner = f"uid {info.st_uid}"
    raise ConfigurationError(
        f"{label} path {path} is owned by {owner}, not by the current user "
        f"(uid {os.geteuid()}); use a path this user owns."
    )


def _reject_shared_writable_ancestors(directories: tuple[Path, ...], label: str) -> None:
    """Reject an ancestor another user could use to replace the state directory.

    Anyone with write access to a directory can rename the private directory
    below it away and substitute their own. A sticky directory such as ``/tmp``
    only lets owners rename their own entries, so it is accepted. Otherwise:

    - other-writable is always rejected;
    - group-writable is accepted only when the directory belongs to the
      effective user's own group and is owned by that user or root. That is the
      umask 002 / user-private-group layout (Ubuntu, Fedora), where the group
      holds only the user (the same trade-off as OpenSSH ``StrictModes``).
      Any other group-writable directory, or one owned by a third user, is
      rejected.
    """
    if os.name != "posix":
        return
    for directory in directories:
        try:
            info = directory.lstat()
        except FileNotFoundError:
            continue
        mode = info.st_mode
        if mode & stat.S_ISVTX:
            continue
        own_group = info.st_gid == os.getegid() and info.st_uid in {os.geteuid(), 0}
        if mode & stat.S_IWOTH or (mode & stat.S_IWGRP and not own_group):
            raise ConfigurationError(
                f"{label} ancestor directory {directory} is writable by other users "
                f"(mode {stat.S_IMODE(mode):04o}, uid {info.st_uid}, gid {info.st_gid}) "
                "without the sticky bit, so they could replace the private directory "
                "below it; tighten its permissions or choose another path."
            )


def _ensure_private_directory(path: Path, label: str) -> None:
    _reject_linked_components(path, label)
    missing: list[Path] = []
    component = path
    while True:
        try:
            info = component.lstat()
        except FileNotFoundError:
            missing.append(component)
            component = component.parent
            continue
        except NotADirectoryError as exc:
            raise ConfigurationError(
                f"{label} path {path} has a parent component which is not a directory; "
                "choose another path."
            ) from exc
        if not stat.S_ISDIR(info.st_mode):
            raise ConfigurationError(f"{label} directory {component} is not a directory.")
        break

    _reject_shared_writable_ancestors(
        (component, *component.parents) if missing else tuple(component.parents), label
    )
    created: list[Path] = []
    for component in reversed(missing):
        try:
            component.mkdir(mode=0o700)
        except FileExistsError:
            # Another process won the creation race. It is now an existing
            # directory and must pass the same private-mode validation below.
            continue
        except OSError as exc:
            raise ConfigurationError(
                f"could not create private {label} directory {component}: {exc}"
            ) from exc
        created.append(component)
        if os.name == "posix":
            try:
                component.chmod(0o700)
            except OSError as exc:
                raise ConfigurationError(
                    "could not set private permissions (0700) on "
                    f"{label} directory {component}: {exc}"
                ) from exc

    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise ConfigurationError(f"{label} directory {path} is not a directory.")
    _reject_foreign_owner(path, info, label)
    if os.name == "posix" and path not in created and stat.S_IMODE(info.st_mode) & 0o077:
        raise ConfigurationError(
            f"{label} directory {path} must already have private permissions (0700); "
            "refusing to change an existing directory."
        )
    _reject_linked_components(path, label)


def default_state_directory(package: str | None) -> Path:
    """Return the default state directory for ``package`` without creating it.

    The name digests the package's resolved install location, so the same
    code installed at another path defaults to a different, empty directory.
    """
    return _absolute_unresolved(_default_state_home() / "modulith" / _namespace(package))


def resolve_state_directory(
    package: str | None,
    *,
    state_dir: str | PathLike[str] | None = None,
) -> Path:
    """Return an absolute package-namespaced state directory and secure it."""
    path = (
        _absolute_unresolved(state_dir)
        if state_dir is not None
        else default_state_directory(package)
    )
    _ensure_private_directory(path, "broker state")
    return path


def _validate_regular_file(path: Path, info: os.stat_result, label: str) -> None:
    if not stat.S_ISREG(info.st_mode):
        raise ConfigurationError(
            f"{label} path {path} is not a regular file; remove it or choose another path."
        )
    _reject_foreign_owner(path, info, label)
    if getattr(info, "st_nlink", 1) != 1:
        raise ConfigurationError(
            f"{label} path {path} has {info.st_nlink} hard links; use a file with exactly one link."
        )


def _open_state_file(path: Path, *, create: bool, label: str) -> None:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    cloexec = getattr(os, "O_CLOEXEC", 0)
    flags = os.O_RDONLY | nofollow | cloexec
    directory_fd = -1
    fd = -1
    use_directory_fd = os.name == "posix"
    try:
        target: str | Path = path
        if use_directory_fd:
            directory_fd = os.open(
                path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow | cloexec,
            )
            opened_parent = os.fstat(directory_fd)
            current_parent = path.parent.lstat()
            if (opened_parent.st_dev, opened_parent.st_ino) != (
                current_parent.st_dev,
                current_parent.st_ino,
            ):
                raise ConfigurationError(
                    f"{label} directory {path.parent} changed while it was being checked."
                )
            target = path.name

        open_kwargs = {"dir_fd": directory_fd} if use_directory_fd else {}
        if create:
            try:
                fd = os.open(
                    target,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    **open_kwargs,
                )
            except FileExistsError:
                fd = os.open(target, flags, **open_kwargs)
        else:
            try:
                fd = os.open(target, flags, **open_kwargs)
            except FileNotFoundError:
                return

        opened = os.fstat(fd)
        current = (
            os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
            if use_directory_fd
            else path.lstat()
        )
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise ConfigurationError(f"{label} path {path} changed while it was being checked.")
        _validate_regular_file(path, opened, label)
        if os.name == "posix":
            os.fchmod(fd, 0o600)
    except OSError as exc:
        raise ConfigurationError(f"could not secure {label} path {path}: {exc}") from exc
    finally:
        if fd >= 0:
            os.close(fd)
        if directory_fd >= 0:
            os.close(directory_fd)


def resolve_state_file(
    package: str | None,
    *,
    filename: str,
    state_dir: str | PathLike[str] | None = None,
    path: str | PathLike[str] | None = None,
    create: bool = True,
    label: str = "broker state file",
) -> Path:
    """Resolve and securely create or validate one private broker state file."""
    directory = resolve_state_directory(package, state_dir=state_dir)
    if path is None:
        result = directory / filename
    else:
        candidate = Path(os.path.expanduser(os.fspath(path)))
        if candidate.is_absolute():
            result = _absolute_unresolved(candidate)
        else:
            result = _absolute_unresolved(directory / candidate)
            if os.path.commonpath((directory, result)) != str(directory):
                raise ConfigurationError(
                    f"{label} relative path {path!r} points outside {directory}."
                )
    _ensure_private_directory(result.parent, label)
    _reject_linked_components(result, label)
    try:
        existing = result.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None:
        _validate_regular_file(result, existing, label)
    try:
        _open_state_file(result, create=create, label=label)
    except OSError as exc:
        raise ConfigurationError(f"could not open {label} path {result}: {exc}") from exc
    _reject_linked_components(result, label)
    if result.exists():
        _validate_regular_file(result, result.lstat(), label)
    return result
