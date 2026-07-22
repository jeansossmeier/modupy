"""Security tests for private broker state paths."""

from __future__ import annotations

import importlib
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from modulith import ConfigurationError
from modulith.adapters import _state_path
from modulith.adapters._state_path import resolve_state_directory, resolve_state_file


def test_default_state_directory_is_absolute_namespaced_and_private(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_home = tmp_path / "xdg-state"
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setattr(
        Path,
        "home",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError("home must not be read"))),
    )

    state_dir = resolve_state_directory("acme.orders")

    assert state_dir.parent == state_home / "modulith"
    assert state_dir.name.startswith("acme.orders-")
    assert len(state_dir.name.rsplit("-", 1)[1]) == 12
    assert state_dir.is_absolute()
    assert state_dir.is_dir()
    if os.name == "posix":
        assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700


def test_default_namespace_distinguishes_same_named_project_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_home = tmp_path / "state-home"
    first_project = tmp_path / "first" / "service"
    second_project = tmp_path / "second" / "service"
    first_project.mkdir(parents=True)
    second_project.mkdir(parents=True)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))

    monkeypatch.chdir(first_project)
    first = resolve_state_directory(None)
    monkeypatch.chdir(second_project)
    second = resolve_state_directory(None)

    assert first.name.startswith("service-")
    assert second.name.startswith("service-")
    assert first != second


def test_explicit_package_namespace_distinguishes_same_named_import_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_home = tmp_path / "state-home"
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    for root in (first_root, second_root):
        package = root / "collision_pkg"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
    original_path = list(sys.path)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))

    monkeypatch.setattr(sys, "path", [str(first_root), *original_path])
    importlib.invalidate_caches()
    first = resolve_state_directory("collision_pkg")
    monkeypatch.setattr(sys, "path", [str(second_root), *original_path])
    importlib.invalidate_caches()
    second = resolve_state_directory("collision_pkg")

    assert first.name.startswith("collision_pkg-")
    assert second.name.startswith("collision_pkg-")
    assert first != second


def test_explicit_package_namespace_is_stable_across_process_hash_seeds(
    tmp_path: Path,
) -> None:
    import_root = tmp_path / "imports"
    package = import_root / "stable_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    script = "from modulith.adapters._state_path import _namespace; print(_namespace('stable_pkg'))"
    base_env = {**os.environ, "PYTHONPATH": str(import_root)}

    names = {
        subprocess.check_output(
            [sys.executable, "-c", script],
            cwd=tmp_path,
            env={**base_env, "PYTHONHASHSEED": seed},
            text=True,
        ).strip()
        for seed in ("1", "987654")
    }

    assert len(names) == 1
    assert re.fullmatch(r"stable_pkg-[0-9a-f]{12}", names.pop())


def test_dotted_package_namespace_does_not_import_parent_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import_root = tmp_path / "imports"
    package = import_root / "bootstrap_side_effect_pkg"
    child = package / "child"
    child.mkdir(parents=True)
    marker = tmp_path / "imported-parent"
    (package / "__init__.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('imported', encoding='utf-8')\n",
        encoding="utf-8",
    )
    (child / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(import_root))
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    importlib.invalidate_caches()

    resolve_state_directory("bootstrap_side_effect_pkg.child")

    assert not marker.exists()


@pytest.mark.skipif(os.name != "posix", reason="private directory modes are POSIX-only")
def test_existing_state_directory_must_already_be_private(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o755)

    with pytest.raises(ConfigurationError, match="private permissions"):
        resolve_state_directory("acme.orders", state_dir=shared)

    assert stat.S_IMODE(shared.stat().st_mode) == 0o755


@pytest.mark.skipif(os.name != "posix", reason="private directory modes are POSIX-only")
def test_only_new_directories_are_chmodded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o755)
    state_dir = shared / "new-parent" / "private-state"
    chmod_paths: list[Path] = []
    original_chmod = Path.chmod

    def record_chmod(path: Path, mode: int) -> None:
        chmod_paths.append(path)
        original_chmod(path, mode)

    monkeypatch.setattr(Path, "chmod", record_chmod)

    assert resolve_state_directory("acme.orders", state_dir=state_dir) == state_dir
    assert chmod_paths == [shared / "new-parent", state_dir]
    assert stat.S_IMODE(shared.stat().st_mode) == 0o755
    assert stat.S_IMODE((shared / "new-parent").stat().st_mode) == 0o700
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700


def test_state_file_is_created_as_private_regular_file(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"

    path = resolve_state_file(
        "acme.orders",
        filename="broker.db",
        state_dir=state_dir,
        label="broker database",
    )

    assert path == state_dir / "broker.db"
    assert path.is_file()
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700


def test_existing_state_file_is_reopened_without_replacing_contents(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    first = resolve_state_file("acme.orders", filename="broker.db", state_dir=state_dir)
    first.write_bytes(b"durable state")

    second = resolve_state_file("acme.orders", filename="broker.db", state_dir=state_dir)

    assert second == first
    assert second.read_bytes() == b"durable state"


def test_missing_state_file_can_be_resolved_without_creation(tmp_path: Path) -> None:
    path = resolve_state_file(
        "acme.orders",
        filename="broker.db",
        state_dir=tmp_path / "private-state",
        create=False,
    )

    assert not path.exists()


def test_relative_explicit_file_path_stays_inside_state_directory(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"

    path = resolve_state_file(
        "acme.orders",
        filename="default.db",
        state_dir=state_dir,
        path="custom.db",
    )

    assert path == state_dir / "custom.db"


def test_absolute_explicit_file_path_is_used_directly(tmp_path: Path) -> None:
    absolute_path = tmp_path / "outside-state-dir" / "broker.db"

    path = resolve_state_file(
        "acme.orders",
        filename="default.db",
        state_dir=tmp_path / "private-state",
        path=absolute_path,
    )

    assert path == absolute_path
    assert path.is_file()


def test_relative_explicit_file_path_cannot_escape_state_directory(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="outside"):
        resolve_state_file(
            "acme.orders",
            filename="default.db",
            state_dir=tmp_path / "private-state",
            path="../outside.db",
        )


def test_state_file_rejects_a_final_symlink_without_touching_target(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.write_text("unchanged", encoding="utf-8")
    (state_dir / "broker.db").symlink_to(target)

    with pytest.raises(ConfigurationError, match="symlink"):
        resolve_state_file(
            "acme.orders",
            filename="broker.db",
            state_dir=state_dir,
            label="broker database",
        )

    assert target.read_text(encoding="utf-8") == "unchanged"


@pytest.mark.skipif(
    not hasattr(os.stat(__file__), "st_nlink") or not hasattr(os, "link"),
    reason="filesystem does not expose reliable hard-link counts",
)
def test_state_file_rejects_hardlinks(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    original = tmp_path / "original"
    original.write_bytes(b"state")
    os.link(original, state_dir / "broker.db")

    with pytest.raises(ConfigurationError, match="hard link"):
        resolve_state_file(
            "acme.orders",
            filename="broker.db",
            state_dir=state_dir,
            label="broker database",
        )


@pytest.mark.skipif(os.name != "posix", reason="FIFO creation is POSIX-only")
def test_state_file_rejects_nonregular_files(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    os.mkfifo(state_dir / "broker.db")

    with pytest.raises(ConfigurationError, match="regular file"):
        resolve_state_file(
            "acme.orders",
            filename="broker.db",
            state_dir=state_dir,
            label="broker database",
        )


def test_state_file_rejects_a_directory_at_the_final_path(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    (state_dir / "broker.db").mkdir()

    with pytest.raises(ConfigurationError, match="regular file"):
        resolve_state_file(
            "acme.orders",
            filename="broker.db",
            state_dir=state_dir,
            label="broker database",
        )


def test_directory_creation_failure_is_translated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state"
    original_mkdir = Path.mkdir

    def deny_private_mkdir(
        path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        if path == state_dir:
            raise PermissionError("permission denied")
        original_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", deny_private_mkdir)

    with pytest.raises(ConfigurationError, match="could not create private broker state"):
        resolve_state_directory("acme.orders", state_dir=state_dir)


@pytest.mark.skipif(os.name != "posix", reason="private chmod is a POSIX contract")
def test_directory_permission_failure_is_translated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state"
    original_chmod = Path.chmod

    def deny_private_chmod(path: Path, mode: int) -> None:
        if path == state_dir:
            raise PermissionError("permission denied")
        original_chmod(path, mode)

    monkeypatch.setattr(Path, "chmod", deny_private_chmod)

    with pytest.raises(ConfigurationError, match="could not set private permissions"):
        resolve_state_directory("acme.orders", state_dir=state_dir)


@pytest.mark.skipif(os.name != "posix", reason="descriptor-relative opens are POSIX-only")
@pytest.mark.parametrize("changed_kind", ["directory", "file"])
def test_state_file_rejects_path_races(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_kind: str,
) -> None:
    state_dir = tmp_path / "private-state"
    original_fstat = os.fstat

    def changed_fstat(fd: int):
        info = original_fstat(fd)
        is_selected_kind = (changed_kind == "directory" and stat.S_ISDIR(info.st_mode)) or (
            changed_kind == "file" and stat.S_ISREG(info.st_mode)
        )
        if is_selected_kind:
            return SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino + 1)
        return info

    monkeypatch.setattr(os, "fstat", changed_fstat)

    with pytest.raises(ConfigurationError, match="changed while it was being checked"):
        resolve_state_file("acme.orders", filename="broker.db", state_dir=state_dir)


def test_state_file_open_failure_is_translated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    original_open = os.open

    def deny_file_open(path, *args, **kwargs):
        if Path(path).name == "broker.db":
            raise PermissionError("permission denied")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", deny_file_open)

    with pytest.raises(ConfigurationError, match="could not secure broker state file"):
        resolve_state_file("acme.orders", filename="broker.db", state_dir=state_dir)


@pytest.mark.skipif(os.name != "posix", reason="private fchmod is a POSIX contract")
def test_state_file_permission_failure_is_translated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)

    def deny_fchmod(fd: int, mode: int) -> None:
        raise PermissionError("permission denied")

    monkeypatch.setattr(os, "fchmod", deny_fchmod)

    with pytest.raises(ConfigurationError, match="could not secure broker state file"):
        resolve_state_file("acme.orders", filename="broker.db", state_dir=state_dir)


def test_empty_xdg_state_home_falls_back_to_local_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("XDG_STATE_HOME", "")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

    state_dir = resolve_state_directory("acme")

    assert state_dir.parent == tmp_path / ".local" / "state" / "modulith"
    assert state_dir.name.startswith("acme-")


def test_macos_default_state_home_uses_application_support(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

    assert _state_path._default_state_home() == tmp_path / "Library" / "Application Support"


def test_windows_default_state_home_prefers_local_app_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_app_data = tmp_path / "LocalAppData"
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setattr(
        Path,
        "home",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError("home must not be read"))),
    )

    assert _state_path._default_state_home() == local_app_data


def test_windows_default_state_home_falls_back_below_user_home(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

    assert _state_path._default_state_home() == tmp_path / "AppData" / "Local"


def test_state_directory_rejects_regular_file_parent_with_configuration_error(
    tmp_path: Path,
) -> None:
    regular_file = tmp_path / "not-a-directory"
    regular_file.write_text("blocking parent", encoding="utf-8")

    with pytest.raises(
        ConfigurationError,
        match=r"parent component .* is not a directory",
    ):
        resolve_state_directory("acme.orders", state_dir=regular_file / "state")
