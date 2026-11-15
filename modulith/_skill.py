"""The Agent Skill `modulith skill install` copies out of the package.

The skill is the ``modulith/skill`` directory: a ``SKILL.md`` and the reference
files it links to, in the format Claude Code, Codex and other coding agents
read. Each agent looks for a skill as a directory inside a skills directory, so
installing is copying that directory to ``<skills directory>/modupy``.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterable
from importlib.resources import as_file, files
from pathlib import Path

# The directory the skill is installed as, and the `name` its SKILL.md declares.
NAME = "modupy"


def _declared_name(skill_md: Path) -> str | None:
    """The ``name`` in a SKILL.md's front matter, or None when it has none to read."""
    try:
        lines = skill_md.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return None
    if not lines or lines[0].strip() != "---":
        return None
    for line in lines[1:]:
        if line.strip() == "---":
            break
        key, _, value = line.partition(":")
        if key == "name":
            return value.strip().strip("'\"")
    return None


def _is_installed(target: Path) -> bool:
    """Whether ``target`` holds an earlier install of the skill.

    Raises ``FileExistsError`` for anything else at that path, a symlink
    included: only a real directory whose SKILL.md names this skill is ours to
    replace. It also raises when a file or a dangling symlink stands where a
    directory above a new install has to be.
    """
    if not target.is_symlink() and not target.exists():
        parent = target.parent
        while not parent.is_dir():
            if parent.is_symlink() or parent.exists():
                raise FileExistsError(f"{parent} is not a directory, so {target} cannot be written")
            parent = parent.parent
        return False
    skill_md = target / "SKILL.md"
    if (
        target.is_symlink()
        or not target.is_dir()
        or skill_md.is_symlink()
        or _declared_name(skill_md) != NAME
    ):
        raise FileExistsError(
            f"{target} exists and is not the {NAME} skill; move it away or pass another --dir"
        )
    return True


def install_skill(
    skills_dirs: Iterable[Path], source: Path | None = None
) -> list[tuple[Path, bool]]:
    """Copy the skill into ``<skills_dir>/modupy`` for each skills directory.

    ``source`` defaults to the packaged skill. Returns each directory written
    and whether it replaced an earlier install, which is removed first so a
    file this version no longer ships does not survive. Every target is checked
    before the first write: one that is not ours raises ``FileExistsError``
    and nothing is touched anywhere.
    """
    if source is None:
        with as_file(files("modulith") / "skill") as packaged:
            return install_skill(skills_dirs, packaged)
    if _declared_name(source / "SKILL.md") != NAME:
        raise FileNotFoundError(f"no {NAME} skill at {source}; reinstall modupy")
    targets = [Path(skills_dir) / NAME for skills_dir in skills_dirs]
    for target in targets:
        _is_installed(target)
    written = []
    for target in targets:
        # Checked again here: two skills directories can be one, through a symlink.
        updated = _is_installed(target)
        if updated:
            shutil.rmtree(target)
        for path in sorted(source.rglob("*")):
            if path.is_file():
                copy = target / path.relative_to(source)
                copy.parent.mkdir(parents=True, exist_ok=True)
                copy.write_bytes(path.read_bytes())
        written.append((target, updated))
    return written
