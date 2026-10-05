"""Guard tests: hand-written prose must stay in sync with the code it describes.

Hand-written prose makes claims nothing recomputes:

- ``docs/STABILITY.md`` scopes its "these exports are considered stable"
  promise to a hand-maintained list. Without a guard that list silently falls
  behind ``modulith.__all__``, and a user checking whether a name is covered
  reads "not listed" as "not guaranteed".
- The shipped Markdown quotes the *size* of the plugin contract ("13
  hookspecs, 5 protocols"). Those numbers have gone stale twice, in different
  directions in different files, because every one of them is typed by hand.
- The shipped Markdown also *enumerates*, by name, what a module declares —
  the driver protocols, the shared plugin-contract types. A count guard does
  not catch a list that names four of five and never says how many there are,
  so the enumerations are checked separately against the same source.
- ``SPEC.md`` Part XVI inventories, by hand, every file the wheel ships under
  ``modulith/``. A data file or a migration directory is as easy to leave out
  as a module, so the shipped set is asked of the build backend and every file
  in it must be named.

``docs/API_REFERENCE.md`` has an equivalent generator-backed guard
(tests/test_api_reference_sync.py); these are the same protection for the
prose documents.

``__version__`` is excluded from the export check: it is a value, not an API
surface.
"""

from __future__ import annotations

import fnmatch
import inspect
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

import modulith

REPO_ROOT = Path(__file__).resolve().parent.parent
STABILITY_DOC = REPO_ROOT / "docs" / "STABILITY.md"
SPEC_DOC = REPO_ROOT / "SPEC.md"


def test_every_public_export_is_named_in_stability_doc() -> None:
    backticked = set(re.findall(r"`([A-Za-z_][A-Za-z0-9_.]*)`", STABILITY_DOC.read_text()))
    # A name may be written bare or qualified (``modulith.types.X``); accept
    # either by also indexing the last dotted segment.
    named = backticked | {token.rsplit(".", 1)[-1] for token in backticked}

    missing = sorted(name for name in modulith.__all__ if name != "__version__")
    missing = [name for name in missing if name not in named]

    assert not missing, (
        "docs/STABILITY.md does not name these exports from modulith.__all__, "
        "so its stability guarantee does not visibly cover them: "
        f"{missing}"
    )


# ---------------------------------------------------------------------------
# Plugin-contract sizes quoted in prose
# ---------------------------------------------------------------------------

# "5 protocols", "**5 protocols**", "all 13 hookspecs and 5 protocols".
QUOTED_PROTOCOL_COUNT = re.compile(r"(\d+)\**\s+protocols\b", re.IGNORECASE)
# "the hookspecs (13 as of ...)" — here the number *trails* the noun, so the
# ``N hookspecs`` sweep in tests/test_hook_contracts.py never sees it.
PARENTHESISED_HOOKSPEC_COUNT = re.compile(r"hookspecs\s*\((\d+)", re.IGNORECASE)


def _protocol_names() -> frozenset[str]:
    """Names of the protocol classes declared by ``modulith/protocols.py``.

    Keyed on the ``_is_protocol`` flag ``typing.Protocol`` stamps on its
    subclasses (the same flag ``runtime_checkable`` validates), so a concrete
    helper class added to the module cannot inflate the set; the module
    filter drops ``Protocol`` itself, which is only imported there.
    """
    from modulith import protocols

    return frozenset(
        name
        for name, obj in vars(protocols).items()
        if inspect.isclass(obj)
        and getattr(obj, "_is_protocol", False)
        and obj.__module__ == protocols.__name__
    )


def _shipped_markdown() -> list[Path]:
    """The Markdown a reader of the repository actually sees."""
    return sorted(REPO_ROOT.glob("*.md")) + sorted((REPO_ROOT / "docs").glob("*.md"))


def _markdown_blocks(text: str) -> Iterator[tuple[int, str]]:
    """Yield ``(line number, block)`` for each separately-readable unit.

    A block is a blank-line-separated paragraph, except inside a Markdown
    table, where every row is its own unit: consecutive rows are claims about
    different files, so merging them would let one row's names silently
    satisfy a check aimed at another's.
    """
    lineno = 1
    for chunk in re.split(r"\n[ \t]*\n", text):
        if chunk.strip():
            if chunk.lstrip().startswith("|"):
                for offset, row in enumerate(chunk.splitlines()):
                    if row.strip():
                        yield lineno + offset, row
            else:
                yield lineno, chunk
        # The split consumed a blank line: one newline ends the chunk, one
        # ends the blank line itself.
        lineno += chunk.count("\n") + 2


def _hookspec_count() -> int:
    """Hookspecs declared by ``modulith/hooks.py``.

    Keyed on the marker attribute pluggy stamps on each spec, so a plain
    helper function added to hooks.py is not mistaken for part of the
    contract.
    """
    from modulith import hooks

    return sum(
        1
        for obj in vars(hooks).values()
        if inspect.isfunction(obj) and hasattr(obj, "modulith_spec")
    )


def test_prose_plugin_contract_sizes_match_the_code() -> None:
    """Every prose count of the plugin contract must state the real number.

    README, CONTRIBUTING, SPEC and the docs/ pages all quote how many
    hookspecs and protocols there are, and each copy is maintained by hand —
    so they drift one file at a time and a reader has no way to tell which
    number is current. Both counts are recomputed from the modules that
    declare them, never restated here.
    """
    quoted = {
        QUOTED_PROTOCOL_COUNT: ("protocols", len(_protocol_names()), "modulith/protocols.py"),
        PARENTHESISED_HOOKSPEC_COUNT: ("hookspecs", _hookspec_count(), "modulith/hooks.py"),
    }
    stale: list[str] = []
    for path in _shipped_markdown():
        text = path.read_text(encoding="utf-8")
        for pattern, (noun, declared, source) in quoted.items():
            for match in pattern.finditer(text):
                if int(match.group(1)) != declared:
                    line = text[: match.start()].count("\n") + 1
                    stale.append(
                        f"{path.relative_to(REPO_ROOT).as_posix()}:{line} says "
                        f"{match.group(1)} {noun}, but {source} declares {declared}"
                    )

    assert not stale, "shipped prose states a stale plugin-contract size: " + "; ".join(stale)


def _public_type_names() -> frozenset[str]:
    """Names of the data types declared by ``modulith/types.py``.

    That module declares no ``__all__``, so the narrowest defensible stand-in
    is used: every class the module itself defines whose name does not start
    with an underscore. The module filter drops the names it merely imports
    (``datetime``, ``UUID``, ``Enum``), leaving exactly the dataclasses and
    the enum that travel through the plugin contract.
    """
    from modulith import types

    return frozenset(
        name
        for name, obj in vars(types).items()
        if inspect.isclass(obj) and not name.startswith("_") and obj.__module__ == types.__name__
    )


def _module_anchor(stem: str) -> re.Pattern[str]:
    """Match prose pointing at a module itself, not at one name inside it.

    ``modulith/types.py`` (a file inventory) and a bare ``modulith.types``
    both point at the module. ``modulith.types.EventPublishReceipt`` cites one
    name, and a block assembled from such citations claims nothing about the
    module's full contents: docs/STABILITY.md's list of names the top-level
    package deliberately does not re-export is exactly that, and it spans two
    modules at once.
    """
    return re.compile(rf"\b{stem}\.py\b|\bmodulith\.{stem}(?!\.)")


# A block that both points at a module and names two or more of what it
# declares is reading as a list of them, and a list that stops short is worse
# than no list: the reader takes the omitted name to not exist. One name is a
# passing mention (``modulith.protocols.HealthAwareConsumer`` in a bullet about
# that one capability), which is why the threshold is two.
_MINIMUM_NAMES_TO_READ_AS_A_LIST = 2

# Modules whose contents the shipped prose enumerates by name. Every expected
# name is derived from the module, so this list never restates one.
_ENUMERATED_MODULES = [
    ("protocols", _protocol_names()),
    ("types", _public_type_names()),
]


@pytest.mark.parametrize(
    ("stem", "names"),
    _ENUMERATED_MODULES,
    ids=[stem for stem, _ in _ENUMERATED_MODULES],
)
def test_prose_lists_of_a_modules_names_leave_none_out(stem: str, names: frozenset[str]) -> None:
    """A prose list of what a module declares must name all of it.

    The count guard above cannot catch this: a list that names four of five
    and never states a number is silently wrong, and that is exactly how the
    file inventory in SPEC.md and the driver-protocol section of
    docs/ARCHITECTURE.md each lost a protocol name. Both the expected names
    and the documents to scan are derived, so adding a protocol or a shared
    type turns this red until every list claiming to enumerate them is
    updated.
    """
    source = f"modulith/{stem}.py"
    anchor = _module_anchor(stem)
    incomplete: list[str] = []
    for path in _shipped_markdown():
        for lineno, block in _markdown_blocks(path.read_text(encoding="utf-8")):
            if not anchor.search(block):
                continue
            # Word boundaries so ``BrokerConsumer`` is not read as ``Broker``.
            listed = {name for name in names if re.search(rf"\b{re.escape(name)}\b", block)}
            if len(listed) < _MINIMUM_NAMES_TO_READ_AS_A_LIST:
                continue
            missing = sorted(names - listed)
            if missing:
                incomplete.append(
                    f"{path.relative_to(REPO_ROOT).as_posix()}:{lineno} lists "
                    f"{sorted(listed)} but {source} also declares {missing}"
                )

    assert not incomplete, (
        f"shipped prose enumerates what {source} declares but leaves some out, so a "
        "reader cannot tell the list is partial: " + "; ".join(incomplete)
    )


# ---------------------------------------------------------------------------
# SPEC Part XVI: the file inventory
# ---------------------------------------------------------------------------


def _shipped_package_files() -> list[str]:
    """Every file the wheel build puts under ``modulith/``, as posix paths.

    Asked of hatchling, the backend named in ``[build-system]``: it applies
    ``packages = ["modulith"]`` and the ``.gitignore`` rules to the files on
    disk, so a new module, ``py.typed`` or migration shows up here without
    anyone editing a list, while ``__pycache__`` and broker state files never
    do.
    """
    from hatchling.builders.wheel import WheelBuilder

    selected = WheelBuilder(str(REPO_ROOT)).recurse_included_files()
    return sorted(
        file.distribution_path
        for file in selected
        if file.distribution_path.startswith("modulith/")
    )


def _inventory_rows() -> Iterator[tuple[str, list[str], str]]:
    """Yield ``(directory, named files, row)`` for each package-table row of Part XVI.

    ``directory`` comes from the ``### ... `modulith/x/` `` heading above the
    table, because a bare ``__init__.py`` row only speaks for its own
    directory. Rows under any other heading (tests, examples, top-level files)
    are not package inventory and are skipped.
    """
    part = SPEC_DOC.read_text(encoding="utf-8").partition("\n## Part XVI")[2].partition("\n## ")[0]
    assert part.strip(), "SPEC.md has no 'Part XVI' section"
    directory = ""
    for _, block in _markdown_blocks(part):
        if block.lstrip().startswith("#"):
            heading_directory = re.search(r"`(modulith/[^`]*)`", block)
            directory = heading_directory.group(1) if heading_directory else ""
        elif block.startswith("|") and directory:
            yield directory, re.findall(r"`([^`]+)`", block.split("|")[1]), block


def _names_file(token: str, relative_path: str) -> bool:
    """Whether an inventory ``token`` accounts for ``relative_path``.

    A token is a file name, a glob over file names (``_shm_*.py``) or a
    directory (``migrations/``), which covers everything beneath it.
    """
    if token.endswith("/"):
        return relative_path.startswith(token)
    return fnmatch.fnmatchcase(relative_path, token)


def test_spec_file_inventory_lists_every_shipped_package_file() -> None:
    """Part XVI must name every file the wheel ships under ``modulith/``.

    Each file is checked against the rows of the deepest inventoried directory
    above it, so one directory's ``__init__.py`` row cannot stand in for
    another's, and ``migrations/`` covers the revisions beneath it without a
    row per revision.
    """
    tokens_by_directory: dict[str, list[str]] = {}
    for directory, tokens, _ in _inventory_rows():
        tokens_by_directory.setdefault(directory, []).extend(tokens)

    unlisted: list[str] = []
    for path in _shipped_package_files():
        directory = max(
            (candidate for candidate in tokens_by_directory if path.startswith(candidate)),
            key=len,
            default="modulith/",
        )
        relative = path.removeprefix(directory)
        if not any(
            _names_file(token, relative) for token in tokens_by_directory.get(directory, [])
        ):
            unlisted.append(path)

    assert not unlisted, (
        "SPEC.md Part XVI does not name these files the wheel ships (a directory row "
        "such as `migrations/` covers everything beneath it): " + ", ".join(unlisted)
    )


def test_spec_file_inventory_calls_no_package_with_an_init_a_namespace_package() -> None:
    """A directory that ships ``__init__.py`` is a regular package.

    Only a directory without an ``__init__.py`` is a namespace package, so a
    row for ``__init__.py`` that says "namespace" contradicts the file it names.
    """
    shipped = set(_shipped_package_files())
    mislabelled = [
        f"{directory}__init__.py"
        for directory, tokens, row in _inventory_rows()
        if "__init__.py" in tokens
        and f"{directory}__init__.py" in shipped
        and "namespace" in row.lower()
    ]

    assert not mislabelled, (
        "SPEC.md Part XVI calls these directories namespace packages, but each ships an "
        "__init__.py and so is a regular package: " + ", ".join(mislabelled)
    )
