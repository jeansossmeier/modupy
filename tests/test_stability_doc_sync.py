"""Guard tests: hand-written prose must stay in sync with the code it describes.

Two documents make claims nothing recomputes:

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

``docs/API_REFERENCE.md`` has an equivalent generator-backed guard
(tests/test_api_reference_sync.py); these are the same protection for the
prose documents.

``__version__`` is excluded from the export check: it is a value, not an API
surface.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

import modulith

REPO_ROOT = Path(__file__).resolve().parent.parent
STABILITY_DOC = REPO_ROOT / "docs" / "STABILITY.md"


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
