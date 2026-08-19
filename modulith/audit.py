"""The `modulith audit` command implementation.

Analyzes an existing Python codebase non-destructively. Produces a
Markdown report that a team can read on Friday afternoon, generate a
baseline from on Monday, and start tightening over weeks.

This is the single biggest adoption lever for brownfield projects.
Without it, modulith is "for new projects only" — which kills the
addressable market.

The whole pipeline is static: files are parsed with ``ast``, never
imported, so auditing an unknown codebase is always safe.

Output sections (in priority order):

  1. Executive summary: readiness score, biggest issues
  2. Proposed module structure: based on directory layout
  3. Cross-module imports that would become violations
  4. Shared database tables that need ownership decisions
  5. Listener-shaped patterns already in the code (for events conversion)
  6. Recommended migration steps
"""

from __future__ import annotations

import ast
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .builtin.verifier import (
    CONTRACTS_MODULE,
    _file_package,
    _ImportCollector,
    _table_refs_from_tree,
)

logger = logging.getLogger("modulith.audit")

# Directories that never contain application source worth auditing.
#
# ``tests``/``test`` are here for a different reason than the vendored and
# build directories: their contents are real source, they are just not module
# candidates. Left in, a ``tests/`` directory becomes a proposed module and
# every ``from myapp.orders import ...`` inside it is reported as a
# cross-module import that would become a boundary violation — pure noise that
# also drags the readiness score down, because reaching across module
# boundaries is precisely what test code is allowed to do.
_SKIP_DIRS = frozenset(
    {
        "__pycache__",
        ".venv",
        "venv",
        ".git",
        ".tox",
        "build",
        "dist",
        "node_modules",
        ".mypy_cache",
        "tests",
        "test",
    }
)

# Function-name prefixes that suggest an event-handler shape.
_LISTENER_PREFIXES = ("on_", "handle_", "process_")

# Decorator name fragments that suggest signal/observer wiring.
_LISTENER_DECORATORS = ("receiver", "signal", "listener", "connect", "subscribe")


# ---------------------------------------------------------------------------
# Audit results
# ---------------------------------------------------------------------------


@dataclass
class AuditResult:
    """Complete analysis output for the report generator."""

    # Top-level metric: percentage 0-100
    readiness_score: int

    # Each entry: {module_candidate: [files_in_it]}
    proposed_modules: dict[str, list[Path]]

    # Each: (source_module, target_module, count, sample_file)
    cross_module_imports: list[tuple[str, str, int, Path]]

    # Tables referenced from more than one proposed module
    shared_tables: list[str]

    # Files containing functions that look like they could become listeners
    listener_candidates: list[Path]

    # How many .py files the audit successfully parsed (excludes files that
    # failed to parse — see parse_failures). Zero means the audited path
    # contained no Python at all (wrong path, docs-only dir, empty
    # scaffold) — the readiness score is meaningless then and the report
    # must say so instead of a confident 100/100.
    files_scanned: int = 0

    # Files that failed to parse (syntax/encoding errors) and were excluded
    # from every analysis stage. Disclosed so a confident-looking report
    # doesn't silently hide that some of the codebase was never analyzed.
    parse_failures: list[Path] = field(default_factory=list)


def audit_codebase(root: Path, contracts_module: str = CONTRACTS_MODULE) -> AuditResult:
    """Run the full audit pipeline over a directory tree.

    Walks ``root`` for ``.py`` files, parses each with ``ast``, and derives
    the proposed module structure, cross-module imports, shared tables,
    listener candidates, and a readiness score. Files that fail to parse
    are excluded from analysis and reported separately in
    ``parse_failures`` rather than silently counted as scanned.

    *contracts_module* is the name exempted from cross-module coupling —
    honor a codebase's already-configured ``[tool.modulith].contracts_module``
    (mid-migration) instead of always assuming the default ``"contracts"``.

    ``root`` is resolved first: the root package name is derived from
    ``root.name``, and a relative root like ``Path(".")`` — the CLI default —
    has an empty name, which would make every ``rootpkg.module`` import look
    external and fabricate a perfect readiness score.
    """
    root = root.resolve()
    files = _iter_py_files(root)
    parsed, failures = _split_by_parseability(files)
    proposed = _propose_module_structure(root, parsed)
    cross = _find_cross_module_imports(root, parsed, proposed, contracts_module)
    shared = _find_shared_tables(root, parsed)
    listeners = _find_listener_candidates(parsed)
    score = _compute_readiness_score(cross, listeners)
    return AuditResult(
        readiness_score=score,
        proposed_modules=proposed,
        cross_module_imports=cross,
        shared_tables=shared,
        listener_candidates=listeners,
        files_scanned=len(parsed),
        parse_failures=failures,
    )


def _split_by_parseability(files: list[Path]) -> tuple[list[Path], list[Path]]:
    """Partition files into (successfully parseable, failed).

    Computed once up front so ``files_scanned``/``parse_failures`` reflect
    reality — every downstream stage already skips unparseable files on its
    own independent parse attempt, but silently, with no accounting.
    """
    parsed: list[Path] = []
    failures: list[Path] = []
    for path in files:
        if _parse(path) is None:
            failures.append(path)
        else:
            parsed.append(path)
    return parsed, failures


# ---------------------------------------------------------------------------
# File discovery + module mapping
# ---------------------------------------------------------------------------


def _iter_py_files(root: Path) -> list[Path]:
    """All ``.py`` files under ``root``, skipping vendored/build dirs."""
    files: list[Path] = []
    for path in sorted(root.rglob("*.py")):
        if any(part in _SKIP_DIRS for part in path.relative_to(root).parts):
            continue
        files.append(path)
    return files


def _module_of(root: Path, path: Path) -> str:
    """The proposed module a file belongs to.

    A top-level directory under ``root`` is a module candidate; files
    directly under ``root`` are attributed to the root package itself.
    """
    rel = path.relative_to(root)
    if len(rel.parts) > 1:
        return rel.parts[0]
    return root.name


def _parse(path: Path) -> ast.Module | None:
    """Parse a file to an AST, returning None on unreadable/invalid source."""
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError, OSError) as exc:
        logger.debug("skipping unparseable file %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Analysis pipeline stages
# ---------------------------------------------------------------------------


def _propose_module_structure(root: Path, files: list[Path]) -> dict[str, list[Path]]:
    """Group files by their top-level directory under ``root``."""
    modules: dict[str, list[Path]] = defaultdict(list)
    for path in files:
        modules[_module_of(root, path)].append(path)
    return dict(modules)


def _target_module(
    dotted: str,
    root_package: str,
    module_names: set[str],
    root_is_package: bool,
) -> str | None:
    """Map a dotted import path to a proposed module name, if it is one.

    A bare top-level import (``import types``) that merely *shares a name*
    with a local module directory must not count as internal coupling: when
    the audited root is itself a package, its children are only importable as
    ``root.child``, so bare names are external by construction; in a flat
    (non-package) layout a bare name can be local, but a stdlib name is
    resolved as stdlib, not as coupling.
    """
    parts = dotted.split(".")
    if not parts or not parts[0]:
        return None
    if parts[0] == root_package and len(parts) > 1:
        candidate = parts[1]
    else:
        if root_is_package or parts[0] in sys.stdlib_module_names:
            return None
        candidate = parts[0]
    return candidate if candidate in module_names else None


def _find_cross_module_imports(
    root: Path,
    files: list[Path],
    modules: dict[str, list[Path]],
    contracts_module: str = CONTRACTS_MODULE,
) -> list[tuple[str, str, int, Path]]:
    """Find imports that cross proposed module boundaries.

    Returns ``(source_module, target_module, count, sample_file)`` tuples,
    worst offenders (highest count) first.
    """
    module_names = set(modules)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    samples: dict[tuple[str, str], Path] = {}
    root_is_package = (root / "__init__.py").exists()

    for path in files:
        tree = _parse(path)
        if tree is None:
            continue
        source_module = _module_of(root, path)
        collector = _ImportCollector(path, _file_package(root, root.name, path))
        collector.visit(tree)
        for record in collector.records:
            target = _target_module(record.target_module, root.name, module_names, root_is_package)
            if target is None or target == source_module:
                continue
            # The conventional contracts module is the sanctioned shared
            # dependency that survives a process split — importing it is the
            # prescribed pattern, so it must not count as coupling (mirrors the
            # verifier's rule-4 exemption and the doctor readiness metric).
            # Honors a codebase's already-configured contracts_module name
            # instead of always assuming the "contracts" default.
            if target == contracts_module:
                continue
            key = (source_module, target)
            counts[key] += 1
            samples.setdefault(key, path)

    edges = [(src, tgt, count, samples[(src, tgt)]) for (src, tgt), count in counts.items()]
    edges.sort(key=lambda e: (-e[2], e[0], e[1]))
    return edges


def _string_table_refs(tree: ast.Module) -> set[str]:
    return {name for name, _line, _kind in _table_refs_from_tree(tree)}


def _find_shared_tables(root: Path, files: list[Path]) -> list[str]:
    """Tables referenced from more than one proposed module."""
    table_modules: dict[str, set[str]] = defaultdict(set)
    for path in files:
        tree = _parse(path)
        if tree is None:
            continue
        module = _module_of(root, path)
        for table in _string_table_refs(tree):
            table_modules[table].add(module)
    return sorted(t for t, mods in table_modules.items() if len(mods) > 1)


def _is_listener_shaped(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True if a function name or decorator suggests an event-handler shape."""
    if node.name.startswith(_LISTENER_PREFIXES):
        return True
    for decorator in node.decorator_list:
        name = _decorator_name(decorator).lower()
        if any(fragment in name for fragment in _LISTENER_DECORATORS):
            return True
    return False


def _decorator_name(decorator: ast.expr) -> str:
    """Best-effort dotted/attr name of a decorator expression."""
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Attribute):
        return decorator.attr
    if isinstance(decorator, ast.Call):
        return _decorator_name(decorator.func)
    return ""


def _find_listener_candidates(files: list[Path]) -> list[Path]:
    """Files containing at least one listener-shaped function."""
    matches: set[Path] = set()
    for path in files:
        tree = _parse(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _is_listener_shaped(
                node
            ):
                matches.add(path)
                break
    return sorted(matches)


def _compute_readiness_score(
    cross_module_imports: list[tuple[str, str, int, Path]],
    listener_candidates: list[Path],
) -> int:
    """Score migration readiness 0-100.

    The signal is the balance between *direct* cross-module coupling (every
    cross-module import is a future boundary violation) and *event-shaped*
    code already present (listener candidates — the patterns that convert
    cleanly to events). A codebase with no cross-module interaction at all
    is already modular, so it scores 100 — note this branch also fires for
    an empty tree (zero files scanned), which ``render_report`` calls out
    explicitly. Shared tables are deliberately not part of the formula; the
    report carries a caveat when they exist.
    """
    direct = sum(count for _src, _tgt, count, _sample in cross_module_imports)
    event_like = len(listener_candidates)
    total = direct + event_like
    if total == 0:
        return 100
    return round(100 * event_like / total)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def render_report(result: AuditResult) -> str:
    """Render an AuditResult as Markdown.

    Pure function of its input (no timestamps) so output is reproducible and
    easy to diff between runs — a team can commit the report and watch the
    readiness score climb.
    """
    lines: list[str] = ["# Modulith Audit Report", ""]

    # Summary
    lines += [
        "## Summary",
        "",
        f"**Readiness score: {result.readiness_score}/100**",
        "",
    ]
    if result.files_scanned == 0:
        lines += [
            "**Warning: no Python files were found under the audited path.** "
            "The readiness score is meaningless for an empty tree — check that "
            "the path points at your codebase.",
            "",
        ]
    lines += [
        f"Your codebase is approximately {result.readiness_score}% ready for "
        "modulith adoption. Top blockers:",
        "",
        f"  - {len(result.cross_module_imports)} cross-module import pattern(s) "
        "(should become events)",
        f"  - {len(result.shared_tables)} shared database table(s) (need ownership decisions)",
        "",
    ]
    if result.shared_tables:
        lines += [
            "Note: the readiness score reflects the import/listener balance only — "
            "shared-table entanglement is listed as a blocker but is not included "
            "in the score.",
            "",
        ]
    if result.parse_failures:
        lines += [
            f"**Warning: {len(result.parse_failures)} file(s) could not be parsed** "
            "and were excluded from this report (fix the syntax error and re-run "
            "for full coverage):",
            "",
        ]
        lines += [f"  - `{path}`" for path in result.parse_failures]
        lines.append("")

    # Proposed module structure
    lines += ["## Proposed Module Structure", ""]
    if result.proposed_modules:
        for module in sorted(result.proposed_modules):
            file_count = len(result.proposed_modules[module])
            lines.append(f"- `{module}` ({file_count} file{'s' if file_count != 1 else ''})")
    else:
        lines.append("None — no Python modules detected.")
    lines.append("")

    # Cross-module violations
    lines += ["## Cross-Module Imports (would become violations)", ""]
    if result.cross_module_imports:
        lines += ["| From | To | Count | Sample |", "| --- | --- | --- | --- |"]
        for src, tgt, count, sample in result.cross_module_imports:
            lines.append(f"| `{src}` | `{tgt}` | {count} | `{sample}` |")
    else:
        lines.append("None — no cross-module imports detected.")
    lines.append("")

    # Shared tables
    lines += ["## Shared Tables (need ownership decisions)", ""]
    if result.shared_tables:
        lines += [f"- `{table}`" for table in result.shared_tables]
    else:
        lines.append("None — no tables referenced from multiple modules.")
    lines.append("")

    # Listener candidates
    lines += ["## Listener-Shaped Code (events conversion candidates)", ""]
    if result.listener_candidates:
        lines += [f"- `{path}`" for path in result.listener_candidates]
    else:
        lines.append("None detected.")
    lines.append("")

    # Next steps
    lines += [
        "## Recommended Next Steps",
        "",
        "1. Run `modulith verify --mode=ratchet --update-baseline` to grandfather "
        "existing violations.",
        "2. Convert the top cross-module import patterns above into events. See the "
        "migration guide.",
        "3. Add a `_manifest.py` to each module declaring its published/consumed "
        "events, listeners, and owned tables.",
        "",
    ]

    return "\n".join(lines) + "\n"


__all__ = [
    "AuditResult",
    "audit_codebase",
    "render_report",
]
