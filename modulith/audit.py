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
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .builtin.verifier import CONTRACTS_MODULE, _file_package, _ImportCollector

logger = logging.getLogger("modulith.audit")

# Directories that never contain application source worth auditing.
_SKIP_DIRS = frozenset(
    {"__pycache__", ".venv", "venv", ".git", ".tox", "build", "dist", "node_modules", ".mypy_cache"}
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


def audit_codebase(root: Path) -> AuditResult:
    """Run the full audit pipeline over a directory tree.

    Walks ``root`` for ``.py`` files, parses each with ``ast``, and derives
    the proposed module structure, cross-module imports, shared tables,
    listener candidates, and a readiness score.
    """
    files = _iter_py_files(root)
    proposed = _propose_module_structure(root, files)
    cross = _find_cross_module_imports(root, files, proposed)
    shared = _find_shared_tables(root, files)
    listeners = _find_listener_candidates(files)
    score = _compute_readiness_score(cross, listeners)
    return AuditResult(
        readiness_score=score,
        proposed_modules=proposed,
        cross_module_imports=cross,
        shared_tables=shared,
        listener_candidates=listeners,
    )


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


def _target_module(dotted: str, root_package: str, module_names: set[str]) -> str | None:
    """Map a dotted import path to a proposed module name, if it is one."""
    parts = dotted.split(".")
    if not parts or not parts[0]:
        return None
    candidate = parts[1] if (parts[0] == root_package and len(parts) > 1) else parts[0]
    return candidate if candidate in module_names else None


def _find_cross_module_imports(
    root: Path, files: list[Path], modules: dict[str, list[Path]]
) -> list[tuple[str, str, int, Path]]:
    """Find imports that cross proposed module boundaries.

    Returns ``(source_module, target_module, count, sample_file)`` tuples,
    worst offenders (highest count) first.
    """
    module_names = set(modules)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    samples: dict[tuple[str, str], Path] = {}

    for path in files:
        tree = _parse(path)
        if tree is None:
            continue
        source_module = _module_of(root, path)
        collector = _ImportCollector(path, _file_package(root, root.name, path))
        collector.visit(tree)
        for record in collector.records:
            target = _target_module(record.target_module, root.name, module_names)
            if target is None or target == source_module:
                continue
            # The conventional ``contracts`` module is the sanctioned shared
            # dependency that survives a process split — importing it is the
            # prescribed pattern, so it must not count as coupling (mirrors the
            # verifier's rule-4 exemption and the doctor readiness metric).
            if target == CONTRACTS_MODULE:
                continue
            key = (source_module, target)
            counts[key] += 1
            samples.setdefault(key, path)

    edges = [(src, tgt, count, samples[(src, tgt)]) for (src, tgt), count in counts.items()]
    edges.sort(key=lambda e: (-e[2], e[0], e[1]))
    return edges


def _string_table_refs(tree: ast.Module) -> set[str]:
    """Table names referenced in a module via ``Table("x")`` or ``__tablename__``."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            fname = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if fname == "Table" and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    names.add(first.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "__tablename__"
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                ):
                    names.add(node.value.value)
    return names


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
    is already modular, so it scores 100.
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
        f"Your codebase is approximately {result.readiness_score}% ready for "
        "modulith adoption. Top blockers:",
        "",
        f"  - {len(result.cross_module_imports)} cross-module import pattern(s) "
        "(should become events)",
        f"  - {len(result.shared_tables)} shared database table(s) (need ownership decisions)",
        "",
    ]

    # Proposed module structure
    lines += ["## Proposed Module Structure", ""]
    for module in sorted(result.proposed_modules):
        file_count = len(result.proposed_modules[module])
        lines.append(f"- `{module}` ({file_count} file{'s' if file_count != 1 else ''})")
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
