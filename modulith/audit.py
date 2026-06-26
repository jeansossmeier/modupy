"""The `modulith audit` command implementation.

Analyzes an existing Python codebase non-destructively. Produces a
Markdown report that a team can read on Friday afternoon, generate a
baseline from on Monday, and start tightening over weeks.

This is the single biggest adoption lever for brownfield projects.
Without it, modulith is "for new projects only" — which kills the
addressable market.

Implementation status: SKELETON. ~150 lines when complete.

Output sections (in priority order):

  1. Executive summary: readiness score, biggest issues
  2. Proposed module structure: based on import patterns
  3. Cross-module imports that would become violations
  4. Shared database tables that need ownership decisions
  5. Listener-shaped patterns already in the code (for events conversion)
  6. Recommended migration steps
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("modulith.audit")


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

    # Tables found in SQLAlchemy declarations or query strings
    shared_tables: list[str]

    # Functions that look like they could become event listeners
    # (callbacks, observers, signal handlers in Django/Flask code)
    listener_candidates: list[Path]


def audit_codebase(root: Path) -> AuditResult:
    """Run the full audit pipeline.

    IMPLEMENTATION TODO:
    1. Walk root.rglob("*.py"), skip __pycache__, .venv, etc.
    2. For each file, parse with ast.parse.
    3. Run analyses:
       - _propose_module_structure: group files by directory + import similarity
       - _find_cross_module_imports: imports between proposed modules
       - _find_shared_tables: SQLAlchemy table declarations + raw SQL strings
       - _find_listener_candidates: functions with names like on_*, handle_*,
         or decorated with signal-handler patterns from Django/Flask
    4. Compute readiness_score:
       readiness = 100 * (event_like_calls / total_cross_module_calls)
       where event-like = via signals, callbacks, or already-existing
       message bus patterns. Direct function calls drop the score.
    5. Return AuditResult.
    """
    raise NotImplementedError("Phase 2 — see TODO above")


# ---------------------------------------------------------------------------
# Analysis pipeline stages
# ---------------------------------------------------------------------------


def _propose_module_structure(files: list[Path]) -> dict[str, list[Path]]:
    """Suggest a module structure based on observed code organization.

    IMPLEMENTATION TODO:
    Heuristics:
      - Top-level directories under the package are module candidates
      - Files frequently imported together belong in one module
      - Run agglomerative clustering on the import graph if no clear
        directory structure exists

    Returns: {proposed_module_name: [files_belonging_to_it]}
    """
    raise NotImplementedError("Phase 2")


def _find_cross_module_imports(
    files: list[Path], modules: dict[str, list[Path]]
) -> list[tuple[str, str, int, Path]]:
    """Find imports that cross proposed module boundaries.

    IMPLEMENTATION TODO:
    1. Build file_to_module map.
    2. For each file, parse imports.
    3. For each import target, check if it's in a different module.
    4. Aggregate counts: (source_module, target_module) -> count.
    5. Pick a sample file demonstrating each pattern.
    6. Return sorted by count desc — the worst offenders first.
    """
    raise NotImplementedError("Phase 2")


def _find_shared_tables(files: list[Path]) -> list[str]:
    """Identify database tables touched by multiple modules.

    IMPLEMENTATION TODO:
    Two strategies:
      1. SQLAlchemy declarative: find Table() and __tablename__ definitions.
      2. Raw SQL strings: regex-match FROM/UPDATE/INSERT INTO patterns.
    Both are approximate. False positives are fine for an audit; we just
    want to surface things to look at.
    """
    raise NotImplementedError("Phase 2")


def _find_listener_candidates(files: list[Path]) -> list[Path]:
    """Find functions that look event-listener-shaped.

    IMPLEMENTATION TODO:
    Heuristics for "this could be a @listener":
      - Function name starts with "on_", "handle_", "process_"
      - Decorated with django signals (@receiver), Flask signals, etc.
      - Function takes a single dataclass-shaped argument
    """
    raise NotImplementedError("Phase 2")


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def render_report(result: AuditResult) -> str:
    """Render an AuditResult as Markdown.

    IMPLEMENTATION TODO:
    Output structure:

        # Modulith Audit Report

        Generated for: <root>
        Date: <today>

        ## Summary

        **Readiness score: 47/100**

        Your codebase is approximately 47% ready for modulith adoption.
        Top blockers:
          - 23 cross-module direct imports (should be events)
          - 5 shared database tables (need ownership decisions)
          - 0 manifest files (recommended for production)

        ## Proposed Module Structure
        ...

        ## Cross-Module Violations
        Sorted by frequency. Each shows source, target, count, sample file.

        ## Recommended Next Steps

        1. Run `modulith verify --mode=ratchet --update-baseline` to
           grandfather existing violations
        2. Pick the top 3 cross-module call patterns and convert them
           to events. See [migration guide].
        3. Add `_manifest.py` files declaring each module's contract.
    """
    raise NotImplementedError("Phase 2")


__all__ = [
    "AuditResult",
    "audit_codebase",
    "render_report",
]
