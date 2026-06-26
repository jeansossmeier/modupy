"""Built-in boundary verifier.

Walks every .py file under the application package with ast.parse(),
collects imports, and emits Violations for any cross-module access
that breaks the rules.

Implementation status: SKELETON. ~200 lines when complete; will likely
need to split across multiple files (rules.py + ast_walker.py + baseline.py).

The five default rules:
  1. No cross-module internal imports — `myapp.orders` cannot import
     from `myapp.inventory._internal.*`.
  2. No cyclic dependencies — module dependency graph must be a DAG.
  3. Declared dependencies match observed — when a manifest declares
     dependencies, only those modules may be imported.
  4. Events flow through contracts module — cross-module type imports
     come from `myapp.contracts.*`, not other modules' packages.
  5. Module data ownership — when manifests declare `owns_tables`,
     queries against another module's tables are violations (best-effort
     static analysis; runtime enforcement is v1.1).

Ratcheting mode: existing violations get grandfathered via a baseline
file. Only new violations fail the build.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from modulith import ModuleInfo, Violation, hookimpl

logger = logging.getLogger("modulith.verifier")


# ---------------------------------------------------------------------------
# The hook entrypoint
# ---------------------------------------------------------------------------


@hookimpl
def modulith_verify_module(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Run all default rules against a module.

    Aggregating hook: this returns one module's violations. Other plugins
    can run alongside us and contribute their own.

    IMPLEMENTATION:
    1. Collect AST-derived imports from every .py file under
       module.package (use _collect_imports below).
    2. Build a set of (other_module_package -> imported_names) mappings.
    3. Apply each rule, collecting violations.
    4. Return the list.

    The full implementation should be split — see _check_no_internal_imports,
    _check_uses_contracts_module, etc. below.
    """
    violations: list[Violation] = []
    imports = _collect_imports(module)

    violations.extend(_check_no_internal_imports(module, imports, all_modules))
    violations.extend(_check_uses_contracts_module(module, imports, all_modules))
    violations.extend(_check_declared_dependencies(module, imports))

    return violations


# Cycle detection runs once over all modules, not per-module. We use a
# separate hookimpl that triggers from a different lifecycle event — TBD
# whether to add a dedicated "verify_all" hookspec or just check on the
# first module's verify call. Decision: add modulith_verify_all_modules
# hookspec in v1.1; for v1, run cycle check inline and dedupe by module.


# ---------------------------------------------------------------------------
# AST walking — collect imports from a module's source files
# ---------------------------------------------------------------------------


@dataclass
class ImportRecord:
    """One observed import statement."""

    source_file: Path
    line: int
    target_module: str  # full dotted path being imported
    imported_names: list[str]  # names brought in (empty for "import x")


def _collect_imports(module: ModuleInfo) -> list[ImportRecord]:
    """Walk every .py file in the module's package, return import records.

    IMPLEMENTATION TODO:
    1. Resolve module.package to a filesystem path via importlib.
    2. Use Path.rglob("*.py") to find all source files.
    3. For each file:
       a. Read source, parse with ast.parse(filename=str(path)).
       b. Walk the AST tree (ast.walk).
       c. For ast.Import nodes: each .names alias contributes an
          ImportRecord with target_module=alias.name, imported_names=[].
       d. For ast.ImportFrom nodes: target_module=node.module (resolve
          relative imports via node.level), imported_names=[a.name for
          a in node.names].
       e. Skip imports inside `if TYPE_CHECKING:` blocks (they're not
          runtime dependencies). Detect via parent being an If with the
          test resolving to TYPE_CHECKING.
    4. Return the flat list.

    Edge cases to handle:
    - Relative imports (level > 0): resolve to absolute via the source
      file's package. `from .. import foo` from `myapp/orders/handlers.py`
      → `myapp.foo`.
    - Star imports (`from x import *`): record imported_names=["*"]; the
      verifier treats these as importing everything.
    - Conditional imports (try/except ImportError): include them; better
      to flag a maybe-import than miss a real dependency.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------


def _check_no_internal_imports(
    module: ModuleInfo,
    imports: list[ImportRecord],
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Rule 1: cannot import from another module's _internal package.

    IMPLEMENTATION TODO:
    For each ImportRecord, check if target_module starts with
    `<other_module>._internal`. If yes, emit Violation with rule
    "no-internal-imports".
    """
    raise NotImplementedError("Phase 1 — see TODO above")


def _check_uses_contracts_module(
    module: ModuleInfo,
    imports: list[ImportRecord],
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Rule 4: cross-module type imports must come from contracts.

    IMPLEMENTATION TODO:
    For each ImportRecord with target_module starting with another
    module's package (and not the contracts module), check if any
    imported name looks like a type (starts with uppercase). If yes,
    emit Violation suggesting they move the type to contracts.

    Heuristic: type names start with uppercase, function names lowercase.
    Imperfect but reasonable. Users who hate the heuristic can disable
    this rule via [tool.modulith.verify.disabled_rules].
    """
    raise NotImplementedError("Phase 1 — see TODO above")


def _check_declared_dependencies(
    module: ModuleInfo,
    imports: list[ImportRecord],
) -> list[Violation]:
    """Rule 3: imports must match the manifest's declared_dependencies.

    IMPLEMENTATION TODO:
    1. Look up the manifest via get_manifest(module.package).
    2. If no manifest, skip (rule only applies to declared modules).
    3. For each ImportRecord targeting another module: check that the
       target's top-level module is in declared_dependencies. If not,
       emit Violation.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------


def detect_cycles(all_modules: list[ModuleInfo]) -> list[Violation]:
    """Find any cyclic dependencies in the module graph.

    IMPLEMENTATION TODO:
    1. Build adjacency dict: {module_name -> [imported_module_names]}.
       Use _collect_imports for each module.
    2. Run Tarjan's strongly connected components algorithm. Any SCC
       larger than 1 is a cycle.
    3. Emit one Violation per cycle, listing the modules involved.

    Standard library has no SCC; networkx does. Either ship networkx as
    a dependency or implement Tarjan inline (~30 lines).
    """
    raise NotImplementedError("Phase 1 — see TODO above")


# ---------------------------------------------------------------------------
# Ratcheting: baseline file management
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BaselineEntry:
    """A grandfathered violation, identified by a stable hash."""

    rule: str
    module: str
    location: str  # file:line
    message_hash: str  # first 8 chars of sha256(message)


def load_baseline(path: Path) -> set[BaselineEntry]:
    """Read the baseline file, return the set of grandfathered violations.

    IMPLEMENTATION TODO:
    1. If path doesn't exist, return empty set.
    2. Open as JSON, parse the array of {rule, module, location, hash}.
    3. Return as a set of BaselineEntry dataclasses.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


def filter_against_baseline(
    violations: list[Violation],
    baseline: set[BaselineEntry],
) -> list[Violation]:
    """Return only violations not present in the baseline.

    IMPLEMENTATION TODO:
    For each violation, compute its BaselineEntry (rule, module, location,
    message_hash). If in baseline, skip; otherwise include.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


def write_baseline(path: Path, violations: list[Violation]) -> None:
    """Write the current violation set as a new baseline.

    IMPLEMENTATION TODO:
    Convert each violation to a BaselineEntry, serialize to JSON with
    sorted keys (for stable diffs in git), write to path.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


__all__ = [
    "detect_cycles",
    "filter_against_baseline",
    "load_baseline",
    "modulith_verify_module",
    "write_baseline",
]
