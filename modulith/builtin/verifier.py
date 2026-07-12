"""Built-in boundary verifier.

Walks every ``.py`` file under each application module with ``ast.parse()``,
collects imports (and table references), and emits ``Violation``s for any
cross-module access that breaks the rules. Source is parsed, never executed.

The six default rules:
  1. No cross-module *private* imports — ``myapp.orders`` cannot import from
     ``myapp.inventory._internal`` (or any other ``_``-prefixed subpackage).
  2. No cyclic dependencies — the module dependency graph must be a DAG
     (``detect_cycles``, run once over all modules by the CLI).
  3. Declared dependencies match observed — when a manifest declares
     ``declared_dependencies`` (any value other than None), only those
     modules (plus ``contracts``) may be imported. A manifest that never
     declares the field (None) leaves the rule off; an explicit empty
     tuple means "depends on nothing".
  4. Cross-module *type* imports must come from the ``contracts`` module, not
     another module's package (uppercase-name heuristic). Wildcard
     cross-module imports are flagged outright — they cannot be resolved to
     specific names.
  5. Module data ownership — when manifests declare ``owns_tables``, a
     ``Table("x")`` reference in another module is flagged (best-effort,
     warning; full coverage is v1.1 with runtime SQLAlchemy events).
     Conflicting ownership declarations (two manifests claiming the same
     table) are surfaced as their own warning.
  6. Contracts is a sink — everyone may import from the contracts module;
     it may not import from any application module (SPEC 5.3).

``if TYPE_CHECKING:`` imports are collected too (tagged ``type_only``) and
checked by the boundary rules (1, 3, 4) — wrapping an import in the guard
must not bypass encapsulation. They are exempt only from cycle detection
(rule 2): a type-only import imposes no runtime dependency and is the
sanctioned idiom for breaking a runtime import cycle.

Ratcheting mode: existing violations get grandfathered via a baseline file;
only new violations fail the build.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import logging
from collections.abc import Collection
from dataclasses import dataclass, field
from pathlib import Path

from modulith import ModuleInfo, Violation, hookimpl
from modulith.types import ViolationSeverity

logger = logging.getLogger("modulith.verifier")

# The default name of the shared-contracts module. A convention, not a
# hardcode — the actual name is read from config (see _configured_contracts_module).
CONTRACTS_MODULE = "contracts"


def _configured_contracts_module() -> str:
    """The configured contracts-module name, or the default convention.

    Read lazily from the runtime config (mirrors observability.py) so the
    rules honor ``[tool.modulith].contracts_module`` without the hookspec
    needing to carry config. Falls back to the default when the runtime
    isn't bootstrapped — e.g. when a rule function is called directly in a
    unit test.
    """
    try:
        from ..runtime import _runtime

        cfg = _runtime.config
    except Exception:  # pragma: no cover - defensive; runtime import is stable
        return CONTRACTS_MODULE
    return cfg.contracts_module if cfg is not None else CONTRACTS_MODULE


# ---------------------------------------------------------------------------
# The hook entrypoint
# ---------------------------------------------------------------------------


@hookimpl
def modulith_verify_module(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Run the per-module default rules (1, 3, 4, 5, 6) against a module.

    Aggregating hook: returns one module's violations; other plugins run
    alongside and contribute their own. Cycle detection (rule 2) is global
    and runs once via ``detect_cycles`` — not here, to avoid N-fold
    duplication across per-module calls.
    """
    violations: list[Violation] = []
    imports = _collect_imports(module)
    contracts_module = _configured_contracts_module()

    violations.extend(_check_no_internal_imports(module, imports, all_modules))
    violations.extend(_check_uses_contracts_module(module, imports, all_modules, contracts_module))
    violations.extend(_check_declared_dependencies(module, imports, all_modules, contracts_module))
    violations.extend(_check_data_ownership(module, all_modules))
    violations.extend(_check_contracts_is_sink(module, imports, all_modules, contracts_module))

    return violations


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

    # Locally-bound names, parallel to imported_names (``asname or name``).
    # Rule 4 matches runtime usage against these so an aliased import
    # (``from x import Y as Z``) is tracked under the name the file
    # actually uses (A10-r2-96).
    local_names: list[str] = field(default_factory=list)

    # True when the import sits inside an ``if TYPE_CHECKING:`` block.
    # Boundary rules (1, 3, 4) still check these; cycle detection (rule 2)
    # skips them — a type-only import imposes no runtime dependency and is
    # the sanctioned idiom for breaking a runtime cycle (A10-r1-34,
    # A10-r3-146).
    type_only: bool = False


def _package_dir(package: str) -> Path | None:
    """Resolve a package's on-disk directory without executing its code."""
    try:
        spec = importlib.util.find_spec(package)
    except (ImportError, ModuleNotFoundError, ValueError):
        return None
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(next(iter(spec.submodule_search_locations)))


def _is_type_checking(test: ast.expr, aliases: Collection[str] = ("TYPE_CHECKING",)) -> bool:
    """True for ``if TYPE_CHECKING:`` guards.

    Recognizes the bare name, any ``<mod>.TYPE_CHECKING`` attribute, and —
    via *aliases* — names bound by ``from typing import TYPE_CHECKING as TC``
    (A10-r5-217: an unresolved alias made the guard invisible, so guarded
    imports were treated as unconditional runtime imports).
    """
    if isinstance(test, ast.Name):
        return test.id in aliases
    if isinstance(test, ast.Attribute):
        return test.attr == "TYPE_CHECKING"
    return False


def _type_checking_aliases(node: ast.ImportFrom) -> list[str]:
    """Aliases bound to ``typing.TYPE_CHECKING`` by this import, if any."""
    if node.level or node.module != "typing":
        return []
    return [a.asname for a in node.names if a.name == "TYPE_CHECKING" and a.asname]


def _file_package(root: Path, root_package: str, path: Path) -> str:
    """The package containing ``path`` (base for resolving relative imports)."""
    rel_parts = path.relative_to(root).parts
    dir_parts = rel_parts[:-1]  # drop the filename
    if dir_parts:
        return ".".join([root_package, *dir_parts])
    return root_package


class _ImportCollector(ast.NodeVisitor):
    """Collect Import/ImportFrom records, tagging TYPE_CHECKING-guarded ones.

    Imports inside ``if TYPE_CHECKING:`` blocks are collected with
    ``type_only=True`` so the boundary rules (1, 3, 4) still see them —
    wrapping an import in the guard must not bypass encapsulation
    (A10-r1-34, A10-r3-146) — while cycle detection (rule 2) can skip them.
    """

    def __init__(self, source_file: Path, file_package: str) -> None:
        self.records: list[ImportRecord] = []
        self.source_file = source_file
        self.file_package = file_package
        self._tc_aliases: set[str] = {"TYPE_CHECKING"}
        self._type_only_depth = 0

    def visit_If(self, node: ast.If) -> None:
        if _is_type_checking(node.test, self._tc_aliases):
            self._type_only_depth += 1
            for child in node.body:
                self.visit(child)
            self._type_only_depth -= 1
            for child in node.orelse:  # the else-branch is still runtime code
                self.visit(child)
            return
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.records.append(
                ImportRecord(
                    self.source_file,
                    node.lineno,
                    alias.name,
                    [],
                    local_names=[],
                    type_only=self._type_only_depth > 0,
                )
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self._tc_aliases.update(_type_checking_aliases(node))
        target = self._resolve(node)
        names = [a.name for a in node.names]
        locals_ = [a.asname or a.name for a in node.names]
        self.records.append(
            ImportRecord(
                self.source_file,
                node.lineno,
                target,
                names,
                local_names=locals_,
                type_only=self._type_only_depth > 0,
            )
        )

    def _resolve(self, node: ast.ImportFrom) -> str:
        if not node.level:  # absolute import
            return node.module or ""
        base_parts = self.file_package.split(".")
        if node.level > 1:
            base_parts = base_parts[: -(node.level - 1)]
        base = ".".join(base_parts)
        return f"{base}.{node.module}" if node.module else base


def _collect_imports(module: ModuleInfo) -> list[ImportRecord]:
    """Parse every ``.py`` file under the module's package; return imports."""
    root = _package_dir(module.package)
    if root is None:
        logger.debug("could not resolve package dir for %s", module.package)
        return []

    records: list[ImportRecord] = []
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError) as exc:
            logger.warning("skipping unparseable file %s: %s", path, exc)
            continue
        collector = _ImportCollector(path, _file_package(root, module.package, path))
        collector.visit(tree)
        records.extend(collector.records)
    return records


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _owning_module(target: str, all_modules: list[ModuleInfo]) -> ModuleInfo | None:
    """The application module a dotted import path belongs to, if any."""
    best: ModuleInfo | None = None
    for mod in all_modules:
        if target == mod.package or target.startswith(mod.package + "."):
            # Prefer the most specific (longest) package match.
            if best is None or len(mod.package) > len(best.package):
                best = mod
    return best


def _location(record: ImportRecord) -> str:
    return f"{record.source_file}:{record.line}"


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------


def _check_no_internal_imports(
    module: ModuleInfo,
    imports: list[ImportRecord],
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Rule 1: cannot import another module's private (``_``-prefixed) package."""
    violations: list[Violation] = []
    for record in imports:
        owner = _owning_module(record.target_module, all_modules)
        if owner is None or owner.package == module.package:
            continue
        remainder = record.target_module[len(owner.package) + 1 :]
        first_segment = remainder.split(".", 1)[0] if remainder else ""
        if first_segment.startswith("_"):
            violations.append(
                Violation(
                    rule="no-internal-imports",
                    message=(
                        f"{module.name} imports {record.target_module!r}, reaching into "
                        f"{owner.name}'s private package. Cross-module access must go "
                        f"through {owner.name}'s public API."
                    ),
                    module=module.name,
                    location=_location(record),
                )
            )
    return violations


class _NameUsageCollector(ast.NodeVisitor):
    """Separate names used in *runtime* positions from those in annotations.

    A name appearing only in a type annotation (``x: PaymentStatus``) — or
    imported but unused — is a *type* import. A name loaded at runtime (raised,
    compared, instantiated, ``Status.PAID``) is a runtime value. Rule 4 should
    flag the former and leave the latter alone.
    """

    def __init__(self) -> None:
        self.runtime_names: set[str] = set()
        self._tc_aliases: set[str] = {"TYPE_CHECKING"}

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        # Track TYPE_CHECKING aliases so visit_If recognizes guarded blocks.
        self._tc_aliases.update(_type_checking_aliases(node))

    def visit_If(self, node: ast.If) -> None:
        # Code inside ``if TYPE_CHECKING:`` never executes, so names
        # referenced there are NOT runtime uses — counting them let an
        # unrelated guarded reference exempt a real annotation-only import
        # from rule 4 (A10-r4-185). The else-branch is runtime code.
        if _is_type_checking(node.test, self._tc_aliases):
            for child in node.orelse:
                self.visit(child)
            return
        self.generic_visit(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        # Argument/return annotations are NOT runtime uses — skip them so an
        # annotation-only type still gets flagged. Everything else is runtime.
        for decorator in node.decorator_list:
            self.visit(decorator)
        args = node.args
        for default in (*args.defaults, *args.kw_defaults):
            if default is not None:
                self.visit(default)
        for stmt in node.body:
            self.visit(stmt)

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        # The annotation is a type position; the assigned value (if any) is runtime.
        if node.value is not None:
            self.visit(node.value)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.runtime_names.add(node.id)


def _runtime_loaded_names(module: ModuleInfo) -> set[str]:
    """Names a module loads at runtime, outside type-annotation positions."""
    root = _package_dir(module.package)
    if root is None:
        return set()
    names: set[str] = set()
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        # Fresh collector per file: TYPE_CHECKING aliases are file-scoped.
        collector = _NameUsageCollector()
        collector.visit(tree)
        names |= collector.runtime_names
    return names


def _check_uses_contracts_module(
    module: ModuleInfo,
    imports: list[ImportRecord],
    all_modules: list[ModuleInfo],
    contracts_module: str = CONTRACTS_MODULE,
) -> list[Violation]:
    """Rule 4: cross-module *type* imports must come from the contracts module.

    The "is it a type?" heuristic is an uppercase leading character, but that
    alone wrongly flags runtime classes/enums/exceptions (``PaymentError``,
    ``PaymentStatus``) imported from a legitimate dependency. So a candidate is
    only flagged when it is NOT used at runtime in this module — i.e. it appears
    only in annotations, or is imported but unused. A name raised, compared, or
    instantiated is a runtime value, not a shared type, and is left alone.

    Runtime usage is matched against the *locally-bound* name (``asname or
    name``) so aliased imports are classified by the name the file actually
    uses (A10-r2-96). Wildcard imports cannot be resolved to specific names,
    so the ``import *`` itself is flagged (A10-r3-147).
    """
    violations: list[Violation] = []
    runtime_names: set[str] | None = None  # computed lazily on first candidate
    for record in imports:
        owner = _owning_module(record.target_module, all_modules)
        if owner is None or owner.package == module.package or owner.name == contracts_module:
            continue
        if "*" in record.imported_names:
            violations.append(
                Violation(
                    rule="use-contracts",
                    message=(
                        f"{module.name} imports * from {owner.name}. Wildcard "
                        f"cross-module imports hide which names are shared — import "
                        f"explicitly, with shared types coming from the "
                        f"{contracts_module!r} module."
                    ),
                    module=module.name,
                    location=_location(record),
                )
            )
            continue
        pairs = (
            list(zip(record.imported_names, record.local_names, strict=True))
            if len(record.local_names) == len(record.imported_names)
            else [(n, n) for n in record.imported_names]
        )
        candidates = [(orig, local) for orig, local in pairs if orig[:1].isupper()]
        if not candidates:
            continue
        if runtime_names is None:
            runtime_names = _runtime_loaded_names(module)
        type_names = [orig for orig, local in candidates if local not in runtime_names]
        if type_names:
            violations.append(
                Violation(
                    rule="use-contracts",
                    message=(
                        f"{module.name} imports type(s) {', '.join(type_names)} from "
                        f"{owner.name}. Shared types must live in the "
                        f"{contracts_module!r} module so modules depend on contracts, "
                        f"not each other's internals."
                    ),
                    module=module.name,
                    location=_location(record),
                )
            )
    return violations


def _check_declared_dependencies(
    module: ModuleInfo,
    imports: list[ImportRecord],
    all_modules: list[ModuleInfo],
    contracts_module: str = CONTRACTS_MODULE,
) -> list[Violation]:
    """Rule 3: imports must match the manifest's declared_dependencies.

    The rule only applies when the manifest *declared* the field: None (the
    default — e.g. a manifest added just for ``owns_tables``) leaves the
    rule off, while an explicit empty tuple means "depends on nothing" and
    enforces deny-all, contracts excepted (A10-r1-36, adjudicated).

    The violation message deliberately does NOT embed the module's current
    declared_dependencies list: the ratchet baseline hashes the message, so
    embedding module-wide state would reopen every grandfathered rule-3
    violation whenever any one dependency is added (A10-r2-97).
    """
    from modulith.manifest import get_manifest

    manifest = get_manifest(module.package)
    if manifest is None or manifest.declared_dependencies is None:
        return []

    allowed = set(manifest.declared_dependencies) | {contracts_module}
    violations: list[Violation] = []
    for record in imports:
        owner = _owning_module(record.target_module, all_modules)
        if owner is None or owner.package == module.package:
            continue
        if owner.name not in allowed:
            violations.append(
                Violation(
                    rule="undeclared-dependency",
                    message=(
                        f"{module.name} imports from {owner.name}, which is not in its "
                        f"declared_dependencies. Add {owner.name!r} to the manifest or "
                        f"remove the import."
                    ),
                    module=module.name,
                    location=_location(record),
                )
            )
    return violations


# ---------------------------------------------------------------------------
# Rule 5: data ownership (best-effort)
# ---------------------------------------------------------------------------


def _collect_table_refs(module: ModuleInfo) -> list[tuple[str, str]]:
    """Find table references; return (table_name, location).

    Detects both SQLAlchemy Core ``Table("name")`` calls and the declarative
    ORM ``__tablename__ = "name"`` assignment (the dominant pattern). Mirrors
    the audit tool's ``_string_table_refs`` so the verifier and audit agree on
    what counts as a table reference — without it, ownership violations on
    declarative models went undetected.
    """
    root = _package_dir(module.package)
    if root is None:
        return []
    refs: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = (
                    func.id
                    if isinstance(func, ast.Name)
                    else (func.attr if isinstance(func, ast.Attribute) else None)
                )
                if name == "Table" and node.args:
                    first = node.args[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        refs.append((first.value, f"{path}:{node.lineno}"))
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if (
                        isinstance(target, ast.Name)
                        and target.id == "__tablename__"
                        and isinstance(node.value, ast.Constant)
                        and isinstance(node.value.value, str)
                    ):
                        refs.append((node.value.value, f"{path}:{node.lineno}"))
    return refs


def _check_data_ownership(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Rule 5: flag references to tables owned by another module (warning).

    Ownership is tracked as table -> *set* of declaring modules: two
    manifests claiming the same table is a manifest-authoring conflict that
    must be surfaced, not silently resolved last-write-wins — which both
    hid the conflict and falsely flagged the first-declared owner
    (A10-r3-149). A module that co-declared ownership is never flagged for
    referencing the table; the conflict itself is reported instead.
    """
    from modulith.manifest import all_manifests

    manifests = all_manifests()
    if not manifests:
        return []

    # table name -> set of module names declaring ownership
    owners_of: dict[str, set[str]] = {}
    pkg_to_name = {m.package: m.name for m in all_modules}
    for pkg, manifest in manifests.items():
        owner_name = pkg_to_name.get(pkg, pkg.rsplit(".", 1)[-1])
        for table in manifest.owns_tables:
            owners_of.setdefault(table, set()).add(owner_name)

    violations: list[Violation] = []
    for table, claimants in sorted(owners_of.items()):
        if len(claimants) > 1 and module.name in claimants:
            names = ", ".join(sorted(claimants))
            violations.append(
                Violation(
                    rule="data-ownership",
                    message=(
                        f"table {table!r} is declared in owns_tables by multiple "
                        f"modules: {names}. Exactly one module may own a table — "
                        f"resolve the manifest conflict."
                    ),
                    module=module.name,
                    severity=ViolationSeverity.WARNING,
                )
            )

    for table, location in _collect_table_refs(module):
        owners = owners_of.get(table)
        if owners is None or module.name in owners:
            continue
        owner_text = " and ".join(sorted(owners))
        violations.append(
            Violation(
                rule="data-ownership",
                message=(
                    f"{module.name} references table {table!r}, owned by {owner_text}. "
                    f"Access another module's data through its events or public API "
                    f"(best-effort static check)."
                ),
                module=module.name,
                severity=ViolationSeverity.WARNING,
                location=location,
            )
        )
    return violations


# ---------------------------------------------------------------------------
# Rule 6: contracts is an import sink
# ---------------------------------------------------------------------------


def _check_contracts_is_sink(
    module: ModuleInfo,
    imports: list[ImportRecord],
    all_modules: list[ModuleInfo],
    contracts_module: str = CONTRACTS_MODULE,
) -> list[Violation]:
    """Rule 6: the contracts module may not import from application modules.

    SPEC 5.3 treats contracts as a sink: everyone may import from it; it
    may not import from any module. None of rules 1/3/4 covers the reverse
    direction (their heuristics gate on privacy, manifests, and type-shaped
    names), so an ordinary runtime import from e.g. ``orders`` into
    contracts sailed through undetected (A10-r1-35). This rule flags ANY
    import — runtime or type-only — whose owner is another application
    module when the module under check IS the contracts module.
    """
    if module.name != contracts_module:
        return []
    violations: list[Violation] = []
    for record in imports:
        owner = _owning_module(record.target_module, all_modules)
        if owner is None or owner.package == module.package:
            continue
        violations.append(
            Violation(
                rule="contracts-is-sink",
                message=(
                    f"{contracts_module} imports from {owner.name}. The contracts "
                    f"module is a dependency sink: any module may import from it, "
                    f"but it may not import from application modules — move the "
                    f"shared code into {contracts_module!r} or invert the dependency."
                ),
                module=module.name,
                location=_location(record),
            )
        )
    return violations


# ---------------------------------------------------------------------------
# Cycle detection (Rule 2) — Tarjan's strongly connected components
# ---------------------------------------------------------------------------


def detect_cycles(all_modules: list[ModuleInfo]) -> list[Violation]:
    """Find cyclic dependencies in the module graph (one Violation per cycle)."""
    names = {m.name for m in all_modules}
    graph: dict[str, set[str]] = {m.name: set() for m in all_modules}
    for mod in all_modules:
        for record in _collect_imports(mod):
            if record.type_only:
                # TYPE_CHECKING-guarded imports impose no runtime dependency
                # and are the sanctioned way to break a runtime cycle — no
                # graph edge (they ARE still checked by rules 1, 3, 4).
                continue
            owner = _owning_module(record.target_module, all_modules)
            if owner is not None and owner.name != mod.name and owner.name in names:
                graph[mod.name].add(owner.name)

    sccs = _tarjan_scc(graph)
    violations: list[Violation] = []
    for scc in sccs:
        if len(scc) > 1:
            members = ", ".join(sorted(scc))
            violations.append(
                Violation(
                    rule="no-cyclic-dependency",
                    message=(
                        f"cyclic dependency among modules: {members}. The module "
                        f"dependency graph must be acyclic — break the cycle by moving "
                        f"shared types to contracts or inverting a dependency via events."
                    ),
                    module=sorted(scc)[0],
                )
            )
    return violations


def _tarjan_scc(graph: dict[str, set[str]]) -> list[set[str]]:
    """Tarjan's SCC algorithm (iterative to avoid recursion limits)."""
    index_counter = 0
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    result: list[set[str]] = []

    for start in graph:
        if start in indices:
            continue
        # work stack of (node, iterator over successors)
        work: list[tuple[str, list[str]]] = [(start, sorted(graph[start]))]
        indices[start] = lowlink[start] = index_counter
        index_counter += 1
        stack.append(start)
        on_stack.add(start)

        while work:
            node, successors = work[-1]
            progressed = False
            while successors:
                succ = successors.pop(0)
                if succ not in indices:
                    indices[succ] = lowlink[succ] = index_counter
                    index_counter += 1
                    stack.append(succ)
                    on_stack.add(succ)
                    work.append((succ, sorted(graph[succ])))
                    progressed = True
                    break
                if succ in on_stack:
                    lowlink[node] = min(lowlink[node], indices[succ])
            if progressed:
                continue
            # all successors processed — finalize this node
            work.pop()
            if work:
                parent = work[-1][0]
                lowlink[parent] = min(lowlink[parent], lowlink[node])
            if lowlink[node] == indices[node]:
                component: set[str] = set()
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    component.add(w)
                    if w == node:
                        break
                result.append(component)
    return result


# ---------------------------------------------------------------------------
# Ratcheting: baseline file management
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BaselineEntry:
    """A grandfathered violation, identified by a stable hash."""

    rule: str
    module: str
    location: str  # source file only — line stripped, see _normalize_location
    message_hash: str  # first 8 chars of sha256(message)


def _normalize_location(location: str | None) -> str:
    """Reduce a ``file:line`` location to just the file.

    The exact line is presentation metadata, not identity: any unrelated
    edit above a grandfathered violation shifts its line, and a line-exact
    fingerprint would resurrect it as a "new" failing violation
    (A10-r1-33). Identity therefore uses the file path only.
    """
    if not location:
        return ""
    head, sep, tail = location.rpartition(":")
    if sep and tail.isdigit():
        return head
    return location


def _entry_for(violation: Violation) -> BaselineEntry:
    digest = hashlib.sha256(violation.message.encode("utf-8")).hexdigest()[:8]
    return BaselineEntry(
        rule=violation.rule,
        module=violation.module,
        location=_normalize_location(violation.location),
        message_hash=digest,
    )


def load_baseline(path: Path) -> set[BaselineEntry]:
    """Read the baseline file; return the set of grandfathered violations.

    A corrupted or schema-mismatched file raises ConfigurationError naming
    the path and the regeneration command — never a raw JSONDecodeError or
    KeyError traceback (A10-r5-219). Legacy entries carrying ``file:line``
    locations are normalized on load so old baselines keep matching.
    """
    from modulith.config import ConfigurationError

    if not path.exists():
        return set()
    hint = "regenerate it with `modulith verify --mode=ratchet --update-baseline`"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigurationError(f"baseline file {path} is not valid JSON ({exc}); {hint}") from exc
    if not isinstance(data, list):
        raise ConfigurationError(
            f"baseline file {path} must contain a JSON list of entries, "
            f"got {type(data).__name__}; {hint}"
        )
    entries: set[BaselineEntry] = set()
    for item in data:
        if not isinstance(item, dict):
            raise ConfigurationError(
                f"baseline file {path} contains a non-object entry {item!r}; {hint}"
            )
        try:
            rule = item["rule"]
            module = item["module"]
            location = item["location"]
            message_hash = item["message_hash"]
        except KeyError as exc:
            raise ConfigurationError(
                f"baseline file {path} has an entry missing key {exc}; {hint}"
            ) from exc
        if not all(isinstance(v, str) for v in (rule, module, location, message_hash)):
            raise ConfigurationError(
                f"baseline file {path} has an entry with non-string fields; {hint}"
            )
        entries.add(
            BaselineEntry(
                rule=rule,
                module=module,
                location=_normalize_location(location),
                message_hash=message_hash,
            )
        )
    return entries


def filter_against_baseline(
    violations: list[Violation],
    baseline: set[BaselineEntry],
) -> list[Violation]:
    """Return only violations not present in the baseline."""
    return [v for v in violations if _entry_for(v) not in baseline]


def write_baseline(path: Path, violations: list[Violation]) -> None:
    """Write the current violation set as a new baseline (stable JSON)."""
    entries = sorted(
        (_entry_for(v) for v in violations),
        key=lambda e: (e.rule, e.module, e.location, e.message_hash),
    )
    payload = [
        {
            "rule": e.rule,
            "module": e.module,
            "location": e.location,
            "message_hash": e.message_hash,
        }
        for e in entries
    ]
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


__all__ = [
    "BaselineEntry",
    "ImportRecord",
    "detect_cycles",
    "filter_against_baseline",
    "load_baseline",
    "modulith_verify_module",
    "write_baseline",
]
