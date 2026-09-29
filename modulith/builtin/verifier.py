"""Built-in boundary verifier.

Walks every ``.py`` file under each application module with ``ast.parse()``,
collects imports (and table references), and emits ``Violation``s for any
cross-module access that breaks the rules. Source is parsed, never executed.
``importlib.import_module(<literal>)`` and ``__import__(<literal>)`` calls
are collected alongside static imports; a dynamically computed target
(a variable, a call result) cannot be resolved statically and is an
undetected residual limitation, not a guarantee.

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
from collections import Counter
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from modulith import ModuleInfo, Violation, hookimpl
from modulith.types import ViolationSeverity

logger = logging.getLogger("modulith.verifier")

# The default name of the shared-contracts module. A convention, not a
# hardcode — the actual name is read from config (see _configured_contracts_module).
CONTRACTS_MODULE = "contracts"

# All rule names emitted by this module. Used to validate configured
# disabled_rules and warn about typos/unknown rule names.
RULE_NAMES: frozenset[str] = frozenset(
    {
        "parse-error",
        "no-internal-imports",
        "use-contracts",
        "undeclared-dependency",
        "data-ownership",
        "contracts-is-sink",
        "no-cyclic-dependency",
    }
)


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


def _configured_disabled_rules() -> frozenset[str]:
    """The rule names disabled via runtime config, or none.

    Mirrors ``_configured_contracts_module``: read lazily from the runtime
    config so ``[tool.modulith.verify].disabled_rules`` is honored.
    Logs a warning for any configured rule names not in RULE_NAMES.
    """
    try:
        from ..runtime import _runtime

        cfg = _runtime.config
    except Exception:  # pragma: no cover - defensive; runtime import is stable
        return frozenset()
    if cfg is None:
        return frozenset()
    disabled = frozenset(cfg.verify_disabled_rules)
    unknown = disabled - RULE_NAMES
    if unknown:
        unknown_str = ", ".join(sorted(unknown))
        known_str = ", ".join(sorted(RULE_NAMES))
        logger.warning(f"disabled_rules names no known rule: {unknown_str} (known: {known_str})")
    return disabled


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

    Each rule (other than the parse-error diagnostic) is skipped when its
    name is in ``_configured_disabled_rules()`` — see that function's
    docstring for the config surface this honors.
    """
    violations: list[Violation] = []
    parse_errors: list[tuple[Path, str]] = []
    imports = _collect_imports(module, parse_errors)
    contracts_module = _configured_contracts_module()
    disabled_rules = _configured_disabled_rules()

    violations.extend(_check_parse_errors(module, parse_errors))
    if "no-internal-imports" not in disabled_rules:
        violations.extend(_check_no_internal_imports(module, imports, all_modules))
    if "use-contracts" not in disabled_rules:
        violations.extend(
            _check_uses_contracts_module(module, imports, all_modules, contracts_module)
        )
    if "undeclared-dependency" not in disabled_rules:
        violations.extend(
            _check_declared_dependencies(module, imports, all_modules, contracts_module)
        )
    if "data-ownership" not in disabled_rules:
        violations.extend(_check_data_ownership(module, all_modules))
    if "contracts-is-sink" not in disabled_rules:
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
    # actually uses.
    local_names: list[str] = field(default_factory=list)

    # True when the import sits inside an ``if TYPE_CHECKING:`` block.
    # Boundary rules (1, 3, 4) still check these; cycle detection (rule 2)
    # skips them — a type-only import imposes no runtime dependency and is
    # the sanctioned idiom for breaking a runtime cycle.
    type_only: bool = False


def _package_dir(package: str) -> Path | None:
    """Resolve a package's on-disk directory without executing its code.

    ``find_spec`` on a dotted name (e.g. ``"myapp.orders"``) imports every
    ancestor package for real to read its ``__path__`` — a genuine
    execution the module docstring's "never executed" guarantee must not
    permit. A top-level name has no ancestor, so resolving only the first
    segment via ``find_spec`` is side-effect free; every remaining dotted
    segment is then just a directory name checked on disk, no import
    machinery involved.

    A PEP 420 namespace root lists one location per portion, in ``sys.path``
    order, and the portion holding the package need not be the first: an
    editable install's ``.pth`` entry lands after site-packages, where other
    installed portions of the same root live. So every portion is searched,
    in the import system's own order, for the full dotted path.
    """
    parts = package.split(".")
    try:
        spec = importlib.util.find_spec(parts[0])
    except (ImportError, ModuleNotFoundError, ValueError):
        return None
    if spec is None or not spec.submodule_search_locations:
        return None
    for location in spec.submodule_search_locations:
        directory = Path(location).joinpath(*parts[1:])
        if directory.is_dir():
            return directory
    return None


def _is_type_checking(test: ast.expr, aliases: Collection[str] = ("TYPE_CHECKING",)) -> bool:
    """True for ``if TYPE_CHECKING:`` guards.

    Recognizes the bare name, any ``<mod>.TYPE_CHECKING`` attribute, and —
    via *aliases* — names bound by ``from typing import TYPE_CHECKING as TC``
    (an unresolved alias makes the guard invisible, and guarded imports are
    then treated as unconditional runtime imports).
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
    — while cycle detection (rule 2) can skip them.

    ``importlib.import_module(<literal>)`` and ``__import__(<literal>)``
    calls (plain, attribute, or aliased form) are also recorded, tagged the
    same as a static ``import <literal>`` — a dynamic import is otherwise
    invisible to every boundary rule. Only a string-literal argument can be
    resolved statically; a computed target (a variable, a call result) is a
    documented residual limitation and produces no ImportRecord.
    """

    def __init__(self, source_file: Path, file_package: str) -> None:
        self.records: list[ImportRecord] = []
        self.source_file = source_file
        self.file_package = file_package
        self._tc_aliases: set[str] = {"TYPE_CHECKING"}
        self._type_only_depth = 0
        self._importlib_aliases: set[str] = set()
        self._import_module_aliases: set[str] = set()

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
            if alias.name == "importlib" or alias.name.startswith("importlib."):
                self._importlib_aliases.add(alias.asname or alias.name.split(".", 1)[0])
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
        if not node.level and node.module == "importlib":
            for alias in node.names:
                if alias.name == "import_module":
                    self._import_module_aliases.add(alias.asname or alias.name)
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

    def visit_Call(self, node: ast.Call) -> None:
        target = self._dynamic_import_target(node.func, node.args)
        if target is not None:
            self.records.append(
                ImportRecord(
                    self.source_file,
                    node.lineno,
                    target,
                    [],
                    local_names=[],
                    type_only=self._type_only_depth > 0,
                )
            )
        self.generic_visit(node)

    def _dynamic_import_target(self, func: ast.expr, args: list[ast.expr]) -> str | None:
        is_dynamic_import = (
            isinstance(func, ast.Name)
            and (func.id == "__import__" or func.id in self._import_module_aliases)
        ) or (
            isinstance(func, ast.Attribute)
            and func.attr == "import_module"
            and isinstance(func.value, ast.Name)
            and func.value.id in self._importlib_aliases
        )
        if not is_dynamic_import or not args:
            return None
        arg = args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
        return None

    def _resolve(self, node: ast.ImportFrom) -> str:
        if not node.level:  # absolute import
            return node.module or ""
        base_parts = self.file_package.split(".")
        if node.level > 1:
            base_parts = base_parts[: -(node.level - 1)]
        base = ".".join(base_parts)
        return f"{base}.{node.module}" if node.module else base


def _collect_imports(
    module: ModuleInfo, parse_errors: list[tuple[Path, str]] | None = None
) -> list[ImportRecord]:
    """Parse every ``.py`` file under the module's package; return imports.

    A file that fails to parse contributes no imports and is skipped — but
    that silently blinds every rule to whatever it would have imported,
    so verification could pass while missing real violations. When
    *parse_errors* is given, each failure is appended as ``(path, message)``
    so the caller can surface it as its own violation (``_check_parse_errors``).
    """
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
            if parse_errors is not None:
                parse_errors.append((path, str(exc)))
            continue
        collector = _ImportCollector(path, _file_package(root, module.package, path))
        collector.visit(tree)
        records.extend(collector.records)
    return records


def _check_parse_errors(
    module: ModuleInfo, parse_errors: list[tuple[Path, str]]
) -> list[Violation]:
    """A file that fails to parse must surface as an ERROR, not be silently
    skipped — a syntax/encoding error hides that file's imports from every
    other rule, so verification could pass while blind to real violations."""
    return [
        Violation(
            rule="parse-error",
            message=f"{path} could not be parsed: {message}",
            module=module.name,
            location=_portable_path(path, module),
            severity=ViolationSeverity.ERROR,
        )
        for path, message in parse_errors
    ]


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


def _project_root(module: ModuleInfo) -> Path | None:
    """The directory containing the top-level application package.

    Ratchet-baseline locations must be relative to this directory, not
    absolute — an absolute path like ``/home/alice/proj/...`` never matches
    a teammate's checkout or a CI runner's (``/home/runner/...``), silently
    reopening every grandfathered violation the moment the baseline is
    regenerated on a different machine.

    The module's own package is resolved and walked up, rather than the
    top-level name: for a PEP 420 namespace root the first portion on
    ``sys.path`` may be another installed distribution, not the project.
    """
    root = _package_dir(module.package)
    if root is None:
        return None
    return root.parents[module.package.count(".")]


def _portable_path(path: Path, module: ModuleInfo) -> str:
    """*path* relative to the application's source root when resolvable."""
    project_root = _project_root(module)
    if project_root is not None:
        try:
            return path.relative_to(project_root).as_posix()
        except ValueError:
            pass
    return str(path)


def _location(record: ImportRecord, module: ModuleInfo) -> str:
    return f"{_portable_path(record.source_file, module)}:{record.line}"


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
        # Any ``_``-prefixed segment makes the whole path private, not just
        # the first: ``owner.models._priv`` reaches into a private subpackage
        # exactly as ``owner._internal`` does.
        if any(seg.startswith("_") for seg in remainder.split(".") if seg):
            violations.append(
                Violation(
                    rule="no-internal-imports",
                    message=(
                        f"{module.name} imports {record.target_module!r}, reaching into "
                        f"{owner.name}'s private package. Cross-module access must go "
                        f"through {owner.name}'s public API."
                    ),
                    module=module.name,
                    location=_location(record, module),
                )
            )
            continue
        # The imported *names* carry the same convention as the path
        # segments, and the check above never sees them: `from <owner-pkg>
        # import <name>` (including the relative `from . import <name>`
        # spelling from inside the owner's own root __init__) resolves
        # target_module to the owner package itself, and `from
        # <owner-pkg>.models import <name>` resolves it to a public
        # submodule. Either way `<name>` may be the private part.
        private_names = sorted(n for n in record.imported_names if n.startswith("_"))
        if private_names:
            violations.append(
                Violation(
                    rule="no-internal-imports",
                    message=(
                        f"{module.name} imports {', '.join(private_names)} from "
                        f"{owner.name}, reaching into {owner.name}'s private package. "
                        f"Cross-module access must go through {owner.name}'s public API."
                    ),
                    module=module.name,
                    location=_location(record, module),
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
        # from rule 4. The else-branch is runtime code.
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


def _runtime_loaded_names_by_file(module: ModuleInfo) -> dict[Path, set[str]]:
    """Names each file in the module loads at runtime, outside type-annotation
    positions — keyed per file, not aggregated across the whole module.

    Rule 4 must check a candidate import against runtime usage in the *same
    file* that imports it. Aggregating across the module let an unrelated
    file's runtime use of a same-named local binding falsely exempt a real
    annotation-only import elsewhere in the module.
    """
    root = _package_dir(module.package)
    if root is None:
        return {}
    names_by_file: dict[Path, set[str]] = {}
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        # Fresh collector per file: TYPE_CHECKING aliases are file-scoped.
        collector = _NameUsageCollector()
        collector.visit(tree)
        names_by_file[path] = collector.runtime_names
    return names_by_file


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
    uses. Wildcard imports cannot be resolved to specific names, so the
    ``import *`` itself is flagged.
    """
    violations: list[Violation] = []
    # Computed lazily on first candidate, keyed per file (not aggregated
    # across the module) so runtime usage is checked against the file that
    # actually imports the candidate.
    names_by_file: dict[Path, set[str]] | None = None
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
                    location=_location(record, module),
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
        if names_by_file is None:
            names_by_file = _runtime_loaded_names_by_file(module)
        runtime_names = names_by_file.get(record.source_file, set())
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
                    location=_location(record, module),
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

    The rule only applies when the manifest *declared* the field. The separate
    ``dependencies_declared`` flag keeps the public dependency tuple iterable
    while preserving omitted versus explicit-empty semantics.

    The violation message deliberately does NOT embed the module's current
    declared_dependencies list: the ratchet baseline hashes the message, so
    embedding module-wide state would reopen every grandfathered rule-3
    violation whenever any one dependency is added.
    """
    from modulith.manifest import get_manifest

    manifest = get_manifest(module.package)
    if manifest is None or not manifest.dependencies_declared:
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
                    location=_location(record, module),
                )
            )
    return violations


# ---------------------------------------------------------------------------
# Rule 5: data ownership (best-effort)
# ---------------------------------------------------------------------------


_SQLALCHEMY_TABLE_SYMBOLS = frozenset({"Table", "ForeignKey", "ForeignKeyConstraint"})
_SQLALCHEMY_TABLE_MODULES = frozenset({"schema", "sql"})


def _dotted_ast_name(node: ast.expr) -> list[str] | None:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute):
        prefix = _dotted_ast_name(node.value)
        return [*prefix, node.attr] if prefix is not None else None
    return None


def _literal_strings(node: ast.expr | None) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.List | ast.Tuple | ast.Set):
        return [value for element in node.elts for value in _literal_strings(element)]
    return []


class _TableRefCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.refs: list[tuple[str, int, str]] = []
        self.module_aliases: dict[str, str] = {}
        self.symbol_aliases: dict[str, str] = {}

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "sqlalchemy" or alias.name.startswith("sqlalchemy."):
                local_name = alias.asname or alias.name.split(".", 1)[0]
                imported_module = alias.name if alias.asname else "sqlalchemy"
                self.module_aliases[local_name] = imported_module

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if (
            node.level
            or not node.module
            or not (node.module == "sqlalchemy" or node.module.startswith("sqlalchemy."))
        ):
            return
        for alias in node.names:
            if alias.name in _SQLALCHEMY_TABLE_SYMBOLS:
                self.symbol_aliases[alias.asname or alias.name] = alias.name
            elif node.module == "sqlalchemy" and alias.name in _SQLALCHEMY_TABLE_MODULES:
                self.module_aliases[alias.asname or alias.name] = f"sqlalchemy.{alias.name}"

    def _resolved_symbol(self, node: ast.expr) -> str | None:
        parts = _dotted_ast_name(node)
        if not parts:
            return None
        if len(parts) == 1:
            return self.symbol_aliases.get(parts[0])
        imported_module = self.module_aliases.get(parts[0])
        if imported_module is None:
            return None
        resolved = ".".join((imported_module, *parts[1:]))
        symbol = resolved.rsplit(".", 1)[-1]
        return symbol if symbol in _SQLALCHEMY_TABLE_SYMBOLS else None

    @staticmethod
    def _argument(node: ast.Call, index: int, keyword: str) -> ast.expr | None:
        if len(node.args) > index:
            return node.args[index]
        return next((item.value for item in node.keywords if item.arg == keyword), None)

    def _append(self, table: str, node: ast.Call | ast.Assign | ast.AnnAssign, kind: str) -> None:
        self.refs.append((table, node.lineno, kind))

    def visit_Call(self, node: ast.Call) -> None:
        symbol = self._resolved_symbol(node.func)
        if symbol == "Table":
            for table in _literal_strings(self._argument(node, 0, "name")):
                self._append(table, node, "define")
        elif symbol == "ForeignKey":
            for target in _literal_strings(self._argument(node, 0, "column")):
                parts = target.split(".")
                if len(parts) >= 2:
                    self._append(parts[-2], node, "reference")
        elif symbol == "ForeignKeyConstraint":
            for target in _literal_strings(self._argument(node, 1, "refcolumns")):
                parts = target.split(".")
                if len(parts) >= 2:
                    self._append(parts[-2], node, "reference")
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if any(
            isinstance(target, ast.Name) and target.id == "__tablename__" for target in node.targets
        ):
            for table in _literal_strings(node.value):
                self._append(table, node, "define")
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.target.id == "__tablename__":
            for table in _literal_strings(node.value):
                self._append(table, node, "define")
        self.generic_visit(node)


def _table_refs_from_tree(tree: ast.Module) -> list[tuple[str, int, str]]:
    """Return table references as ``(name, line, kind)`` without importing source."""
    collector = _TableRefCollector()
    collector.visit(tree)
    return collector.refs


def _collect_table_refs(module: ModuleInfo) -> list[tuple[str, str, str]]:
    """Return best-effort SQLAlchemy references as ``(name, location, kind)`` tuples."""
    root = _package_dir(module.package)
    if root is None:
        return []
    refs: list[tuple[str, str, str]] = []
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        portable_path: str | None = None
        for table, line, kind in _table_refs_from_tree(tree):
            if portable_path is None:
                portable_path = _portable_path(path, module)
            refs.append((table, f"{portable_path}:{line}", kind))
    return refs


def _table_owners(all_modules: list[ModuleInfo]) -> dict[str, set[str]]:
    """Table name -> set of module names declaring ownership via manifest.

    Two manifests claiming the same table is a manifest-authoring conflict
    that must be surfaced, not silently resolved last-write-wins — hence a
    *set* of claimants per table rather than a single owner.
    """
    from modulith.manifest import all_manifests

    owners_of: dict[str, set[str]] = {}
    pkg_to_name = {m.package: m.name for m in all_modules}
    for pkg, manifest in all_manifests().items():
        owner_name = pkg_to_name.get(pkg, pkg.rsplit(".", 1)[-1])
        for table in manifest.owns_tables:
            owners_of.setdefault(table, set()).add(owner_name)
    return owners_of


def _check_data_ownership(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Rule 5: flag references to tables owned by another module (warning).

    A module that co-declared ownership of a conflicted table is never
    flagged for referencing it; the conflict itself is reported instead.

    A module that declares a non-empty ``owns_tables`` is also held to it: any
    table it *defines* but does not list is flagged, so the manifest stays a
    complete inventory of the module's data. Modules with an empty
    ``owns_tables`` opted out and get no such warning.
    """
    from modulith.manifest import get_manifest

    owners_of = _table_owners(all_modules)
    if not owners_of:
        # No manifest anywhere declares owns_tables, so neither check can fire.
        return []

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

    manifest = get_manifest(module.package)
    declared: frozenset[str] = (
        frozenset(manifest.owns_tables) if manifest is not None else frozenset()
    )

    for table, location, kind in _collect_table_refs(module):
        if declared and kind == "define" and table not in declared:
            violations.append(
                Violation(
                    rule="data-ownership",
                    message=(
                        f"{module.name} defines table {table!r} but does not declare it "
                        f"in owns_tables. Declare it or move it to its owning module."
                    ),
                    module=module.name,
                    severity=ViolationSeverity.WARNING,
                    location=location,
                )
            )
        owners = owners_of.get(table)
        if owners is None or module.name in owners:
            continue
        owner_text = " and ".join(sorted(owners))
        if kind == "define":
            message = (
                f"{module.name} defines table {table!r}, which is already owned by "
                f"{owner_text}'s manifest. Rename or remove the duplicate definition, "
                f"or coordinate ownership between the modules."
            )
        else:
            message = (
                f"{module.name} references table {table!r}, owned by {owner_text}. "
                f"Access another module's data through its events or public API "
                f"(best-effort static check)."
            )
        violations.append(
            Violation(
                rule="data-ownership",
                message=message,
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
    contracts sailed through undetected. This rule flags ANY
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
                location=_location(record, module),
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
    fingerprint would resurrect it as a "new" failing violation.
    Identity therefore uses the file path only.
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


def load_baseline(path: Path) -> dict[BaselineEntry, int]:
    """Read the baseline file; return grandfathered fingerprints with counts.

    The ratchet is count-aware: each fingerprint maps to the
    number of violations grandfathered under it, so a NEW violation that is
    identical to a baselined one (same rule/module/file/message, different
    line) still fails the build. Entries written by older versions carry no
    ``count`` and read as an allowance of exactly one; duplicate fingerprints
    in one file accumulate.

    A corrupted or schema-mismatched file raises ConfigurationError naming
    the path and the regeneration command — never a raw JSONDecodeError or
    KeyError traceback. Legacy entries carrying ``file:line``
    locations are normalized on load so old baselines keep matching.
    """
    from modulith.config import ConfigurationError

    if not path.exists():
        return {}
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
    entries: dict[BaselineEntry, int] = {}
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
        count = item.get("count", 1)
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ConfigurationError(
                f"baseline file {path} has an entry with an invalid count "
                f"{count!r} (expected a positive integer); {hint}"
            )
        entry = BaselineEntry(
            rule=rule,
            module=module,
            location=_normalize_location(location),
            message_hash=message_hash,
        )
        entries[entry] = entries.get(entry, 0) + count
    return entries


def filter_against_baseline(
    violations: list[Violation],
    baseline: Mapping[BaselineEntry, int],
) -> list[Violation]:
    """Return the violations beyond the baseline's per-fingerprint allowance.

    Each grandfathered fingerprint admits at most its baselined count; every
    violation past that allowance is reported. Fewer violations
    than baselined is an improvement and passes — the ratchet only tightens.
    """
    remaining = dict(baseline)
    reported: list[Violation] = []
    for v in violations:
        entry = _entry_for(v)
        if remaining.get(entry, 0) > 0:
            remaining[entry] -= 1
        else:
            reported.append(v)
    return reported


def write_baseline(path: Path, violations: list[Violation]) -> None:
    """Write the current violation set as a new baseline (stable JSON).

    One entry per fingerprint with its ``count``, so the
    multiplicity of identical violations is part of the ratchet.
    """
    counts = Counter(_entry_for(v) for v in violations)
    entries = sorted(
        counts,
        key=lambda e: (e.rule, e.module, e.location, e.message_hash),
    )
    payload = [
        {
            "rule": e.rule,
            "module": e.module,
            "location": e.location,
            "message_hash": e.message_hash,
            "count": counts[e],
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
