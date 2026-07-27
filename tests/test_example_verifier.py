"""Regression tests for examples/naming_convention_verifier.py.

The sample is published as the copy-paste starting point for writing a custom
verification rule (docs/COOKBOOK.md links it as the worked example), so two
things a reader inherits from it must stay true: it registers against the
current ``modulith_verify_module`` hookspec, and its rule actually fires. This
file is the only thing in the repo that imports the sample, so without it a
hookspec signature change or a rename of the ``Violation`` fields leaves the
published example broken while the whole suite stays green.

The example is loaded from disk (examples/ is not a package and is not on
sys.path) and registered through ``create_plugin_manager``, which is where
pluggy validates the hookimpl signature against the spec: a drifted signature
fails at registration rather than degrading into a rule that silently never
runs.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

from modulith import ModuleInfo, Violation, ViolationSeverity
from modulith.manager import create_plugin_manager

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"

APP = "fakeapp"

# Two events, one of each shape: a command-style name the rule must flag and a
# past-tense name it must leave alone.
ORDERS_SOURCE = """
    from modulith import event

    @event
    class CreateOrder:
        '''Command-shaped name — the rule flags this.'''

    @event
    class OrderCreated:
        '''Past tense — the rule leaves this alone.'''

    class PlainHelper:
        '''Not an event; the rule must ignore it despite the verb-first name.'''
"""

INVENTORY_SOURCE = """
    from modulith import event

    @event
    class StockReserved:
        '''Past tense — clean module, no violations expected.'''
"""


def _load_example_verifier() -> ModuleType:
    """Import examples/naming_convention_verifier.py from disk."""
    spec = importlib.util.spec_from_file_location(
        "example_naming_convention_verifier", EXAMPLES_DIR / "naming_convention_verifier.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _verify(module_names: list[str], *, load_builtins: bool) -> list[Violation]:
    """Run the example rule over ``module_names`` the way the runtime does.

    Mirrors the aggregation in ``modulith.cli._collect_violations``: the hook is
    called once per module and every plugin's list is concatenated.
    """
    pm = create_plugin_manager(
        extra_plugins=[_load_example_verifier()],
        load_entrypoints=False,
        load_builtins=load_builtins,
    )
    modules = [ModuleInfo(name=name, package=f"{APP}.{name}") for name in module_names]
    violations: list[Violation] = []
    for module in modules:
        for result in pm.hook.modulith_verify_module(module=module, all_modules=modules):
            violations.extend(result)
    return violations


def test_example_verifier_flags_a_command_named_event(make_fake_app) -> None:
    """The rule reports the command-shaped event and nothing else."""
    make_fake_app({"orders": ORDERS_SOURCE}, package_name=APP)

    violations = _verify(["orders"], load_builtins=False)

    assert len(violations) == 1, violations
    violation = violations[0]
    assert violation.rule == "event-past-tense"
    assert violation.module == "orders"
    assert violation.severity is ViolationSeverity.WARNING
    assert "CreateOrder" in violation.message


def test_example_verifier_passes_a_past_tense_event(make_fake_app) -> None:
    """A module whose events are all past tense produces no violations."""
    make_fake_app({"inventory": INVENTORY_SOURCE}, package_name=APP)

    assert _verify(["inventory"], load_builtins=False) == []


def test_example_verifier_blames_only_the_module_that_defines_the_event(make_fake_app) -> None:
    """A contracts-owned event is reported once, against ``contracts``.

    Cross-module events live in the shared contracts package and are imported
    into every module that consumes them, so ``inspect.getmembers`` on a
    consumer's namespace sees them too. Reporting each importer would emit one
    violation per consumer and name modules that cannot rename the event —
    on the very layout modulith prescribes (``contracts_module`` config).
    """
    make_fake_app(
        {
            "contracts": """
                from modulith import event

                @event
                class CreateOrder:
                    '''Command-shaped name, owned by contracts.'''
            """,
            "orders": "from fakeapp.contracts import CreateOrder\n",
            "inventory": "from fakeapp.contracts import CreateOrder\n",
        },
        package_name=APP,
    )

    violations = _verify(["contracts", "orders", "inventory"], load_builtins=False)

    assert [v.module for v in violations] == ["contracts"]


def test_example_verifier_composes_with_the_builtin_rules(make_fake_app) -> None:
    """Built-in rules and the example rule both contribute to one report.

    This is the aggregate-hook behaviour the example's module docstring
    advertises: a module with both a boundary problem and a naming problem must
    report both. Running the example alongside the built-ins is the only way to
    catch a registration regression that leaves the sample's rule loaded but
    never invoked.
    """
    make_fake_app(
        {
            "orders": ORDERS_SOURCE + "\n    from fakeapp.inventory._internal import VALUE\n",
            "inventory": INVENTORY_SOURCE,
        },
        package_name=APP,
        extra_files={"inventory/_internal/__init__.py": "VALUE = 1\n"},
    )

    rules = {v.rule for v in _verify(["orders", "inventory"], load_builtins=True)}

    assert "event-past-tense" in rules  # the example's rule
    assert "no-internal-imports" in rules  # a built-in rule
