#!/usr/bin/env python3
"""Generate ``docs/API_REFERENCE.md`` from the public API's docstrings.

The reference documents every name exported from the ``modulith`` package
(``modulith.__all__``): the signature and docstring of each function, and the
fields/members/methods and docstring of each class. Nothing here is written by
hand — the source of truth is the code, so the reference cannot drift from the
docstrings it renders.

Usage::

    python scripts/gen_api_reference.py            # write docs/API_REFERENCE.md
    python scripts/gen_api_reference.py --check     # exit 1 if the file is stale

``--check`` is what CI (and tests/test_api_reference_sync.py) run: it renders
into memory and compares against the committed file, so a public docstring or
signature change that was not regenerated fails loudly instead of shipping a
stale reference.
"""

from __future__ import annotations

import dataclasses
import enum
import inspect
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import modulith

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "docs" / "API_REFERENCE.md"

# Curated section ordering. Every name in ``modulith.__all__`` is placed in
# exactly one section here (the generator asserts this), so adding a public
# export without slotting it fails the generator rather than silently dropping
# it from the reference. ``__version__`` is intentionally excluded — it is a
# plain string, not an API symbol, and would make the output non-deterministic.
SECTIONS: list[tuple[str, str, list[str]]] = [
    (
        "Application API",
        "The everyday surface — the names most applications import.",
        [
            "event",
            "listener",
            "publish",
            "publish_sync",
            "PublishSyncTimeout",
            "configure",
            "bootstrap",
            "externalized",
        ],
    ),
    (
        "Manifests",
        "Declare and read a module's contract (what it publishes, consumes, owns).",
        ["declare_module", "get_manifest", "Manifest"],
    ),
    (
        "Configuration",
        "The resolved runtime configuration and its error type.",
        ["Configuration", "ConfigurationError"],
    ),
    (
        "Contract types",
        "Shared data types that appear in hookspec and protocol signatures.",
        ["EventPublication", "ModuleInfo", "Violation", "ViolationSeverity"],
    ),
    (
        "Driver protocols",
        'The "one wins" adapter contracts — implement by duck typing.',
        ["PublicationStore", "EventSerializer", "Broker", "Consumer"],
    ),
    (
        "Broker registry",
        "Scheme-based dispatch for cross-process brokers and its error types.",
        ["BrokerRegistry", "DuplicateBrokerError", "UnknownBrokerError"],
    ),
    (
        "Consumer registry",
        "Scheme-based factories for the cross-process consumer half, plus the "
        "per-module spec and error types.",
        ["ConsumerRegistry", "ConsumerSpec", "DuplicateConsumerError", "UnknownConsumerError"],
    ),
    (
        "Plugin authoring",
        "The marker plugin authors use to implement hooks.",
        ["hookimpl"],
    ),
    (
        "Plugin manager (advanced)",
        "Constructing a configured plugin manager — most applications never need this.",
        ["create_plugin_manager"],
    ),
]

# Names exported for reasons other than being documentable API symbols.
EXCLUDED = {"__version__"}


def _anchor(text: str) -> str:
    """GitHub-style anchor slug for a heading."""
    slug = text.lower()
    slug = "".join(ch if ch.isalnum() or ch in " -" else "" for ch in slug)
    return slug.replace(" ", "-")


def _unique_anchor(text: str, seen: dict[str, int]) -> str:
    """Anchor slug disambiguated against every anchor already emitted.

    Two DIFFERENT headings can render to the SAME slug — e.g. the
    "Configuration" section and the "Configuration" class nested inside it.
    GitHub itself resolves this by document order: the first heading with a
    given slug keeps it bare, every later one gets it suffixed "-1", "-2",
    etc. ``seen`` must be threaded through calls in the exact order headings
    appear in the rendered document, or the computed anchor won't match the
    one GitHub actually assigns — silently linking a TOC entry to the wrong
    heading.
    """
    base = _anchor(text)
    count = seen.get(base, 0)
    seen[base] = count + 1
    return base if count == 0 else f"{base}-{count}"


def _docstring(obj: Any) -> str:
    """Cleaned docstring (dedented, trailing blanks stripped) or a placeholder."""
    doc = inspect.getdoc(obj)
    return doc.rstrip() if doc else "_No docstring._"


def _annotation_str(annotation: Any) -> str:
    """Render an annotation as source text.

    The whole codebase uses ``from __future__ import annotations``, so
    annotations arrive as the exact strings the author wrote (``type[T]``,
    ``Callable[..., Any]``, ``Manifest | None``). That is far more readable
    than the resolved object's repr (``collections.abc.Callable[...]``), so we
    render the string as-is and only fall back to a name for eager annotations.
    """
    if isinstance(annotation, str):
        return annotation
    return getattr(annotation, "__name__", str(annotation))


def _format_param(param: inspect.Parameter) -> str:
    if param.kind == inspect.Parameter.VAR_POSITIONAL:
        prefix = "*"
    elif param.kind == inspect.Parameter.VAR_KEYWORD:
        prefix = "**"
    else:
        prefix = ""
    token = prefix + param.name
    if param.annotation is not inspect.Parameter.empty:
        token += f": {_annotation_str(param.annotation)}"
    if param.default is not inspect.Parameter.empty:
        sep = " = " if param.annotation is not inspect.Parameter.empty else "="
        token += f"{sep}{param.default!r}"
    return token


def _param_tokens(params: list[inspect.Parameter]) -> list[str]:
    """Rendered parameter tokens with ``/`` and ``*`` separators inserted."""
    parts: list[str] = []
    star_emitted = False
    for i, param in enumerate(params):
        prev_positional_only = i > 0 and params[i - 1].kind == inspect.Parameter.POSITIONAL_ONLY
        if prev_positional_only and param.kind != inspect.Parameter.POSITIONAL_ONLY:
            parts.append("/")
        if param.kind == inspect.Parameter.KEYWORD_ONLY and not star_emitted:
            parts.append("*")
            star_emitted = True
        parts.append(_format_param(param))
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            star_emitted = True
    if params and params[-1].kind == inspect.Parameter.POSITIONAL_ONLY:
        parts.append("/")
    return parts


def _signature(obj: Callable[..., Any]) -> str:
    """A source-faithful signature string, or empty if uninspectable.

    Formats from the (PEP 563 string) annotations directly rather than
    ``str(inspect.signature(...))``, which would quote every annotation. Only
    genuine string *defaults* are quoted (via ``repr``); annotations never are.
    """
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):
        return ""
    inner = ", ".join(_param_tokens(list(sig.parameters.values())))
    ret = ""
    if sig.return_annotation is not inspect.Signature.empty:
        ret = f" -> {_annotation_str(sig.return_annotation)}"
    return f"({inner}){ret}"


def _render_function(name: str, obj: Callable[..., Any]) -> list[str]:
    sig = _signature(obj)
    keyword = "async def " if inspect.iscoroutinefunction(obj) else "def "
    return [
        f"### `{name}`",
        "",
        "```python",
        f"{keyword}{name}{sig}:",
        "```",
        "",
        _docstring(obj),
        "",
    ]


def _render_enum(name: str, obj: type[enum.Enum]) -> list[str]:
    lines = [f"### `{name}`", "", _docstring(obj), "", "**Members:**", ""]
    for member in obj:
        lines.append(f"- `{member.name}` = `{member.value!r}`")
    lines.append("")
    return lines


def _public_methods(obj: type) -> list[tuple[str, Any]]:
    """(name, function) for public methods defined directly on ``obj``.

    Definition order (``vars`` preserves it) so the rendered order matches the
    source. Inherited methods are skipped — a Protocol or class documents its
    own surface, not ``object``'s.
    """
    methods: list[tuple[str, Any]] = []
    for attr, value in vars(obj).items():
        if attr.startswith("_"):
            continue
        if inspect.isfunction(value):
            methods.append((attr, value))
    return methods


def _render_dataclass(name: str, obj: type) -> list[str]:
    lines = [f"### `{name}`", "", _docstring(obj), "", "**Fields:**", ""]
    for f in dataclasses.fields(obj):
        annotation = f.type if isinstance(f.type, str) else getattr(f.type, "__name__", str(f.type))
        if f.default is not dataclasses.MISSING:
            lines.append(f"- `{f.name}: {annotation}` = `{f.default!r}`")
        elif f.default_factory is not dataclasses.MISSING:
            lines.append(f"- `{f.name}: {annotation}` (default factory)")
        else:
            lines.append(f"- `{f.name}: {annotation}` (required)")
    lines.append("")
    lines.extend(_render_methods_block(obj))
    return lines


def _render_methods_block(obj: type) -> list[str]:
    methods = _public_methods(obj)
    if not methods:
        return []
    lines = ["**Methods:**", ""]
    for method_name, method in methods:
        sig = _signature(method)
        prefix = "async def " if inspect.iscoroutinefunction(method) else "def "
        lines.append(f"- `{prefix}{method_name}{sig}:`")
        doc = inspect.getdoc(method)
        if doc:
            first = doc.strip().splitlines()[0]
            lines.append(f"  — {first}")
    lines.append("")
    return lines


def _render_class(name: str, obj: type) -> list[str]:
    if issubclass(obj, enum.Enum):
        return _render_enum(name, obj)
    if dataclasses.is_dataclass(obj):
        return _render_dataclass(name, obj)

    lines = [f"### `{name}`"]
    if issubclass(obj, BaseException):
        bases = ", ".join(base.__name__ for base in obj.__bases__)
        lines += ["", f"*Exception — subclasses `{bases}`.*"]
    elif getattr(obj, "_is_protocol", False):
        lines += ["", "*Protocol — implement by duck typing; no need to subclass.*"]
    lines += ["", _docstring(obj), ""]
    lines.extend(_render_methods_block(obj))
    return lines


def _render_marker(name: str, obj: Any) -> list[str]:
    return [
        f"### `{name}`",
        "",
        f"*Instance of `{type(obj).__module__}.{type(obj).__name__}`.*",
        "",
        _docstring(obj),
        "",
    ]


def _render_symbol(name: str, obj: Any) -> list[str]:
    if inspect.isclass(obj):
        return _render_class(name, obj)
    if inspect.isfunction(obj):
        return _render_function(name, obj)
    return _render_marker(name, obj)


def render_api_reference() -> str:
    """Render the full API reference markdown from ``modulith.__all__``."""
    exported = set(modulith.__all__) - EXCLUDED
    placed = {name for _, _, names in SECTIONS for name in names}
    missing = exported - placed
    if missing:
        raise SystemExit(
            f"gen_api_reference: public export(s) {sorted(missing)} are in "
            "modulith.__all__ but not slotted into any SECTIONS group. Add them "
            "to scripts/gen_api_reference.py."
        )
    unknown = placed - set(modulith.__all__)
    if unknown:
        raise SystemExit(
            f"gen_api_reference: SECTIONS reference(s) {sorted(unknown)} that are "
            "no longer in modulith.__all__. Remove them from "
            "scripts/gen_api_reference.py."
        )

    lines: list[str] = [
        "<!-- GENERATED by scripts/gen_api_reference.py — do not edit by hand.",
        "     Regenerate with: python scripts/gen_api_reference.py -->",
        "",
        "# modulith API Reference",
        "",
        "The public API of the `modulith` package — every name exported from",
        "`modulith` (`modulith.__all__`). This file is generated from the",
        "docstrings and signatures of that surface; run",
        "`python scripts/gen_api_reference.py` to regenerate it after changing a",
        "public docstring or signature. For the design behind these APIs see",
        "[ARCHITECTURE.md](ARCHITECTURE.md) and [SPEC.md](../SPEC.md); for",
        "task-oriented usage see [COOKBOOK.md](COOKBOOK.md).",
        "",
        "The package also exports `__version__` (the installed package version).",
        "",
        "## Contents",
        "",
    ]
    seen_anchors: dict[str, int] = {}
    for title, _, names in SECTIONS:
        lines.append(f"- [{title}](#{_unique_anchor(title, seen_anchors)})")
        for name in names:
            lines.append(f"  - [`{name}`](#{_unique_anchor(name, seen_anchors)})")
    lines.append("")

    for title, blurb, names in SECTIONS:
        lines.append(f"## {title}")
        lines.append("")
        lines.append(blurb)
        lines.append("")
        for name in names:
            obj = getattr(modulith, name)
            lines.extend(_render_symbol(name, obj))

    text = "\n".join(lines).rstrip() + "\n"
    return text


def main(argv: list[str]) -> int:
    check = "--check" in argv[1:]
    rendered = render_api_reference()
    if check:
        if not OUTPUT_PATH.exists():
            print(f"{OUTPUT_PATH} does not exist — run: python {argv[0]}", file=sys.stderr)
            return 1
        current = OUTPUT_PATH.read_text(encoding="utf-8")
        if current != rendered:
            print(
                f"{OUTPUT_PATH} is out of date. Regenerate it with:\n    python {argv[0]}",
                file=sys.stderr,
            )
            return 1
        return 0
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(rendered, encoding="utf-8")
    print(f"wrote {OUTPUT_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
