"""Unit tests for scripts/gen_api_reference.py's rendering internals.

tests/test_api_reference_sync.py only guards that the committed
docs/API_REFERENCE.md matches what the generator currently produces; it says
nothing about whether the generator's OWN output is correct. These tests
exercise the generator's rendering functions directly.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATOR_PATH = REPO_ROOT / "scripts" / "gen_api_reference.py"

_spec = importlib.util.spec_from_file_location("gen_api_reference", GENERATOR_PATH)
assert _spec is not None and _spec.loader is not None
gen_api_reference = importlib.util.module_from_spec(_spec)
sys.modules["gen_api_reference"] = gen_api_reference
_spec.loader.exec_module(gen_api_reference)


# ---------------------------------------------------------------------------
# def / async def rendering
# ---------------------------------------------------------------------------


def test_render_function_includes_def_keyword():
    """A rendered top-level function's code block must read like real Python
    (``def name(...):``) — previously it omitted ``def`` and the trailing
    colon entirely, rendering a bare call-looking expression."""

    def sample(x: int) -> str:
        """Doc."""
        return str(x)

    rendered = "\n".join(gen_api_reference._render_function("sample", sample))
    assert "def sample(x: int) -> str:" in rendered
    assert "async def" not in rendered


def test_render_function_marks_async_def():
    """An async function must render as ``async def``, not plain ``def`` —
    previously neither prefix was ever emitted."""

    async def sample_async(x: int) -> str:
        """Doc."""
        return str(x)

    rendered = "\n".join(gen_api_reference._render_function("sample_async", sample_async))
    assert "async def sample_async(x: int) -> str:" in rendered


def test_render_methods_block_includes_def_keyword():
    """A rendered method entry must also read like real Python — previously
    it omitted ``def``/``async def`` and the trailing colon."""

    class Sample:
        def sync_method(self) -> None:
            """Doc."""

        async def async_method(self) -> None:
            """Doc."""

    rendered = "\n".join(gen_api_reference._render_methods_block(Sample))
    assert "`def sync_method(self) -> None:`" in rendered
    assert "`async def async_method(self) -> None:`" in rendered


def test_render_methods_block_keeps_full_first_sentence_across_line_wrap():
    """The one-line summary took only the first PHYSICAL line of the
    docstring (``doc.strip().splitlines()[0]``), so a first sentence wrapped
    onto a second physical line — as ``Consumer.stop`` in
    modulith/protocols.py does — was cut off mid-sentence, silently dropping
    the rest of the safety contract."""

    class Sample:
        def stop(self) -> None:
            """Stop the poll loop and release resources. Must never raise —
            a consumer that already died is logged, not re-raised, at
            shutdown.
            """

    rendered = "\n".join(gen_api_reference._render_methods_block(Sample))
    assert "Must never raise — a consumer that already died is logged" in rendered
    assert "Must never raise — a\n" not in rendered


# ---------------------------------------------------------------------------
# Stable, namespaced (collision-safe) anchors
# ---------------------------------------------------------------------------


def test_anchor_keeps_underscores_like_github():
    """The slug builder dropped every character that is neither alphanumeric
    nor a space/hyphen, so underscores vanished. GitHub keeps them (``_`` is a
    word character), which made every TOC entry for an underscore-bearing
    symbol — publish_sync, declare_module, get_manifest,
    create_plugin_manager — point at a fragment no heading owns."""
    assert gen_api_reference._anchor("publish_sync") == "publish_sync"
    assert gen_api_reference._anchor("create_plugin_manager") == "create_plugin_manager"
    # Punctuation is still stripped and spaces still become hyphens.
    assert gen_api_reference._anchor("Application API") == "application-api"


def test_every_toc_link_targets_an_emitted_heading():
    """A TOC entry whose fragment matches no heading in the same document
    silently fails to scroll. The heading anchors here are derived by an
    independent implementation of GitHub's slug rule — lowercase, drop
    everything outside ``[\\w -]``, spaces to hyphens, repeats suffixed by
    document order — deliberately NOT by calling the generator's own
    ``_anchor``, which would make the check agree with any bug it has."""
    rendered = gen_api_reference.render_api_reference()
    toc_end = rendered.index("## Application API")

    seen: dict[str, int] = {}
    heading_anchors = set()
    for line in rendered.splitlines():
        if not (line.startswith("## ") or line.startswith("### ")):
            continue
        base = re.sub(r"[^\w\- ]", "", line.lstrip("#").strip().lower()).replace(" ", "-")
        count = seen.get(base, 0)
        seen[base] = count + 1
        heading_anchors.add(base if count == 0 else f"{base}-{count}")

    toc_targets = set(re.findall(r"\]\(#([^)]+)\)", rendered[:toc_end]))

    assert toc_targets, "the generated reference must emit a table of contents"
    assert toc_targets <= heading_anchors, (
        f"TOC links with no matching heading: {sorted(toc_targets - heading_anchors)}"
    )


def test_toc_disambiguates_colliding_anchors_like_github():
    """The 'Configuration' SECTION and the 'Configuration' CLASS inside it
    render to the identical GitHub slug ('configuration'). GitHub itself
    disambiguates repeat headings by document order, suffixing the second
    occurrence '-1' — so the class heading's real anchor is
    '#configuration-1', not '#configuration'. The generator computed both
    TOC links as plain '#configuration', silently pointing the class link
    at the SECTION heading instead of the class's own heading."""
    rendered = gen_api_reference.render_api_reference()
    toc_end = rendered.index("## Application API")
    toc = rendered[:toc_end]

    assert "(#configuration)" in toc  # the section keeps the bare slug
    assert "(#configuration-1)" in toc  # the class gets GitHub's '-1' suffix

    # And the class heading's ACTUAL position in the body matches: the
    # 'Configuration' class heading is the second '## '/'### ' heading with
    # slug 'configuration' emitted in document order.
    body = rendered[toc_end:]
    assert "### `Configuration`" in body
