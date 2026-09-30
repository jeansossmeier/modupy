"""Team plugin for the marketplace, registered through the ``modulith`` entry point.

It lives beside the ``marketplace`` package rather than inside it: discovery treats every
subpackage of ``marketplace`` as a module, and an extracted service ships its own copy of
``marketplace`` that would shadow an entry point defined there. It imports nothing from
``marketplace`` for the same reason.

modulith loads every installed entry-point plugin into every application, so both hooks
return early for modules outside ``marketplace``.
"""

import os
import sys

from modulith import ModuleInfo, Violation, ViolationSeverity, get_manifest, hookimpl
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

ROOT_PACKAGE = "marketplace"


def in_marketplace(module: ModuleInfo) -> bool:
    return module.package == ROOT_PACKAGE or module.package.startswith(f"{ROOT_PACKAGE}.")


@hookimpl
def modulith_verify_module(module: ModuleInfo, all_modules: list[ModuleInfo]) -> list[Violation]:
    manifest = get_manifest(module.package)
    if manifest is None or not in_marketplace(module):
        return []
    prefix = f"{module.name}_"
    return [
        Violation(
            rule="table-prefix",
            message=f"table {table!r} must start with {prefix!r} so its owner is obvious",
            module=module.name,
            severity=ViolationSeverity.ERROR,
        )
        for table in manifest.owns_tables
        if not table.startswith(prefix)
    ]


@hookimpl
def modulith_after_module_load(module: ModuleInfo) -> None:
    if not in_marketplace(module) or isinstance(trace.get_tracer_provider(), TracerProvider):
        return
    name = os.environ.get("MODULITH_MODULE") or "monolith"
    provider = TracerProvider(resource=Resource.create({"service.name": f"marketplace-{name}"}))
    if os.environ.get("OTEL_TRACES_EXPORTER") == "console":
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(out=sys.stdout)))
    trace.set_tracer_provider(provider)
