from pathlib import Path

import pytest
from modulith import bootstrap, publish
from modulith.testing import ModulithTestApp
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

pytestmark = [pytest.mark.modulith_isolated, pytest.mark.modulith_no_outbox]


@pytest.fixture(autouse=True)
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))


def installed_provider() -> TracerProvider:
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    return provider


def service_name(provider: TracerProvider) -> str:
    return str(provider.resource.attributes["service.name"])


def test_the_monolith_traces_as_marketplace_monolith(
    modulith_app: ModulithTestApp, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MODULITH_MODULE", raising=False)

    bootstrap()

    assert service_name(installed_provider()) == "marketplace-monolith"


def test_a_worker_traces_under_its_module_name(
    modulith_app: ModulithTestApp, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MODULITH_MODULE", "notifications")

    bootstrap()

    assert service_name(installed_provider()) == "marketplace-notifications"


def test_an_sdk_provider_installed_beforehand_is_kept(
    modulith_app: ModulithTestApp, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MODULITH_MODULE", raising=False)
    own = TracerProvider()
    trace.set_tracer_provider(own)

    bootstrap()

    assert trace.get_tracer_provider() is own


def test_the_console_exporter_prints_finished_spans(
    modulith_app: ModulithTestApp,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "console")

    bootstrap()
    trace.get_tracer("probe").start_span("probe-span").end()

    assert '"name": "probe-span"' in capsys.readouterr().out


def test_no_exporter_is_added_by_default(
    modulith_app: ModulithTestApp,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("OTEL_TRACES_EXPORTER", raising=False)

    bootstrap()
    trace.get_tracer("probe").start_span("probe-span").end()

    assert capsys.readouterr().out == ""


async def test_publish_and_dispatch_spans_are_parented(marketplace: ModulithTestApp) -> None:
    bootstrap()
    exporter = InMemorySpanExporter()
    installed_provider().add_span_processor(SimpleSpanProcessor(exporter))

    from marketplace.contracts import ProductListed

    await publish(ProductListed(sku="SKU-MUG", name="Stoneware mug", price_cents=1200, stock=10))

    spans = exporter.get_finished_spans()
    published = [s for s in spans if s.name == "modulith.event.publish"]
    dispatched = [s for s in spans if s.name == "modulith.event.dispatch"]
    assert len(published) == 1
    assert len(dispatched) == 1
    assert_child_of(dispatched[0], published[0])
    assert service_name_of(published[0]) == "marketplace-monolith"


def assert_child_of(child: ReadableSpan, parent: ReadableSpan) -> None:
    assert parent.context is not None
    assert child.context is not None
    assert child.parent is not None
    assert child.parent.span_id == parent.context.span_id
    assert child.context.trace_id == parent.context.trace_id


def service_name_of(span: ReadableSpan) -> str:
    return str(span.resource.attributes["service.name"])
