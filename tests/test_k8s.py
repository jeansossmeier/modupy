"""Tests for the Kubernetes manifest generator (``modulith.k8s``).

Unit tests build a ``Configuration``/``WorkerSpec`` pair directly and parse
``render_manifests``' output with ``yaml.safe_load_all`` (PyYAML is a
``test``-extra dependency, never a runtime one — see ``modulith/k8s.py``).
CLI tests drive the ``k8s-manifest`` command end-to-end through
``typer.testing.CliRunner`` against a ``make_fake_app``-built package.
"""

from __future__ import annotations

import json

import pytest
import yaml
from typer.testing import CliRunner

from modulith.cli import app
from modulith.config import Configuration, ConfigurationError
from modulith.k8s import k8s_name, render_manifests
from modulith.supervisor import WorkerSpec

runner = CliRunner()


def _two_module_specs() -> list[WorkerSpec]:
    return [
        WorkerSpec(module_name="orders", package="fakeapp", port=9001, worker_count=3),
        WorkerSpec(module_name="inventory", package="fakeapp", port=9004, worker_count=1),
    ]


def _cfg(**overrides: object) -> Configuration:
    defaults: dict[str, object] = {"package": "fakeapp", "broker": "database"}
    defaults.update(overrides)
    return Configuration(**defaults)  # type: ignore[arg-type]


def _docs_by_kind(text: str) -> dict[str, list[dict]]:
    by_kind: dict[str, list[dict]] = {}
    for doc in yaml.safe_load_all(text):
        if doc is None:
            continue
        by_kind.setdefault(doc["kind"], []).append(doc)
    return by_kind


# ---------------------------------------------------------------------------
# k8s_name
# ---------------------------------------------------------------------------


def test_k8s_name_lowercases_and_hyphenates_underscores() -> None:
    assert k8s_name("order_items") == "order-items"
    assert k8s_name("Orders") == "orders"


# ---------------------------------------------------------------------------
# render_manifests — document kinds and shape
# ---------------------------------------------------------------------------


def test_render_manifests_produces_two_deployments_two_services_one_ingress() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)

    assert len(by_kind["Deployment"]) == 2
    assert len(by_kind["Service"]) == 2
    assert len(by_kind["Ingress"]) == 1


def test_render_manifests_orders_replicas_matches_worker_count() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    orders = next(d for d in by_kind["Deployment"] if d["metadata"]["name"] == "fakeapp-orders")

    assert orders["spec"]["replicas"] == 3


def test_render_manifests_command_targets_worker_factory_on_all_interfaces() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    orders = next(d for d in by_kind["Deployment"] if d["metadata"]["name"] == "fakeapp-orders")
    command = orders["spec"]["template"]["spec"]["containers"][0]["command"]

    assert "modulith._worker:create_app" in command
    assert "--host" in command
    assert "0.0.0.0" in command


def test_render_manifests_env_has_module_identity_vars() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    orders = next(d for d in by_kind["Deployment"] if d["metadata"]["name"] == "fakeapp-orders")
    container = orders["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    assert env_by_name["MODULITH_MODULE"]["value"] == "orders"
    assert env_by_name["MODULITH_APP_PACKAGE"]["value"] == "fakeapp"
    assert env_by_name["MODULITH_TOPOLOGY"]["value"] == "processes"
    assert env_by_name["MODULITH_BROKER"]["value"] == "database"


def test_render_manifests_readiness_and_liveness_probes() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    orders = next(d for d in by_kind["Deployment"] if d["metadata"]["name"] == "fakeapp-orders")
    container = orders["spec"]["template"]["spec"]["containers"][0]

    assert container["readinessProbe"]["httpGet"]["path"] == "/health"
    assert container["readinessProbe"]["httpGet"]["port"] == 8000
    assert container["livenessProbe"]["tcpSocket"]["port"] == 8000


def test_render_manifests_omits_resources() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    orders = next(d for d in by_kind["Deployment"] if d["metadata"]["name"] == "fakeapp-orders")
    container = orders["spec"]["template"]["spec"]["containers"][0]

    assert "resources" not in container


def test_render_manifests_redis_streams_broker_url_and_alias_use_same_secret() -> None:
    cfg = _cfg(broker="redis-streams", broker_options={"url": "redis://example/0"})
    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    for var_name in ("MODULITH_BROKER_URL", "REDIS_URL"):
        secret_ref = env_by_name[var_name]["valueFrom"]["secretKeyRef"]
        assert secret_ref == {"name": "fakeapp-broker", "key": "url"}


def test_render_manifests_ingress_paths_sorted_prefix_and_backend() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    ingress = by_kind["Ingress"][0]
    paths = ingress["spec"]["rules"][0]["http"]["paths"]

    assert [p["path"] for p in paths] == ["/inventory", "/orders"]
    assert all(p["pathType"] == "Prefix" for p in paths)
    orders_path = next(p for p in paths if p["path"] == "/orders")
    assert orders_path["backend"]["service"]["name"] == "fakeapp-orders"
    assert orders_path["backend"]["service"]["port"]["number"] == 8000


def test_render_manifests_namespace_and_host_absent_by_default() -> None:
    text = render_manifests(_cfg(), _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    for doc in by_kind["Deployment"] + by_kind["Service"] + by_kind["Ingress"]:
        assert "namespace" not in doc["metadata"]
    assert "host" not in by_kind["Ingress"][0]["spec"]["rules"][0]


def test_render_manifests_namespace_and_host_present_when_passed() -> None:
    text = render_manifests(
        _cfg(), _two_module_specs(), image="shop:dev", namespace="prod", host="api.example.com"
    )

    by_kind = _docs_by_kind(text)
    for doc in by_kind["Deployment"] + by_kind["Service"] + by_kind["Ingress"]:
        assert doc["metadata"]["namespace"] == "prod"
    assert by_kind["Ingress"][0]["spec"]["rules"][0]["host"] == "api.example.com"


def test_render_manifests_underscore_module_name_becomes_hyphenated_resource() -> None:
    specs = [WorkerSpec(module_name="order_items", package="fakeapp", port=9001)]
    text = render_manifests(_cfg(), specs, image="shop:dev")

    by_kind = _docs_by_kind(text)
    assert by_kind["Deployment"][0]["metadata"]["name"] == "fakeapp-order-items"
    path = by_kind["Ingress"][0]["spec"]["rules"][0]["http"]["paths"][0]
    assert path["path"] == "/order_items"


@pytest.mark.parametrize("broker", ["shm", "memory"])
def test_render_manifests_single_host_brokers_refuse(broker: str) -> None:
    with pytest.raises(ConfigurationError, match=broker):
        render_manifests(_cfg(broker=broker), _two_module_specs(), image="shop:dev")


def test_render_manifests_database_broker_with_sqlite_url_refuses() -> None:
    cfg = _cfg(broker="database", broker_options={"url": "sqlite:///local.db"})

    with pytest.raises(ConfigurationError, match="sqlite"):
        render_manifests(cfg, _two_module_specs(), image="shop:dev")


def test_render_manifests_database_broker_without_url_renders() -> None:
    cfg = _cfg(broker="database", broker_options={})

    text = render_manifests(cfg, _two_module_specs(), image="shop:dev")

    by_kind = _docs_by_kind(text)
    assert len(by_kind["Deployment"]) == 2


def test_render_manifests_broker_options_passthrough_includes_redis_alias() -> None:
    cfg = _cfg(
        broker="redis-streams",
        broker_options={"url": "redis://example/0", "stream_prefix": "wf"},
    )
    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    assert env_by_name["MODULITH_BROKER_STREAM_PREFIX"]["value"] == "wf"
    assert env_by_name["MODULITH_STREAM_PREFIX"]["value"] == "wf"


def test_render_manifests_broker_options_json_encodes_structured_values() -> None:
    cfg = _cfg(broker_options={"expected_consumer_groups": {"a.B": ["inventory"]}})
    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    assert json.loads(env_by_name["MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS"]["value"]) == {
        "a.B": ["inventory"]
    }


def test_render_manifests_k8s_name_collision_raises() -> None:
    specs = [
        WorkerSpec(module_name="order-items", package="fakeapp", port=9001),
        WorkerSpec(module_name="order_items", package="fakeapp", port=9002),
    ]

    with pytest.raises(ConfigurationError, match="order"):
        render_manifests(_cfg(), specs, image="shop:dev")


# ---------------------------------------------------------------------------
# CLI: modulith k8s-manifest
# ---------------------------------------------------------------------------


def test_k8s_manifest_cli_writes_two_deployments_with_image(make_fake_app, monkeypatch, tmp_path):
    make_fake_app({"orders": "", "inventory": ""})
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n"
        'package = "fakeapp"\n'
        'broker = "database"\n'
        "[tool.modulith.workers]\n"
        "default = 2\n"
    )
    output = tmp_path / "f"

    result = runner.invoke(app, ["k8s-manifest", "--output", str(output), "--image", "reg/app:1"])

    assert result.exit_code == 0, result.output
    text = output.read_text()
    assert text.count("kind: Deployment") == 2
    assert text.count("replicas: 2") == 2
    assert 'image: "reg/app:1"' in text


def test_k8s_manifest_cli_default_shm_broker_exits_one(make_fake_app, monkeypatch, tmp_path):
    make_fake_app({"orders": ""})
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\npackage = "fakeapp"\n')

    result = runner.invoke(app, ["k8s-manifest"])

    assert result.exit_code == 1
    assert "shm" in result.output


def test_k8s_manifest_cli_stdout_output_starts_with_comment_and_has_ingress(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\npackage = "fakeapp"\nbroker = "database"\n'
    )

    result = runner.invoke(app, ["k8s-manifest", "--output", "-"])

    assert result.exit_code == 0, result.output
    assert result.output.startswith("# ")
    assert "kind: Ingress" in result.output
