"""Tests for the Kubernetes manifest generator (``modulith.k8s``).

Unit tests build a ``Configuration``/``WorkerSpec`` pair directly and parse
``render_manifests``' output with ``yaml.safe_load_all`` (PyYAML is a
``test``-extra dependency, never a runtime one — see ``modulith/k8s.py``).
CLI tests drive the ``k8s-manifest`` command end-to-end through
``typer.testing.CliRunner`` against a ``make_fake_app``-built package.
"""

from __future__ import annotations

import hashlib
import json
import re

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


@pytest.mark.parametrize(
    ("raw_name", "expected"),
    [
        ("orders.api", "orders-api"),
        ("--orders__api--", "orders-api"),
        ("Café", "cafe"),
    ],
)
def test_k8s_name_normalizes_rfc1123_edge_cases(raw_name: str, expected: str) -> None:
    assert k8s_name(raw_name) == expected


def test_k8s_name_hashes_unicode_only_name_stably() -> None:
    first = k8s_name("订单")
    second = k8s_name("订单")

    assert first == second
    assert first != k8s_name("支付")
    assert re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", first)


def test_k8s_name_truncates_with_stable_hash() -> None:
    raw_name = "orders-" + ("a" * 80)
    expected_hash = hashlib.sha256(raw_name.encode()).hexdigest()[:10]

    name = k8s_name(raw_name)

    assert len(name) == 63
    assert name == f"{raw_name[:52]}-{expected_hash}"


@pytest.mark.parametrize("raw_name", ["", "---", "..."])
def test_k8s_name_rejects_empty_generated_name(raw_name: str) -> None:
    with pytest.raises(ConfigurationError, match="Kubernetes"):
        k8s_name(raw_name)


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
    text = render_manifests(
        _cfg(contracts_module="fakeapp.contracts"), _two_module_specs(), image="shop:dev"
    )

    by_kind = _docs_by_kind(text)
    orders = next(d for d in by_kind["Deployment"] if d["metadata"]["name"] == "fakeapp-orders")
    container = orders["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    assert env_by_name["MODULITH_MODULE"]["value"] == "orders"
    assert env_by_name["MODULITH_APP_PACKAGE"]["value"] == "fakeapp"
    assert env_by_name["MODULITH_TOPOLOGY"]["value"] == "processes"
    assert env_by_name["MODULITH_BROKER"]["value"] == "database"
    assert env_by_name["MODULITH_CONTRACTS_MODULE"]["value"] == "fakeapp.contracts"


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


def test_render_manifests_rejects_module_name_that_is_not_an_identifier() -> None:
    """The Ingress path is the raw module name (it must match the worker's
    ``/<module>`` mount), so a name carrying YAML syntax is refused outright
    instead of being interpolated into a manifest an operator applies as-is."""
    evil_name = (
        "orders\n"
        "          - path: /evil\n"
        "            pathType: Prefix\n"
        "            backend:\n"
        "              service:\n"
        "                name: attacker-svc\n"
        "                port:\n"
        "                  number: 9999"
    )
    specs = [WorkerSpec(module_name=evil_name, package="fakeapp", port=9001)]

    with pytest.raises(ConfigurationError, match="not a dotted Python identifier"):
        render_manifests(_cfg(), specs, image="shop:dev")


def test_render_manifests_ingress_path_is_the_raw_module_name_the_worker_mounts() -> None:
    specs = [WorkerSpec(module_name="order_items", package="fakeapp", port=9001)]

    text = render_manifests(_cfg(), specs, image="shop:dev")

    by_kind = _docs_by_kind(text)
    path = by_kind["Ingress"][0]["spec"]["rules"][0]["http"]["paths"][0]
    assert path["path"] == "/order_items"
    assert '"/order_items"' in text


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


@pytest.mark.parametrize(
    "namespace",
    [
        "Prod",
        "prod_namespace",
        "prod\nmetadata:\n  name: injected",
        "prod\x00evil",
    ],
)
def test_render_manifests_rejects_invalid_namespace(namespace: str) -> None:
    with pytest.raises(ConfigurationError, match=r"namespace.*RFC-1123"):
        render_manifests(
            _cfg(),
            [WorkerSpec("orders", "fakeapp", 9001)],
            image="shop:dev",
            namespace=namespace,
        )


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


def test_render_manifests_redis_options_use_consumable_aliases() -> None:
    cfg = _cfg(
        broker="redis-streams",
        broker_options={
            "url": "redis://example/0",
            "stream_prefix": "wf",
            "consumer_group": "workers",
            "max_stream_len": 5000,
        },
    )
    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    assert env_by_name["MODULITH_STREAM_PREFIX"]["value"] == "wf"
    assert env_by_name["MODULITH_CONSUMER_GROUP"]["value"] == "workers"
    assert env_by_name["MODULITH_STREAM_MAXLEN"]["value"] == "5000"
    assert "MODULITH_BROKER_STREAM_PREFIX" not in env_by_name
    assert "MODULITH_BROKER_CONSUMER_GROUP" not in env_by_name
    assert "MODULITH_BROKER_MAX_STREAM_LEN" not in env_by_name


def test_render_manifests_broker_options_json_encodes_structured_values() -> None:
    cfg = _cfg(
        broker_options={
            "expected_consumer_groups": {"a.B": ["inventory"]},
            "schema": "tenant_orders",
        }
    )
    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {e["name"]: e for e in container["env"]}

    assert env_by_name["MODULITH_BROKER_SCHEMA"]["value"] == "tenant_orders"
    assert json.loads(env_by_name["MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS"]["value"]) == {
        "a.B": ["inventory"]
    }


def test_render_manifests_drops_unknown_and_credential_like_broker_options() -> None:
    cfg = _cfg(
        broker_options={
            "URL": "postgresql://admin:db-password-value@example/db",
            "DsN": "postgresql://admin:dsn-password-value@example/db",
            "password": "hunter2",
            "api_token": "token-value",
            "custom": "arbitrary-value",
            "completion_mode": "mark",
            "COMPLETION_MODE": "delete",
        }
    )
    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_names = [entry["name"] for entry in container["env"]]

    assert len(env_names) == len(set(env_names))
    assert env_names.count("MODULITH_BROKER_COMPLETION_MODE") == 1
    for secret in (
        "db-password-value",
        "dsn-password-value",
        "hunter2",
        "token-value",
        "arbitrary-value",
    ):
        assert secret not in text


def test_render_manifests_drops_mixed_case_allowlisted_broker_option() -> None:
    cfg = _cfg(broker_options={"Completion_Mode": "mark"})

    text = render_manifests(cfg, [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev")

    by_kind = _docs_by_kind(text)
    container = by_kind["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    env_names = {entry["name"] for entry in container["env"]}
    assert "MODULITH_BROKER_COMPLETION_MODE" not in env_names


def test_render_manifests_k8s_name_collision_raises() -> None:
    specs = [
        WorkerSpec(module_name="order-items", package="fakeapp", port=9001),
        WorkerSpec(module_name="order_items", package="fakeapp", port=9002),
    ]

    with pytest.raises(ConfigurationError, match="order"):
        render_manifests(_cfg(), specs, image="shop:dev")


@pytest.mark.parametrize("port", [0, -1, 65536])
def test_render_manifests_rejects_invalid_port(port: int) -> None:
    with pytest.raises(ConfigurationError, match="port"):
        render_manifests(_cfg(), _two_module_specs(), image="shop:dev", port=port)


def test_render_manifests_long_composite_names_are_valid_and_distinct() -> None:
    package = "shop." + ("a" * 55)
    specs = [
        WorkerSpec(module_name=f"orders_{suffix}", package=package, port=9001)
        for suffix in ("east", "west")
    ]

    text = render_manifests(_cfg(package=package), specs, image="shop:dev")

    names = [doc["metadata"]["name"] for doc in _docs_by_kind(text)["Deployment"]]
    assert len(names) == len(set(names))
    assert all(
        len(name) <= 63 and re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", name)
        for name in names
    )


def test_render_manifests_secret_commands_include_namespace() -> None:
    text = render_manifests(
        _cfg(), [WorkerSpec("orders", "fakeapp", 9001)], image="shop:dev", namespace="prod"
    )

    secret_commands = [
        line for line in text.splitlines() if line.startswith("#   kubectl create secret")
    ]
    assert len(secret_commands) == 2
    assert all("--namespace prod" in command for command in secret_commands)


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


def test_generated_header_names_the_outbox_url_as_a_required_env_secret_key() -> None:
    text = render_manifests(_cfg(outbox="postgres"), _two_module_specs(), image="shop:dev")

    header = text.split("\n---\n", 1)[0]
    assert "MODULITH_OUTBOX_URL" in header
    assert "fakeapp-env" in header
    assert 'outbox is not "memory"' in header
    assert "never required" not in header


def test_module_docstring_names_the_outbox_url_as_a_required_env_secret_key() -> None:
    from modulith import k8s

    assert k8s.__doc__ is not None
    assert "MODULITH_OUTBOX_URL" in k8s.__doc__
    assert '``outbox`` is not ``"memory"``' in k8s.__doc__
