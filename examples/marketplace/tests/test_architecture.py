import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
import yaml

PROJECT = Path(__file__).resolve().parents[1]
MODULITH = Path(sys.executable).parent / "modulith"
ROUTED_MODULES = {"inventory", "notifications", "orders", "reporting", "shipping"}

Run = Callable[..., subprocess.CompletedProcess[str]]


@pytest.fixture
def run(tmp_path: Path) -> Run:
    env = {key: value for key, value in os.environ.items() if not key.startswith("MODULITH_")}
    env["XDG_STATE_HOME"] = str(tmp_path / "state")

    def invoke(*args: str, cwd: Path = PROJECT) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(MODULITH), *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    return invoke


def output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


def scalars(node: object) -> Iterator[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            yield from scalars(key)
            yield from scalars(value)
    elif isinstance(node, list):
        for item in node:
            yield from scalars(item)


def schema_refs(node: object) -> Iterator[str]:
    for text in scalars(node):
        if text.startswith("#/components/schemas/"):
            yield text.removeprefix("#/components/schemas/")


def copy_of_example(tmp_path: Path) -> Path:
    copy = tmp_path / "copy"
    shutil.copytree(
        PROJECT,
        copy,
        ignore=shutil.ignore_patterns("__pycache__", "tests", "build", ".pytest_cache"),
    )
    return copy


def test_verify_passes_on_the_example_as_committed(run: Run) -> None:
    result = run("verify")

    assert result.returncode == 0, output(result)


def test_verify_rejects_a_table_without_its_module_prefix(run: Run, tmp_path: Path) -> None:
    copy = copy_of_example(tmp_path)
    untouched = run("verify", cwd=copy)
    assert untouched.returncode == 0, output(untouched)

    for name in ("tables.py", "_manifest.py"):
        path = copy / "marketplace" / "notifications" / name
        text = path.read_text()
        assert '"notifications_notification"' in text
        path.write_text(text.replace('"notifications_notification"', '"notification"'))
    result = run("verify", cwd=copy)

    assert result.returncode == 1, output(result)
    assert "table-prefix" in output(result)


def test_extracting_notifications_leaves_a_standalone_tree_without_orders(
    run: Run, tmp_path: Path
) -> None:
    target = tmp_path / "service"

    result = run("extract", "notifications", "--output", str(target))

    assert result.returncode == 0, output(result)
    assert (target / "marketplace" / "notifications").is_dir()
    assert (target / "marketplace" / "contracts").is_dir()
    assert (target / "marketplace" / "db.py").is_file()
    assert not (target / "marketplace" / "orders").exists()


def test_extracting_orders_is_blocked_by_its_catalog_import(run: Run, tmp_path: Path) -> None:
    target = tmp_path / "service"

    result = run("extract", "orders", "--output", str(target))

    assert result.returncode != 0
    assert "extraction blocked" in output(result)
    assert "catalog" in output(result)
    assert not target.exists()


def test_openapi_prefixes_every_schema_with_its_module_and_skips_modules_without_a_router(
    run: Run, tmp_path: Path
) -> None:
    target = tmp_path / "openapi.json"

    result = run("openapi", "--output", str(target))

    assert result.returncode == 0, output(result)
    assert "skipped (no router): catalog, payments" in result.stdout
    document = json.loads(target.read_text())
    schemas = document["components"]["schemas"]
    assert all(name.split("_", 1)[0] in ROUTED_MODULES for name in schemas)
    for path, operations in document["paths"].items():
        module = path.split("/")[1]
        assert all(ref.startswith(f"{module}_") for ref in schema_refs(operations)), path


def test_k8s_manifest_holds_one_deployment_per_module_wired_to_secrets_not_urls(
    run: Run, tmp_path: Path
) -> None:
    target = tmp_path / "k8s.yaml"

    result = run("k8s-manifest", "--image", "marketplace:1.0.0", "--output", str(target))

    assert result.returncode == 0, output(result)
    documents = [document for document in yaml.safe_load_all(target.read_text()) if document]
    deployments = [document for document in documents if document["kind"] == "Deployment"]
    assert len(deployments) == 7
    replicas = {d["metadata"]["name"]: d["spec"]["replicas"] for d in deployments}
    assert replicas.pop("marketplace-reporting") == 2
    assert set(replicas.values()) == {1}
    assert not [text for text in scalars(documents) if "://" in text]
    for deployment in deployments:
        (container,) = deployment["spec"]["template"]["spec"]["containers"]
        broker = [e for e in container["env"] if e["name"] == "MODULITH_BROKER_URL"]
        assert [e["valueFrom"]["secretKeyRef"]["name"] for e in broker] == ["marketplace-broker"]
        assert [e["secretRef"]["name"] for e in container["envFrom"]] == ["marketplace-env"]
