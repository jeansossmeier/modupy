"""Kubernetes manifest generator for the process-per-module topology.

Renders one Deployment + Service per discovered module plus a single
Ingress fanning out ``/<module>`` paths to each module's Service, matching
the process-per-module shape ``modulith.supervisor`` runs locally: one
``modulith._worker:create_app`` uvicorn process per module, health-checked
via the ``/health`` endpoint that worker exposes.

Stdlib-only by design: SQLAlchemy and PyYAML are both optional extras (see
``pyproject.toml``), so manifests are hand-assembled YAML text rather than
parsed/rendered through either library. The broker connection URL is never
embedded in the manifest — it is expected to live in a Kubernetes Secret,
created once per cluster/namespace outside of this generator::

    kubectl create secret generic <package>-broker \\
        --from-literal=url=<broker connection URL>

Every container also references an optional ``<package>-env`` Secret
(``envFrom``, ``optional: true``) for any additional environment variables
an operator wants injected without editing the generated manifest::

    kubectl create secret generic <package>-env \\
        --from-literal=SOME_KEY=some-value
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import unicodedata
from urllib.parse import urlsplit

from .config import Configuration, ConfigurationError, _configured_broker_url
from .supervisor import _REDIS_BROKER_ENV_ALIASES, WorkerSpec

# Generated manifests always read broker credentials from this Secret key.
_BROKER_SECRET_KEY = "url"

_URL_LIKE_OPTION_KEYS = frozenset({"url", "dsn"})

_SINGLE_HOST_BROKERS = frozenset({"memory", "shm"})

# Forward only documented database option env vars; credentials stay in a Secret.
_DATABASE_BROKER_OPTION_KEYS = frozenset(
    {
        "batch_size",
        "busy_timeout_ms",
        "completion_mode",
        "dispatch_concurrency",
        "expected_consumer_groups",
        "max_delivery_attempts",
        "max_overflow",
        "max_payload_bytes",
        "no_subscriber_policy",
        "no_subscriber_wait_poll_interval_ms",
        "no_subscriber_wait_timeout_seconds",
        "orphan_replay_policy",
        "orphan_retention_seconds",
        "poll_interval_ms",
        "pool_size",
        "prune_interval_seconds",
        "reclaim_stale_seconds",
        "retention_age_seconds",
        "retention_count",
        "schema",
        "sqlite_synchronous",
        "state_dir",
    }
)

# Redis accepts these established names; its other options lack an environment contract.
_REDIS_OPTION_ENV_NAMES = {
    "consumer_group": _REDIS_BROKER_ENV_ALIASES["MODULITH_BROKER_CONSUMER_GROUP"],
    "max_stream_len": _REDIS_BROKER_ENV_ALIASES["MODULITH_BROKER_MAX_STREAM_LEN"],
    "stream_prefix": _REDIS_BROKER_ENV_ALIASES["MODULITH_BROKER_STREAM_PREFIX"],
}

_RFC1123_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_K8S_NAME_MAX_LENGTH = 63
_K8S_HASH_LENGTH = 10


def k8s_name(name: str) -> str:
    """Normalize a package/module name into an RFC-1123 DNS label.

    Long names retain a readable prefix and a stable hash suffix so distinct
    inputs do not collapse merely because Kubernetes limits labels to 63 bytes.
    """
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:_K8S_HASH_LENGTH]
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    normalized = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")

    if not normalized:
        if any(character.isalnum() for character in name):
            normalized = f"x-{digest}"
        else:
            raise ConfigurationError(
                f"{name!r} cannot produce a non-empty Kubernetes RFC-1123 name"
            )

    if len(normalized) > _K8S_NAME_MAX_LENGTH:
        prefix_length = _K8S_NAME_MAX_LENGTH - _K8S_HASH_LENGTH - 1
        prefix = normalized[:prefix_length].rstrip("-")
        normalized = f"{prefix}-{digest}"

    if not _RFC1123_LABEL.fullmatch(normalized):
        raise ConfigurationError(f"{name!r} cannot produce a valid Kubernetes RFC-1123 name")
    return normalized


def _is_sqlite_url(url: str) -> bool:
    """True when ``url`` names the SQLite backend (any driver).

    Mirrors ``adapters.db_broker._is_sqlite_url`` without importing
    SQLAlchemy (an optional extra this stdlib-only module must not depend
    on): the backend name is the URL scheme with any ``+driver`` suffix
    stripped, exactly as SQLAlchemy's ``make_url().get_backend_name()``
    computes it.
    """
    scheme = urlsplit(url).scheme
    return scheme.split("+", 1)[0].lower() == "sqlite"


def render_manifests(
    cfg: Configuration,
    specs: list[WorkerSpec],
    *,
    image: str,
    port: int = 8000,
    namespace: str | None = None,
    host: str | None = None,
) -> str:
    """Render a multi-document YAML manifest for ``specs``.

    One Deployment + Service per spec, plus a single Ingress routing
    ``/<module_name>`` to each module's Service. Raises
    ``ConfigurationError`` for invalid ports or names, resource-name
    collisions, or brokers that cannot be shared across pods.
    """
    if type(port) is not int or not 1 <= port <= 65535:
        raise ConfigurationError(f"port must be an integer from 1 to 65535, got {port!r}")
    if namespace is not None and not _RFC1123_LABEL.fullmatch(namespace):
        raise ConfigurationError(
            f"namespace {namespace!r} must be an RFC-1123 DNS label of at most 63 characters"
        )

    if cfg.broker in _SINGLE_HOST_BROKERS:
        raise ConfigurationError(
            f"the {cfg.broker!r} broker only works within a single process/host and "
            "cannot back a Kubernetes deployment where each module runs in its own "
            "pod; configure a shared broker (database or redis-streams) via "
            "[tool.modulith.broker] before generating manifests"
        )
    if cfg.broker == "database":
        url = _configured_broker_url(cfg.broker_options)
        if url is not None and _is_sqlite_url(url):
            raise ConfigurationError(
                "the database broker is configured with a sqlite:// URL, which — "
                "like the shm broker — lives on a single host's filesystem and "
                "cannot be shared across Kubernetes pods; point "
                "[tool.modulith.broker_options].url at a networked database "
                "(postgresql://, mysql://) before generating manifests"
            )

    package = cfg.package or ""
    pkg_name = k8s_name(package)
    resource_names: dict[str, str] = {}
    modules_by_resource_name: dict[str, str] = {}
    for spec in specs:
        module_name = k8s_name(spec.module_name)
        name = k8s_name(f"{pkg_name}-{module_name}")
        if name in modules_by_resource_name:
            raise ConfigurationError(
                f"modules {modules_by_resource_name[name]!r} and {spec.module_name!r} "
                f"both normalize to the Kubernetes resource name {name!r} — rename one of them"
            )
        modules_by_resource_name[name] = spec.module_name
        resource_names[spec.module_name] = name

    broker_secret_name = k8s_name(f"{pkg_name}-broker")
    env_secret_name = k8s_name(f"{pkg_name}-env")
    namespace_arg = f" --namespace {shlex.quote(namespace)}" if namespace is not None else ""
    header = (
        "# Generated by `modulith k8s-manifest`.\n"
        "#\n"
        f"# Before applying, create the broker connection Secret:\n"
        f"#   kubectl create secret generic {broker_secret_name} "
        f"--from-literal={_BROKER_SECRET_KEY}=<broker connection URL>{namespace_arg}\n"
        "#\n"
        "# Optionally, create an env Secret for any extra variables a module needs\n"
        "# (referenced with `optional: true`, so it is never required):\n"
        f"#   kubectl create secret generic {env_secret_name} "
        f"--from-literal=SOME_KEY=value{namespace_arg}\n"
    )

    documents = [header.rstrip("\n")]
    for spec in specs:
        name = resource_names[spec.module_name]
        documents.append(
            _deployment(
                cfg,
                spec,
                image=image,
                port=port,
                name=name,
                broker_secret_name=broker_secret_name,
                env_secret_name=env_secret_name,
                namespace=namespace,
            )
        )
        documents.append(_service(spec, port=port, name=name, namespace=namespace))
    documents.append(
        _ingress(
            specs,
            resource_names,
            port=port,
            pkg_name=pkg_name,
            namespace=namespace,
            host=host,
        )
    )
    return "\n---\n".join(documents) + "\n"


def _yaml_str(value: str) -> str:
    """A YAML-safe double-quoted scalar. JSON string escaping is a valid
    subset of YAML's double-quoted scalar escaping, so ``json.dumps`` is
    reused rather than duplicating an escaper."""
    return json.dumps(value)


def _metadata_lines(*, indent: str, name: str, namespace: str | None, extra: str = "") -> str:
    lines = [f"{indent}name: {_yaml_str(name)}"]
    if namespace is not None:
        lines.append(f"{indent}namespace: {_yaml_str(namespace)}")
    if extra:
        lines.append(extra)
    return "\n".join(lines)


def _env_lines(cfg: Configuration, spec: WorkerSpec, broker_secret_name: str) -> list[str]:
    """Build container env entries while keeping broker credentials in a Secret."""
    entries: dict[str, str] = {
        "MODULITH_MODULE": f"              value: {_yaml_str(spec.module_name)}",
        "MODULITH_APP_PACKAGE": f"              value: {_yaml_str(spec.package)}",
        "MODULITH_TOPOLOGY": '              value: "processes"',
        "MODULITH_BROKER": f"              value: {_yaml_str(cfg.broker)}",
        "MODULITH_CONTRACTS_MODULE": (f"              value: {_yaml_str(cfg.contracts_module)}"),
    }

    secret_ref = (
        "              valueFrom:\n"
        "                secretKeyRef:\n"
        f"                  name: {_yaml_str(broker_secret_name)}\n"
        f"                  key: {_yaml_str(_BROKER_SECRET_KEY)}"
    )
    url_vars = ["MODULITH_BROKER_URL"]
    if cfg.broker == "redis-streams":
        url_vars.append(_REDIS_BROKER_ENV_ALIASES["MODULITH_BROKER_URL"])
    for var_name in url_vars:
        entries[var_name] = secret_ref

    for key, value in sorted((cfg.broker_options or {}).items()):
        if type(key) is not str or key != key.lower():
            continue
        if key in _URL_LIKE_OPTION_KEYS:
            continue
        if cfg.broker == "database" and key in _DATABASE_BROKER_OPTION_KEYS:
            env_key = f"MODULITH_BROKER_{key.upper()}"
        elif cfg.broker == "redis-streams" and key in _REDIS_OPTION_ENV_NAMES:
            env_key = _REDIS_OPTION_ENV_NAMES[key]
        else:
            continue
        rendered = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        entries[env_key] = f"              value: {_yaml_str(rendered)}"

    lines: list[str] = []
    for var_name, snippet in entries.items():
        lines.append(f"            - name: {_yaml_str(var_name)}")
        lines.append(snippet)
    return lines


def _deployment(
    cfg: Configuration,
    spec: WorkerSpec,
    *,
    image: str,
    port: int,
    name: str,
    broker_secret_name: str,
    env_secret_name: str,
    namespace: str | None,
) -> str:
    """Render one Deployment document."""
    env_block = "\n".join(_env_lines(cfg, spec, broker_secret_name))
    metadata = _metadata_lines(
        indent="  ",
        name=name,
        namespace=namespace,
        extra=f"  labels:\n    app: {_yaml_str(name)}",
    )
    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
{metadata}
spec:
  replicas: {spec.worker_count}
  selector:
    matchLabels:
      app: {_yaml_str(name)}
  template:
    metadata:
      labels:
        app: {_yaml_str(name)}
    spec:
      containers:
        - name: {_yaml_str(name)}
          image: {_yaml_str(image)}
          command:
            - "python"
            - "-m"
            - "uvicorn"
            - "modulith._worker:create_app"
            - "--factory"
            - "--host"
            - "0.0.0.0"
            - "--port"
            - {_yaml_str(str(port))}
          ports:
            - containerPort: {port}
          env:
{env_block}
          envFrom:
            - secretRef:
                name: {_yaml_str(env_secret_name)}
                optional: true
          readinessProbe:
            httpGet:
              path: /health
              port: {port}
          livenessProbe:
            tcpSocket:
              port: {port}
"""


def _service(spec: WorkerSpec, *, port: int, name: str, namespace: str | None) -> str:
    """Render one Service document."""
    metadata = _metadata_lines(indent="  ", name=name, namespace=namespace)
    return f"""apiVersion: v1
kind: Service
metadata:
{metadata}
spec:
  selector:
    app: {_yaml_str(name)}
  ports:
    - port: {port}
      targetPort: {port}
"""


def _ingress(
    specs: list[WorkerSpec],
    resource_names: dict[str, str],
    *,
    port: int,
    pkg_name: str,
    namespace: str | None,
    host: str | None,
) -> str:
    """Render the single Ingress document routing to every module's Service."""
    path_entries = []
    for spec in sorted(specs, key=lambda s: s.module_name):
        name = resource_names[spec.module_name]
        path_entries.append(
            f"""          - path: /{spec.module_name}
            pathType: Prefix
            backend:
              service:
                name: {_yaml_str(name)}
                port:
                  number: {port}"""
        )
    paths_block = "\n".join(path_entries)
    rule_lines = []
    if host is not None:
        rule_lines.append(f"    - host: {_yaml_str(host)}")
        rule_lines.append("      http:")
    else:
        rule_lines.append("    - http:")
    rule_lines.append("        paths:")
    rule_lines.append(paths_block)
    rules_block = "\n".join(rule_lines)
    metadata = _metadata_lines(
        indent="  ", name=k8s_name(f"{pkg_name}-ingress"), namespace=namespace
    )
    return f"""apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
{metadata}
spec:
  rules:
{rules_block}
"""
