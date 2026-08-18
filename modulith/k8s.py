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

import json
from urllib.parse import urlsplit

from .config import Configuration, ConfigurationError, _configured_broker_url
from .supervisor import _REDIS_BROKER_ENV_ALIASES, WorkerSpec

# Fixed key inside the <package>-broker Secret — see the module docstring's
# kubectl create secret example. Independent of whatever key name a local
# pyproject.toml uses ("url" or "dsn"): the manifest never embeds a URL, so
# the Secret's key is a generator-wide convention, not a passthrough of it.
_BROKER_SECRET_KEY = "url"

# broker_options keys carrying a connection string are never forwarded as a
# literal env var (that would put a credential in the manifest); they are
# replaced by the secretKeyRef wiring in _env_lines instead.
_URL_LIKE_OPTION_KEYS = frozenset({"url", "dsn"})

_SINGLE_HOST_BROKERS = frozenset({"memory", "shm"})


def k8s_name(name: str) -> str:
    """Normalize a package/module name into an RFC-1123 DNS label.

    Kubernetes object names must be lowercase alphanumerics and ``-``.
    Python package/module names use identifier rules (letters, digits,
    underscores), so the only translation needed is lowercasing and
    turning underscores into hyphens.
    """
    return name.lower().replace("_", "-")


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
    ``ConfigurationError`` when ``cfg.broker`` cannot be shared across
    pods (``memory``, ``shm``, or a ``database`` broker pointed at a
    sqlite:// URL — both are single-host storage, unreachable from a
    sibling pod), or when two module names normalize to the same
    Kubernetes resource name via ``k8s_name``.
    """
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

    pkg_name = k8s_name(cfg.package or "")
    names: dict[str, str] = {}
    for spec in specs:
        name = f"{pkg_name}-{k8s_name(spec.module_name)}"
        if name in names and names[name] != spec.module_name:
            raise ConfigurationError(
                f"modules {names[name]!r} and {spec.module_name!r} both normalize to "
                f"the Kubernetes resource name {name!r} — rename one of them"
            )
        names[name] = spec.module_name

    header = (
        "# Generated by `modulith k8s-manifest`.\n"
        "#\n"
        f"# Before applying, create the broker connection Secret:\n"
        f"#   kubectl create secret generic {pkg_name}-broker "
        f"--from-literal={_BROKER_SECRET_KEY}=<broker connection URL>\n"
        "#\n"
        "# Optionally, create an env Secret for any extra variables a module needs\n"
        "# (referenced with `optional: true`, so it is never required):\n"
        f"#   kubectl create secret generic {pkg_name}-env --from-literal=SOME_KEY=value\n"
    )

    documents = [header.rstrip("\n")]
    for spec in specs:
        name = f"{pkg_name}-{k8s_name(spec.module_name)}"
        documents.append(
            _deployment(
                cfg, spec, image=image, port=port, name=name, pkg_name=pkg_name, namespace=namespace
            )
        )
        documents.append(_service(spec, port=port, name=name, namespace=namespace))
    documents.append(
        _ingress(specs, names, port=port, pkg_name=pkg_name, namespace=namespace, host=host)
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


def _env_lines(cfg: Configuration, spec: WorkerSpec, pkg_name: str) -> list[str]:
    """Build the container's ``env:`` entries, indented under the ``env:`` key.

    ``env:`` itself sits at column 10 (a sibling of ``image:``/``command:``
    inside the container mapping); each list item's ``-`` therefore goes two
    columns deeper (12), with the item's own keys two deeper still (14).
    """
    entries: list[tuple[str, str]] = [
        ("MODULITH_MODULE", f"              value: {_yaml_str(spec.module_name)}"),
        ("MODULITH_APP_PACKAGE", f"              value: {_yaml_str(spec.package)}"),
        ("MODULITH_TOPOLOGY", '              value: "processes"'),
        ("MODULITH_BROKER", f"              value: {_yaml_str(cfg.broker)}"),
    ]

    secret_ref = (
        "              valueFrom:\n"
        "                secretKeyRef:\n"
        f"                  name: {_yaml_str(f'{pkg_name}-broker')}\n"
        f"                  key: {_yaml_str(_BROKER_SECRET_KEY)}"
    )
    url_vars = ["MODULITH_BROKER_URL"]
    if cfg.broker == "redis-streams":
        url_vars.append(_REDIS_BROKER_ENV_ALIASES["MODULITH_BROKER_URL"])
    for var_name in url_vars:
        entries.append((var_name, secret_ref))

    for key, value in sorted((cfg.broker_options or {}).items()):
        if key in _URL_LIKE_OPTION_KEYS:
            continue
        env_key = f"MODULITH_BROKER_{key.upper()}"
        rendered = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        entries.append((env_key, f"              value: {_yaml_str(rendered)}"))
        if cfg.broker == "redis-streams" and env_key in _REDIS_BROKER_ENV_ALIASES:
            alias = _REDIS_BROKER_ENV_ALIASES[env_key]
            entries.append((alias, f"              value: {_yaml_str(rendered)}"))

    lines: list[str] = []
    for var_name, snippet in entries:
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
    pkg_name: str,
    namespace: str | None,
) -> str:
    """Render one Deployment document."""
    env_block = "\n".join(_env_lines(cfg, spec, pkg_name))
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
                name: {_yaml_str(f"{pkg_name}-env")}
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
    names: dict[str, str],
    *,
    port: int,
    pkg_name: str,
    namespace: str | None,
    host: str | None,
) -> str:
    """Render the single Ingress document routing to every module's Service."""
    by_module = {module_name: name for name, module_name in names.items()}
    path_entries = []
    for spec in sorted(specs, key=lambda s: s.module_name):
        name = by_module[spec.module_name]
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
    metadata = _metadata_lines(indent="  ", name=f"{pkg_name}-ingress", namespace=namespace)
    return f"""apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
{metadata}
spec:
  rules:
{rules_block}
"""
