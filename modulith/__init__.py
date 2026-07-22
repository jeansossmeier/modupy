"""modulith — modular monolith pattern for Python.

Public API for two audiences:

  Application authors — the everyday API:
      from modulith import event, listener, publish, configure

  Plugin authors — the extension contract:
      from modulith import hookimpl
      # plus hookspecs in modulith.hooks and protocols in modulith.protocols

Anything not re-exported here is internal and may change without notice.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("modulith")
except PackageNotFoundError:  # running from a source checkout that isn't installed
    __version__ = "1.0.0"

# ----- Application-facing API (what most users need) -----------------------
# ----- Broker dispatch registry --------------------------------------------
from .brokers import (
    BrokerRegistry,
    ConsumerRegistry,
    ConsumerSpec,
    DuplicateBrokerError,
    DuplicateConsumerError,
    UnknownBrokerError,
    UnknownConsumerError,
)

# ----- Configuration ------------------------------------------------------
from .config import Configuration, ConfigurationError
from .decorators import bootstrap, configure, event, externalized, listener, publish

# ----- Plugin manager (advanced — most users don't need this) -------------
from .manager import create_plugin_manager

# ----- Manifest (contract types + registration) ----------------------------
from .manifest import Manifest, declare_module, get_manifest

# ----- Plugin authoring surface --------------------------------------------
from .markers import hookimpl

# ----- Driver protocols ----------------------------------------------------
from .protocols import (
    Broker,
    Consumer,
    EventSerializer,
    PublicationStore,
)

# ----- Sync entrypoint (sync views, scripts, sync DB code) -----------------
from .sync import PublishSyncTimeout, publish_sync

# ----- Plugin contract types -----------------------------------------------
from .types import (
    EventPublication,
    ModuleInfo,
    Violation,
    ViolationSeverity,
)

__all__ = [
    # Driver protocols
    "Broker",
    # Broker registry
    "BrokerRegistry",
    # Configuration
    "Configuration",
    "ConfigurationError",
    # Consumer contract (cross-process, process-per-module)
    "Consumer",
    "ConsumerRegistry",
    "ConsumerSpec",
    "DuplicateBrokerError",
    "DuplicateConsumerError",
    # Contract types
    "EventPublication",
    "EventSerializer",
    "Manifest",
    "ModuleInfo",
    "PublicationStore",
    "PublishSyncTimeout",
    "UnknownBrokerError",
    "UnknownConsumerError",
    "Violation",
    "ViolationSeverity",
    "__version__",
    "bootstrap",
    "configure",
    # Manager (advanced)
    "create_plugin_manager",
    # Manifest
    "declare_module",
    # Application API
    "event",
    "externalized",
    "get_manifest",
    # Plugin authoring
    "hookimpl",
    "listener",
    "publish",
    "publish_sync",
]
