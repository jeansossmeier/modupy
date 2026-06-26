"""modulith — modular monolith pattern for Python.

Public API for two audiences:

  Application authors — the everyday API:
      from modulith import event, listener, publish, configure

  Plugin authors — the extension contract:
      from modulith import hookimpl
      # plus hookspecs in modulith.hooks and protocols in modulith.protocols

Anything not re-exported here is internal and may change without notice.
"""

# ----- Application-facing API (what most users need) -----------------------
# ----- Broker dispatch registry --------------------------------------------
from .brokers import (
    BrokerRegistry,
    DuplicateBrokerError,
    UnknownBrokerError,
)

# ----- Configuration ------------------------------------------------------
from .config import Configuration, ConfigurationError
from .decorators import configure, event, listener, publish

# ----- Plugin manager (advanced — most users don't need this) -------------
from .manager import create_plugin_manager

# ----- Manifest (contract types + registration) ----------------------------
from .manifest import Manifest, declare_module, get_manifest

# ----- Plugin authoring surface --------------------------------------------
from .markers import hookimpl

# ----- Driver protocols ----------------------------------------------------
from .protocols import (
    Broker,
    EventSerializer,
    PublicationStore,
)

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
    "DuplicateBrokerError",
    # Contract types
    "EventPublication",
    "EventSerializer",
    "Manifest",
    "ModuleInfo",
    "PublicationStore",
    "UnknownBrokerError",
    "Violation",
    "ViolationSeverity",
    "configure",
    # Manager (advanced)
    "create_plugin_manager",
    # Manifest
    "declare_module",
    # Application API
    "event",
    "get_manifest",
    # Plugin authoring
    "hookimpl",
    "listener",
    "publish",
]
