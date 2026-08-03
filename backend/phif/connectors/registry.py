"""Connector auto-discovery.

Walks the ``phif.connectors`` package, imports every subpackage, and collects
all concrete :class:`HypervisorConnector` subclasses. This avoids a central
registration file — each connector agent adds only its own subpackage, so
parallel work never collides on a shared file.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from functools import lru_cache

from phif.connectors.base import HypervisorConnector

log = logging.getLogger(__name__)


def _iter_connector_modules():
    import phif.connectors as pkg

    for mod in pkgutil.iter_modules(pkg.__path__):
        # subpackages only; skip base.py / registry.py / __init__.py modules
        if mod.ispkg:
            yield f"phif.connectors.{mod.name}"


def _all_subclasses(cls: type) -> set[type]:
    found: set[type] = set()
    for sub in cls.__subclasses__():
        found.add(sub)
        found |= _all_subclasses(sub)
    return found


@lru_cache
def discover() -> dict[str, type[HypervisorConnector]]:
    """Return ``{connector_key: connector_class}`` for all discovered connectors."""
    for module_name in _iter_connector_modules():
        try:
            importlib.import_module(module_name)
        except Exception:  # a broken connector must not take down the whole app
            log.exception("Failed to import connector module %s", module_name)

    registry: dict[str, type[HypervisorConnector]] = {}
    for cls in _all_subclasses(HypervisorConnector):
        if getattr(cls, "__abstractmethods__", None):
            continue  # skip abstract intermediates
        if not cls.key:
            log.warning("Connector %s has no `key`; skipping", cls.__name__)
            continue
        if cls.key in registry:
            log.warning("Duplicate connector key %r (%s); keeping first", cls.key, cls.__name__)
            continue
        registry[cls.key] = cls
    return registry


def get_connector_class(key: str) -> type[HypervisorConnector] | None:
    return discover().get(key)


def list_descriptors() -> list[dict]:
    return [cls.descriptor() for cls in discover().values()]
