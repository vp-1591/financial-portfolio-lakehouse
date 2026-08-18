"""Connector registry: register, discover, and list broker connectors."""

from __future__ import annotations

from pipeline.connectors.base import BrokerConnector

_CONNECTORS: dict[str, BrokerConnector] = {}


def register(connector: BrokerConnector | type[BrokerConnector]) -> BrokerConnector:
    """Register a connector.  Accepts an instance or a class (auto-instantiated).

    Returns the connector instance for convenience.
    """
    if isinstance(connector, type):
        connector = connector()
    _CONNECTORS[connector.name] = connector
    return connector


def get(name: str) -> BrokerConnector:
    """Look up a connector by name."""
    try:
        return _CONNECTORS[name]
    except KeyError:
        raise ValueError(
            f"Unknown connector '{name}'. Available: {', '.join(sorted(_CONNECTORS))}"
        ) from None


def unregister(name: str) -> None:
    """Remove a connector from the registry.

    Used by tests to clean up connectors registered during a test so they do
    not leak into ``all()`` for the rest of the session.  Raises
    :class:`ValueError` if ``name`` is not registered (mirroring :func:`get`).
    """
    try:
        del _CONNECTORS[name]
    except KeyError:
        raise ValueError(
            f"Unknown connector '{name}'. Available: {', '.join(sorted(_CONNECTORS))}"
        ) from None


def all() -> list[BrokerConnector]:
    """Return all registered connectors."""
    return list(_CONNECTORS.values())
