"""Broker connector auto-discovery.

Importing this package registers all built-in connectors that are available.
"""

from pipeline.connectors import (
    ibkr,  # noqa: F401
    trading212,  # noqa: F401
    xtb,  # noqa: F401
)
