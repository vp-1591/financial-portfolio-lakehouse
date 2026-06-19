"""Broker connector auto-discovery.

Importing this package registers all built-in connectors that are available.
"""

from pipeline.connectors import ibkr  # noqa: F401

try:
    from pipeline.connectors import trading212  # noqa: F401
except ImportError:
    pass

try:
    from pipeline.connectors import xtb  # noqa: F401
except ImportError:
    pass