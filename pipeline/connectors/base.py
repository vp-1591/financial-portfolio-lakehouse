"""BrokerConnector protocol definition."""

from __future__ import annotations

import argparse
from typing import Protocol, runtime_checkable

import pyarrow as pa
import polars as pl

from pipeline.normalized.consolidate import Holding


@runtime_checkable
class BrokerConnector(Protocol):
    """Protocol that every broker connector must implement."""

    name: str  # e.g. "ibkr", "trading212", "xtb"
    display_name: str  # e.g. "IBKR", "Trading 212", "XTB"
    enabled_env_var: str  # e.g. "IBKR_ENABLED", "T212_ENABLED", "XTB_ENABLED"

    def fetch_kwargs(self, args: argparse.Namespace) -> dict:
        """Build connector-specific keyword arguments for ``fetch_snapshot``.

        Resolves secrets and config from environment variables and CLI args.
        Returns an empty dict if required secrets are missing (the caller
        should skip the connector in that case).
        """
        ...

    def fetch_cdc_kwargs(self) -> dict:
        """Build keyword arguments for ``fetch_cdc``.

        Returns the snapshot kwargs for brokers that share the same credentials
        for CDC (e.g. Trading 212), or an empty dict otherwise.
        """
        ...

    def required_secrets(self) -> list[str]:
        """Return the base secret env-var names this connector requires.

        Used for validation and documentation.  Demo-mode resolution is
        handled by :func:`pipeline.secrets.resolve_secret` at fetch time.
        """
        ...

    def extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]:
        """Extract :class:`Holding` objects from a normalized snapshot DataFrame.

        Each connector knows its own display name, description column, and
        ``security_currency`` source, so the per-broker branch ladder in
        :func:`pipeline.normalized.extract.extract_holdings` can be replaced
        by delegating to this method.
        """
        ...

    def fetch_snapshot(self, **kwargs: object) -> pa.Table:
        """Fetch a raw snapshot from the broker and return a raw-layer PyArrow table."""
        ...

    def fetch_cdc(self, **kwargs: object) -> pa.Table:
        """Fetch CDC (change data capture) events from the broker.

        Brokers that do not yet support CDC should raise ``NotImplementedError``.
        """
        ...

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        """Transform a raw snapshot table into the normalized schema."""
        ...

    def transform_cdc(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        """Transform a raw CDC table into the normalized schema.

        Brokers that do not yet support CDC should raise ``NotImplementedError``.
        """
        ...
