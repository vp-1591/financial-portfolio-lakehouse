"""BrokerConnector protocol definition."""

from __future__ import annotations

import argparse
from typing import Protocol, runtime_checkable

import polars as pl
import pyarrow as pa

from pipeline.normalized.consolidate import Holding


class UnparseableAccountIdError(Exception):
    """Raised by ``fetch_snapshot`` when the broker account id cannot be
    derived from the fetched artifact (e.g. an XTB report filename that does
    not match ``{CCY}_{account_id}_{from}_{to}.xlsx``). The row is dropped at
    fetch time; the caller records a data_quality WARN and continues the run."""

    def __init__(self, filename: str) -> None:
        super().__init__(
            f"could not derive account id from fetched artifact: {filename!r}"
        )
        self.filename = filename


@runtime_checkable
class BrokerConnector(Protocol):
    """Protocol that every broker connector must implement."""

    name: str  # e.g. "ibkr", "trading212", "xtb"
    display_name: str  # e.g. "IBKR", "Trading 212", "XTB"

    def fetch_kwargs(self, args: argparse.Namespace) -> list[dict]:
        """Build one or more keyword-argument batches for ``fetch_snapshot``.

        Every connector writes to the single merged bronze table
        ``raw/{name}`` (AD-5); ``fetch_kwargs`` returns one batch per fetch
        so the generic path in ``run.fetch_connector`` can iterate them
        (e.g. XTB returns one batch per ``--xtb-file``). Resolves secrets and
        config from environment variables and CLI args. Returns an empty list
        if required secrets are missing (the caller should skip the connector
        in that case).
        """
        ...

    def required_secrets(self) -> list[str]:
        """Return the base secret env-var names this connector requires.

        Used for validation and documentation.  Staging-mode resolution is
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
        """Fetch a raw snapshot from the broker and return a raw-layer PyArrow table.

        May raise :class:`UnparseableAccountIdError` when the account id
        cannot be derived from the artifact.
        """
        ...

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        """Transform a raw snapshot table into the normalized schema."""
        ...

    def transform_events(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        """Transform a raw events table into the normalized schema.

        Brokers that do not yet support events should raise ``NotImplementedError``.
        """
        ...
