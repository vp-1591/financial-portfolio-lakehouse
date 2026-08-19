"""XTB connector: BrokerConnector implementation."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import polars as pl
import pyarrow as pa

from pipeline.connectors.registry import register
from pipeline.connectors.xtb import fetch, transform
from pipeline.normalized.consolidate import Holding

logger = logging.getLogger(__name__)


@register
class XtbConnector:
    name = "xtb"
    display_name = "XTB"
    # D17 shared bronze: events transform reads the snapshot raw table, not a
    # separate events raw. ``run.transform_connector`` reads
    # ``get_raw_path(name, events_raw_layer)`` for the events layer.
    events_raw_layer = "snapshot"

    def fetch_kwargs(self, args: argparse.Namespace) -> dict:
        xtb_file = getattr(args, "xtb_file", None)
        if not xtb_file:
            logger.debug("Skipping XTB: no --xtb-file provided")
            return {}
        # XTB supports multiple files — return kwargs for the first file.
        # The caller (fetch_connector) iterates over all files for XTB.
        file_path = xtb_file[0] if isinstance(xtb_file, list) else xtb_file
        return {"file_path": file_path}

    def required_secrets(self) -> list[str]:
        # XTB reads from an uploaded file, not from API secrets.
        return []

    def extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]:
        holdings: list[Holding] = []
        for row in df.iter_rows(named=True):
            # D12: no ISIN available in the new format; use the ticker (the
            # ``label`` column) as the identifier, mirroring the
            # ``ISIN:{isin}`` convention used by IBKR/T212 with a TICKER
            # namespace.
            ticker = str(row.get("label", "") or "").strip()
            identifier = f"TICKER:{ticker}" if ticker else ""
            holdings.append(
                Holding(
                    broker="XTB",
                    ticker=ticker,
                    # D5: account currency from the summary block; XTB exposes
                    # no per-position instrument currency, so security_ccy
                    # (account currency) is the chart-currency-exposure source.
                    currency=str(row.get("security_ccy", "")),
                    value=row["security_value_decrypted"],
                    identifier=identifier,
                    security_currency=str(row.get("security_ccy", "")),
                    description=str(row.get("description", "")),
                    position_type=str(row.get("position_type", "EQUITY")),
                )
            )
        return holdings

    def fetch_snapshot(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_snapshot(**kwargs)

    # D17 shared bronze: XTB has no dedicated events fetch. Events are derived from
    # the snapshot raw via ``events_raw_layer = "snapshot"`` (transform_events reads
    # ``xtb_snapshot`` raw). The ``fetch_connector`` XTB branch returns before
    # reaching the generic ``fetch_events`` call site, so these stubs are never
    # invoked at runtime; they exist solely to satisfy the BrokerConnector
    # structural protocol (pyright requires the methods to be declared on the
    # class, not just inherited from the Protocol's abstract bodies).
    def fetch_events_kwargs(self) -> dict:
        return {}

    def fetch_events(self, **kwargs: Any) -> pa.Table:
        raise NotImplementedError("XTB events are produced from the snapshot raw (D17)")

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_snapshot(raw, fernet_key)

    def transform_events(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_events(raw, fernet_key)
