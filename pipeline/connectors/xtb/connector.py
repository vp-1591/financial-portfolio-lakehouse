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
    enabled_env_var = "XTB_ENABLED"

    def fetch_kwargs(self, args: argparse.Namespace) -> dict:
        xtb_file = getattr(args, "xtb_file", None)
        if not xtb_file:
            logger.debug("Skipping XTB: no --xtb-file provided")
            return {}
        # XTB supports multiple files — return kwargs for the first file.
        # The caller (cmd_fetch) iterates over all files for XTB.
        file_path = xtb_file[0] if isinstance(xtb_file, list) else xtb_file
        return {"file_path": file_path}

    def fetch_cdc_kwargs(self) -> dict:
        return {}

    def required_secrets(self) -> list[str]:
        # XTB reads from an uploaded file, not from API secrets.
        return []

    def extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]:
        holdings: list[Holding] = []
        for row in df.iter_rows(named=True):
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(
                Holding(
                    broker="XTB",
                    ticker=str(row["label"]),
                    currency=str(row.get("value_currency", row.get("currency", ""))),
                    value=row["value_decrypted"],
                    identifier=identifier,
                    security_currency=str(
                        row.get("value_currency", row.get("currency", ""))
                    ),
                    description=str(row.get("name", "")),
                )
            )
        return holdings

    def fetch_snapshot(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_snapshot(**kwargs)

    def fetch_cdc(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_cdc(**kwargs)

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_snapshot(raw, fernet_key)

    def transform_cdc(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_cdc(raw, fernet_key)
