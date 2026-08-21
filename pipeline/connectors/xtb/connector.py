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
    # transform_events applies _latest_per_account over the WHOLE raw table to
    # retain last-known reports for accounts not re-uploaded this run — the
    # in-memory handoff stays off for xtb (see ADR 0116).
    handoff_supported = False

    def fetch_kwargs(self, args: argparse.Namespace) -> list[dict]:
        xtb_file = getattr(args, "xtb_file", None)
        if not xtb_file:
            logger.debug("Skipping XTB: no --xtb-file provided")
            return []
        # D17 shared bronze: one raw row per uploaded file, all rows landing in
        # raw/xtb. Return one kwarg batch per --xtb-file so the generic fetch
        # path iterates them and appends each XTB_REPORT row (AD-6).
        files = xtb_file if isinstance(xtb_file, list) else [xtb_file]
        return [{"file_path": file_path} for file_path in files]

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

    # D17 shared bronze: XTB has no dedicated events fetch — events are derived
    # from the same ``XTB_REPORT`` raw row in ``raw/xtb`` (transform_events
    # reads the shared bronze). ``run.fetch_connector`` gates the events fetch
    # with ``getattr(connector, "fetch_events_kwargs", None)``, so its absence
    # here is the entire contract (AD-6: no events stubs).

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_snapshot(raw, fernet_key)

    def transform_events(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_events(raw, fernet_key)
