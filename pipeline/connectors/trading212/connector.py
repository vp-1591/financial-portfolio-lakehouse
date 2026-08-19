"""Trading 212 connector: BrokerConnector implementation."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import polars as pl
import pyarrow as pa

from pipeline.connectors.registry import register
from pipeline.connectors.trading212 import fetch, transform
from pipeline.normalized.consolidate import Holding
from pipeline.secrets import get_env, is_staging, resolve_secret

logger = logging.getLogger(__name__)

_LIVE_BASE_URL = "https://live.trading212.com/api/v0"
_DEMO_BASE_URL = "https://demo.trading212.com/api/v0"


@register
class Trading212Connector:
    name = "trading212"
    display_name = "Trading 212"
    events_raw_layer = "events"

    def fetch_kwargs(self, args: argparse.Namespace) -> dict:
        api_key = resolve_secret("T212_API_KEY")
        if not api_key:
            logger.debug("Skipping Trading 212: T212_API_KEY not set")
            return {}
        api_secret = resolve_secret("T212_API_SECRET") or ""
        default_base = _DEMO_BASE_URL if is_staging() else _LIVE_BASE_URL
        base_url = get_env("T212_BASE_URL") or default_base
        return {
            "api_key": api_key,
            "api_secret": api_secret,
            "base_url": base_url,
        }

    def fetch_events_kwargs(self) -> dict:
        """Trading 212 uses the same credentials for events as for snapshots."""
        return self.fetch_kwargs(argparse.Namespace())

    def required_secrets(self) -> list[str]:
        return ["T212_API_KEY", "T212_API_SECRET"]

    def extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]:
        holdings: list[Holding] = []
        for row in df.iter_rows(named=True):
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(
                Holding(
                    broker="Trading 212",
                    ticker=str(row["label"]),
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

    def fetch_events(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_events(**kwargs)

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_snapshot(raw, fernet_key)

    def transform_events(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_events(raw, fernet_key)
