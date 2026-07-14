"""IBKR connector: BrokerConnector implementation."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import polars as pl
import pyarrow as pa

from pipeline.connectors.ibkr import fetch, transform
from pipeline.connectors.registry import register
from pipeline.normalized.consolidate import Holding
from pipeline.secrets import get_env, is_demo, resolve_secret

logger = logging.getLogger(__name__)


@register
class IbkrConnector:
    name = "ibkr"
    display_name = "IBKR"
    enabled_env_var = "IBKR_ENABLED"

    def fetch_kwargs(self, args: argparse.Namespace) -> dict:
        flex_token = resolve_secret("IBKR_FLEX_TOKEN")
        if not flex_token:
            logger.debug("Skipping IBKR: IBKR_FLEX_TOKEN not set")
            return {}
        flex_query_id = resolve_secret("IBKR_FLEX_QUERY_ID")
        if not flex_query_id:
            logger.debug("Skipping IBKR: IBKR_FLEX_QUERY_ID not set")
            return {}
        return {
            "flex_token": flex_token,
            "flex_query_id": flex_query_id,
            "flex_base_url": get_env(
                "IBKR_FLEX_BASE_URL",
                "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
            ),
        }

    def fetch_cdc_kwargs(self) -> dict:
        flex_token = resolve_secret("IBKR_FLEX_TOKEN")
        if not flex_token:
            logger.debug("Skipping IBKR CDC: IBKR_FLEX_TOKEN not set")
            return {}
        # Prefer a dedicated CDC query ID, fall back to the snapshot query ID
        flex_query_id = resolve_secret("IBKR_FLEX_CDC_QUERY_ID") or resolve_secret(
            "IBKR_FLEX_QUERY_ID"
        )
        if not flex_query_id:
            logger.debug("Skipping IBKR CDC: no Flex query ID available")
            return {}
        return {
            "token": flex_token,
            "query_id": flex_query_id,
            "base_url": get_env(
                "IBKR_FLEX_BASE_URL",
                "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
            ),
        }

    def required_secrets(self) -> list[str]:
        return ["IBKR_FLEX_TOKEN", "IBKR_FLEX_QUERY_ID"]

    def extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]:
        holdings: list[Holding] = []
        for row in df.iter_rows(named=True):
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(
                Holding(
                    broker="IBKR",
                    ticker=str(row["label"]),
                    currency=str(row.get("value_currency", row.get("currency", ""))),
                    value=row["value_decrypted"],
                    identifier=identifier,
                    security_currency=str(row.get("security_currency", "")),
                    description=str(row.get("description", "")),
                )
            )
        return holdings

    def fetch_snapshot(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_snapshot_via_flex(
            token=kwargs["flex_token"],
            query_id=kwargs["flex_query_id"],
            base_url=kwargs.get(
                "flex_base_url",
                "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
            ),
            timeout=kwargs.get("flex_timeout", 30.0),
            retries=kwargs.get("flex_retries", 6),
            delay=kwargs.get("flex_delay", 3.0),
        )

    def fetch_cdc(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_cdc_via_flex(
            token=kwargs["token"],
            query_id=kwargs["query_id"],
            base_url=kwargs.get(
                "base_url",
                "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
            ),
            timeout=kwargs.get("timeout", 30.0),
            retries=kwargs.get("retries", 6),
            delay=kwargs.get("delay", 3.0),
        )

    def transform_snapshot(
        self, raw: pa.Table, fernet_key: bytes, **kwargs: Any
    ) -> pa.Table:
        base_currency_override = kwargs.get("base_currency_override")
        return transform.transform_snapshot(
            raw, fernet_key, base_currency_override=base_currency_override
        )

    def transform_cdc(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_cdc(raw, fernet_key, is_demo=is_demo())
