"""IBKR connector: transform raw snapshot data into normalized schema."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pyarrow as pa

from pipeline.connectors.ibkr.client import (
    as_float,
    cash_assets,
    exchange_rates,
    net_liquidation_value,
    position_conid,
    position_description,
    position_isin,
    position_label,
    position_value,
    to_base_currency,
)
from pipeline.crypto import encrypt_float, encrypt_string
from pipeline.normalized.models import ibkr_snapshot_normalized_schema


def _ibkr_base_currency(ledger: dict, override: str | None = None) -> str:
    """Determine IBKR account base currency from the ledger."""
    if override:
        return override.upper()
    base = ledger.get("BASE")
    if isinstance(base, dict):
        currency = base.get("currency")
        if currency and str(currency).upper() != "BASE":
            return str(currency).upper()
    for currency, entry in ledger.items():
        if not isinstance(entry, dict):
            continue
        if as_float(entry.get("exchangerate")) == 1.0:
            inferred = str(entry.get("currency") or currency).upper()
            if inferred and inferred != "BASE":
                return inferred
    return "USD"  # Fallback


def _real_currency(value: object, fallback: str) -> str:
    currency = str(value or "").upper()
    if not currency or currency == "BASE":
        return fallback
    return currency


def transform_snapshot(raw: pa.Table, fernet_key: bytes, base_currency_override: str | None = None) -> pa.Table:
    """Transform raw IBKR snapshot data into the normalized IBKR snapshot schema.

    Parameters
    ----------
    raw:
        Raw-layer table from :func:`fetch_snapshot`.
    fernet_key:
        Fernet key for encrypting value columns.
    base_currency_override:
        When the IBKR ledger reports the placeholder ``BASE`` instead of a real
        currency code, use this value as the base currency.
    """
    fetched_ats: list[datetime] = []
    account_ids: list[str] = []
    position_types: list[str] = []
    labels: list[str] = []
    asset_classes: list[str] = []
    currencies: list[str] = []
    values: list[bytes] = []  # encrypted
    value_currencies: list[str] = []
    conids: list[str] = []
    isins: list[str] = []
    descriptions: list[str] = []
    security_currencies: list[str] = []

    # Group by account_id, then reconstruct positions + ledger per account
    import pandas as pd

    raw_df = raw.to_pandas()
    for acct_id, acct_group in raw_df.groupby("account_id"):
        positions_data = None
        ledger_data = None

        for _, row in acct_group.iterrows():
            source = row["source"]
            payload_bytes = row["payload"]
            # payload is raw bytes (possibly encrypted in raw layer, but we
            # transform before ingestion so it should still be plaintext here)
            if isinstance(payload_bytes, memoryview):
                payload_bytes = bytes(payload_bytes)
            try:
                parsed = json.loads(payload_bytes)
            except (json.JSONDecodeError, TypeError):
                continue

            if "/positions" in source:
                positions_data = parsed
            elif "/ledger" in source:
                ledger_data = parsed

        if positions_data is None or ledger_data is None:
            continue

        if not isinstance(positions_data, list) or not isinstance(ledger_data, dict):
            continue

        rates = exchange_rates(ledger_data)
        base_currency = _ibkr_base_currency(ledger_data, base_currency_override)

        fetched_at = acct_group["fetched_at"].iloc[0]
        if isinstance(fetched_at, str):
            fetched_at = datetime.fromisoformat(fetched_at)

        for position in positions_data:
            value = position_value(position)
            if value == 0:
                continue
            currency = str(position.get("currency") or "")
            real_currency = _real_currency(currency, base_currency)
            conid = position_conid(position)

            fetched_ats.append(fetched_at)
            account_ids.append(str(acct_id))
            position_types.append("EQUITY")
            labels.append(position_label(position))
            asset_classes.append(str(position.get("assetClass") or position.get("secType") or "UNKNOWN"))
            currencies.append(base_currency)
            values.append(encrypt_float(to_base_currency(value, real_currency, rates), fernet_key))
            value_currencies.append(real_currency)
            conids.append(conid)
            isins.append(position_isin(position))
            descriptions.append(position_description(position))
            security_currencies.append(real_currency)

        for cash in cash_assets(str(acct_id), ledger_data):
            fetched_ats.append(fetched_at)
            account_ids.append(str(acct_id))
            position_types.append("CASH")
            labels.append(cash["label"])
            asset_classes.append("CASH")
            currencies.append(base_currency)
            values.append(encrypt_float(cash["value"], fernet_key))
            value_currencies.append(cash["currency"])
            conids.append("")
            isins.append("")
            descriptions.append(cash["description"])
            security_currencies.append(cash["currency"])

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "account_id": account_ids,
            "position_type": position_types,
            "label": labels,
            "asset_class": asset_classes,
            "currency": currencies,
            "value": values,
            "value_currency": value_currencies,
            "conid": conids,
            "isin": isins,
            "description": descriptions,
            "security_currency": security_currencies,
        },
        schema=ibkr_snapshot_normalized_schema,
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """IBKR CDC transformation is not yet implemented."""
    raise NotImplementedError("IBKR CDC transformation is not yet implemented")