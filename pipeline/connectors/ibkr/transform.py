"""IBKR connector: transform raw Flex snapshot data into normalized schema."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import pyarrow as pa

from pipeline.connectors.ibkr.client import (
    CashReportResult,
    as_float,
    parse_account_info,
    parse_cash_report,
    parse_conversion_rates,
    parse_positions,
)
from pipeline.connectors.transform_utils import (
    build_normalized_table,
    decode_payload,
    filter_latest_snapshot,
)
from pipeline.normalized.models import ibkr_snapshot_normalized_schema


def transform_snapshot(
    raw: pa.Table, fernet_key: bytes, base_currency_override: str | None = None
) -> pa.Table:
    """Transform raw Flex Web Service snapshot data into the normalized schema.

    Flex payloads are stored as raw XML (not JSON) with ``source="flex"``.
    This function parses the XML, extracts OpenPosition, AccountInformation,
    CashReportCurrency, and ConversionRate elements, and produces the
    normalized IBKR snapshot schema.

    Parameters
    ----------
    raw:
        Raw-layer table from :func:`fetch_snapshot_via_flex`.
    fernet_key:
        Fernet key for encrypting value columns.
    base_currency_override:
        When the Flex response reports ``BASE`` instead of a real currency
        code, use this value as the base currency.
    """
    raw = filter_latest_snapshot(raw)

    records: list[dict[str, Any]] = []

    # Flex payloads are XML, not JSON — iterate raw table columns directly
    sources = raw.column("source").to_pylist()
    fetched_ats_col = raw.column("fetched_at").to_pylist()
    payloads = raw.column("payload").to_pylist()

    for i in range(len(sources)):
        if sources[i] != "flex":
            continue

        payload_bytes = payloads[i]
        decrypted = decode_payload(payload_bytes, fernet_key)
        if decrypted is not None:
            payload_bytes = decrypted
        elif isinstance(payload_bytes, memoryview):
            payload_bytes = bytes(payload_bytes)

        root = ET.fromstring(payload_bytes)
        positions = parse_positions(root)
        account_infos = parse_account_info(root)
        cash_result: CashReportResult = parse_cash_report(root)
        conversion_rates = parse_conversion_rates(root)

        # Build account_id -> base_currency lookup
        base_currency_by_account: dict[str, str] = {}
        for info in account_infos:
            acct_id = str(info.get("accountId", ""))
            currency = str(info.get("currency", "") or "").upper()
            if currency and currency != "BASE":
                base_currency_by_account[acct_id] = currency
            elif not base_currency_by_account.get(acct_id):
                base_currency_by_account[acct_id] = "USD"

        # Override base currency if requested
        if base_currency_override:
            for acct_id in base_currency_by_account:
                base_currency_by_account[acct_id] = base_currency_override.upper()

        # Build FX rate lookup from positions and conversion rates
        fx_rate_lookup: dict[tuple[str, str], float] = {}
        for pos in positions:
            pos_acct = str(pos.get("accountId", ""))
            pos_currency = str(pos.get("currency", "") or "").upper()
            if pos_acct and pos_currency:
                fx_rate_lookup[(pos_acct, pos_currency)] = as_float(
                    pos.get("fxRateToBase"), 1.0
                )
        for acct_id in base_currency_by_account:
            for ccy, rate in conversion_rates.items():
                key = (acct_id, ccy)
                if key not in fx_rate_lookup:
                    fx_rate_lookup[key] = rate

        # Determine which account(s) this payload covers
        account_ids_in_payload = set(base_currency_by_account.keys())
        if not account_ids_in_payload:
            # Fallback: collect unique accountIds from positions
            for pos in positions:
                acct = str(pos.get("accountId", ""))
                if acct:
                    account_ids_in_payload.add(acct)
            if not account_ids_in_payload:
                account_ids_in_payload = {""}

        fetched_at = fetched_ats_col[i]
        if isinstance(fetched_at, str):
            fetched_at = datetime.fromisoformat(fetched_at)

        for pos in positions:
            acct_id = str(pos.get("accountId", ""))
            value = as_float(pos.get("positionValue"))
            if value == 0:
                quantity = as_float(pos.get("quantity"))
                mark_price = as_float(pos.get("markPrice"))
                value = quantity * mark_price
            if value == 0:
                continue

            currency = str(pos.get("currency", "") or "").upper()
            fx_rate = as_float(pos.get("fxRateToBase"), 1.0)
            base_currency = base_currency_by_account.get(acct_id, currency)

            if base_currency_override:
                base_currency = base_currency_override.upper()

            if currency and currency != base_currency and fx_rate and fx_rate != 0:
                base_value = value * fx_rate
            else:
                base_value = value

            label = _flex_position_label(pos)
            asset_class = str(pos.get("assetClass", "") or "STK").upper()
            isin = str(pos.get("isin", "") or "").strip().upper()
            description = str(
                pos.get("description", "") or pos.get("symbol", "") or label
            )

            records.append(
                {
                    "fetched_at": fetched_at,
                    "account_id": acct_id,
                    "position_type": "EQUITY",
                    "label": label,
                    "asset_class": asset_class,
                    "currency": base_currency,
                    "value": base_value,
                    "value_currency": currency if currency else base_currency,
                    "isin": isin,
                    "description": description,
                    "security_currency": currency if currency else base_currency,
                }
            )

        # Process cash entries — use BASE_SUMMARY fallback when no per-currency
        # entries exist (e.g. single-currency demo accounts).
        cash_entries = cash_result.per_currency
        if not cash_entries and cash_result.base_summary:
            for summary in cash_result.base_summary:
                ending_cash = as_float(summary.get("endingCash"))
                if ending_cash != 0:
                    acct_id = str(summary.get("accountId", ""))
                    base_ccy = base_currency_by_account.get(acct_id, "USD")
                    if base_currency_override:
                        base_ccy = base_currency_override.upper()
                    cash_entries = [
                        {
                            "accountId": acct_id,
                            "currency": base_ccy,
                            "endingCash": str(ending_cash),
                        }
                    ]
                    break

        if cash_entries:
            for entry in cash_entries:
                acct_id = str(entry.get("accountId", ""))
                currency = str(entry.get("currency", "") or "").upper()
                ending_cash = as_float(entry.get("endingCash"))
                if not currency or ending_cash == 0:
                    continue

                base_currency = base_currency_by_account.get(acct_id, currency)
                if base_currency_override:
                    base_currency = base_currency_override.upper()

                fx_rate = fx_rate_lookup.get((acct_id, currency))
                if fx_rate is None:
                    if currency != base_currency:
                        fx_rate = 1.0
                    else:
                        fx_rate = 1.0

                if currency != base_currency and fx_rate and fx_rate != 0:
                    base_value = ending_cash * fx_rate
                else:
                    base_value = ending_cash

                if base_value != 0:
                    records.append(
                        {
                            "fetched_at": fetched_at,
                            "account_id": acct_id,
                            "position_type": "CASH",
                            "label": f"CASH {currency}",
                            "asset_class": "CASH",
                            "currency": base_currency,
                            "value": base_value,
                            "value_currency": currency,
                            "isin": "",
                            "description": f"Cash {currency}",
                            "security_currency": currency,
                        }
                    )

    return build_normalized_table(
        records,
        ibkr_snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=["value"],
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """IBKR CDC transformation is not yet implemented."""
    raise NotImplementedError("IBKR CDC transformation is not yet implemented")


def _flex_position_label(position: dict) -> str:
    """Extract a display label from a Flex OpenPosition element."""
    for key in ("symbol", "description"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)
    return "UNKNOWN"
