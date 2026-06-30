"""IBKR connector: transform raw snapshot data into normalized schema."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

import pyarrow as pa

from pipeline.connectors.ibkr.client import (
    as_float,
    cash_assets,
    exchange_rates,
    position_conid,
    position_description,
    position_isin,
    position_label,
    position_value,
    to_base_currency,
)
from pipeline.connectors.transform_utils import (
    DecodedRow,
    decode_payload,
    iter_raw_payloads,
)
from pipeline.crypto import encrypt_float
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


def transform_snapshot(
    raw: pa.Table, fernet_key: bytes, base_currency_override: str | None = None
) -> pa.Table:
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
    by_account: dict[str, list[DecodedRow]] = defaultdict(list)
    for row in iter_raw_payloads(raw, fernet_key):
        by_account[row.account_id].append(row)

    for acct_id, rows in by_account.items():
        positions_data = None
        ledger_data = None

        for row in rows:
            if "/positions" in row.source:
                positions_data = row.payload_parsed
            elif "/ledger" in row.source:
                ledger_data = row.payload_parsed

        if positions_data is None or ledger_data is None:
            continue

        if not isinstance(positions_data, list) or not isinstance(ledger_data, dict):
            continue

        rates = exchange_rates(ledger_data)
        base_currency = _ibkr_base_currency(ledger_data, base_currency_override)

        fetched_at = rows[0].fetched_at

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
            asset_classes.append(
                str(position.get("assetClass") or position.get("secType") or "UNKNOWN")
            )
            currencies.append(base_currency)
            values.append(
                encrypt_float(to_base_currency(value, real_currency, rates), fernet_key)
            )
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


def _transform_flex_snapshot(
    raw: pa.Table, fernet_key: bytes, base_currency_override: str | None = None
) -> pa.Table:
    """Transform raw Flex Web Service snapshot data into the normalized schema.

    Flex payloads are stored as raw XML (not JSON) with ``source="flex"``.
    This function parses the XML, extracts OpenPosition, AccountInformation,
    CashReportCurrency, and ConversionRate elements, and produces the same
    normalized output as :func:`transform_snapshot`.
    """
    from scripts.ibkr_net_worth import (
        parse_account_info,
        parse_cash_report,
        parse_conversion_rates,
        parse_positions,
    )

    fetched_ats: list[datetime] = []
    account_ids: list[str] = []
    position_types: list[str] = []
    labels: list[str] = []
    asset_classes: list[str] = []
    currencies: list[str] = []
    values: list[bytes] = []
    value_currencies: list[str] = []
    conids: list[str] = []
    isins: list[str] = []
    descriptions: list[str] = []
    security_currencies: list[str] = []

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
        cash_report_entries = parse_cash_report(root)
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
            conid = str(pos.get("conid", "") or "")
            description = str(
                pos.get("description", "") or pos.get("symbol", "") or label
            )

            fetched_ats.append(fetched_at)
            account_ids.append(acct_id)
            position_types.append("EQUITY")
            labels.append(label)
            asset_classes.append(asset_class)
            currencies.append(base_currency)
            values.append(encrypt_float(base_value, fernet_key))
            value_currencies.append(currency if currency else base_currency)
            conids.append(conid)
            isins.append(isin)
            descriptions.append(description)
            security_currencies.append(currency if currency else base_currency)

        # Process cash entries
        cash_from_report = False
        if cash_report_entries:
            for entry in cash_report_entries:
                acct_id = str(entry.get("accountId", ""))
                currency = str(entry.get("currency", "") or "").upper()
                ending_cash = as_float(entry.get("endingCash"))
                if ending_cash == 0:
                    ending_cash = as_float(entry.get("startingCash"))
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
                    fetched_ats.append(fetched_at)
                    account_ids.append(acct_id)
                    position_types.append("CASH")
                    labels.append(f"CASH {currency}")
                    asset_classes.append("CASH")
                    currencies.append(base_currency)
                    values.append(encrypt_float(base_value, fernet_key))
                    value_currencies.append(currency)
                    conids.append("")
                    isins.append("")
                    descriptions.append(f"Cash {currency}")
                    security_currencies.append(currency)
                    cash_from_report = True

        # Fallback: AccountInformation.cashBalance
        if not cash_from_report and account_infos:
            for info in account_infos:
                acct_id = str(info.get("accountId", ""))
                cash_balance = as_float(info.get("cashBalance"))
                if not cash_balance:
                    continue
                currency = str(info.get("currency", "") or "").upper()
                if not currency or currency == "BASE":
                    currency = base_currency_by_account.get(acct_id, "USD")
                base_currency = base_currency_by_account.get(acct_id, currency)
                if base_currency_override:
                    base_currency = base_currency_override.upper()

                fx_rate = fx_rate_lookup.get((acct_id, currency), 1.0)
                base_value = (
                    cash_balance * fx_rate
                    if currency != base_currency
                    else cash_balance
                )

                if base_value != 0:
                    fetched_ats.append(fetched_at)
                    account_ids.append(acct_id)
                    position_types.append("CASH")
                    labels.append(f"CASH {currency}")
                    asset_classes.append("CASH")
                    currencies.append(base_currency)
                    values.append(encrypt_float(base_value, fernet_key))
                    value_currencies.append(currency)
                    conids.append("")
                    isins.append("")
                    descriptions.append(f"Cash {currency}")
                    security_currencies.append(currency)

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


def _flex_position_label(position: dict) -> str:
    """Extract a display label from a Flex OpenPosition element."""
    for key in ("symbol", "description", "conid"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)
    return "UNKNOWN"
