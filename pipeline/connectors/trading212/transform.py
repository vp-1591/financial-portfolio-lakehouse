"""Trading 212 connector: transform raw snapshot and CDC data into normalized schema."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pyarrow as pa

from pipeline.connectors.trading212.client import (
    account_currency,
    as_float,
    cash_value,
    first_value,
    instrument_currency_by_ticker,
    instrument_isin_by_ticker,
    instrument_name_by_ticker,
    nested_dict,
    net_worth_value,
    position_currency,
    position_isin,
    position_label,
    position_name,
    position_security_currency,
    position_value,
)
from pipeline.crypto import decrypt, encrypt_float
from pipeline.normalized.models import (
    trading212_cdc_normalized_schema,
    trading212_snapshot_normalized_schema,
)


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 snapshot data into the normalized schema."""
    import pandas as pd

    fetched_ats: list[datetime] = []
    account_ids: list[str] = []
    position_types: list[str] = []
    labels: list[str] = []
    names: list[str] = []
    asset_classes: list[str] = []
    currencies: list[str] = []
    values: list[bytes] = []
    value_currencies: list[str] = []
    isins: list[str] = []
    security_currencies: list[str] = []

    raw_df = raw.to_pandas()

    # Group by account_id to reconstruct per-account data
    for acct_id, acct_group in raw_df.groupby("account_id"):
        summary_data = None
        positions_data = None
        instruments_data = None

        for _, row in acct_group.iterrows():
            source = row["source"]
            payload_bytes = row["payload"]
            if isinstance(payload_bytes, memoryview):
                payload_bytes = bytes(payload_bytes)

            # Payloads are stored encrypted in the raw Delta table — decrypt first
            try:
                payload_bytes = decrypt(payload_bytes, fernet_key)
            except Exception:
                continue

            try:
                parsed = json.loads(payload_bytes)
            except (json.JSONDecodeError, TypeError):
                continue

            if "/account/summary" in source:
                summary_data = parsed
            elif "/positions" in source:
                positions_data = parsed
            elif "/metadata/instruments" in source:
                instruments_data = parsed

        if summary_data is None or positions_data is None:
            continue

        currency = account_currency(summary_data)
        instruments = instruments_data if isinstance(instruments_data, list) else []
        instrument_currencies = instrument_currency_by_ticker(instruments)
        instrument_names = instrument_name_by_ticker(instruments)
        instrument_isins = instrument_isin_by_ticker(instruments)

        fetched_at = acct_group["fetched_at"].iloc[0]
        if isinstance(fetched_at, str):
            fetched_at = datetime.fromisoformat(fetched_at)

        for position in positions_data if isinstance(positions_data, list) else []:
            value = position_value(position)
            if value == 0:
                continue

            fetched_ats.append(fetched_at)
            account_ids.append(str(acct_id))
            position_types.append("EQUITY")
            labels.append(position_label(position))
            names.append(position_name(position, instrument_names))
            asset_classes.append("EQUITY")
            currencies.append(position_currency(position, instrument_currencies, currency))
            values.append(encrypt_float(value, fernet_key))
            value_currencies.append(position_currency(position, instrument_currencies, currency))
            isins.append(position_isin(position, instrument_isins))
            security_currencies.append(
                position_security_currency(position, instrument_currencies, currency)
            )

        cash_balance = cash_value(summary_data) if isinstance(summary_data, dict) else 0.0
        if cash_balance:
            fetched_ats.append(fetched_at)
            account_ids.append(str(acct_id))
            position_types.append("CASH")
            labels.append(f"CASH {currency}".rstrip())
            names.append(f"Cash {currency}".rstrip())
            asset_classes.append("CASH")
            currencies.append(currency)
            values.append(encrypt_float(cash_balance, fernet_key))
            value_currencies.append(currency)
            isins.append("")
            security_currencies.append(currency)

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "account_id": account_ids,
            "position_type": position_types,
            "label": labels,
            "name": names,
            "asset_class": asset_classes,
            "currency": currencies,
            "value": values,
            "value_currency": value_currencies,
            "isin": isins,
            "security_currency": security_currencies,
        },
        schema=trading212_snapshot_normalized_schema,
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 CDC data into the normalized CDC schema."""
    import pandas as pd

    fetched_ats: list[datetime] = []
    account_ids: list[str] = []
    event_types: list[str] = []
    event_ids: list[str] = []
    tickers: list[str] = []
    isins: list[str] = []
    currencies: list[str] = []
    values: list[bytes] = []
    quantities: list[bytes] = []
    event_dates: list[str] = []

    raw_df = raw.to_pandas()

    for _, row in raw_df.iterrows():
        source = row["source"]
        payload_bytes = row["payload"]
        if isinstance(payload_bytes, memoryview):
            payload_bytes = bytes(payload_bytes)

        # Payloads are stored encrypted in the raw Delta table — decrypt first
        try:
            payload_bytes = decrypt(payload_bytes, fernet_key)
        except Exception:
            continue

        try:
            events = json.loads(payload_bytes)
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(events, list):
            continue

        # Determine event type from source path
        if "/orders" in source:
            event_type = "ORDER"
        elif "/dividends" in source:
            event_type = "DIVIDEND"
        elif "/transactions" in source:
            event_type = "TRANSACTION"
        else:
            event_type = "UNKNOWN"

        fetched_at = row["fetched_at"]
        if isinstance(fetched_at, str):
            fetched_at = datetime.fromisoformat(fetched_at)

        for event in events:
            if not isinstance(event, dict):
                continue

            fetched_ats.append(fetched_at)
            account_ids.append(str(row["account_id"]))
            event_types.append(event_type)
            event_ids.append(str(event.get("id", event.get("orderId", ""))))
            tickers.append(str(event.get("ticker", event.get("instrument", ""))))
            isins.append(str(event.get("isin", "")))

            # Value currency
            currency = event.get("currency", event.get("currencyCode", ""))
            currencies.append(str(currency))

            # Encrypt value
            value = as_float(event.get("price", event.get("amount", event.get("value", 0))))
            values.append(encrypt_float(value, fernet_key))

            # Encrypt quantity
            quantity = as_float(event.get("quantity", event.get("shares", 0)))
            quantities.append(encrypt_float(quantity, fernet_key))

            # Event date
            event_dates.append(str(event.get("date", event.get("createdDate", ""))))

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "account_id": account_ids,
            "event_type": event_types,
            "event_id": event_ids,
            "ticker": tickers,
            "isin": isins,
            "currency": currencies,
            "value": values,
            "quantity": quantities,
            "event_date": event_dates,
        },
        schema=trading212_cdc_normalized_schema,
    )