"""Trading 212 connector: transform raw snapshot and CDC data into normalized schema."""

from __future__ import annotations

import pyarrow as pa

from pipeline.connectors.transform_utils import (
    build_normalized_table,
    filter_latest_snapshot,
    iter_raw_payloads,
)
from pipeline.connectors.trading212.client import (
    account_currency,
    as_float,
    cash_value,
    instrument_currency_by_ticker,
    instrument_isin_by_ticker,
    instrument_name_by_ticker,
    position_currency,
    position_isin,
    position_label,
    position_name,
    position_security_currency,
    position_value,
)
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    trading212_snapshot_normalized_schema,
)


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 snapshot data into the normalized schema."""
    raw = filter_latest_snapshot(raw)
    records: list[dict] = []

    # Collect decoded rows to reconstruct per-account data
    rows = list(iter_raw_payloads(raw, fernet_key))

    summary_data = None
    positions_data = None
    instruments_data = None

    for row in rows:
        if "/account/summary" in row.source:
            summary_data = row.payload_parsed
        elif "/positions" in row.source:
            positions_data = row.payload_parsed
        elif "/metadata/instruments" in row.source:
            instruments_data = row.payload_parsed

    if summary_data is None or positions_data is None:
        return build_normalized_table(
            records,
            trading212_snapshot_normalized_schema,
            fernet_key,
            encrypt_columns=["value"],
        )

    currency = account_currency(summary_data)
    instruments = instruments_data if isinstance(instruments_data, list) else []
    instrument_currencies = instrument_currency_by_ticker(instruments)
    instrument_names = instrument_name_by_ticker(instruments)
    instrument_isins = instrument_isin_by_ticker(instruments)

    fetched_at = rows[0].fetched_at

    for position in positions_data if isinstance(positions_data, list) else []:
        value = position_value(position)
        if value == 0:
            continue

        records.append(
            {
                "fetched_at": fetched_at,
                "account_id": "",
                "position_type": "EQUITY",
                "label": position_label(position),
                "name": position_name(position, instrument_names),
                "asset_class": "EQUITY",
                "currency": position_currency(
                    position, instrument_currencies, currency
                ),
                "value": value,
                "value_currency": position_currency(
                    position, instrument_currencies, currency
                ),
                "isin": position_isin(position, instrument_isins),
                "security_currency": position_security_currency(
                    position, instrument_currencies, currency
                ),
            }
        )

    cash_balance = cash_value(summary_data) if isinstance(summary_data, dict) else 0.0
    if cash_balance:
        records.append(
            {
                "fetched_at": fetched_at,
                "account_id": "",
                "position_type": "CASH",
                "label": f"CASH {currency}".rstrip(),
                "name": f"Cash {currency}".rstrip(),
                "asset_class": "CASH",
                "currency": currency,
                "value": cash_balance,
                "value_currency": currency,
                "isin": "",
                "security_currency": currency,
            }
        )

    return build_normalized_table(
        records,
        trading212_snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=["value"],
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 CDC data into the broker-neutral CDC events schema."""
    records: list[dict] = []

    for row in iter_raw_payloads(raw, fernet_key):
        events = row.payload_parsed
        if not isinstance(events, list):
            continue

        # Determine event type from source path
        if "/orders" in row.source:
            event_type = "TRADE"
            raw_event_type = "ORDER"
        elif "/dividends" in row.source:
            event_type = "DIVIDEND"
            raw_event_type = "DIVIDEND"
        elif "/transactions" in row.source:
            event_type = "TRANSACTION"
            raw_event_type = "TRANSACTION"
        else:
            event_type = "UNKNOWN"
            raw_event_type = "UNKNOWN"

        for event in events:
            if not isinstance(event, dict):
                continue

            currency = event.get("currency", event.get("currencyCode", ""))

            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "broker": "Trading 212",
                    "account_id": "",
                    "event_id": str(event.get("id", event.get("orderId", ""))),
                    "source": row.source,
                    "event_type": event_type,
                    "raw_event_type": raw_event_type,
                    "event_datetime": str(
                        event.get("date", event.get("createdDate", ""))
                    ),
                    "currency": str(currency),
                    "cash_amount": as_float(
                        event.get("price", event.get("amount", event.get("value", 0)))
                    ),
                    "ticker": str(event.get("ticker", event.get("instrument", ""))),
                    "isin": str(event.get("isin", "")),
                    "quantity": as_float(event.get("quantity", event.get("shares", 0))),
                }
            )

    return build_normalized_table(
        records,
        cdc_events_normalized_schema,
        fernet_key,
        encrypt_columns=["cash_amount", "quantity"],
    )
