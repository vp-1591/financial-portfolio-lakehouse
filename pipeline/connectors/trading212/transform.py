"""Trading 212 connector: transform raw snapshot and CDC data into normalized schema.

Uses Polars expressions for CDC field extraction — ``struct.field()`` for
nested access and ``coalesce()`` for fallback chains — instead of error-prone
``dict.get()`` patterns that silently return None for nested structures.
"""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from pipeline.connectors.transform_utils import (
    build_normalized_table,
    decrypt_cdc_payloads,
    filter_latest_snapshot,
    finalize_table,
    iter_raw_payloads,
)
from pipeline.connectors.trading212.client import (
    account_currency,
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


# ---------------------------------------------------------------------------
# CDC transform — Polars-native field extraction
# ---------------------------------------------------------------------------

_CDC_ENCRYPT_COLUMNS = [
    "cash_amount",
    "quantity",
    "price",
    "gross_amount",
    "fee_amount",
    "tax_amount",
    "net_amount",
    "fx_rate_to_base",
    "amount_base",
]

_T212_TXN_TYPE_MAP = {
    "WITHDRAW": "WITHDRAWAL",
    "DEPOSIT": "DEPOSIT",
    "FEE": "FEE",
    "TRANSFER": "TRANSFER",
}

_T212_FEE_NAMES = frozenset(
    {
        "CURRENCY_CONVERSION_FEE",
        "FINRA_FEE",
        "PTM_LEVY",
        "STAMP_DUTY",
        "STAMP_DUTY_RESERVE_TAX",
        "TRANSACTION_FEE",
    }
)

_T212_TAX_NAMES = frozenset({"FRENCH_TRANSACTION_TAX"})


def _extract_fee_amount(taxes: list | None) -> float:
    """Sum fee-class tax entries from fill.walletImpact.taxes."""
    if not taxes:
        return 0.0
    return sum(
        abs(t.get("quantity", 0)) for t in taxes if t.get("name") in _T212_FEE_NAMES
    )


def _extract_tax_amount(taxes: list | None) -> float:
    """Sum government tax entries from fill.walletImpact.taxes."""
    if not taxes:
        return 0.0
    return sum(
        abs(t.get("quantity", 0)) for t in taxes if t.get("name") in _T212_TAX_NAMES
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 CDC data using Polars-native field extraction.

    Splits events by source type (orders, dividends, transactions) and
    applies per-endpoint Polars expressions that use ``struct.field()`` for
    nested access and ``coalesce()`` for fallback chains, instead of
    error-prone ``dict.get()`` patterns.
    """
    dfs: list[pl.DataFrame] = []

    for fetched_at, source, events in decrypt_cdc_payloads(raw, fernet_key):
        if "/orders" in source:
            dfs.append(_transform_orders(events, fetched_at, source))
        elif "/dividends" in source:
            dfs.append(_transform_dividends(events, fetched_at, source))
        elif "/transactions" in source:
            dfs.append(_transform_transactions(events, fetched_at, source))

    if not dfs:
        return build_normalized_table(
            [], cdc_events_normalized_schema, fernet_key, _CDC_ENCRYPT_COLUMNS
        )

    result = pl.concat(dfs)
    return finalize_table(
        result, cdc_events_normalized_schema, fernet_key, _CDC_ENCRYPT_COLUMNS
    )


def _get_taxes(event: dict) -> list | None:
    """Extract the taxes list from a HistoricalOrder event dict."""
    fill = event.get("fill")
    if not isinstance(fill, dict):
        return None
    wallet_impact = fill.get("walletImpact")
    if not isinstance(wallet_impact, dict):
        return None
    return wallet_impact.get("taxes")


def _transform_orders(events: list[dict], fetched_at, source: str) -> pl.DataFrame:
    """Transform T212 HistoricalOrder events using Polars expressions.

    Each event is a nested ``{order: Order, fill: Fill}`` dict.  Polars
    infers struct schemas from the dicts, then ``struct.field()`` extracts
    nested values explicitly — no silent ``None`` from ``dict.get()`` on
    wrong nesting levels.

    Tax extraction from ``fill.walletImpact.taxes`` (a nested list of
    structs) is pre-computed in Python because Polars ``map_elements``
    does not reliably pass scalar list-of-struct values to UDFs.
    """
    # Pre-compute tax amounts from nested structures before DataFrame construction
    fee_amounts = [_extract_fee_amount(_get_taxes(e)) for e in events]
    tax_amounts = [_extract_tax_amount(_get_taxes(e)) for e in events]

    df = pl.DataFrame(events)

    # Shortcuts for repeated struct columns
    order = pl.col("order")
    fill = pl.col("fill")
    instrument = order.struct.field("instrument")
    wallet_impact = fill.struct.field("walletImpact")

    # Derived columns used in multiple expressions
    net_value = pl.coalesce(
        [wallet_impact.struct.field("netValue"), order.struct.field("filledValue")]
    ).cast(pl.Float64)
    fx_rate = pl.coalesce([wallet_impact.struct.field("fxRate"), pl.lit(1.0)]).cast(
        pl.Float64
    )

    return df.select(
        fetched_at=pl.lit(fetched_at),
        broker=pl.lit("Trading 212"),
        account_id=pl.lit(""),
        event_id=order.struct.field("id").cast(pl.Utf8),
        source=pl.lit(source),
        event_type=pl.lit("TRADE"),
        raw_event_type=pl.lit("ORDER"),
        event_datetime=pl.coalesce(
            [order.struct.field("createdAt"), fill.struct.field("filledAt")]
        ),
        currency=pl.coalesce(
            [wallet_impact.struct.field("currency"), order.struct.field("currency")]
        ),
        cash_amount=net_value,
        settle_date=pl.coalesce(
            [fill.struct.field("filledAt"), order.struct.field("createdAt")]
        ),
        ticker=pl.coalesce(
            [order.struct.field("ticker"), instrument.struct.field("ticker")]
        ),
        isin=instrument.struct.field("isin"),
        description=instrument.struct.field("name"),
        quantity=pl.coalesce(
            [fill.struct.field("quantity"), order.struct.field("filledQuantity")]
        ).cast(pl.Float64),
        price=fill.struct.field("price").cast(pl.Float64),
        side=order.struct.field("side"),
        gross_amount=pl.coalesce(
            [order.struct.field("filledValue"), order.struct.field("value")]
        ).cast(pl.Float64),
        fee_amount=pl.Series("fee_amount", fee_amounts, dtype=pl.Float64),
        tax_amount=pl.Series("tax_amount", tax_amounts, dtype=pl.Float64),
        net_amount=net_value,
        base_currency=pl.coalesce(
            [wallet_impact.struct.field("currency"), order.struct.field("currency")]
        ),
        fx_rate_to_base=fx_rate,
        amount_base=(net_value * fx_rate),
    )


def _transform_dividends(events: list[dict], fetched_at, source: str) -> pl.DataFrame:
    """Transform T212 HistoryDividendItem events using Polars expressions.

    Dividend items have a nested ``instrument`` object but are otherwise
    flat.  The ``type`` field is stored as ``raw_event_type`` for
    diagnostics; ``event_type`` is always ``DIVIDEND``.
    """
    df = pl.DataFrame(events)

    instrument = pl.col("instrument")
    price = pl.coalesce([pl.col("grossAmountPerShare"), pl.lit(0.0)]).cast(pl.Float64)
    qty = pl.col("quantity").cast(pl.Float64)
    amount = pl.col("amount").cast(pl.Float64)

    return df.select(
        fetched_at=pl.lit(fetched_at),
        broker=pl.lit("Trading 212"),
        account_id=pl.lit(""),
        event_id=pl.col("reference").cast(pl.Utf8),
        source=pl.lit(source),
        event_type=pl.lit("DIVIDEND"),
        raw_event_type=pl.coalesce([pl.col("type"), pl.lit("DIVIDEND")]),
        event_datetime=pl.col("paidOn").cast(pl.Utf8),
        currency=pl.coalesce([pl.col("currency"), pl.col("tickerCurrency")]),
        cash_amount=amount,
        settle_date=pl.col("paidOn").cast(pl.Utf8),
        ticker=pl.coalesce([pl.col("ticker"), instrument.struct.field("ticker")]),
        isin=instrument.struct.field("isin"),
        description=instrument.struct.field("name"),
        quantity=qty,
        price=price,
        side=pl.lit(""),
        gross_amount=(price * qty),
        fee_amount=pl.lit(0.0),
        tax_amount=pl.lit(0.0),
        net_amount=amount,
        base_currency=pl.col("currency").cast(pl.Utf8),
        fx_rate_to_base=pl.lit(1.0),
        amount_base=amount,
    )


def _transform_transactions(
    events: list[dict], fetched_at, source: str
) -> pl.DataFrame:
    """Transform T212 HistoryTransactionItem events using Polars expressions.

    Transaction items are flat dicts with no nested objects, so this is the
    simplest of the three transforms.
    """
    df = pl.DataFrame(events)

    raw_type = pl.col("type").cast(pl.Utf8)
    event_type = raw_type.replace_strict(_T212_TXN_TYPE_MAP, default="UNKNOWN")
    amount = pl.col("amount").cast(pl.Float64)

    return df.select(
        fetched_at=pl.lit(fetched_at),
        broker=pl.lit("Trading 212"),
        account_id=pl.lit(""),
        event_id=pl.col("reference").cast(pl.Utf8),
        source=pl.lit(source),
        event_type=event_type,
        raw_event_type=raw_type,
        event_datetime=pl.col("dateTime").cast(pl.Utf8),
        currency=pl.col("currency").cast(pl.Utf8),
        cash_amount=amount,
        settle_date=pl.lit(""),
        ticker=pl.lit(""),
        isin=pl.lit(""),
        description=pl.lit(""),
        quantity=pl.lit(0.0),
        price=pl.lit(0.0),
        side=pl.lit(""),
        gross_amount=pl.lit(0.0),
        fee_amount=pl.lit(0.0),
        tax_amount=pl.lit(0.0),
        net_amount=amount,
        base_currency=pl.col("currency").cast(pl.Utf8),
        fx_rate_to_base=pl.lit(1.0),
        amount_base=amount,
    )
