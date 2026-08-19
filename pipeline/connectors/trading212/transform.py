"""Trading 212 connector: transform raw snapshot and events data into normalized schema.

Uses Polars expressions for events field extraction — ``struct.field()`` for
nested access and ``coalesce()`` for fallback chains — instead of error-prone
``dict.get()`` patterns that silently return None for nested structures.
"""

from __future__ import annotations

import logging

import polars as pl
import pyarrow as pa

from pipeline.connectors.trading212.client import (
    account_currency,
    as_float,
    cash_value,
)
from pipeline.connectors.transform_utils import (
    build_normalized_table,
    decrypt_events_payloads,
    dedup_events,
    filter_latest_snapshot,
    finalize_table,
    iter_raw_payloads,
)
from pipeline.normalized.models import (
    events_normalized_schema,
    snapshot_normalized_schema,
)

_SNAPSHOT_ENCRYPT_COLUMNS = ["security_value"]

logger = logging.getLogger(__name__)


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 snapshot data into the normalized schema."""
    raw = filter_latest_snapshot(raw)
    records: list[dict] = []

    # Collect decoded rows to reconstruct per-account data
    rows = list(iter_raw_payloads(raw, fernet_key))

    summary_data = None
    positions_data = None

    for row in rows:
        if "/account/summary" in row.source:
            summary_data = row.payload_parsed
        elif "/positions" in row.source:
            positions_data = row.payload_parsed

    if summary_data is None or positions_data is None:
        return build_normalized_table(
            records,
            snapshot_normalized_schema,
            fernet_key,
            encrypt_columns=_SNAPSHOT_ENCRYPT_COLUMNS,
        )

    currency = account_currency(summary_data)
    fetched_at = rows[0].fetched_at

    for position in positions_data if isinstance(positions_data, list) else []:
        instrument = position["instrument"]
        price = position.get("currentPrice")
        quantity = position.get("quantity")
        # currentPrice and quantity are present and non-null on every position
        # across all staging snapshots (72/72, verified). A null here is not a
        # normal API state (e.g. a suspended instrument still carries both); it
        # means the payload is corrupted or truncated. Fast-fail loudly so data
        # corruption surfaces at the transform rather than silently dropping a
        # position from the portfolio. A genuinely zero-value position (both
        # present, value 0) is still skipped quietly below.
        if price is None or quantity is None:
            # ``instrument`` may itself be None on a corrupted payload; guard the
            # ticker read so the intended ValueError surfaces instead of an
            # AttributeError masking it.
            ticker = instrument.get("ticker") if isinstance(instrument, dict) else None
            raise ValueError(
                f"T212 position {ticker!r} has null "
                f"currentPrice/quantity (currentPrice={price!r}, "
                f"quantity={quantity!r}); cannot compute instrument value. "
                "This indicates a corrupted/truncated payload, not a normal "
                "API state."
            )
        value = as_float(price) * as_float(quantity)
        if value == 0:
            continue
        records.append(
            {
                "fetched_at": fetched_at,
                "account_id": "",
                "position_type": "EQUITY",
                "label": str(instrument["ticker"]),
                "description": str(instrument["name"]),
                "asset_class": "EQUITY",
                "security_value": value,
                "security_ccy": str(instrument["currency"]),
                "isin": str(instrument["isin"]),
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
                "description": f"Cash {currency}".rstrip(),
                "asset_class": "CASH",
                "security_value": cash_balance,
                "security_ccy": currency,
                "isin": "",
            }
        )

    return build_normalized_table(
        records,
        snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=_SNAPSHOT_ENCRYPT_COLUMNS,
    )


# ---------------------------------------------------------------------------
# events transform — Polars-native field extraction
# ---------------------------------------------------------------------------

_EVENTS_ENCRYPT_COLUMNS = [
    "cash_amount",
    "quantity",
    "price",
    "fee_amount",
    "tax_amount",
    "target_fx_rate",
    "target_value",
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

# Expected struct fields for nested objects in T212 events.
# Polars infers struct schemas from data — if a field is absent from all
# events, struct.field() raises StructFieldNotFoundError.  These sets are
# used by _ensure_struct_fields() to backfill missing keys with None so
# that Polars always infers a complete schema.
_ORDER_STRUCT_FIELDS = {
    "id",
    "createdAt",
    "currency",
    "filledQuantity",
    "filledValue",
    "instrument",
    "quantity",
    "side",
    "ticker",
    "value",
}

_FILL_STRUCT_FIELDS = {
    "id",
    "filledAt",
    "quantity",
    "price",
    "walletImpact",
}

_WALLET_IMPACT_FIELDS = {
    "currency",
    "fxRate",
    "netValue",
    "realisedProfitLoss",
    "taxes",
}

_INSTRUMENT_FIELDS = {
    "ticker",
    "isin",
    "name",
    "currency",
}


def _ensure_struct_fields(
    d: dict | None, fields: set[str], nested: dict[str, set[str]] | None = None
) -> None:
    """Add missing keys with None values to ensure consistent Polars struct schemas.

    When Polars creates a DataFrame from dicts, it infers struct schemas from
    the data.  If a field is absent from all events, ``struct.field()`` raises
    ``StructFieldNotFoundError``.  This helper pre-populates missing keys with
    ``None`` so Polars always infers the complete schema.

    Parameters
    ----------
    d:
        The dict to patch.  Modified in-place; if None, nothing happens.
    fields:
        Top-level field names that must exist in *d*.
    nested:
        Mapping of field name → expected sub-fields.  If *d[field]* is a
        dict, its missing keys are backfilled with None.
    """
    if d is None:
        return
    for field in fields:
        if field not in d:
            d[field] = None
    if nested:
        for field, sub_fields in nested.items():
            inner = d.get(field)
            if isinstance(inner, dict):
                for sub in sub_fields:
                    if sub not in inner:
                        inner[sub] = None


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


def transform_events(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw Trading 212 events data using Polars-native field extraction.

    Splits events by source type (orders, dividends, transactions) and
    applies per-endpoint Polars expressions that use ``struct.field()`` for
    nested access and ``coalesce()`` for fallback chains, instead of
    error-prone ``dict.get()`` patterns.
    """
    dfs: list[pl.DataFrame] = []

    for fetched_at, source, events in decrypt_events_payloads(raw, fernet_key):
        if "/orders" in source:
            dfs.append(_transform_orders(events, fetched_at, source))
        elif "/dividends" in source:
            dfs.append(_transform_dividends(events, fetched_at, source))
        elif "/transactions" in source:
            dfs.append(_transform_transactions(events, fetched_at, source))

    if not dfs:
        return build_normalized_table(
            [], events_normalized_schema, fernet_key, _EVENTS_ENCRYPT_COLUMNS
        )

    # vertical_relaxed promotes mismatched column types across endpoints at
    # the concat boundary (e.g. instrument_ccy is Null for orders/transactions
    # but String for dividends) so per-endpoint casts are not needed.
    # Decision: docs/adr/0105-fix-t212-cdc-dedup-and-concat-type-mismatch.md
    result = pl.concat(dfs, how="vertical_relaxed")

    # T212 events fetches the full order/dividend/transaction history on every
    # run.  Re-fetched pages produce raw payloads with different byte content
    # (and therefore different SHA-256 hashes), so the raw layer re-appends
    # them.  Dedup by (event_type, event_id) -- order.id is an integer cast to
    # string while dividend/transaction reference is a separate string ID
    # space, so event_type scopes the uniqueness -- keeping the version from
    # the latest fetched_at.  Decision: docs/adr/0105-fix-t212-cdc-dedup-and-concat-type-mismatch.md
    result = dedup_events(
        result,
        subset=["event_type", "event_id"],
        sort_after=["event_type", "event_id"],
        label="T212 events",
    )

    return finalize_table(
        result, events_normalized_schema, fernet_key, _EVENTS_ENCRYPT_COLUMNS
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

    Before creating the DataFrame, events are pre-processed via
    ``_ensure_struct_fields()`` to backfill any optional API fields with
    None.  This prevents ``StructFieldNotFoundError`` when the real API
    data omits fields like ``filledQuantity`` or ``filledValue``.

    Tax extraction from ``fill.walletImpact.taxes`` (a nested list of
    structs) is pre-computed in Python because Polars ``map_elements``
    does not reliably pass scalar list-of-struct values to UDFs.
    """
    # Pre-compute tax amounts from nested structures before DataFrame construction
    fee_amounts = [_extract_fee_amount(_get_taxes(e)) for e in events]
    tax_amounts = [_extract_tax_amount(_get_taxes(e)) for e in events]

    # Ensure all expected struct fields exist so Polars infers complete schemas.
    # The T212 API may omit optional fields (e.g. filledQuantity) when they
    # are not applicable, causing struct.field() to fail if absent.
    for event in events:
        _ensure_struct_fields(
            event.get("order"),
            _ORDER_STRUCT_FIELDS,
            nested={"instrument": _INSTRUMENT_FIELDS},
        )
        _ensure_struct_fields(
            event.get("fill"),
            _FILL_STRUCT_FIELDS,
            nested={"walletImpact": _WALLET_IMPACT_FIELDS},
        )

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
    # wallet_fx_rate: the rate from wallet currency to security trading currency.
    # E.g. for a PLN wallet buying a USD security, this is the PLN→USD rate.
    # Consumed here for wallet→security conversion; NOT stored in the output
    # schema (target_fx_rate is computed later by normalize_currency).
    fx_rate = pl.coalesce([wallet_impact.struct.field("fxRate"), pl.lit(1.0)]).cast(
        pl.Float64
    )

    # security_ccy: the instrument's trading currency (e.g. USD for SPYI).
    security_ccy = pl.coalesce(
        [instrument.struct.field("currency"), order.struct.field("currency")]
    ).cast(pl.Utf8)

    # Decision: docs/adr/0112-remove-yagni-gross-amount-column.md
    # Sign convention (origin ADR 0058, carried forward via the active schema
    # ADR 0077; gross_amount half removed in ADR 0112, cash_amount retained):
    # positive = inflow, negative = outflow.  T212 reports the trade cash
    # impact as an unsigned magnitude in walletImpact.netValue for both
    # BUY and SELL, carrying direction only in ``side``.  IBKR already reports a
    # signed netCash (BUY negative).  Apply the direction sign here so T212
    # trades conform to the convention and match IBKR: BUY = outflow ->
    # negative, SELL = inflow -> positive.  Without this, the Cash Flow
    # Breakdown chart nets unsigned T212 magnitudes against signed IBKR
    # values within the TRADE event type.
    side = order.struct.field("side")
    direction = pl.when(side == "BUY").then(-1.0).otherwise(1.0)

    # Cash amount in security currency, converted from wallet currency using
    # walletImpact.fxRate.  For same-currency trades (wallet ccy == security ccy)
    # the fx_rate is 1.0 and this is equivalent to net_value.  Signed by
    # direction so buys are negative (outflow) and sells positive (inflow).
    cash_amount_security_ccy = net_value * fx_rate * direction

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
        security_ccy=security_ccy,
        # Null-typed; pl.concat(how="vertical_relaxed") at the concat site
        # promotes it to String to match dividends' tickerCurrency.
        # Decision: docs/adr/0105-fix-t212-cdc-dedup-and-concat-type-mismatch.md
        instrument_ccy=pl.lit(None),
        cash_amount=cash_amount_security_ccy,
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
        # Decision: docs/adr/0078-fix-t212-wallet-fx-rate.md
        # fee_amount and tax_amount are converted from wallet ccy to
        # security_ccy using walletImpact.fxRate (the wallet→security rate),
        # the same rate used for cash_amount conversion.
        fee_amount=pl.Series("fee_amount", fee_amounts, dtype=pl.Float64) * fx_rate,
        tax_amount=pl.Series("tax_amount", tax_amounts, dtype=pl.Float64) * fx_rate,
        # target_fx_rate, target_value, target_ccy are null for T212 orders;
        # they are populated by the normalize_currency step.
        target_fx_rate=pl.lit(None),
        target_value=pl.lit(None),
        target_ccy=pl.lit(None),
    )


def _transform_dividends(events: list[dict], fetched_at, source: str) -> pl.DataFrame:
    """Transform T212 HistoryDividendItem events using Polars expressions.

    Dividend items have a nested ``instrument`` object but are otherwise
    flat.  The ``type`` field is stored as ``raw_event_type`` for
    diagnostics; ``event_type`` is always ``DIVIDEND``.
    """
    # Ensure nested instrument struct has all expected fields.
    for event in events:
        _ensure_struct_fields(event.get("instrument"), _INSTRUMENT_FIELDS)

    # Log warnings for dividends where the payout currency differs from the
    # instrument's trading currency.  FX conversion for these dividends is
    # handled by normalize_currency() in the normalization step, not here.
    for event in events:
        div_ccy = event.get("currency", "")
        ticker_ccy = event.get("tickerCurrency", "")
        if div_ccy and ticker_ccy and div_ccy != ticker_ccy:
            logger.warning(
                "T212 dividend %s: currency=%s differs from tickerCurrency=%s; "
                "FX conversion handled by normalize_currency()",
                event.get("ticker", event.get("reference", "?")),
                div_ccy,
                ticker_ccy,
            )

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
        security_ccy=pl.coalesce([pl.col("currency"), pl.col("tickerCurrency")]),
        instrument_ccy=pl.col("tickerCurrency"),
        cash_amount=amount,
        settle_date=pl.col("paidOn").cast(pl.Utf8),
        ticker=pl.coalesce([pl.col("ticker"), instrument.struct.field("ticker")]),
        isin=instrument.struct.field("isin"),
        description=instrument.struct.field("name"),
        quantity=qty,
        price=price,
        side=pl.lit(""),
        fee_amount=pl.lit(0.0),
        tax_amount=pl.lit(0.0),
        # target_fx_rate, target_value, target_ccy are null for T212 dividends;
        # they are populated by the normalize_currency step.
        target_fx_rate=pl.lit(None),
        target_value=pl.lit(None),
        target_ccy=pl.lit(None),
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
        security_ccy=pl.col("currency").cast(pl.Utf8),
        # Null-typed; promoted to String by pl.concat(how="vertical_relaxed").
        instrument_ccy=pl.lit(None),
        cash_amount=amount,
        settle_date=pl.lit(""),
        ticker=pl.lit(""),
        isin=pl.lit(""),
        description=pl.lit(""),
        quantity=pl.lit(0.0),
        price=pl.lit(0.0),
        side=pl.lit(""),
        fee_amount=pl.lit(0.0),
        tax_amount=pl.lit(0.0),
        # target_fx_rate, target_value, target_ccy are null for T212 transactions;
        # they are populated by the normalize_currency step.
        target_fx_rate=pl.lit(None),
        target_value=pl.lit(None),
        target_ccy=pl.lit(None),
    )
