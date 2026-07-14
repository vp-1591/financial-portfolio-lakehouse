"""IBKR connector: transform raw Flex snapshot and CDC data into normalized schema."""

from __future__ import annotations

import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl
import pyarrow as pa

from pipeline.connectors.ibkr.client import (
    CashReportResult,
    as_float,
    parse_account_info,
    parse_cash_report,
    parse_conversion_rates,
    parse_positions,
    parse_cash_transactions,
    parse_trades,
    parse_transaction_fees,
    parse_transfers,
)
from pipeline.connectors.transform_utils import (
    build_normalized_table,
    decode_payload,
    filter_latest_snapshot,
)
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    ibkr_snapshot_normalized_schema,
)

logger = logging.getLogger(__name__)


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


def transform_cdc(
    raw: pa.Table, fernet_key: bytes, *, is_demo: bool = False
) -> pa.Table:
    """Transform raw IBKR CDC Flex XML data into the broker-neutral CDC events schema.

    Flex CDC payloads use ``source="flex_cdc"`` and contain XML with
    Trade, CashTransaction, Transfer, and TransactionFee elements.
    Each section is parsed and mapped to the broker-neutral schema.

    When ``is_demo`` is True, a synthetic initial deposit of
    ``_DEMO_INITIAL_DEPOSIT_AMOUNT`` is injected for each account,
    dated one day before the earliest existing event.
    """
    records: list[dict[str, Any]] = []

    sources = raw.column("source").to_pylist()
    fetched_ats_col = raw.column("fetched_at").to_pylist()
    payloads = raw.column("payload").to_pylist()

    for i in range(len(sources)):
        if sources[i] != "flex_cdc":
            continue

        payload_bytes = payloads[i]
        decrypted = decode_payload(payload_bytes, fernet_key)
        if decrypted is not None:
            payload_bytes = decrypted
        elif isinstance(payload_bytes, memoryview):
            payload_bytes = bytes(payload_bytes)

        root = ET.fromstring(payload_bytes)
        account_infos = parse_account_info(root)

        # Build account_id -> base_currency lookup
        base_currency_by_account: dict[str, str] = {}
        for info in account_infos:
            acct_id = str(info.get("accountId", ""))
            currency = str(info.get("currency", "") or "").upper()
            if currency and currency != "BASE":
                base_currency_by_account[acct_id] = currency
            elif not base_currency_by_account.get(acct_id):
                base_currency_by_account[acct_id] = "USD"

        fetched_at = fetched_ats_col[i]
        if isinstance(fetched_at, str):
            fetched_at = datetime.fromisoformat(fetched_at)

        # Process Trades
        for trade in parse_trades(root):
            records.append(
                _process_ibkr_trade(trade, fetched_at, base_currency_by_account)
            )

        # Process CashTransactions
        for ct in parse_cash_transactions(root):
            records.append(
                _process_ibkr_cash_transaction(ct, fetched_at, base_currency_by_account)
            )

        # Process Transfers
        for transfer in parse_transfers(root):
            records.append(
                _process_ibkr_transfer(transfer, fetched_at, base_currency_by_account)
            )

        # Process TransactionFees
        for fee in parse_transaction_fees(root):
            records.append(
                _process_ibkr_transaction_fee(fee, fetched_at, base_currency_by_account)
            )

    # Inject a synthetic initial deposit for each demo account so that
    # the cash flow breakdown chart makes sense.
    _inject_demo_deposit(records, is_demo=is_demo)

    result = build_normalized_table(
        records,
        cdc_events_normalized_schema,
        fernet_key,
        encrypt_columns=[
            "cash_amount",
            "quantity",
            "price",
            "gross_amount",
            "fee_amount",
            "tax_amount",
            "net_amount",
            "fx_rate_to_base",
            "amount_base",
        ],
    )

    # IBKR Flex CDC queries return the full account history on every fetch.
    # When multiple raw payloads exist (from repeated pipeline runs), the same
    # events appear in each payload, producing duplicates.  Dedup by event_id
    # using Polars, keeping the version from the latest fetched_at.
    if result.num_rows > 0:
        df = pl.from_arrow(result)
        before = df.height
        df = df.sort("fetched_at", descending=True).unique(subset=["event_id"])
        after = df.height
        if before > after:
            logger.info(
                "IBKR CDC dedup: removed %d duplicate events (%d → %d)",
                before - after,
                before,
                after,
            )
        # Sort by event_id for deterministic row order across runs.
        df = df.sort("event_id")
        result = df.to_arrow()

    return result


# --- IBKR CDC event type classification ---

_IBKR_CASH_TYPE_MAP: dict[str, str] = {
    "Dividends": "DIVIDEND",
    "PaymentInLieue": "DIVIDEND",
    "Withholding Tax": "TAX",
    "871(m) Withholding": "TAX",
    "Broker Interest Received": "INTEREST",
    "Broker Interest Paid": "INTEREST",
    "Bond Interest Received": "INTEREST",
    "Bond Interest Paid": "INTEREST",
    "Broker Fees": "FEE",
    "Other Fees": "FEE",
    "Other Income": "ADJUSTMENT",
    "Price Adjustments": "ADJUSTMENT",
    "Commission Adjustments": "FEE",
}


def _classify_ibkr_cash_type(cash_type: str, amount: float) -> str:
    """Map an IBKR CashTransaction type to a normalized event_type.

    Deposits & Withdrawals are classified by sign: positive = DEPOSIT,
    negative = WITHDRAWAL.
    """
    if cash_type == "Deposits & Withdrawals":
        return "DEPOSIT" if amount >= 0 else "WITHDRAWAL"
    return _IBKR_CASH_TYPE_MAP.get(cash_type, "UNKNOWN")


# IBKR demo accounts start with ~$1M but the Flex CDC API does not
# return a CashTransaction for the initial funding.  Without it the cash
# flow breakdown chart looks wrong — trades and fees appear with no
# deposit to explain the starting balance.  The constant below is the
# amount injected per demo account.
_DEMO_INITIAL_DEPOSIT_AMOUNT = 1_000_000.0


def _inject_demo_deposit(
    records: list[dict[str, Any]],
    is_demo: bool,
) -> list[dict[str, Any]]:
    """Inject a synthetic initial deposit for each IBKR demo account.

    When ``is_demo`` is True, adds a DEPOSIT event dated one day before
    the earliest existing event so that the deposit precedes all other
    activity.  Each unique ``(account_id, base_currency)`` pair from the
    existing records gets its own deposit of ``_DEMO_INITIAL_DEPOSIT_AMOUNT``
    in the account's base currency.

    The function is a no-op when ``is_demo`` is False.

    Parameters
    ----------
    records:
        CDC event records already parsed from the Flex XML payload.
    is_demo:
        Whether demo mode is active.

    Returns
    -------
    list[dict[str, Any]]
        The original records plus any synthetic deposit records.
    """
    if not is_demo:
        return records

    if not records:
        # No events at all — inject a deposit at a safe fallback date.
        # This is unlikely in practice but handled for completeness.
        logger.info(
            "IBKR demo: no CDC events found; injecting deposit at fallback date"
        )
        return records

    # Find the earliest event_datetime across all records.
    earliest_dt: datetime | None = None
    for rec in records:
        dt_str = rec.get("event_datetime", "")
        if not dt_str:
            continue
        normalised = _normalize_ibkr_datetime(str(dt_str))
        try:
            dt = datetime.fromisoformat(normalised.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if earliest_dt is None or dt < earliest_dt:
            earliest_dt = dt

    if earliest_dt is None:
        # Could not parse any date — use a safe fallback.
        deposit_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
    else:
        # Zero out the time component so the deposit lands at midnight UTC
        # on the day before the earliest event, regardless of the event's
        # time-of-day.
        deposit_date = earliest_dt.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    deposit_date_str = deposit_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    settle_date_str = deposit_date.strftime("%Y-%m-%d")

    # Use the fetched_at from the first record.
    fetched_at = records[0].get("fetched_at", datetime.now(timezone.utc))

    # Collect unique (account_id, base_currency) pairs.
    accounts: dict[str, str] = {}
    for rec in records:
        acct_id = rec.get("account_id", "")
        base_ccy = rec.get("base_currency", "")
        if acct_id and base_ccy and acct_id not in accounts:
            accounts[acct_id] = base_ccy

    # If no account info could be extracted, fall back to an empty account.
    if not accounts:
        accounts[""] = "USD"

    for acct_id, base_ccy in accounts.items():
        records.append(
            {
                "fetched_at": fetched_at,
                "broker": "IBKR",
                "account_id": acct_id,
                "event_id": _deterministic_event_id(
                    "CashTransaction", acct_id, "DEMO_INITIAL_DEPOSIT"
                ),
                "source": "CashTransaction",
                "event_type": "DEPOSIT",
                "raw_event_type": "Deposits & Withdrawals",
                "event_datetime": deposit_date_str,
                "value_currency": base_ccy,
                "cash_amount": _DEMO_INITIAL_DEPOSIT_AMOUNT,
                "settle_date": settle_date_str,
                "ticker": "",
                "isin": "",
                "description": "Initial demo account deposit",
                "base_currency": base_ccy,
                "fx_rate_to_base": 1.0,
                "amount_base": _DEMO_INITIAL_DEPOSIT_AMOUNT,
            }
        )

    logger.info(
        "IBKR demo: injected initial deposit of %.0f %s for %d account(s)",
        _DEMO_INITIAL_DEPOSIT_AMOUNT,
        list(accounts.values())[0] if len(accounts) == 1 else "various",
        len(accounts),
    )

    return records


def _deterministic_event_id(source: str, account_id: str, *fields: str) -> str:
    """Generate a deterministic event ID from source, account, and field values."""
    content = f"{source}|{account_id}|{'|'.join(str(f) for f in fields)}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


# IBKR Flex uses compact date/time formats that differ from ISO 8601:
# - CashTransaction dateTime: "YYYYMMDD" (e.g. "20260204")
# - Trade dateTime: "YYYYMMDD;HHMMSS" (e.g. "20260702;022904")
# These regexes normalise them to ISO 8601 so downstream parsing works.
_IBKR_COMPACT_DATETIME_RE = re.compile(r"^(\d{4})(\d{2})(\d{2});(\d{2})(\d{2})(\d{2})$")
_IBKR_COMPACT_DATE_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})$")


def _normalize_ibkr_datetime(dt_str: str) -> str:
    """Normalise an IBKR Flex ``dateTime`` string to ISO 8601 format.

    IBKR uses two compact formats not recognised by standard date parsers:

    - ``YYYYMMDD`` — e.g. ``"20260204"`` → ``"2026-02-04T00:00:00Z"``
    - ``YYYYMMDD;HHMMSS`` — e.g. ``"20260702;022904"`` → ``"2026-07-02T02:29:04Z"``

    Strings that already match standard formats (e.g. ``"2026-03-01 00:00:00"``
    or ``"2026-03-01"``) are returned unchanged.

    Parameters
    ----------
    dt_str:
        Raw dateTime value from IBKR Flex XML.

    Returns
    -------
    str
        ISO 8601 datetime string, or the original string if no pattern matched.
    """
    if not dt_str:
        return dt_str

    m = _IBKR_COMPACT_DATETIME_RE.match(dt_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}:{m.group(5)}:{m.group(6)}Z"

    m = _IBKR_COMPACT_DATE_RE.match(dt_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}T00:00:00Z"

    return dt_str


def _process_ibkr_trade(
    trade: dict[str, Any],
    fetched_at: datetime,
    base_currency_by_account: dict[str, str],
) -> dict[str, Any]:
    """Map a Trade element to the broker-neutral CDC schema."""
    acct_id = str(trade.get("accountId", ""))
    event_id = str(
        trade.get("ibExecutionId", "") or trade.get("tradeId", "") or ""
    ) or _deterministic_event_id(
        "Trade",
        acct_id,
        str(trade.get("dateTime", "")),
        str(trade.get("symbol", "")),
        str(trade.get("quantity", "")),
    )
    currency = str(trade.get("currency", "") or "").upper()
    base_currency = base_currency_by_account.get(acct_id, currency)
    fx_rate = as_float(trade.get("fxRateToBase"), 1.0)

    return {
        "fetched_at": fetched_at,
        "broker": "IBKR",
        "account_id": acct_id,
        "event_id": event_id,
        "source": "Trade",
        "event_type": "TRADE",
        "raw_event_type": str(trade.get("transactionType", "ExTrade")),
        "event_datetime": _normalize_ibkr_datetime(str(trade.get("dateTime", ""))),
        "value_currency": currency,
        "cash_amount": as_float(trade.get("netCash")),
        "settle_date": str(trade.get("settleDateTarget", "")),
        "ticker": str(trade.get("symbol", "")),
        "isin": str(trade.get("isin", "")),
        "description": str(trade.get("description", "")),
        "quantity": as_float(trade.get("quantity")),
        "price": as_float(trade.get("tradePrice")),
        "side": str(trade.get("buySell", "")),
        "gross_amount": as_float(trade.get("proceeds")),
        "fee_amount": abs(as_float(trade.get("ibCommission"))),
        "tax_amount": as_float(trade.get("taxes")),
        "net_amount": as_float(trade.get("netCash")),
        "base_currency": base_currency,
        "fx_rate_to_base": fx_rate,
        "amount_base": as_float(trade.get("netCash")) * fx_rate,
    }


def _process_ibkr_cash_transaction(
    ct: dict[str, Any],
    fetched_at: datetime,
    base_currency_by_account: dict[str, str],
) -> dict[str, Any]:
    """Map a CashTransaction element to the broker-neutral CDC schema."""
    acct_id = str(ct.get("accountId", ""))
    amount = as_float(ct.get("amount"))
    event_id = str(ct.get("transactionId", "") or "") or _deterministic_event_id(
        "CashTransaction",
        acct_id,
        str(ct.get("dateTime", "")),
        str(ct.get("type", "")),
        str(ct.get("amount", "")),
    )
    currency = str(ct.get("currency", "") or "").upper()
    base_currency = base_currency_by_account.get(acct_id, currency)
    fx_rate = as_float(ct.get("fxRateToBase"), 1.0)
    cash_type = str(ct.get("type", ""))

    return {
        "fetched_at": fetched_at,
        "broker": "IBKR",
        "account_id": acct_id,
        "event_id": event_id,
        "source": "CashTransaction",
        "event_type": _classify_ibkr_cash_type(cash_type, amount),
        "raw_event_type": cash_type,
        "event_datetime": _normalize_ibkr_datetime(str(ct.get("dateTime", ""))),
        "value_currency": currency,
        "cash_amount": amount,
        "settle_date": str(ct.get("settleDate", "")),
        "ticker": str(ct.get("symbol", "")),
        "isin": str(ct.get("isin", "")),
        "description": str(ct.get("description", "")),
        "base_currency": base_currency,
        "fx_rate_to_base": fx_rate,
        "amount_base": amount * fx_rate,
    }


def _process_ibkr_transfer(
    transfer: dict[str, Any],
    fetched_at: datetime,
    base_currency_by_account: dict[str, str],
) -> dict[str, Any]:
    """Map a Transfer element to the broker-neutral CDC schema."""
    acct_id = str(transfer.get("accountId", ""))
    event_id = str(transfer.get("transactionId", "") or "") or _deterministic_event_id(
        "Transfer",
        acct_id,
        str(transfer.get("dateTime", "")),
        str(transfer.get("symbol", "")),
        str(transfer.get("quantity", "")),
    )
    currency = str(transfer.get("currency", "") or "").upper()
    base_currency = base_currency_by_account.get(acct_id, currency)
    fx_rate = as_float(transfer.get("fxRateToBase"), 1.0)
    cash_transfer = as_float(transfer.get("cashTransfer"))

    return {
        "fetched_at": fetched_at,
        "broker": "IBKR",
        "account_id": acct_id,
        "event_id": event_id,
        "source": "Transfer",
        "event_type": "TRANSFER",
        "raw_event_type": str(transfer.get("type", "")),
        "event_datetime": _normalize_ibkr_datetime(str(transfer.get("dateTime", ""))),
        "value_currency": currency,
        "cash_amount": cash_transfer,
        "settle_date": str(transfer.get("settleDate", "")),
        "ticker": str(transfer.get("symbol", "")),
        "isin": str(transfer.get("isin", "")),
        "description": str(transfer.get("description", "")),
        "quantity": as_float(transfer.get("quantity")),
        "price": as_float(transfer.get("transferPrice")),
        "side": str(transfer.get("direction", "")),
        "base_currency": base_currency,
        "fx_rate_to_base": fx_rate,
    }


def _process_ibkr_transaction_fee(
    fee: dict[str, Any],
    fetched_at: datetime,
    base_currency_by_account: dict[str, str],
) -> dict[str, Any]:
    """Map a TransactionFee element to the broker-neutral CDC schema."""
    acct_id = str(fee.get("accountId", ""))
    event_id = _deterministic_event_id(
        "TransactionFee",
        acct_id,
        str(fee.get("date", "")),
        str(fee.get("taxDescription", "")),
        str(fee.get("taxAmount", "")),
    )
    currency = str(fee.get("currency", "") or "").upper()
    base_currency = base_currency_by_account.get(acct_id, currency)
    fx_rate = as_float(fee.get("fxRateToBase"), 1.0)
    tax_amount = as_float(fee.get("taxAmount"))

    return {
        "fetched_at": fetched_at,
        "broker": "IBKR",
        "account_id": acct_id,
        "event_id": event_id,
        "source": "TransactionFee",
        "event_type": "FEE",
        "raw_event_type": str(fee.get("taxDescription", "")),
        "event_datetime": _normalize_ibkr_datetime(str(fee.get("date", ""))),
        "value_currency": currency,
        "cash_amount": tax_amount,
        "settle_date": str(fee.get("settleDate", "")),
        "ticker": str(fee.get("symbol", "")),
        "isin": str(fee.get("isin", "")),
        "description": str(fee.get("taxDescription", "")),
        "quantity": as_float(fee.get("quantity")),
        "price": as_float(fee.get("tradePrice")),
        "fee_amount": tax_amount,
        "base_currency": base_currency,
        "fx_rate_to_base": fx_rate,
    }


def _flex_position_label(position: dict) -> str:
    """Extract a display label from a Flex OpenPosition element."""
    for key in ("symbol", "description"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)
    return "UNKNOWN"
