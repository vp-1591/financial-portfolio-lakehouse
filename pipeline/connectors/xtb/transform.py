"""XTB connector: transform raw snapshot and CDC data into normalized schema.

Stage 2 rewrite (overhaul plan D2, D4-D22). Both transforms read the shared
bronze ``xtb_snapshot`` raw table (D17 — one raw row per uploaded file with
``source == "XTB_REPORT"`` carrying the full 3-sheet workbook). The parser
(:func:`pipeline.connectors.xtb.parser.parse_report`) turns each raw payload
into an :class:`XtbReport`; the transforms keep the latest ``fetched_at``
**per ``account_id``** (D18/D9 — not :func:`filter_latest_snapshot`, which
keys on ``source`` alone and collapses distinct accounts) and emit normalized
rows for every surviving account.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import polars as pl
import pyarrow as pa

from pipeline.connectors.transform_utils import (
    DecodedRow,
    build_normalized_table,
    dedup_cdc_events,
    iter_raw_payloads,
)
from pipeline.connectors.xtb.parser import (
    XtbCashOperation,
    XtbClosedPosition,
    XtbReport,
    parse_report,
)
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    snapshot_normalized_schema,
)

logger = logging.getLogger(__name__)

# XTB operation_type -> normalized event_type mapping (D6 + guard 10 aliases).
# Replaces the old map entirely: drops "Stock sale", "Interest", and
# "Currency exchange"; adds "Free funds interest", "Free funds interest tax",
# and keeps "Open position"/"Close position" as TRADE aliases (guard 10).
_XTB_EVENT_TYPE_MAP: dict[str, str] = {
    "Free funds interest": "INTEREST",
    "Free funds interest tax": "TAX",
    "Stock purchase": "TRADE",
    "Stock sell": "TRADE",
    "Open position": "TRADE",
    "Close position": "TRADE",
    "Transfer": "TRANSFER",
    "Deposit": "DEPOSIT",
    "Withdrawal": "WITHDRAWAL",
    "Dividend": "DIVIDEND",
    "Fee": "FEE",
    "Correction": "ADJUSTMENT",
    "Profit/loss adjustment": "ADJUSTMENT",
}

# Trade-row operation types (carry qty/price/side in the comment).
_TRADE_OPERATION_TYPES = frozenset(
    {"Stock sell", "Stock purchase", "Open position", "Close position"}
)
# Closing-row operation types (fee/gross/settle enriched from Closed Positions, D8).
_SELL_OPERATION_TYPES = frozenset({"Stock sell", "Close position"})

# Guard 6: trade-row comment pattern "OPEN/CLOSE {side} {qty} @ {price}".
_TRADE_COMMENT_RE = re.compile(
    r"^(?:OPEN|CLOSE)\s+(?P<side>BUY|SELL)\s+"
    r"(?P<qty>\d+(?:\.\d+)?)\s+@\s+(?P<price>\d+(?:\.\d+)?)"
)


def _classify_xtb_event_type(raw_type: str) -> str:
    """Map an XTB operation_type to a normalized event_type (D6)."""
    return _XTB_EVENT_TYPE_MAP.get(raw_type, "UNKNOWN")


def _account_id_from_filename(source_file: str) -> str | None:
    """Extract the account id from an XTB export filename.

    New-format filenames follow ``{CCY}_{account_id}_{from}_{to}.xlsx`` (e.g.
    ``PLN_12345678_2006-01-01_2026-08-03.xlsx`` -> ``12345678``). Returns
    ``None`` when the filename does not match the pattern, so the caller can
    fall back to a guarded parse for account_id discovery (no silent data loss).
    """
    parts = Path(source_file).stem.split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    return None


def _latest_per_account(
    raw: pa.Table, fernet_key: bytes
) -> list[tuple[DecodedRow, XtbReport]]:
    """Keep the latest parseable report per ``account_id`` (D18/D9, guard 9).

    The account id is read from the filename pattern
    ``{CCY}_{account_id}_{from}_{to}.xlsx`` (== R1 ``Account number``), so raw
    rows are grouped WITHOUT parsing. Only the latest row per account is parsed,
    and every parse is guarded: a malformed latest row falls back to the
    previous good row for that account, and if all rows for an account fail the
    account is skipped with a warning (the connector can no longer be killed by
    one bad row). Rows whose filename does not match the pattern fall back to a
    guarded parse for account_id discovery. R1 ``account_id`` is authoritative;
    a filename/R1 mismatch is logged. Guard 9: the sort key stays
    ``(fetched_at, source_file)`` desc so ties on ``fetched_at`` break
    deterministically and do not emit duplicate holdings.

    Decision: docs/adr/0108-xtb-new-format-connector-overhaul.md
    """
    by_account: dict[str, list[tuple[tuple[object, str], DecodedRow]]] = {}
    unparsed: list[tuple[tuple[object, str], DecodedRow]] = []
    for row in iter_raw_payloads(raw, fernet_key, require_json=False):
        if row.source != "XTB_REPORT":
            continue
        account_id = _account_id_from_filename(row.source_file)
        sort_key = (row.fetched_at, row.source_file)
        if account_id is None:
            unparsed.append((sort_key, row))
        else:
            by_account.setdefault(account_id, []).append((sort_key, row))

    # Fallback: rows whose filename doesn't match the pattern — parse (guarded)
    # to discover account_id rather than silently dropping the account.
    for sort_key, row in unparsed:
        try:
            report = parse_report(row.payload_raw)
        except Exception as exc:
            logger.warning(
                "XTB: cannot derive account_id from %r and parse failed (%s); skipping",
                row.source_file,
                exc,
            )
            continue
        if not report.account_id:
            logger.warning("XTB: no account_id in %r; skipping", row.source_file)
            continue
        by_account.setdefault(report.account_id, []).append((sort_key, row))

    # Parse only the latest row per account; fall back to older rows on failure.
    result: list[tuple[DecodedRow, XtbReport]] = []
    for account_id, rows in by_account.items():
        rows.sort(key=lambda t: t[0], reverse=True)
        for sort_key, row in rows:
            try:
                report = parse_report(row.payload_raw)
            except Exception as exc:
                logger.warning(
                    "XTB account %s: parse failed for %s (%s); trying older row",
                    account_id,
                    row.source_file,
                    exc,
                )
                continue
            if report.account_id and report.account_id != account_id:
                logger.warning(
                    "XTB: filename account_id %s != report R1 %s for %s",
                    account_id,
                    report.account_id,
                    row.source_file,
                )
            if report.dropped_cash_rows:
                logger.warning(
                    "XTB account %s: %d cash-operation row(s) dropped (empty Time "
                    "cell) in %s",
                    account_id,
                    report.dropped_cash_rows,
                    row.source_file,
                )
            result.append((row, report))
            break
    return result


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB snapshot data into the normalized schema (D18/D22).

    Iterates all ``source == "XTB_REPORT"`` rows, parses each workbook via
    :func:`parse_report`, keeps the latest ``fetched_at`` per ``account_id``
    (D18, guard 9), and emits one EQUITY row per per-ticker aggregate (D4)
    plus one CASH holding per account from ``XtbReport.free_cash`` (D22).
    """
    payloads = _latest_per_account(raw, fernet_key)
    records: list[dict] = []

    for row, report in payloads:
        # EQUITY rows from per-ticker aggregates (child lots skipped by parser, D4).
        for pos in report.open_positions:
            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "account_id": pos.account_id,
                    "position_type": "EQUITY",
                    "label": pos.ticker,
                    "asset_class": pos.category,
                    "security_value": pos.value,
                    "security_ccy": report.account_ccy,
                    "isin": "",  # no ISIN in the new format (D12)
                    "description": pos.instrument,
                }
            )
        # CASH holding from the Cash Operations Total row (D22).
        if report.free_cash is not None:
            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "account_id": report.account_id,
                    "position_type": "CASH",
                    "label": f"CASH {report.account_ccy}",
                    "asset_class": "CASH",
                    "security_value": round(report.free_cash, 2),
                    "security_ccy": report.account_ccy,
                    "isin": "",
                    "description": f"Cash {report.account_ccy}",
                }
            )

    return build_normalized_table(
        records,
        snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=["security_value"],
    )


def _build_closed_lookup(
    closed_positions: list[XtbClosedPosition],
    account_id: str,
) -> dict[str, XtbClosedPosition]:
    """Build a ``position_id -> XtbClosedPosition`` lookup (guard 8).

    Guard 8: warn on duplicate ``position_id`` in Closed Positions (dict
    last-wins would pick an arbitrary commission, making ``fee_amount``
    non-deterministic). The warning is logged per duplicate; the last row
    wins by insertion order.
    """
    lookup: dict[str, XtbClosedPosition] = {}
    for cp in closed_positions:
        if cp.position_id in lookup:
            logger.warning(
                "XTB CDC account %s: duplicate position_id %s in Closed Positions "
                "(last row wins; fee_amount may be non-deterministic)",
                account_id,
                cp.position_id,
            )
        lookup[cp.position_id] = cp
    return lookup


def _parse_trade_comment(comment: str) -> tuple[float | None, float | None, str | None]:
    """Parse ``OPEN/CLOSE {side} {qty} @ {price}`` into (quantity, price, side).

    Guard 6: if the comment does not match, log a warning and return
    ``(None, None, None)`` — do NOT silently emit nulls with no signal.
    """
    match = _TRADE_COMMENT_RE.match(comment.strip())
    if match is None:
        logger.warning(
            "XTB CDC: unparseable trade comment %r (expected "
            "'OPEN/CLOSE {side} {qty} @ {price}'); leaving quantity/price/side null",
            comment,
        )
        return None, None, None
    qty = float(match.group("qty"))
    price = float(match.group("price"))
    side = match.group("side")
    return qty, price, side


def _build_cdc_record(
    op: XtbCashOperation,
    row: DecodedRow,
    report: XtbReport,
    closed_lookup: dict[str, XtbClosedPosition],
) -> dict:
    """Build a single CDC event record from a cash operation (D8, guards 6/7)."""
    event_type = _classify_xtb_event_type(op.operation_type)
    is_trade = op.operation_type in _TRADE_OPERATION_TYPES

    # Trade rows: parse qty/price/side from the comment (guard 6); set ticker.
    quantity: float | None = None
    price: float | None = None
    side: str | None = None
    if is_trade:
        quantity, price, side = _parse_trade_comment(op.comment)
    ticker = op.ticker if is_trade else ""

    # Sell-row enrichment from Closed Positions (D8, guard 7).
    fee_amount: float | None = None
    gross_amount: float | None = None
    settle_date: str | None = None
    if op.operation_type in _SELL_OPERATION_TYPES:
        closed = closed_lookup.get(op.position_id)
        if closed is None:
            if op.position_id:
                logger.warning(
                    "XTB CDC account %s: sell row position_id %s has no Closed "
                    "Positions match; leaving fee_amount/gross_amount/settle_date null",
                    op.account_id,
                    op.position_id,
                )
            else:
                logger.warning(
                    "XTB CDC account %s: sell row has no position_id; leaving "
                    "fee_amount/gross_amount/settle_date null",
                    op.account_id,
                )
        else:
            fee_amount = closed.commission
            # Guard 2: round gross_amount to 2dp to avoid IEEE-754 artifacts (D11).
            gross_amount = round(closed.sale_value - closed.purchase_value, 2)
            settle_date = closed.close_time.isoformat()

    return {
        "fetched_at": row.fetched_at,
        "broker": "XTB",
        "account_id": op.account_id,
        "event_id": op.operation_id,
        "source": row.source,  # "XTB_REPORT" (D17)
        "event_type": event_type,
        "raw_event_type": op.operation_type,
        "event_datetime": op.time.isoformat(),
        "security_ccy": report.account_ccy,
        "instrument_ccy": None,  # XTB exposes no per-instrument trading currency
        "cash_amount": op.amount,
        "settle_date": settle_date,
        "ticker": ticker,
        "isin": "",  # no ISIN (D12)
        "description": op.comment,
        "quantity": quantity,
        "price": price,
        "side": side,
        "gross_amount": gross_amount,
        "fee_amount": fee_amount,
        "tax_amount": None,  # tax is its own TAX event in cash_amount
        "target_fx_rate": None,  # D7: do NOT parse Exchange rate:X
        "target_value": None,  # filled by normalize_currency
        "target_ccy": None,  # set to EUR by normalize_currency
    }


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB CDC data into the broker-neutral CDC events schema.

    Reads the shared bronze ``xtb_snapshot`` raw (D17 — same raw as snapshot),
    iterates all ``source == "XTB_REPORT"`` rows, parses each via
    :func:`parse_report`, keeps the latest ``fetched_at`` per ``account_id``
    (D9/D18 — same selection as snapshot, NOT a union of all uploads), and
    emits one event per cash operation (Total rows excluded; subaccount
    transfers filtered by the parser — D7/D10). Trade rows carry
    qty/price/side parsed from the comment; closing rows are enriched with
    ``fee_amount``/``gross_amount``/``settle_date`` from Closed Positions (D8).
    ``dedup_cdc_events`` on ``(event_type, event_id, account_id)`` is a safety
    net (D9, ADR 0105 parity).
    """
    payloads = _latest_per_account(raw, fernet_key)
    records: list[dict] = []

    for row, report in payloads:
        closed_lookup = _build_closed_lookup(report.closed_positions, report.account_id)
        for op in report.cash_operations:
            records.append(_build_cdc_record(op, row, report, closed_lookup))

    # D9: dedup on (event_type, event_id, account_id) — account_id keeps
    # same-ID events from different accounts distinct. Safety net; latest-
    # per-account selection already yields one payload per account.
    if records:
        df = pl.DataFrame(records)
        df = dedup_cdc_events(
            df,
            subset=["event_type", "event_id", "account_id"],
            label="XTB CDC",
        )
        records = df.to_dicts()

    return build_normalized_table(
        records,
        cdc_events_normalized_schema,
        fernet_key,
        # Mirror IBKR: all pa.binary() trade + target columns are encrypted.
        # The old list left trade columns unencrypted, which failed the
        # cast(schema) since they are pa.binary() in cdc_events_normalized_schema.
        encrypt_columns=[
            "cash_amount",
            "quantity",
            "price",
            "gross_amount",
            "fee_amount",
            "tax_amount",
            "target_fx_rate",
            "target_value",
        ],
    )
