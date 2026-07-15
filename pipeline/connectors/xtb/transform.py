"""XTB connector: transform raw snapshot and CDC data into normalized schema."""

from __future__ import annotations

import pyarrow as pa

from pipeline.connectors.transform_utils import (
    build_normalized_table,
    filter_latest_snapshot,
    iter_raw_payloads,
)
from pipeline.connectors.xtb.parser import (
    load_cash_operations_from_bytes,
    load_positions_from_bytes,
)
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    xtb_snapshot_normalized_schema,
)


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB snapshot data into the normalized schema.

    The raw payload is expected to contain .xlsx bytes (the original
    XTB report file). This function decrypts the payload, parses the
    .xlsx, and extracts positions and cash balances.
    """
    raw = filter_latest_snapshot(raw)
    records: list[dict] = []

    for row in iter_raw_payloads(raw, fernet_key, require_json=False):
        if "OPEN POSITION" not in row.source.upper():
            continue

        positions, _net_worth = load_positions_from_bytes(row.payload_raw)

        for pos in positions:
            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "account_id": pos.account_id,
                    "position_type": pos.asset_class,
                    "label": pos.label,
                    "name": pos.name,
                    "asset_class": pos.asset_class,
                    "security_value": pos.value,
                    "security_ccy": pos.currency,
                    "isin": pos.isin,
                }
            )

    return build_normalized_table(
        records,
        xtb_snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=["security_value"],
    )


# XTB operation_type → normalized event_type mapping
_XTB_EVENT_TYPE_MAP: dict[str, str] = {
    "Deposit": "DEPOSIT",
    "Withdrawal": "WITHDRAWAL",
    "Fee": "FEE",
    "Interest": "INTEREST",
    "Dividend": "DIVIDEND",
    "Transfer": "TRANSFER",
    "Stock purchase": "TRADE",
    "Stock sale": "TRADE",
    "Open position": "TRADE",
    "Close position": "TRADE",
    "Profit/loss adjustment": "ADJUSTMENT",
    "Currency exchange": "TRANSFER",
    "Correction": "ADJUSTMENT",
}


def _classify_xtb_event_type(raw_type: str) -> str:
    """Map an XTB operation_type to a normalized event_type."""
    return _XTB_EVENT_TYPE_MAP.get(raw_type, "UNKNOWN")


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB CDC data into the broker-neutral CDC events schema.

    The raw payload is expected to contain .xlsx bytes (the original
    XTB report file). This function decrypts the payload, parses the
    .xlsx, and extracts cash operations.
    """
    records: list[dict] = []

    for row in iter_raw_payloads(raw, fernet_key, require_json=False):
        if "CASH OPERATION" not in row.source.upper():
            continue

        operations = load_cash_operations_from_bytes(row.payload_raw)

        for op in operations:
            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "broker": "XTB",
                    "account_id": op.account_id,
                    "event_id": op.operation_id,
                    "source": row.source,
                    "event_type": _classify_xtb_event_type(op.operation_type),
                    "raw_event_type": op.operation_type,
                    "event_datetime": op.operation_date,
                    "security_ccy": op.currency,
                    "cash_amount": op.amount,
                    "description": op.comment,
                    # XTB does not provide FX rates; target columns are
                    # populated by the normalize_currency step.
                    "target_fx_rate": None,
                    "target_value": None,
                    "target_ccy": None,
                    "instrument_ccy": None,
                }
            )

    return build_normalized_table(
        records,
        cdc_events_normalized_schema,
        fernet_key,
        encrypt_columns=["cash_amount", "target_fx_rate", "target_value"],
    )
