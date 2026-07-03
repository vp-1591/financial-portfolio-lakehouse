"""XTB connector: transform raw snapshot and CDC data into normalized schema."""

from __future__ import annotations

import pyarrow as pa

from pipeline.connectors.transform_utils import (
    build_normalized_table,
    iter_raw_payloads,
)
from pipeline.connectors.xtb.parser import (
    load_cash_operations_from_bytes,
    load_positions_from_bytes,
)
from pipeline.normalized.models import (
    xtb_cdc_normalized_schema,
    xtb_snapshot_normalized_schema,
)


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB snapshot data into the normalized schema.

    The raw payload is expected to contain .xlsx bytes (the original
    XTB report file). This function decrypts the payload, parses the
    .xlsx, and extracts positions and cash balances.
    """
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
                    "currency": pos.currency,
                    "value": pos.value,
                    "value_currency": pos.currency,
                    "isin": pos.isin,
                }
            )

    return build_normalized_table(
        records,
        xtb_snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=["value"],
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB CDC data into the normalized CDC schema.

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
                    "account_id": op.account_id,
                    "operation_id": op.operation_id,
                    "operation_type": op.operation_type,
                    "amount": op.amount,
                    "currency": op.currency,
                    "comment": op.comment,
                    "operation_date": op.operation_date,
                }
            )

    return build_normalized_table(
        records,
        xtb_cdc_normalized_schema,
        fernet_key,
        encrypt_columns=["amount"],
    )
