"""XTB connector: transform raw snapshot and CDC data into normalized schema."""

from __future__ import annotations

import pyarrow as pa

from pipeline.connectors.transform_utils import (
    build_normalized_table,
    iter_raw_payloads,
)
from pipeline.normalized.models import (
    xtb_cdc_normalized_schema,
    xtb_snapshot_normalized_schema,
)


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB snapshot data into the normalized schema."""
    records: list[dict] = []

    for row in iter_raw_payloads(raw, fernet_key):
        if "OPEN POSITION" not in row.source.upper():
            continue

        positions = row.payload_parsed.get("positions", [])
        for pos in positions:
            if not isinstance(pos, dict):
                continue

            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "account_id": str(pos.get("account_id", row.account_id)),
                    "position_type": pos.get("asset_class", "EQUITY"),
                    "label": str(pos.get("label", "")),
                    "name": str(pos.get("name", "")),
                    "asset_class": pos.get("asset_class", "EQUITY"),
                    "currency": str(pos.get("currency", "")),
                    "value": float(pos.get("value", 0)),
                    "value_currency": str(pos.get("currency", "")),
                    "isin": str(pos.get("isin", "")),
                }
            )

    return build_normalized_table(
        records,
        xtb_snapshot_normalized_schema,
        fernet_key,
        encrypt_columns=["value"],
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB CDC data into the normalized CDC schema."""
    records: list[dict] = []

    for row in iter_raw_payloads(raw, fernet_key):
        operations = row.payload_parsed
        if not isinstance(operations, list):
            continue

        for op in operations:
            if not isinstance(op, dict):
                continue

            records.append(
                {
                    "fetched_at": row.fetched_at,
                    "account_id": str(op.get("account_id", row.account_id)),
                    "operation_id": str(op.get("operation_id", "")),
                    "operation_type": str(op.get("operation_type", "")),
                    "amount": float(op.get("amount", 0)),
                    "currency": str(op.get("currency", "")),
                    "comment": str(op.get("comment", "")),
                    "operation_date": str(op.get("operation_date", "")),
                }
            )

    return build_normalized_table(
        records,
        xtb_cdc_normalized_schema,
        fernet_key,
        encrypt_columns=["amount"],
    )
