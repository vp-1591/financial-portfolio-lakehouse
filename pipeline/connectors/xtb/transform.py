"""XTB connector: transform raw snapshot and CDC data into normalized schema."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from pipeline.connectors.transform_utils import iter_raw_payloads
from pipeline.crypto import encrypt_float
from pipeline.normalized.models import xtb_cdc_normalized_schema, xtb_snapshot_normalized_schema


def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB snapshot data into the normalized schema."""
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

    for row in iter_raw_payloads(raw, fernet_key):
        if "OPEN POSITION" not in row.source.upper():
            continue

        positions = row.payload_parsed.get("positions", [])
        for pos in positions:
            if not isinstance(pos, dict):
                continue

            fetched_ats.append(row.fetched_at)
            account_ids.append(str(pos.get("account_id", row.account_id)))
            position_types.append(pos.get("asset_class", "EQUITY"))
            labels.append(str(pos.get("label", "")))
            names.append(str(pos.get("name", "")))
            asset_classes.append(pos.get("asset_class", "EQUITY"))
            currencies.append(str(pos.get("currency", "")))
            values.append(encrypt_float(float(pos.get("value", 0)), fernet_key))
            value_currencies.append(str(pos.get("currency", "")))
            isins.append(str(pos.get("isin", "")))

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
        },
        schema=xtb_snapshot_normalized_schema,
    )


def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table:
    """Transform raw XTB CDC data into the normalized CDC schema."""
    fetched_ats: list[datetime] = []
    account_ids: list[str] = []
    operation_ids: list[str] = []
    operation_types: list[str] = []
    amounts: list[bytes] = []
    currencies: list[str] = []
    comments: list[str] = []
    operation_dates: list[str] = []

    for row in iter_raw_payloads(raw, fernet_key):
        operations = row.payload_parsed
        if not isinstance(operations, list):
            continue

        for op in operations:
            if not isinstance(op, dict):
                continue

            fetched_ats.append(row.fetched_at)
            account_ids.append(str(op.get("account_id", row.account_id)))
            operation_ids.append(str(op.get("operation_id", "")))
            operation_types.append(str(op.get("operation_type", "")))
            amounts.append(encrypt_float(float(op.get("amount", 0)), fernet_key))
            currencies.append(str(op.get("currency", "")))
            comments.append(str(op.get("comment", "")))
            operation_dates.append(str(op.get("operation_date", "")))

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "account_id": account_ids,
            "operation_id": operation_ids,
            "operation_type": operation_types,
            "amount": amounts,
            "currency": currencies,
            "comment": comments,
            "operation_date": operation_dates,
        },
        schema=xtb_cdc_normalized_schema,
    )