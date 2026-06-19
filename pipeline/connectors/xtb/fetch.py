"""XTB connector: fetch raw snapshot and CDC data from XLS files."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from pipeline.connectors.xtb.parser import (
    XtbCashOperation,
    XtbPosition,
    load_cash_operations_from_report,
    load_positions,
)
from pipeline.raw.models import RAW_SCHEMA


def fetch_snapshot(
    file_path: str | Path,
    account_id: str | None = None,
) -> pa.Table:
    """Fetch XTB positions and cash from an Excel report.

    Parameters
    ----------
    file_path:
        Absolute path to the XTB .xlsx report.
    account_id:
        Optional account ID override.
    """
    report_path = Path(file_path)
    positions, net_worth = load_positions(report_path, account_id_override=account_id)

    now = datetime.now(timezone.utc)
    filename = report_path.name

    # Serialize positions as JSON for the raw layer
    positions_data = []
    for pos in positions:
        positions_data.append({
            "account_id": pos.account_id,
            "label": pos.label,
            "name": pos.name,
            "asset_class": pos.asset_class,
            "currency": pos.currency,
            "value": pos.value,
            "isin": pos.isin,
        })

    payload = json.dumps({
        "positions": positions_data,
        "net_worth": net_worth,
    }).encode("utf-8")

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["OPEN POSITION"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "account_id": [positions[0].account_id if positions else (account_id or "XTB")],
            "source_file": [filename],
        },
        schema=RAW_SCHEMA,
    )


def fetch_cdc(
    file_path: str | Path,
    account_id: str | None = None,
) -> pa.Table:
    """Fetch XTB cash operations (CDC) from an Excel report.

    Parameters
    ----------
    file_path:
        Absolute path to the XTB .xlsx report.
    account_id:
        Optional account ID override.
    """
    report_path = Path(file_path)
    operations = load_cash_operations_from_report(report_path, account_id_override=account_id)

    now = datetime.now(timezone.utc)
    filename = report_path.name

    # Serialize operations as JSON for the raw layer
    ops_data = []
    for op in operations:
        ops_data.append({
            "operation_id": op.operation_id,
            "operation_type": op.operation_type,
            "amount": op.amount,
            "currency": op.currency,
            "comment": op.comment,
            "operation_date": op.operation_date,
        })

    payload = json.dumps(ops_data).encode("utf-8")

    if not operations:
        acct_id = account_id or "XTB"
    else:
        acct_id = operations[0].account_id

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["CASH OPERATION"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "account_id": [acct_id],
            "source_file": [filename],
        },
        schema=RAW_SCHEMA,
    )