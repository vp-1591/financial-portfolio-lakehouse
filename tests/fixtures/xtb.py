"""XTB fixture builders for raw and normalized Delta tables.

Provides factory functions that return realistic ``pa.Table`` objects
matching the actual schemas used by the XTB connector.
"""

from __future__ import annotations

import hashlib
import zipfile
from datetime import datetime, timezone

import pyarrow as pa

from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.raw.models import RAW_SCHEMA
from pipeline.normalized.models import xtb_snapshot_normalized_schema


def _build_minimal_xlsx_bytes() -> bytes:
    """Build minimal valid .xlsx bytes for fixture use.

    Creates a workbook with OPEN POSITION and CASH OPERATION sheets
    containing enough data for the XTB parser to produce positions.
    """
    import io

    def _cell(ref: str, value: object) -> str:
        if isinstance(value, (int, float)):
            return f'<c r="{ref}"><v>{value}</v></c>'
        return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'

    def _row(index: int, values: dict[str, object]) -> str:
        cells = "".join(_cell(f"{col}{index}", val) for col, val in values.items())
        return f'<row r="{index}">{cells}</row>'

    def _sheet(rows: list[str]) -> str:
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"<sheetData>{''.join(rows)}</sheetData>"
            "</worksheet>"
        )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        '<sheet name="OPEN POSITION 15062026" sheetId="1" r:id="rId1"/>'
        '<sheet name="CASH OPERATION HISTORY" sheetId="2" r:id="rId2"/>'
        "</sheets>"
        "</workbook>"
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet2.xml"/>'
        "</Relationships>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/worksheets/sheet2.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )

    header_row = {
        "B": "Position",
        "C": "Symbol",
        "D": "Type",
        "E": "Volume",
        "I": "Purchase value",
        "P": "Gross P/L",
        "Q": "ISIN",
    }
    first_position = {
        "B": 1001,
        "C": "VWCE.DE",
        "D": "BUY",
        "E": 2,
        "I": 150,
        "P": 25,
        "Q": "IE00BK5BQT80",
    }
    second_position = {
        "B": 1002,
        "C": "CDR.PL",
        "D": "BUY",
        "E": 1,
        "I": 40,
        "P": -20,
        "Q": "PL9999900006",
    }

    open_sheet = _sheet(
        [
            _row(5, {"F": "Name and surname", "I": "Account", "L": "Currency"}),
            _row(6, {"F": "Anon User", "I": "XTB-12345", "L": "PLN"}),
            _row(7, {"F": "Balance", "I": "Equity"}),
            _row(8, {"F": 25, "I": 220}),
            _row(11, header_row),
            _row(12, first_position),
            _row(13, second_position),
            _row(14, {"B": "Total", "I": 190, "P": 5}),
        ]
    )

    cash_header = {
        "B": "ID",
        "C": "Type",
        "D": "Comment",
        "E": "Currency",
        "F": "Time",
        "G": "Amount",
    }
    cash_sheet = _sheet(
        [
            _row(11, cash_header),
            _row(
                12,
                {
                    "B": 1,
                    "C": "Deposit",
                    "D": "Initial deposit",
                    "E": "PLN",
                    "F": "2026-01-01",
                    "G": 200,
                },
            ),
        ]
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", open_sheet)
        archive.writestr("xl/worksheets/sheet2.xml", cash_sheet)

    return buf.getvalue()


# Cache the minimal xlsx bytes at module level
_MINIMAL_XLSX_BYTES: bytes | None = None


def _get_minimal_xlsx_bytes() -> bytes:
    """Return cached minimal .xlsx bytes, building on first call."""
    global _MINIMAL_XLSX_BYTES
    if _MINIMAL_XLSX_BYTES is None:
        _MINIMAL_XLSX_BYTES = _build_minimal_xlsx_bytes()
    return _MINIMAL_XLSX_BYTES


def xtb_raw_snapshot(
    fernet_key: bytes | None = None,
) -> pa.Table:
    """Build a raw XTB snapshot table with encrypted payloads.

    The payload is a minimal .xlsx file (binary), matching the
    fetch behavior that stores raw .xlsx bytes.
    """
    if fernet_key is None:
        fernet_key = generate_key()

    now = datetime.now(timezone.utc)
    payload = _get_minimal_xlsx_bytes()
    encrypted_payload = encrypt(payload, fernet_key)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["xtb"],
            "source": ["OPEN POSITION"],
            "payload": [encrypted_payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": ["report.xlsx"],
        },
        schema=RAW_SCHEMA,
    )


def xtb_normalized_snapshot(
    fernet_key: bytes | None = None,
    account_id: str = "XTB-12345",
) -> pa.Table:
    """Build a normalized XTB snapshot table with encrypted values.

    Default data: 2 equities (VWCE.DE, CDR.PL) + 1 cash entry (PLN).
    """
    if fernet_key is None:
        fernet_key = generate_key()
    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "fetched_at": [now, now, now],
            "account_id": [account_id, account_id, account_id],
            "position_type": ["EQUITY", "EQUITY", "CASH"],
            "label": ["VWCE.DE", "CDR.PL", "CASH:PLN"],
            "name": [
                "Vanguard FTSE All-World UCITS ETF",
                "CD Projekt",
                "Cash PLN",
            ],
            "asset_class": ["STK", "STK", "CASH"],
            "currency": ["EUR", "PLN", "PLN"],
            "value": [
                encrypt_float(1000.0, fernet_key),
                encrypt_float(2500.0, fernet_key),
                encrypt_float(5000.0, fernet_key),
            ],
            "value_currency": ["EUR", "PLN", "PLN"],
            "isin": ["IE00BK5BQT80", "PL9999900006", ""],
        },
        schema=xtb_snapshot_normalized_schema,
    )
