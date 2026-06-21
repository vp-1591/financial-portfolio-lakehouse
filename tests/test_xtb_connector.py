"""Tests for the XTB pipeline connector."""

from __future__ import annotations

import hashlib
import json
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from pipeline.connectors.xtb.parser import (
    XtbError,
    XtbPosition,
    as_float,
    cell_value,
    column_name,
    find_cash_operations_header,
    find_open_positions_header,
    find_sheet_name,
    header_map,
    load_cash_operations,
    load_cash_operations_from_report,
    load_positions,
    normalize_header,
    read_shared_strings,
    row_isin,
    sheet_paths_by_name,
    value_below_label,
)
from pipeline.connectors.xtb.transform import transform_cdc, transform_snapshot
from pipeline.crypto import decrypt, decrypt_float, encrypt, generate_key


# --- XLS test helpers (preserved from tests/test_xtb_net_worth.py) ---

def cell(ref: str, value: object) -> str:
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    return (
        f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'
    )


def row(index: int, values: dict[str, object]) -> str:
    cells = "".join(cell(f"{column}{index}", value) for column, value in values.items())
    return f'<row r="{index}">{cells}</row>'


def sheet(rows: list[str]) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(rows)}</sheetData>'
        "</worksheet>"
    )


def write_xtb_workbook(path: Path, include_isin: bool = False, include_cash_ops: bool = False) -> None:
    """Create a minimal XLSX workbook for testing."""
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
    }
    first_position = {
        "B": 1001,
        "C": "VWCE.DE",
        "D": "BUY",
        "E": 2,
        "I": 150,
        "P": 25,
    }
    second_position = {
        "B": 1002,
        "C": "VWCE.DE",
        "D": "BUY",
        "E": 1,
        "I": 40,
        "P": -20,
    }
    if include_isin:
        header_row["Q"] = "ISIN"
        first_position["Q"] = "IE00BK5BQT80"
        second_position["Q"] = "IE00BK5BQT80"

    open_sheet = sheet(
        [
            row(5, {"F": "Name and surname", "I": "Account", "L": "Currency"}),
            row(6, {"F": "Anon User", "I": "123456", "L": "PLN"}),
            row(7, {"F": "Balance", "I": "Equity"}),
            row(8, {"F": 25, "I": 220}),
            row(11, header_row),
            row(12, first_position),
            row(13, second_position),
            row(14, {"B": "Total", "I": 190, "P": 5}),
        ]
    )

    cash_header = {"B": "ID", "C": "Type", "G": "Amount"}
    if include_cash_ops:
        cash_header["D"] = "Comment"
        cash_header["E"] = "Currency"
        cash_header["F"] = "Time"
        cash_rows = [
            row(11, cash_header),
            row(12, {"B": 1, "C": "Deposit", "D": "Initial deposit", "E": "PLN", "F": "2026-01-01", "G": 200}),
            row(13, {"B": 2, "C": "Dividend", "D": "VWCE dividend", "E": "EUR", "F": "2026-03-15", "G": 5}),
        ]
    else:
        cash_rows = [
            row(11, {"B": "ID", "C": "Type", "G": "Amount"}),
            row(12, {"B": 1, "C": "Deposit", "G": 200}),
        ]

    cash_sheet_xml = sheet(cash_rows)

    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", open_sheet)
        archive.writestr("xl/worksheets/sheet2.xml", cash_sheet_xml)


def temporary_report_path() -> Path:
    directory = Path(__file__).resolve().parents[1] / ".tmp-tests"
    directory.mkdir(exist_ok=True)
    return directory / f"xtb-test-{uuid.uuid4().hex}.xlsx"


class TestParserHelpers:
    """Tests preserved from tests/test_xtb_net_worth.py."""

    def test_as_float(self) -> None:
        assert as_float(None) == 0.0
        assert as_float("") == 0.0
        assert as_float(42) == 42.0
        assert as_float("3.14") == 3.14
        assert as_float("1,5") == 1.5
        assert as_float("abc", -1.0) == -1.0

    def test_column_name(self) -> None:
        assert column_name("A1") == "A"
        assert column_name("AB12") == "AB"
        assert column_name("") == ""

    def test_normalize_header(self) -> None:
        assert normalize_header("  Purchase  value  ") == "purchase value"
        assert normalize_header(None) == ""
        assert normalize_header("Gross P/L") == "gross p/l"

    def test_relative_file_path_is_rejected(self) -> None:
        try:
            load_positions(Path("xtb.xlsx"))
        except XtbError as exc:
            assert "absolute path" in str(exc)
        else:
            raise AssertionError("relative paths should be rejected")

    def test_load_assets_reads_open_positions_and_cash(self) -> None:
        report = temporary_report_path()
        write_xtb_workbook(report)
        try:
            assets, net_worth = load_positions(report.resolve())
        finally:
            report.unlink(missing_ok=True)

        assert net_worth == 220.0
        assert assets == [
            XtbPosition("123456", "VWCE.DE", "VWCE.DE", "EQUITY", "PLN", 195.0),
            XtbPosition("123456", "CASH PLN", "Cash PLN", "CASH", "PLN", 25.0),
        ]

    def test_load_assets_can_override_account_id(self) -> None:
        report = temporary_report_path()
        write_xtb_workbook(report)
        try:
            assets, _net_worth = load_positions(
                report.resolve(),
                account_id_override="XTB-1",
            )
        finally:
            report.unlink(missing_ok=True)

        assert {asset.account_id for asset in assets} == {"XTB-1"}

    def test_load_assets_preserves_isin(self) -> None:
        report = temporary_report_path()
        write_xtb_workbook(report, include_isin=True)
        try:
            assets, _net_worth = load_positions(report.resolve())
        finally:
            report.unlink(missing_ok=True)

        assert assets[0].isin == "IE00BK5BQT80"

    def test_load_cash_operations(self) -> None:
        report = temporary_report_path()
        write_xtb_workbook(report, include_cash_ops=True)
        try:
            ops = load_cash_operations_from_report(report.resolve())
        finally:
            report.unlink(missing_ok=True)

        assert len(ops) == 2
        assert ops[0].operation_type == "Deposit"
        assert ops[0].amount == pytest.approx(200.0)
        assert ops[0].currency == "PLN"
        assert ops[1].operation_type == "Dividend"
        assert ops[1].amount == pytest.approx(5.0)


class TestTransformSnapshot:
    """Tests for the raw → normalized transform."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        key = generate_key()
        self._fernet_key = key
        return key

    def _build_raw_table(self, positions_data: dict, account_id: str = "123456") -> pa.Table:
        """Build a raw-layer table from parsed XTB data.

        Payloads are encrypted to match the real pipeline flow where
        raw Delta tables store encrypted payloads.
        """
        key = self._fernet_key
        raw_payload = json.dumps(positions_data).encode("utf-8")
        encrypted_payload = encrypt(raw_payload, key)
        now = datetime.now(timezone.utc)

        return pa.table(
            {
                "fetched_at": [now],
                "broker": ["XTB"],
                "source": ["OPEN POSITION"],
                "payload": [encrypted_payload],
                "payload_hash": [hashlib.sha256(raw_payload).hexdigest()],
                "account_id": [account_id],
                "source_file": ["test_report.xlsx"],
            },
            schema=pa.schema([
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("broker", pa.string()),
                pa.field("source", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("payload_hash", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("source_file", pa.string()),
            ]),
        )

    def test_transform_produces_position_rows(self, fernet_key: bytes) -> None:
        data = {
            "positions": [
                {"label": "VWCE.DE", "name": "VWCE.DE", "asset_class": "EQUITY",
                 "currency": "PLN", "value": 195.0, "isin": "IE00BK5BQT80"},
                {"label": "CASH PLN", "name": "Cash PLN", "asset_class": "CASH",
                 "currency": "PLN", "value": 25.0, "isin": ""},
            ],
            "net_worth": 220.0,
        }
        raw = self._build_raw_table(data)
        result = transform_snapshot(raw, fernet_key)

        assert result.num_rows == 2
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

        values = result.column("value").to_pylist()
        decrypted = [decrypt_float(v, fernet_key) for v in values]
        assert any(v == pytest.approx(195.0) for v in decrypted)
        assert any(v == pytest.approx(25.0) for v in decrypted)

    def test_transform_preserves_isin(self, fernet_key: bytes) -> None:
        data = {
            "positions": [
                {"label": "VWCE.DE", "name": "VWCE.DE", "asset_class": "EQUITY",
                 "currency": "PLN", "value": 195.0, "isin": "IE00BK5BQT80"},
            ],
            "net_worth": 195.0,
        }
        raw = self._build_raw_table(data)
        result = transform_snapshot(raw, fernet_key)

        isins = result.column("isin").to_pylist()
        assert "IE00BK5BQT80" in isins


class TestTransformCDC:
    """Tests for the raw → normalized CDC transform."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        return generate_key()

    def test_transform_cdc_produces_operation_rows(self, fernet_key: bytes) -> None:
        now = datetime.now(timezone.utc)
        ops_data = [
            {"operation_id": "1", "operation_type": "Deposit", "amount": 200.0,
             "currency": "PLN", "comment": "Initial deposit", "operation_date": "2026-01-01"},
            {"operation_id": "2", "operation_type": "Dividend", "amount": 5.0,
             "currency": "EUR", "comment": "VWCE dividend", "operation_date": "2026-03-15"},
        ]
        raw_payload = json.dumps(ops_data).encode("utf-8")
        encrypted_payload = encrypt(raw_payload, fernet_key)

        raw = pa.table(
            {
                "fetched_at": [now],
                "broker": ["XTB"],
                "source": ["CASH OPERATION"],
                "payload": [encrypted_payload],
                "payload_hash": [hashlib.sha256(raw_payload).hexdigest()],
                "account_id": ["123456"],
                "source_file": ["test_report.xlsx"],
            },
            schema=pa.schema([
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("broker", pa.string()),
                pa.field("source", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("payload_hash", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("source_file", pa.string()),
            ]),
        )

        result = transform_cdc(raw, fernet_key)
        assert result.num_rows == 2

        op_types = result.column("operation_type").to_pylist()
        assert "Deposit" in op_types
        assert "Dividend" in op_types

        amounts = result.column("amount").to_pylist()
        decrypted = [decrypt_float(v, fernet_key) for v in amounts]
        assert any(v == pytest.approx(200.0) for v in decrypted)
        assert any(v == pytest.approx(5.0) for v in decrypted)