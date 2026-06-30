from __future__ import annotations

from pathlib import Path
import sys
import uuid
import zipfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import xtb_net_worth as xtb  # noqa: E402


def cell(ref: str, value: object) -> str:
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'


def row(index: int, values: dict[str, object]) -> str:
    cells = "".join(cell(f"{column}{index}", value) for column, value in values.items())
    return f'<row r="{index}">{cells}</row>'


def sheet(rows: list[str]) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(rows)}</sheetData>"
        "</worksheet>"
    )


def write_xtb_workbook(path: Path, include_isin: bool = False) -> None:
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
    cash_sheet = sheet(
        [
            row(11, {"B": "ID", "C": "Type", "G": "Amount"}),
            row(12, {"B": 1, "C": "Deposit", "G": 200}),
        ]
    )

    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", open_sheet)
        archive.writestr("xl/worksheets/sheet2.xml", cash_sheet)


def temporary_report_path() -> Path:
    directory = ROOT / ".tmp-tests"
    directory.mkdir(exist_ok=True)
    return directory / f"xtb-test-{uuid.uuid4().hex}.xlsx"


def test_load_assets_reads_open_positions_and_cash_from_absolute_xlsx() -> None:
    report = temporary_report_path()
    write_xtb_workbook(report)

    try:
        assets, net_worth = xtb.load_assets(report.resolve())
    finally:
        report.unlink(missing_ok=True)

    assert net_worth == 220.0
    assert assets == [
        xtb.Asset("123456", "VWCE.DE", "VWCE.DE", "EQUITY", "PLN", 195.0),
        xtb.Asset("123456", "CASH PLN", "Cash PLN", "CASH", "PLN", 25.0),
    ]


def test_load_assets_can_override_account_id() -> None:
    report = temporary_report_path()
    write_xtb_workbook(report)

    try:
        assets, _net_worth = xtb.load_assets(
            report.resolve(),
            account_id_override="XTB-1",
        )
    finally:
        report.unlink(missing_ok=True)

    assert {asset.account_id for asset in assets} == {"XTB-1"}


def test_load_assets_preserves_isin_when_report_contains_column() -> None:
    report = temporary_report_path()
    write_xtb_workbook(report, include_isin=True)

    try:
        assets, _net_worth = xtb.load_assets(report.resolve())
    finally:
        report.unlink(missing_ok=True)

    assert assets[0].isin == "IE00BK5BQT80"


def test_relative_file_path_is_rejected() -> None:
    try:
        xtb.load_assets(Path("xtb.xlsx"))
    except xtb.XtbError as exc:
        assert "absolute path" in str(exc)
    else:
        raise AssertionError("relative paths should be rejected")


def test_print_assets_includes_asset_name_column(capsys) -> None:
    xtb.print_assets(
        [xtb.Asset("XTB-1", "VWCE.DE", "VWCE.DE", "EQUITY", "PLN", 175.0)],
        net_worth=200.0,
    )

    output = capsys.readouterr().out

    assert "Asset Name" in output
    assert "VWCE.DE" in output
    assert "87.50%" in output
