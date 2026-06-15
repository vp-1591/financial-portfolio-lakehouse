#!/usr/bin/env python3
"""Print XTB report assets as percentages of account net worth."""

from __future__ import annotations

import argparse
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"m": MAIN_NS, "r": REL_NS, "pr": PACKAGE_REL_NS}


class XtbError(RuntimeError):
    pass


@dataclass(frozen=True)
class Asset:
    account_id: str
    label: str
    name: str
    asset_class: str
    currency: str
    value: float


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    normalized = str(value).strip().replace("\xa0", "").replace(" ", "")
    if "," in normalized and "." not in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return float(normalized)
    except (TypeError, ValueError):
        return default


def first_value(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def column_name(cell_reference: str) -> str:
    match = re.match(r"([A-Z]+)", cell_reference)
    return match.group(1) if match else ""


def normalize_header(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def read_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []

    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall("m:si", NS):
        strings.append("".join(text.text or "" for text in item.findall(".//m:t", NS)))
    return strings


def cell_value(cell: ET.Element, shared_strings: list[str]) -> Any:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.findall(".//m:t", NS))

    value = cell.find("m:v", NS)
    if value is None or value.text is None:
        return ""

    raw = value.text
    if cell_type == "s":
        index = int(raw)
        return shared_strings[index] if index < len(shared_strings) else ""
    if cell_type in {"str", "b"}:
        return raw

    try:
        number = float(raw)
    except ValueError:
        return raw
    return int(number) if number.is_integer() else number


def read_sheet_rows(workbook: zipfile.ZipFile, sheet_path: str) -> list[dict[str, Any]]:
    shared_strings = read_shared_strings(workbook)
    root = ET.fromstring(workbook.read(sheet_path))
    rows: list[dict[str, Any]] = []
    for row in root.findall(".//m:sheetData/m:row", NS):
        values: dict[str, Any] = {}
        for cell in row.findall("m:c", NS):
            column = column_name(cell.attrib.get("r", ""))
            if column:
                values[column] = cell_value(cell, shared_strings)
        rows.append(values)
    return rows


def sheet_paths_by_name(workbook: zipfile.ZipFile) -> dict[str, str]:
    workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
    rels_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))

    targets_by_id = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("pr:Relationship", NS)
    }

    paths: dict[str, str] = {}
    for sheet in workbook_root.findall(".//m:sheet", NS):
        name = sheet.attrib["name"]
        relationship_id = sheet.attrib[f"{{{REL_NS}}}id"]
        target = targets_by_id[relationship_id].lstrip("/")
        paths[name] = target if target.startswith("xl/") else f"xl/{target}"
    return paths


def find_sheet_name(sheet_names: list[str], expected_fragment: str) -> str:
    fragment = expected_fragment.lower()
    for name in sheet_names:
        if fragment in name.lower():
            return name
    raise XtbError(f"Could not find XTB sheet containing '{expected_fragment}'.")


def value_below_label(rows: list[dict[str, Any]], label: str) -> Any:
    wanted = normalize_header(label)
    for index, row in enumerate(rows[:-1]):
        for column, value in row.items():
            if normalize_header(value) == wanted:
                return rows[index + 1].get(column)
    return None


def header_map(row: dict[str, Any]) -> dict[str, str]:
    return {normalize_header(value): column for column, value in row.items() if value != ""}


def find_open_positions_header(rows: list[dict[str, Any]]) -> tuple[int, dict[str, str]]:
    for index, row in enumerate(rows):
        headers = header_map(row)
        if {"position", "symbol", "type", "volume"}.issubset(headers):
            return index, headers
    raise XtbError("Could not find open positions table in XTB report.")


def load_open_position_assets(
    rows: list[dict[str, Any]],
    account_id_value: str,
    currency: str,
) -> list[Asset]:
    header_index, headers = find_open_positions_header(rows)
    values_by_symbol: dict[tuple[str, str], float] = {}

    for row in rows[header_index + 1 :]:
        position = row.get(headers["position"])
        if normalize_header(position) == "total":
            break

        symbol = row.get(headers["symbol"])
        if symbol in (None, ""):
            continue

        purchase_value = as_float(row.get(headers.get("purchase value", "")))
        gross_profit_loss = as_float(row.get(headers.get("gross p/l", "")))
        current_value = purchase_value + gross_profit_loss
        if current_value == 0:
            continue

        key = (str(symbol), currency)
        values_by_symbol[key] = values_by_symbol.get(key, 0.0) + current_value

    return [
        Asset(
            account_id=account_id_value,
            label=symbol,
            name=symbol,
            asset_class="EQUITY",
            currency=symbol_currency,
            value=value,
        )
        for (symbol, symbol_currency), value in values_by_symbol.items()
        if value != 0
    ]


def cash_operations_total(rows: list[dict[str, Any]]) -> float:
    for index, row in enumerate(rows):
        headers = header_map(row)
        if {"id", "type", "amount"}.issubset(headers):
            amount_column = headers["amount"]
            total = 0.0
            for data_row in rows[index + 1 :]:
                if not any(value not in (None, "") for value in data_row.values()):
                    continue
                total += as_float(data_row.get(amount_column))
            return total
    return 0.0


def load_assets(report_path: Path, account_id_override: str | None = None) -> tuple[list[Asset], float]:
    if not report_path.is_absolute():
        raise XtbError("--file must be an absolute path to the XTB Excel report.")
    if not report_path.exists():
        raise XtbError(f"XTB report does not exist: {report_path}")

    try:
        workbook = zipfile.ZipFile(report_path)
    except zipfile.BadZipFile as exc:
        raise XtbError(f"XTB report is not a valid .xlsx file: {report_path}") from exc

    with workbook:
        paths = sheet_paths_by_name(workbook)
        sheet_names = list(paths)
        open_sheet = find_sheet_name(sheet_names, "OPEN POSITION")
        cash_sheet = find_sheet_name(sheet_names, "CASH OPERATION")
        open_rows = read_sheet_rows(workbook, paths[open_sheet])
        cash_rows = read_sheet_rows(workbook, paths[cash_sheet])

    account_id_value = str(
        account_id_override or value_below_label(open_rows, "Account") or "XTB"
    )
    currency = str(value_below_label(open_rows, "Currency") or "")

    assets = load_open_position_assets(open_rows, account_id_value, currency)

    balance = value_below_label(open_rows, "Balance")
    cash_balance = as_float(balance) if balance is not None else cash_operations_total(cash_rows)
    if cash_balance:
        assets.append(
            Asset(
                account_id=account_id_value,
                label=f"CASH {currency}".rstrip(),
                name=f"Cash {currency}".rstrip(),
                asset_class="CASH",
                currency=currency,
                value=cash_balance,
            )
        )

    equity = as_float(value_below_label(open_rows, "Equity"))
    net_worth = equity if equity else sum(asset.value for asset in assets)
    return assets, net_worth


def print_assets(assets: list[Asset], net_worth: float) -> None:
    if net_worth == 0:
        raise XtbError("Net worth is zero; cannot calculate percentages.")

    rows = sorted(assets, key=lambda asset: abs(asset.value), reverse=True)
    print(f"Net worth: {net_worth:,.2f}")
    print()
    print(
        f"{'Account':<14} "
        f"{'Asset':<18} "
        f"{'Asset Name':<34} "
        f"{'Class':<8} "
        f"{'Currency':<8} "
        f"{'Value':>16} "
        f"{'Net Worth %':>12}"
    )
    print("-" * 122)
    for asset in rows:
        percentage = asset.value / net_worth * 100
        print(
            f"{asset.account_id:<14} "
            f"{asset.label[:18]:<18} "
            f"{asset.name[:34]:<34} "
            f"{asset.asset_class[:8]:<8} "
            f"{asset.currency[:8]:<8} "
            f"{asset.value:>16,.2f} "
            f"{percentage:>11.2f}%"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print XTB positions and cash from an Excel report as net worth percentages."
    )
    parser.add_argument(
        "--file",
        required=True,
        type=Path,
        help="Absolute path to the XTB .xlsx report.",
    )
    parser.add_argument(
        "--account-id",
        help="Optional account id or label to display in the Account column.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        started = time.monotonic()
        assets, net_worth = load_assets(args.file, account_id_override=args.account_id)
        print_assets(assets, net_worth)
        print(f"\nFetched in {time.monotonic() - started:.1f}s")
        return 0
    except XtbError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
