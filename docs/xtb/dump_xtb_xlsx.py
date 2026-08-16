"""Dump every sheet of the anonymized XTB sample xlsx to a readable text form.

Uses only the stdlib zipfile + xml (same approach as pipeline/connectors/xtb/parser.py),
so no openpyxl/pandas needed. Writes tmp/xtb_sample_dump.txt.
"""
from __future__ import annotations

import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

XLSX = Path("docs/xtb/xtb-report-sample/PLN_12345678_2006-01-01_2026-08-03.xlsx")
OUT = Path("tmp/xtb_sample_dump.txt")


def col_letter_to_idx(letters: str) -> int:
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1


def cell_ref_to_col(ref: str) -> int:
    m = re.match(r"([A-Z]+)\d+", ref)
    return col_letter_to_idx(m.group(1)) if m else -1


def read_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    out: list[str] = []
    for si in root.findall(f"{NS}si"):
        # rich text or plain
        texts = [t.text or "" for t in si.iter(f"{NS}t")]
        out.append("".join(texts))
    return out


def read_sheet_rows(zf: zipfile.ZipFile, sheet_path: str, shared: list[str]) -> list[list[str]]:
    root = ET.fromstring(zf.read(sheet_path))
    rows: list[list[str]] = []
    for row in root.iter(f"{NS}row"):
        cells: dict[int, str] = {}
        max_col = -1
        for c in row.findall(f"{NS}c"):
            ref = c.get("r", "")
            col = cell_ref_to_col(ref)
            t = c.get("t")
            v = c.find(f"{NS}v")
            isn = c.find(f"{NS}is")
            if t == "s" and v is not None:
                val = shared[int(v.text)]
            elif t == "inlineStr" and isn is not None:
                val = "".join((tt.text or "") for tt in isn.iter(f"{NS}t"))
            elif v is not None:
                val = v.text or ""
            else:
                val = ""
            cells[col] = val
            if col > max_col:
                max_col = col
        row_vals = [cells.get(i, "") for i in range(max_col + 1)]
        rows.append(row_vals)
    return rows


def sheet_paths_by_name(zf: zipfile.ZipFile) -> dict[str, str]:
    # workbook.xml maps sheet name -> r:id; workbook.xml.rels maps r:id -> target
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    RNS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
    sheets: list[tuple[str, str]] = []
    for s in wb.iter(f"{NS}sheet"):
        sheets.append((s.get("name", ""), s.get(f"{RNS}id", "")))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map: dict[str, str] = {}
    for r in rels:
        rel_map[r.get("Id", "")] = r.get("Target", "")
    out: dict[str, str] = {}
    for name, rid in sheets:
        target = rel_map.get(rid, "")
        if target and not target.startswith("xl/"):
            target = "xl/" + target
        out[name] = target
    return out


def main() -> int:
    if not XLSX.exists():
        print(f"missing {XLSX}", file=sys.stderr)
        return 1
    OUT.parent.mkdir(parents=True, exist_ok=True)
    zf = zipfile.ZipFile(XLSX)
    shared = read_shared_strings(zf)
    by_name = sheet_paths_by_name(zf)
    lines: list[str] = []
    lines.append(f"# XTB sample dump: {XLSX.name}")
    lines.append(f"# sheets: {list(by_name.keys())}")
    lines.append("")
    for name, path in by_name.items():
        lines.append(f"===== SHEET: {name}  ({path}) =====")
        rows = read_sheet_rows(zf, path, shared)
        for r_i, row in enumerate(rows):
            if not any(c != "" for c in row):
                continue
            lines.append(f"R{r_i + 1:03d}: " + " | ".join(row))
        lines.append("")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {OUT} ({len(lines)} lines, {len(by_name)} sheets)")
    # also print to stdout for quick view
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())