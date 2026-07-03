# 0047: Move XLSX Parsing to Silver Layer and Remove account_id from Raw Schema

## Context

The XTB connector's fetch layer parses `.xlsx` files into structured JSON before storing in the raw/bronze layer. This violates the medallion architecture principle (ADR 0003) that the raw layer should store original, unmodified source data. Parsing in the fetch layer also means `account_id` — which is extracted from the XLSX during parsing — gets written to the raw table, making it the only broker to populate that column. IBKR and Trading 212 both leave `account_id` empty in the raw layer.

After this change, the XTB raw layer stores the original `.xlsx` bytes, and all account ID extraction happens in the transform (silver) layer where it belongs. Since no broker populates `account_id` at the raw layer anymore, the column is removed from `RAW_SCHEMA`.

## Decision

1. **Move XLSX parsing from XTB `fetch.py` to `transform.py`.** The fetch layer now stores the raw `.xlsx` bytes as the payload. The transform layer decrypts the payload and parses it using `load_positions_from_bytes()` and `load_cash_operations_from_bytes()` — new functions added to `parser.py` that accept `bytes` instead of `Path`.

2. **Remove `account_id` from `RAW_SCHEMA`.** The `pa.field("account_id", pa.string())` column is removed from all 6 raw tables. Account ID is derived during transformation (from XLSX for XTB, from XML for IBKR, hardcoded `""` for T212) and belongs in the normalized (silver) layer, not the raw (bronze) layer.

3. **Remove `account_id` from `DecodedRow` and `iter_raw_payloads()`.** The `account_id` field is removed from the `DecodedRow` dataclass and the `iter_raw_payloads()` function no longer reads the `account_id` column from raw tables.

4. **Simplify Trading 212 `transform.py`.** The `by_account` grouping (which grouped by `row.account_id`, always `""`) is replaced with flat iteration. Normalized records set `account_id: ""` since T212's API doesn't provide one.

5. **Fix `parse_json()` to handle binary payloads.** Added `UnicodeDecodeError` to the caught exceptions in `parse_json()`, since `.xlsx` bytes fail UTF-8 decoding before reaching JSON parsing.

## Consequences

- Raw layer now stores original broker data unmodified (true medallion architecture)
- `account_id` is exclusively a silver/gold-layer concept, derived from parsed data
- XTB transform now uses `require_json=False` in `iter_raw_payloads()` to handle binary `.xlsx` payloads
- XTB `fetch.py` is dramatically simpler — just reads file bytes and builds a raw table
- Existing raw Delta tables with 7 columns (including `account_id`) require migration before new code can write to them
- `build_raw_table()` in `ingest.py` now accepts `list[bytes]` instead of `list[tuple[bytes, str]]`

## Validation

- All 339 tests pass, including:
  - XTB parser tests (unchanged — they test `parser.py` directly)
  - XTB transform tests (now use `.xlsx` bytes instead of JSON)
  - Integration tests (XTB transform decrypts and parses `.xlsx` payloads)
  - T212 and IBKR transform tests (work with 6-column raw schema)
  - `iter_raw_payloads` tests (work without `account_id` column)
- `ruff check --fix .` and `ruff format .` produce no errors