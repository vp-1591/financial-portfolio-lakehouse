# XTB Connector Code Audit — New Excel Report Format

Audit of all XTB connector code paths against the new XTB Excel report format
(sample dump: [xtb_sample_dump.txt](xtb_sample_dump.txt)). No production code
changed; this is the audit artifact only.

**Companion docs:** implementation decisions & stages →
[xtb_overhaul_plan.md](xtb_overhaul_plan.md); fixture verification & corrections →
[xtb_data_verification.md](xtb_data_verification.md).

---

## 1. Current-State Map

### `pipeline/connectors/xtb/parser.py` — the XLSX parser (core)

Stdlib `zipfile` + `xml.etree.ElementTree`; no openpyxl. Helpers:
`read_shared_strings` (L84), `read_sheet_rows` (L117), `sheet_paths_by_name`
(L131), `find_sheet_name` (L149, case-insensitive substring match),
`value_below_label` (L157, finds a cell matching a label then reads the cell
directly below it), `header_map` (L166), `normalize_header` (L80, lowercases +
collapses whitespace).

Dataclasses:
- `XtbPosition` (L27-38): `account_id, label, name, asset_class, currency, value, isin`
- `XtbCashOperation` (L40-51): `account_id, operation_id, operation_type, amount, currency, comment, operation_date`

Sheet discovery — `_load_positions_from_workbook` (L306-346):
- `find_sheet_name(sheet_names, "OPEN POSITION")` (L316)
- `find_sheet_name(sheet_names, "CASH OPERATION")` (L317)
- Account ID: `value_below_label(open_rows, "Account")` (L322)
- Currency: `value_below_label(open_rows, "Currency")` (L324)
- Balance: `value_below_label(open_rows, "Balance")` (L328)
- Equity: `value_below_label(open_rows, "Equity")` (L344)

Open Positions parsing — `find_open_positions_header` (L172-179) +
`load_open_position_assets` (L190-232):
- Header detection requires the set `{"position", "symbol", "type", "volume"}`
  to be a subset of normalized headers (L177).
- Columns read: `headers["position"]`, `headers["symbol"]`,
  `headers.get("purchase value", "")`, `headers.get("gross p/l", "")`.
- ISIN via `row_isin` (L182-188) trying `"isin"`, `"isin code"`,
  `"instrument isin"`.
- Value model: `current_value = purchase_value + gross_profit_loss` (L210);
  skips rows where `current_value == 0` (L211).
- Aggregation: sums `current_value` per `(symbol, currency)` (L214-215).
- Stops at a row whose `position` cell normalizes to `"total"` (L201).

Cash Operations parsing — `find_cash_operations_header` (L235-246) +
`load_cash_operations` (L249-289):
- Header detection requires `{"id", "type", "amount"}` (L244).
- Columns read: `id`, `type`, `amount`, `comment` (fallback `details`),
  `currency`, `time` (fallback `date`).
- Includes a row if `op_id or op_type` is truthy (L276) — **no "Total" row
  exclusion**.
- `cash_operations_total` (L292-303) sums the amount column for all rows
  after the header, including the Total row.

No "CLOSED POSITION" sheet is referenced anywhere in the parser.

### `pipeline/connectors/xtb/transform.py` — silver transform

`transform_snapshot` (L22-62):
- `filter_latest_snapshot(raw)` (L29).
- Iterates `iter_raw_payloads(..., require_json=False)`, keeps rows where
  `"OPEN POSITION" in row.source.upper()` (L33).
- Calls `load_positions_from_bytes(row.payload_raw)` (L36).
- Maps each `XtbPosition` to `snapshot_normalized_schema` fields:
  `position_type=pos.asset_class`, `label=pos.label`,
  `description=pos.name`, `asset_class=pos.asset_class`,
  `security_value=pos.value`, `security_ccy=pos.currency` (account currency;
  ADR 0102 deferred XTB instrument-ccy), `isin=pos.isin`.
- `encrypt_columns=["security_value"]` (L61).

`transform_cdc` (L88-131):
- Keeps rows where `"CASH OPERATION" in row.source.upper()` (L98).
- Calls `load_cash_operations_from_bytes(row.payload_raw)` (L101).
- Event-type map `_XTB_EVENT_TYPE_MAP` (L66-80): Deposit→DEPOSIT,
  Withdrawal→WITHDRAWAL, Fee→FEE, Interest→INTEREST, Dividend→DIVIDEND,
  Transfer→TRANSFER, "Stock purchase"→TRADE, "Stock sale"→TRADE,
  "Open position"→TRADE, "Close position"→TRADE,
  "Profit/loss adjustment"→ADJUSTMENT, "Currency exchange"→TRANSFER,
  Correction→ADJUSTMENT. `_classify_xtb_event_type` (L83-85) → "UNKNOWN"
  on miss.
- Record fields set (L104-124): `fetched_at, broker="XTB", account_id,
  event_id=op.operation_id, source, event_type, raw_event_type,
  event_datetime=op.operation_date, security_ccy=op.currency,
  cash_amount=op.amount, description=op.comment`,
  `target_fx_rate/target_value/target_ccy/instrument_ccy = None`.
- **Does not populate**: `settle_date, ticker, isin, quantity, price, side,
  gross_amount, fee_amount, tax_amount` (all left null by
  `build_normalized_table`).
- **No transform-level dedup** (no `dedup_cdc_events` call).
- `encrypt_columns=["cash_amount", "target_fx_rate", "target_value"]` (L130).

### `pipeline/connectors/xtb/connector.py` — protocol integration

`XtbConnector` (L19-71): `name="xtb"`, `cdc_supported=False` (L24).
`fetch_kwargs` reads `args.xtb_file` (L25-33). `extract_holdings` (L42-59)
maps normalized rows → `Holding(broker="XTB", ticker=row["label"],
currency=row["security_ccy"], value=row["security_value_decrypted"],
identifier=ISIN:..., security_currency, description, position_type)`.

### `pipeline/connectors/xtb/fetch.py` — raw ingest

`fetch_snapshot` (L59-83): reads file bytes, emits one row with
`source="OPEN POSITION"`, `broker="XTB"`, `payload=<xlsx bytes>`,
`payload_hash`, `source_file`. `fetch_cdc` (L86-110): same with
`source="CASH OPERATION"`. `_read_file_bytes` (L15-56) handles local + S3.

### `pipeline/connectors/xtb/__init__.py`

Registers `XtbConnector` on import (L3).

### Tests & fixtures — `tests/test_xtb_connector.py`, `tests/fixtures/xtb.py`

Both encode the OLD format. Fixture workbook sheet names:
`"OPEN POSITION 15062026"`, `"CASH OPERATION HISTORY"`. Open Positions
header row (test L93-100): `Position, Symbol, Type, Volume, Purchase value,
Gross P/L` (+ optional `ISIN`). Label rows (test L124-127): `Account,
Currency, Balance, Equity`. Cash header (test L135-139): `ID, Type, Comment,
Currency, Time, Amount`. Dates are ISO strings (`"2026-01-01"`).

### Docs — `docs/brokers/xtb.md`

States XTB has no API; data from Excel exports via `--xtb-file`. Warns
(L9-12): "not been tested with real account data ... only sample data has
been verified (as of 2026-07-16)".

### ADRs read

- 0047 — Move XLSX parsing to silver; raw stores `.xlsx` bytes.
- 0048 — XTB cloud upload (S3 + EventBridge).
- 0102 — Standardize snapshot schemas; XTB is a no-op semantically (keeps
  account-currency `security_ccy`, deferred instrument-ccy).
- 0100 — Per-source snapshot dedup; XTB single-fetch unaffected.
- 0104 — T212 trade sign convention (positive=inflow, negative=outflow);
  XTB not addressed.
- 0105 — T212 CDC dedup + consolidate boundary dedup; XTB explicitly
  "optional and file-based", assumed "truly incremental" (wrong for re-uploads).

---

## 2. Old Format vs New Format Diff

Sheet names: new dump = `['Closed Positions', 'Cash Operations', 'Open Positions']`.
`find_sheet_name` substring match still resolves "OPEN POSITION"→"Open Positions"
and "CASH OPERATION"→"Cash Operations", so sheet discovery survives. "Closed
Positions" is entirely new and unmatched.

### Open Positions

| Aspect | Old (parser + fixtures) | New (dump) |
|---|---|---|
| Detail header | `Position, Symbol, Type, Volume, Purchase value, Gross P/L` (+ optional `ISIN`) | `Product, Instrument/Position, Ticker, Category, Type, Volume, Value, Current price, Open price, Open time (UTC), Stop Loss, Take Profit, Net Profit %, Net Profit, Gross Profit, Margin, Open Commission, Swap, Rollover` |
| Header keys matched | `position`, `symbol`, `type`, `volume` | `instrument/position` (not `position`), `ticker` (not `symbol`), `type`, `volume` — **detection fails** |
| Value model | `current_value = purchase_value + gross_p/l` (computed) | `Value` column is the market value directly; `Net Profit`/`Gross Profit` separate |
| ISIN | `isin`/`isin code`/`instrument isin` columns | **No ISIN column at all** |
| Row structure | Flat list of positions, one row per position | **Aggregate + child rows**: aggregate row (real name e.g. "Core S&P 500", no Type/Volume/Open time) + child rows (numeric ID e.g. "1334567890", with Type/Volume/price). Grouped by `Product`. |
| Summary block | None | Preceding block: `Product, Metric, Amount, Currency` (per-product Value & Profit), then a `Note` row, before the detail header |
| Total row | `Total` row (parser breaks on it) | No `Total` row in detail section |
| Labels above table | `Account`, `Currency`, `Balance`, `Equity` (value-below-label) | `Account number | 12345678`; `Data as of report generated | 46237.25`. **No `Account`/`Currency`/`Balance`/`Equity` labels.** |
| Dates | ISO strings in tests | Excel serials (e.g. `46223.424`) in `Open time (UTC)` |

### Cash Operations

| Aspect | Old | New |
|---|---|---|
| Header | `ID, Type, Comment, Currency, Time, Amount` | `Type, Instrument, Ticker, Category, Time, Amount, ID, Comment, Product, Position ID` |
| Header keys matched | `id`, `type`, `amount` | `id`, `type`, `amount` still present — **detection survives** |
| Currency | `Currency` column present | **No `Currency` column** — values are in PLN (account ccy) |
| Date | `Time` as ISO string (`"2026-01-01"`) | `Time` as Excel serial (`46236.875`) |
| Types seen | Deposit, Dividend (test) | `Free funds interest`, `Free funds interest tax`, `Transfer` (currency conversion; "Exchange rate:X" in comment), `Stock sell`, `Stock purchase`, `Subaccount transfer`, `Deposit` |
| Total row | None in test | `Total` row present (ID empty, Amount populated) — **included as a bogus operation** by current logic |
| Trade enrichment | n/a | `Instrument`, `Ticker`, `Position ID` columns link to Closed/Open Positions |

### Closed Positions (NEW sheet — no existing code)

Header: `Instrument, Ticker, Category, Type, Volume, Open Price, Open Time (UTC),
Close Price, Close Time (UTC), Product, Profit/Loss, Gross Profit, Purchase
Value, Sale Value, Stop Loss, Take Profit, Commission, Margin, Swap, Rollover,
Open Conversion Rate, Close Conversion Rate, Close Origin, Position ID,
Comment`. Dates are Excel serials. Values (Purchase/Sale/Profit) appear
pre-converted to PLN via the Open/Close Conversion Rate. One total row at the
bottom.

### Date format (cross-cutting)

Old parser/tests assume ISO date strings. `cell_value` (parser L94-114)
returns numeric cells as `int`/`float`, so new Excel-serial dates arrive as
floats (e.g. `46204.29`). `load_cash_operations` stringifies via `str(...)`
(L274) → `"46204.29"`. The analytics period parser
(`pipeline/analytics/cdc_tables.py` L122-146) has no Excel-serial format
branch → all XTB CDC rows are dropped from dividend/interest/cash-flow
aggregation.

### FX / value currency

Old parser does no FX; values are assumed account-currency. New format:
Closed Positions values are pre-converted to PLN via Open/Close Conversion
Rate; Cash Operations amounts are in PLN; Open Positions `Value` is in PLN.
The conversion-rate columns are new and currently unused.

---

## 3. Bug & Gap Inventory

| # | Severity | File:line | Issue |
|---|---|---|---|
| 1 | **Critical** | parser.py:177 | `find_open_positions_header` requires `{"position","symbol","type","volume"}`. New header has `instrument/position` (not `position`) and `ticker` (not `symbol`). Detection raises `XtbError("Could not find open positions table...")`. Snapshot transform produces zero rows. |
| 2 | **Critical** | parser.py:273, transform.py:114 | Cash Operations has no `Currency` column. `op.currency` = `""` for every row. `security_ccy=""` flows into `normalize_currency` (normalize.py L129): `security_ccy == target_ccy` false → `converter.convert(1.0, "")` → FX fetch error → `target_value` null. Cash-flow analytics get no target_value for XTB. |
| 3 | **Critical** | parser.py:274, analytics/cdc_tables.py:122-146 | Excel-serial dates. `operation_date` becomes `"46236.875"`. `_add_period_columns` strptime chain has no Excel-serial format → `_event_dt` null → row filtered out (cdc_tables.py L160). **All XTB CDC rows dropped from gold aggregation.** Same affects Open Positions `Open time (UTC)` if ever used. |
| 4 | **Critical** | parser.py:190-232 | Open Positions aggregate+child structure. The parser has no concept of grouping by `Product` or distinguishing aggregate rows (real name, no Type/Volume) from child rows (numeric ID, with Type/Volume). It would either treat aggregate rows as positions (wrong value semantics — aggregate `Value` is the product total) or treat child rows separately and double-count against the aggregate. The preceding summary block (`Product, Metric, Amount, Currency`) would also be misread as position rows. |
| 5 | **Critical** | parser.py:322-345 | Account/Currency/Balance/Equity label lookup. New format has `Account number` (not `Account`), no `Currency`, no `Balance`, no `Equity`. `value_below_label(open_rows, "Account")` returns None → account_id falls back to `"XTB"`. `currency` = `""`. `Balance`/`Equity` = None → net_worth falls back to sum of assets; CASH position gets label `"CASH "` (trailing space, no currency). |
| 6 | **High** | transform.py:66-80 | Event-type map misses new Cash Operations types. `"Free funds interest"` → UNKNOWN (should be INTEREST). `"Free funds interest tax"` → UNKNOWN (should be TAX). `"Subaccount transfer"` → UNKNOWN (should be TRANSFER). `"Stock sell"` → UNKNOWN (map has `"Stock sale"`, not `"Stock sell"`). `"Transfer"` with currency conversion → TRANSFER but the embedded `Exchange rate:X` is not parsed into `target_fx_rate`. |
| 7 | **High** | transform.py:104-124 | No trade columns populated. New Cash Operations trade rows carry `Instrument, Ticker, Position ID`; Closed Positions carry `Volume, Open Price, Close Price, Commission, Swap, Open/Close Conversion Rate`. The transform emits none of `ticker, isin, quantity, price, side, gross_amount, fee_amount, tax_amount, settle_date`. Dividend-income and cash-flow analytics group by `ticker`/`isin` — XTB trades contribute nulls. |
| 8 | **High** | transform.py:88-131 (no dedup) | No transform-level CDC dedup. ADR 0105 added `dedup_cdc_events` to T212 and the consolidate boundary. XTB has neither at the transform. XTB re-uploads the same `.xlsx` (full history) → raw append duplicates → duplicate CDC events. The consolidate boundary dedup (consolidate_cdc.py:92) catches cross-broker dups on `(broker, event_type, event_id)`, but XTB's `event_id` is the cash-op `ID` (e.g. `900041122`) which is stable, so the boundary dedup *does* save XTB — but ADR 0105's stated assumption ("XTB truly incremental") is wrong and relying solely on the boundary is fragile. |
| 9 | **High** | parser.py:201 | `load_open_position_assets` breaks at `normalize_header(position) == "total"`. New Open Positions has no `Total` row in the detail section; the parser runs past the detail rows into whatever follows (the sheet ends, so it may just stop, but the aggregate/child rows above have no Total guard — the summary-block rows would be consumed first). |
| 10 | **High** | parser.py:276, 301 | Cash Operations `Total` row included as a bogus operation. `load_cash_operations` includes any row where `op_id or op_type` is truthy; the `Total` row has `op_type="Total"` → appended as an operation with `amount=9821.02`, `event_type=UNKNOWN`. `cash_operations_total` also double-counts by summing including the Total row. |
| 11 | **Medium** | parser.py:208-210 | Value computation uses `purchase_value + gross_p/l`. New format has a direct `Value` column and separate `Net Profit`/`Gross Profit`; the old column names don't exist, so even if header detection were fixed, `as_float(None)` → 0.0 for both → every position skipped (`current_value == 0`). |
| 12 | **Medium** | transform.py:52, ADR 0102 | Snapshot `security_ccy` = account currency (PLN) for all positions. New format still has no per-position instrument currency in Open Positions, but Closed Positions' `Open Conversion Rate`/`Close Conversion Rate` could derive it. Known limitation now partially solvable. |
| 13 | **Medium** | connector.py:24 | `cdc_supported = False` despite Cash Operations being a real CDC feed. This is a design choice (file-based), but with the new Closed Positions sheet there's a stronger case for trade-event CDC. |
| 14 | **Low** | parser.py:329-342 | CASH position label `"CASH {currency}".rstrip()` with `currency=""` → `"CASH"`. Downstream `Holding.ticker="CASH"` may collide with other brokers' cash labels or confuse currency-exposure grouping. |
| 15 | **Low** | transform.py:98 | Source filter `"CASH OPERATION" in row.source.upper()`. fetch.py emits `"CASH OPERATION"` (singular). New sheet name is `"Cash Operations"` (plural) but that's the in-workbook name, not the `source` column — `source` is set by `fetch_cdc`, so this filter survives. No bug, but worth noting the indirection. |

### Likely-rotten code from 30+ commits of neglect

- **ADR 0105 dedup gap**: XTB transform never got the `dedup_cdc_events`
  treatment IBKR (0069) and T212 (0105) received. The boundary dedup masks
  it, but the transform contract is inconsistent across brokers.
- **ADR 0104 sign convention**: XTB cash-op amounts are stored as-is (no
  direction sign applied). `Stock purchase` (outflow) has `Amount=-4300.04`
  (already negative in the dump) and `Stock sell` (inflow) has
  `Amount=5040.05` (positive) — so XTB's raw signs happen to match the
  convention, but this is accidental, not enforced. Deposits are positive,
  subaccount transfers are signed in the dump. Worth verifying explicitly
  in the rewrite.
- **ADR 0102 instrument-ccy**: XTB still emits account-currency
  `security_ccy`; the new Conversion Rate columns offer a path to fix this
  but no code uses them.

---

## 4. Downstream Impact

### Snapshot path (Open Positions → consolidated_holdings → portfolio_holdings)

- `transform_snapshot` → `xtb_snapshot` (normalized). Required fields
  (quality.py L131-136): `fetched_at, account_id, security_ccy,
  security_value`. With bug #1 the transform produces **zero rows**; with
  bugs #5/#11 account_id=`"XTB"` and `security_ccy=""`.
- `extract_holdings` (extract.py) → `XtbConnector.extract_holdings`
  (connector.py L42-59) reads `label, security_value_decrypted,
  security_ccy, isin, position_type, description`. With no snapshot rows,
  XTB contributes nothing to `consolidated_holdings`.
- `consolidate_holdings` (consolidate.py) converts via `CurrencyConverter`.
  `security_ccy=""` → FX error → `PortfolioConnectorError`.
- `portfolio_holdings` (gold) — XTB absent from allocation/positions/currency
  charts.

### CDC path (Cash Operations → cdc_events → gold analytics)

- `transform_cdc` → `xtb_cdc` (normalized). Required fields (quality.py
  L151-157): `fetched_at, broker, event_id, event_type, cash_amount`. Schema
  check passes structurally; null check passes (`""` is non-null).
- `consolidate_cdc_events` (consolidate_cdc.py) — XTB optional; concatenated
  into `cdc_events`. Boundary dedup on `(broker, event_type, event_id)`.
- `normalize_currency` (normalize.py) — `security_ccy=""` → target_value
  null for every XTB row.
- `build_dividend_income` (cdc_tables.py) — filters `event_type=="DIVIDEND"`.
  XTB produces no DIVIDEND events (map has Dividend but new dump has none);
  OK.
- `build_interest_income` — filters `event_type=="INTEREST"`. XTB's
  `"Free funds interest"` → UNKNOWN (bug #6) → **XTB interest missing from
  gold**.
- `build_cash_flow_summary` — groups all event_types. XTB rows: mostly
  UNKNOWN + TRADE + DEPOSIT + TRANSFER. `event_datetime` unparseable
  (bug #3) → **all XTB rows dropped before aggregation**. XTB entirely
  absent from cash-flow chart.
- Reconciliation check (quality.py L379-434): `consolidated_holdings`
  brokers vs `cdc_events` brokers. If XTB snapshot has rows but CDC is
  empty/dropped, reconciliation WARNs "Brokers in holdings but not in CDC".

### Columns the new format can now populate but current code doesn't

| CDC column | Source in new format |
|---|---|
| `ticker` | Cash Operations `Ticker`; Closed Positions `Ticker` |
| `quantity` | Closed Positions `Volume` |
| `price` | Closed Positions `Open Price`/`Close Price` |
| `side` | Closed Positions `Type` (BUY/SELL) |
| `gross_amount` | Closed Positions `Gross Profit` / `Sale Value` - `Purchase Value` |
| `fee_amount` | Closed Positions `Commission` |
| `settle_date` | Closed Positions `Close Time (UTC)` |
| `target_fx_rate` | Cash Operations `Transfer` comment "Exchange rate:X"; Closed Positions `Open/Close Conversion Rate` |
| `instrument_ccy` | Derivable from Conversion Rate + value (ADR 0102 path) |

---

## 5. Fix-vs-Rewrite Recommendation

**Recommendation: delete-and-rewrite the XTB connector (parser + transform).**

Justification (concrete):

- **Open Positions: ~100% of matched columns changed.** Every header the
  parser matches (`position`, `symbol`, `purchase value`, `gross p/l`,
  `isin*`) is gone or renamed. The value model (computed sum) is replaced
  by a direct `Value` column. The row structure changed from flat to
  aggregate+child grouped by `Product` with a preceding summary block —
  a concept the parser has no abstraction for.
- **Cash Operations: ~50% of columns changed** and a critical column
  (`Currency`) dropped. New types unmapped. Total-row handling absent.
- **Closed Positions: entirely new sheet, zero existing code.** Carries the
  richest trade data (Volume, prices, commission, conversion rates) that
  should feed CDC trade events.
- **Date format is fundamentally different** (Excel serials vs ISO strings).
  This touches every date field across all three sheets and breaks the
  analytics period parser. A serial→ISO conversion helper is needed
  system-wide for XTB.
- **Label-based metadata lookup is dead.** `value_below_label("Account")` /
  `("Currency")` / `("Balance")` / `("Equity")` have no matching labels in
  the new format; the new report puts `Account number` in R001 and has no
  Balance/Equity at all.
- **~70% of the parsing surface changed.** Patching the existing parser
  would mean replacing every header constant, every label string, the value
  model, the row-iteration logic, and adding aggregate/child handling +
  Excel-serial decoding + a third sheet — while keeping dead old-format
  branches that never execute. The parser's core abstractions
  (header-set matching, value-below-label, aggregate-by-symbol) do not map
  to the new structure.

The fetch layer (`fetch.py`) and connector protocol wiring (`connector.py`,
`__init__.py`) are format-agnostic and can stay; only `parser.py` and
`transform.py` need rewriting.

### Sketch — new module layout & signatures

```
pipeline/connectors/xtb/
  parser.py        # rewritten
  transform.py     # rewritten
  connector.py     # unchanged (extract_holdings may gain isin from new data)
  fetch.py         # unchanged (one payload carries all 3 sheets; see plan O1)
  __init__.py      # unchanged
```

The full implementation spec — new dataclasses (`XtbOpenPosition`,
`XtbClosedPosition`, `XtbCashOperation`, `XtbReport`), `excel_serial_to_datetime`,
`parse_report`, and the `transform_snapshot`/`transform_cdc` contracts — is in
[xtb_overhaul_plan.md](xtb_overhaul_plan.md) Stage 1–2. Excel-serial decoding
(1900 epoch, `datetime(1899,12,30) + timedelta(days=serial)`, UTC) is plan D3.

---

## 6. Open questions — resolved by the plan

The audit's original open questions were resolved by the overhaul plan's
decisions ([xtb_overhaul_plan.md](xtb_overhaul_plan.md) §1):

| Audit question | Resolution |
|---|---|
| Q1 Closed Positions as CDC events? | D2/D8 — Cash Ops is the sole CDC source; Closed Positions is fee-enrichment lookup only |
| Q2 Cash Operations currency? | D5 — all PLN (account ccy) |
| Q3 Aggregate vs child rows for holdings? | D4 — child rows only; Ticker is the identity key |
| Q4 Subaccount + currency-conversion transfers? | D7 — filter subaccount transfers; keep conversion Transfer with `target_ccy`/`target_fx_rate` |
| Q5 `cdc_supported` / fetch source granularity? | O1 — verify what `cdc_supported` gates; one payload carries all 3 sheets |
| Q6 ISIN absence? | D14 — Ticker as identifier (ADR 0002) |
| Q7 Excel-serial epoch? | D3 — 1900 system, confirmed by sample |
| Q8 quality `xtb_cdc` non-empty? | O7 — leave excluded for now |

Items still open against a real (non-anonymized) report: plan §4 O1–O7.