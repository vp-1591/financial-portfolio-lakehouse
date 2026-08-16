# XTB Connector Overhaul Plan

**Goal:** Overhaul the XTB connector to parse the new XTB Excel report format
(3 sheets: Open Positions, Cash Operations, Closed Positions) and feed holdings
+ CDC events into the medallion pipeline correctly.

**Inputs (this session's artifacts, tracked under `docs/xtb/`):**
- [xtb_sample_dump.txt](xtb_sample_dump.txt) — full cell dump of the anonymized sample.
- [xtb_data_verification.md](xtb_data_verification.md) — 49-check consistency report + fixture corrections (applied).
- [xtb_code_audit.md](xtb_code_audit.md) — current-state map, old-vs-new diff, bug inventory, fix-vs-rewrite.
- `dump_xtb_xlsx.py`, `verify_xtb.py` — extraction/verification scripts (kept to re-run the dump/checks).

**Sample file:** `docs/xtb/xtb-report-sample/PLN_12345678_2006-01-01_2026-08-03.xlsx`

This plan is the blueprint for next-session subagent implementation. It is
self-contained but references the audit files for line-level detail.

---

## 1. Decisions (binding for implementation)

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | **Rewrite `parser.py` + `transform.py`** from scratch. Keep `fetch.py`, `connector.py`, `__init__.py`. | ~70% of the parsing surface changed (audit §5). Patching leaves dead branches and the core abstractions (header-set match, value-below-label, aggregate-by-symbol) don't map to the new structure. |
| D2 | Parse **3 sheets**. Open Positions → holdings/snapshot. Cash Operations → **all** CDC events. Closed Positions → **fee-enrichment lookup only**, keyed by Position ID; never emitted as its own event. | Cash Ops is the canonical cash ledger and carries trades (with qty/price in the comment) + deposits/interest/taxes/transfers. Closed Positions adds commission (fee lookup only). One event source ⇒ no double-count. |
| D3 | **Excel-serial dates** decoded to timezone-aware UTC datetimes in the parser via `openpyxl.utils.datetime.from_excel(serial)` (then attach `tzinfo=UTC`). Transform emits ISO datetimes so downstream analytics (`cdc_tables.py`) is unchanged. | New format stores all dates as Excel serials; old ISO-string path is dead. Confirmed by sample: 38718→2006-01-01, 46237.25→2026-08-03 06:00. `from_excel` handles the 1900 epoch and the 1900-02-29 leap-year bug. |
| D4 | Open Positions: use **child rows** (`is_aggregate=False`) for holdings. Aggregate rows are validation-only (assert Σ child Value ≈ aggregate Value; warn on mismatch). Use **Ticker** as label/identifier (Instrument column is polymorphic — real name on aggregates, numeric ID on children). | Children carry per-position detail; Ticker is the stable join key present on every row. No ISIN anywhere in the new format. |
| D5 | Cash Operations: all amounts in **PLN** (account currency). `security_ccy = "PLN"` (derived from filename prefix `PLN_` or report metadata). No Currency column exists. | Sample has no Currency column; filename and account confirm PLN. |
| D6 | Updated event-type map: `Free funds interest`→INTEREST, `Free funds interest tax`→TAX, `Stock sell`/`Stock purchase`→TRADE, `Transfer`→TRANSFER, `Deposit`→DEPOSIT, `Withdrawal`→WITHDRAWAL, `Dividend`→DIVIDEND, `Fee`→FEE, `Correction`/`Profit/loss adjustment`→ADJUSTMENT. Unknown→UNKNOWN (kept, but should be empty after the map). | Audit bug #6: current map misses `Free funds interest`, `Stock sell` (has `Stock sale`), `Subaccount transfer`. |
| D7 | **Filter out `Subaccount transfer`** rows entirely (internal moves between the trading and investment-plan subaccounts; net zero, clutter). **Keep currency-conversion `Transfer`** as a TRANSFER event with `target_fx_rate` parsed from "Exchange rate:X". Do **not** parse `target_ccy` from the comment — `normalize_currency` always overwrites it with the pipeline target (EUR). | User-confirmed: subaccount moves are internal noise; the -1000 PLN transfer is a real outbound transfer to a EUR account. This assumes the transfer targets EUR; a non-EUR target would misinterpret the rate (see O8). |
| D8 | Trade rows enriched from Closed Positions via Position ID: `Commission`→`fee_amount` on the **closing (`Stock sell`) row only**. Opening (`Stock purchase`) row gets no fee from the lookup. `Swap`/`Rollover`/`Margin`/`Open/Close Conversion Rate` are **dropped** — `fee_amount` = Commission only. | One fee per round-trip, no double-count; matches "open positions have no commission" (commission recorded at close). The CDC schema (`cdc_events_normalized_schema`) has no columns for swap/rollover/margin/conversion rate, so there is nowhere to write them. They are not folded into `fee_amount` — that would conflate commission with financing/position costs and distort fee analytics. The raw layer still preserves the full xlsx bytes if a destination is added later. |
| D9 | **Open→closed lifecycle:** CDC dedup by Cash Ops `ID` at the transform (`dedup_cdc_events`, ADR 0105 parity). Snapshot stays latest-only (`filter_latest_snapshot`). Holdings (state) and CDC (event log) are distinct — a position moving from Open Positions to Closed Positions is correct lifecycle, not double-count. | Re-uploads are full-history; stable IDs prevent dup. Position leaving Open Positions drops from holdings; realized trade enters CDC with fee attached. |
| D10 | **Sign convention (ADR 0104):** enforce in transform — purchases negative, sales/interest/deposits positive. Verify XTB raw signs already match (sample: purchase −4300.04, sell +5040.05, deposit +10000, interest +100.01) and normalize defensively rather than rely on the accidental match. | Audit "likely-rotten": XTB signs happen to match the convention but it's not enforced. |
| D11 | Exclude **Total rows** (Cash Operations `Total`, Closed Positions `Profit/loss` total, Open Positions summary block) from events. | Audit bug #10: Total row currently emitted as a bogus UNKNOWN operation. |
| D12 | Round `Value` (open), `Amount` (cash), and the closed-position `Commission`, `Purchase value`, `Sale value`, `Profit/Loss` to **2 decimals on read** to kill IEEE-754 artifacts (940.7399999999991 → 940.74). | Verification finding 6: benign float noise in the source xlsx. |
| D13 | Set `connector.py cdc_supported = True` (verify what it gates first — see open item O1). Cash Operations is a real CDC feed; leaving it False likely skips CDC in the orchestrator. | Audit bug #13. |
| D14 | Identifier = **Ticker** (no ISIN available). ADR 0002 (broker-native identifiers) supports this. | Audit OQ6; new format has no ISIN column on any sheet. |
| D15 | **Adopt openpyxl** as the XLSX library for the parser rewrite. Load via `openpyxl.load_workbook(BytesIO(data))`, access sheets by name (`wb["Cash Operations"]`, etc.), iterate `ws.iter_rows(values_only=True)`. Drop the manual `read_shared_strings` / `read_sheet_rows` / `sheet_paths_by_name` zipfile-XML helpers. | Cleaner than raw ZIP/XML: native sheet/cell access, shared-strings handled internally, number/date cell handling, easier to maintain. Already added to `pyproject.toml` pipeline deps (`openpyxl==3.1.5`) and installed in the venv ahead of this rewrite. Decode dates at the parser boundary via `openpyxl.utils.datetime.from_excel` (+ `tzinfo=UTC`) — openpyxl handles the 1900 epoch and the 1900-02-29 leap-year bug, so no hand-rolled helper. openpyxl auto-converts date-*formatted* cells to `datetime`; for cells read as a raw numeric serial, pass them through `from_excel` explicitly. |

---

## 2. Fixture corrections (APPLIED — sample is ready as a test fixture)

The anonymized sample had internal per-row inconsistencies; all were fixed in
pass 2 and verified. The full record — 49-check verification, findings, the
corrections applied, and the withdrawn (INTENDED) items — is in
[xtb_data_verification.md](xtb_data_verification.md). Summary of what changed:

1. **R012 Net Profit %** 8.31 → 8.32; **R011 aggregate** 29.76 → 29.77 (sum-of-children).
2. **R011 aggregate Open price** 105 → 106.36 (volume-weighted).
3. **Cash-op ID order** — the 3 added open-order IDs made time-monotonic.
4. **Buy/sell reorder** — pos 1334567890 purchase moved 07-20 → 08-02 08:00, ID
   900035425 → 900045000; running balance no longer goes negative on a purchase.

Note: corrections 1–2 only matter if a test asserts on `Net Profit %`. Per D4 we
map `Value` only (not `%`, `Net Profit`, or `Gross Profit`), so they affect
neither processing nor tests — fixed for fixture correctness anyway. Correction 3
(Open price) matters only if a test asserts the aggregate Open price; correction 4
(ID order) matters if a test asserts cash-op IDs or relies on ID-time monotonicity.

---

## 3. Implementation stages (subagent-sized, sequential dependencies)

> Each stage lists files, the change, signatures, and acceptance criteria.
> Run the project checks after stages that touch code (see §5).

### Stage 0 — Fixture correction + new programmatic fixture

**Why first:** tests in later stages need a valid new-format workbook.

**Do:**
- Apply corrections §2 to the sample xlsx **or** (recommended) build a new
  `tests/fixtures/xtb.py` that constructs a new-format workbook programmatically
  with **openpyxl** (D15) with known values
  and a closed position that has a **nonzero commission** (the sample has
  Commission=0, which hides fee handling). Include: one closed trade with
  commission, open positions with an aggregate+child group, a currency-conversion
  transfer, a subaccount transfer pair, free-funds interest + tax, a deposit.
- Keep the real xlsx sample as a secondary integration fixture.

**Acceptance:** the fixture workbook round-trips through `parse_report()` (Stage 2)
and every value is assertable.

### Stage 1 — Rewrite `pipeline/connectors/xtb/parser.py`

**New dataclasses** (replace `XtbPosition`, `XtbCashOperation`):

```python
@dataclass(frozen=True)
class XtbOpenPosition:
    # Only fields with a destination in snapshot_normalized_schema.
    account_id: str
    product: str            # "Investment Plan" | "My Trades"; groups optional aggregate validation
    instrument: str         # real name (aggregate) or numeric ID (child) -> description
    ticker: str             # reliable identity key -> label / identifier
    category: str           # -> asset_class / position_type
    value: float            # PLN market value (Value column), 2dp -> security_value
    is_aggregate: bool      # aggregate rows kept for optional validation only

@dataclass(frozen=True)
class XtbClosedPosition:
    # Fee-enrichment lookup only (D2); never emitted as its own event.
    position_id: str        # join key to Cash Ops trade rows
    commission: float       # -> fee_amount on the closing (Stock sell) row
    purchase_value: float   # -> gross_amount (sale_value - purchase_value)
    sale_value: float
    profit_loss: float      # fallback for gross_amount
    close_time: datetime    # -> settle_date on the closing row (UTC)

@dataclass(frozen=True)
class XtbCashOperation:
    account_id: str
    operation_type: str     # raw "Type" text -> raw_event_type / event_type
    ticker: str             # populated on trade rows
    time: datetime          # UTC -> event_datetime
    amount: float           # PLN, 2dp -> cash_amount
    operation_id: str       # -> event_id (CDC dedup key)
    comment: str            # -> description; carries trade qty/price + transfer FX details
    position_id: str        # join key to Closed Positions (trade rows only)

@dataclass(frozen=True)
class XtbReport:
    account_id: str
    open_positions: list[XtbOpenPosition]      # aggregate + child, flagged
    closed_positions: list[XtbClosedPosition]
    cash_operations: list[XtbCashOperation]    # Total/summary rows excluded
```

**Field scope (YAGNI):** each dataclass carries only fields with a confirmed
destination in `snapshot_normalized_schema` / `cdc_events_normalized_schema`.
Broker-native detail not mapped to a normalized column is dropped — the raw
layer already preserves the full xlsx bytes, so capturing it here adds nothing.
Both reference connectors do the same: IBKR's `client.py` returns plain
`list[dict]` and the transform picks fields; Trading 212 consumes JSON directly
with no intermediate dataclass. In particular `XtbClosedPosition` drops `swap`,
`rollover`, `margin`, `open_conversion_rate`, `close_conversion_rate` — D8
proposed "attaching" them to the sell row, but the CDC schema has no such
columns, so there is nowhere to write them. Per D8 they are dropped entirely
(`fee_amount` = Commission only, not folded — folding would conflate commission
with financing/position costs). The raw layer preserves the full xlsx bytes.

**Helpers:**
- Date decoding via `openpyxl.utils.datetime.from_excel` (returns a naive `datetime`; attach `tzinfo=UTC` at the parser boundary). openpyxl already handles the 1900 epoch and the 1900-02-29 leap-year bug, so no hand-rolled `excel_serial_to_datetime`. Date-*formatted* cells are auto-converted by openpyxl; for cells read as a raw numeric serial, pass them through `from_excel` explicitly.
- `parse_report(data: bytes, account_id_override: str | None = None) -> XtbReport` — top-level; reads all 3 sheets.
- XLSX access via **openpyxl** (D15): `wb = openpyxl.load_workbook(BytesIO(data), data_only=True)`; select sheets by name; iterate `ws.iter_rows(values_only=True)`. The old low-level zipfile/XML helpers (`read_shared_strings`, `read_sheet_rows`, `sheet_paths_by_name`) are removed.
- Sheet discovery: `find_sheet_name` substring match survives ("OPEN POSITION"→"Open Positions", "CASH OPERATION"→"Cash Operations"); add "CLOSED POSITION"→"Closed Positions".
- Account ID: read from `Account number` (R1 of each sheet), not the dead `value_below_label("Account")`.
- Per-sheet parsers: `parse_open_positions(rows)`, `parse_cash_operations(rows)`, `parse_closed_positions(rows)`. Each extracts **only the fields on its dataclass** — do not populate columns with no normalized destination.
- Open Positions: skip the summary block (rows where header normalizes to `Product|Metric|Amount|Currency`); the detail header is `Product, Instrument/Position, Ticker, Category, Type, Volume, Value, Current price, Open price, Open time (UTC), …, Net Profit %, Net Profit, Gross Profit, …`. Extract only `product, instrument, ticker, category, value` plus the `is_aggregate` flag; ignore the rest. Mark `is_aggregate` = (non-empty instrument AND empty Type AND empty Current price) — read Current price only to detect the flag, do not store it. Round `value` to 2dp.
- Cash Operations: header `Type, Instrument, Ticker, Category, Time, Amount, ID, Comment, Product, Position ID`. Extract `Type, Ticker, Time, Amount, ID, Comment, Position ID` (drop `Instrument, Category, Product` — unused). Exclude rows where `Type` normalizes to `total`. Filter `Subaccount transfer` rows out here (D7) — or leave to transform; pick one place and document it.
- Closed Positions: header per dump; extract only `Position ID, Commission, Purchase value, Sale value, Profit/Loss, Close time` (the fee-enrichment fields). Exclude the `Profit/loss` total row.

**Acceptance:** `parse_report(fixture_bytes)` returns all three lists with only the
mapped fields populated, correct types, dates decoded via
`openpyxl.utils.datetime.from_excel`, `value`/`amount` rounded to 2dp, aggregate
flags set, and Total rows excluded.

### Stage 2 — Rewrite `pipeline/connectors/xtb/transform.py`

Keep the same public signatures so the connector protocol is undisturbed:

```python
def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table
def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table
```

**`transform_snapshot` (Open Positions → snapshot_normalized_schema):**
- `filter_latest_snapshot(raw)`.
- Parse the payload via `parse_report`; use **child rows only** (`is_aggregate=False`).
- Map: `ticker`→`label`, `value`→`security_value` (encrypt), `account ccy (PLN)`→`security_ccy`, `category`→`asset_class`/`position_type`, `instrument`→`description`. `identifier` = ticker (D14).
- Optional: assert Σ child Value per (product, ticker) ≈ aggregate Value; log warning on mismatch (catches fixture/format drift).

**`transform_cdc` (Cash Operations → cdc_events_normalized_schema, with Closed Positions fee enrichment):**
- Parse the payload via `parse_report` (one payload carries all 3 sheets).
- Build a `position_id → XtbClosedPosition` lookup from `closed_positions`.
- For each cash operation (Total rows already excluded; subaccount transfers filtered — D7):
  - `event_id = operation_id`, `event_datetime = time` (ISO), `cash_amount = amount` (encrypt), `security_ccy = "PLN"`, `broker = "XTB"`, `raw_event_type = operation_type`.
  - `event_type` via the D6 map.
  - **Trade rows** (`Stock sell`/`Stock purchase`): populate `ticker`, parse `quantity`/`price`/`side` from the comment (`"OPEN BUY 10.0001 @ 100.00"`). For the **sell** row, join via `position_id` to Closed Positions and set `fee_amount = commission`, `gross_amount = sale_value − purchase_value` (or `profit_loss`), `settle_date = close_time`. Leave the **purchase** row's fee empty (D8).
  - **Currency-conversion Transfer**: parse `target_fx_rate` from the comment ("Exchange rate:0.230001"→0.230001). Do **not** parse `target_ccy` — `normalize_currency` sets it (always EUR), not the transform. Assumes the transfer targets EUR (see O8).
  - Enforce sign convention (D10).
- `dedup_cdc_events` on `(event_type, event_id)` (D9, ADR 0105 parity).
- `encrypt_columns = ["cash_amount", "target_fx_rate", "target_value"]` (keep).

**Acceptance:** snapshot produces one row per open child position; CDC produces
one event per cash op (subaccount transfers excluded), trades carry qty/price/
side/fee, currency transfer carries target fields, no Total/UNKNOWN events,
re-running on the same payload dedups.

### Stage 3 — `pipeline/connectors/xtb/connector.py` (small edits)

- `cdc_supported = True` (D13) **after verifying O1**.
- `extract_holdings`: use `ticker` as `identifier` and `Holding.identifier` (no ISIN). `currency = security_ccy` (PLN). Map `position_type`/`description` from the new fields.
- Confirm `fetch_kwargs`/`args.xtb_file` unchanged.

### Stage 4 — Analytics date handling (verify, likely no change)

- The transform now emits ISO `event_datetime`, so `cdc_tables.py`'s strptime chain should work unchanged. **Add a regression test** that an XTB CDC row survives `_add_period_columns` (the old failure was the serial-string `"46236.875"`; the fix is emitting a real datetime).

### Stage 5 — Tests

- Update `tests/test_xtb_connector.py` and `tests/fixtures/xtb.py` to the new format (Stage 0 fixture).
- Cover: 3-sheet parsing, Excel-serial decoding (cash-op `time` + closed `close_time`; open positions carry no date field after the Stage 1 narrowing), aggregate-vs-child distinction, child-only holdings, Total-row exclusion, subaccount-transfer filtering, currency-transfer target fields, trade enrichment (commission on sell row only), event-type map (interest/tax/sell), CDC dedup on re-upload, sign convention, 2dp rounding, open→closed lifecycle (a position open in one snapshot, closed in the next, fee captured once). Confirm no test references dropped fields (`net_profit`, `gross_profit`, `swap`, `rollover`, `margin`, `open_price`, `current_price`, `volume`, `side` on positions).
- Keep the integration test against the real xlsx sample.

### Stage 6 — Checks + docs

- Run `.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .`
- Run `.venv/Scripts/python -m pyright pipeline/ tests/`
- Run `.venv/Scripts/python -m pytest tests/ -q -rf` (re-run after lint auto-fixes)
- Update `docs/brokers/xtb.md` (remove the "only sample data verified" caveat; document the 3-sheet format, Excel-serial dates, PLN assumption, subaccount-transfer filtering, ticker-as-identifier, no-ISIN limitation).
- **Record an ADR** (invoke `manage-adr` skill) for the rewrite: context = new XTB format, decision = rewrite parser/transform, 3-sheet model, Cash Ops as sole CDC source + Closed Positions fee lookup, subaccount-transfer filtering, ticker identifier. Supersede any prior XTB ADRs whose decisions this reverses (check 0047, 0048, 0102's XTB instrument-ccy deferral).

---

## 4. Open items to verify against a REAL (non-anonymized) XTB report

These can't be resolved from the sample alone and shouldn't block implementation
— implement the documented default, then confirm:

| # | Item | Default | Risk if wrong |
|---|------|---------|---------------|
| O1 | What does `cdc_supported` gate? Does False skip XTB CDC in the orchestrator? | Set True (D13) | If False skips CDC, current Cash Ops never reach gold — setting True is required. If True has side effects (e.g. mandatory non-empty check), handle. |
| O2 | Is commission a separate Cash Operations row, or only in Closed Positions? | Closed Positions lookup (D8); separate cash-op rows would be a bonus source | If commission is ONLY a cash-op row type we don't map, we'd miss it — add the type to the map. |
| O3 | Open Positions aggregate Net Profit %: sum of children or weighted? | Don't map % (we use `Value` only), so no processing impact | None for processing; document only. |
| O4 | Open Positions aggregate OpenPrice: simple vs volume-weighted average? | Don't rely on aggregate OpenPrice (use child rows) | None — we use child rows for holdings. |
| O5 | Are Cash Ops amounts ever in a non-PLN currency? | All PLN (D5) | A multi-currency cash op would be mislabeled; unlikely given no Currency column. |
| O6 | Excel-serial epoch 1900 vs 1904? | 1900 (confirmed: 38718→2006-01-01) | 1904 would shift all dates 4 years — already ruled out by the sample. |
| O7 | Does `quality.py NON_EMPTY_REQUIRED` need `xtb_cdc` added if CDC becomes first-class? | Leave excluded for now | A non-empty requirement could break deploys with no XTB activity; leave optional. |
| O8 | Can a currency-conversion `Transfer` target a non-EUR currency? | Assume EUR-target (sample is PLN→EUR) | `normalize_currency` treats a broker-supplied `target_fx_rate` as `security_ccy→EUR`; a PLN→USD rate fed in here would be misinterpreted. If non-EUR targets exist, do **not** parse the rate — leave `target_fx_rate` null and let `normalize_currency` fall back to `CurrencyConverter` (as T212/IBKR do). |

O3/O4 correspond to fixture ambiguities 1–2 in
[xtb_data_verification.md](xtb_data_verification.md) §4.

---

## 5. Check / verification commands (project venv, Windows)

```bash
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
# single file:
.venv/Scripts/python -m pytest tests/test_xtb_connector.py -v
# query staging to confirm XTB rows land (after a deploy):
PYTHONIOENCODING=utf-8 .venv/Scripts/python -m pipeline.run query "SELECT * FROM xtb_snapshot LIMIT 5" --decrypt --mode staging
PYTHONIOENCODING=utf-8 .venv/Scripts/python -m pipeline.run query "SELECT * FROM xtb_cdc LIMIT 5" --decrypt --mode staging
```

---

## 6. Subagent orchestration for next session

Suggested handoff sequence (matches the user's staged-subagent + tmp-handoff
preference; keep main context lean):

1. **Subagent A — Stage 0+1**: build new fixture, rewrite `parser.py`, write parser unit tests. Hand off `tmp/` status.
2. **Subagent B — Stage 2+3**: rewrite `transform.py`, edit `connector.py`, add transform tests. Depends on A.
3. **Subagent C — Stage 4+5**: analytics date regression test, full test suite update, integration test against the real sample. Depends on B.
4. **Main — Stage 6**: run checks, update `docs/brokers/xtb.md`, invoke `manage-adr`.

Each subagent writes a short `tmp/xtb_stageN_report.md` on completion.

---

## 7. What NOT to do

- Do not convert `pl.DataFrame` to `pa.Table` for `write_deltalake` (project rule — accept `pl.DataFrame` directly).
- Do not construct `DeltaTable()` manually for queries — use `pipeline.run query`.
- Do not reference tmp scripts/reports in tracked docs or code.
- Do not hand-write an ADR — use the `manage-adr` skill.
- Do not emit Closed Positions as CDC events (D2) — fee lookup only.
- Do not commit directly to `main` — feature branch `feat/xtb-new-format`, PR via `gh pr create --fill`, regular merge with branch deletion.