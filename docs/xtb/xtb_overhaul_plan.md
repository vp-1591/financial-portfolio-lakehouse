# XTB Connector Overhaul Plan

**Goal:** Overhaul the XTB connector to parse the new XTB Excel report format
(3 sheets: Open Positions, Cash Operations, Closed Positions) and feed holdings
+ CDC events into the medallion pipeline correctly.

**Inputs (this session's artifacts, tracked under `docs/xtb/`):**
- [xtb_sample_dump.txt](xtb_sample_dump.txt) — full cell dump of the anonymized sample.
- `dump_xtb_xlsx.py`, `verify_xtb.py` — extraction/verification scripts (kept to re-run the dump/checks).

**Sample file:** `docs/xtb/xtb-report-sample/PLN_12345678_2006-01-01_2026-08-03.xlsx`

This plan is the blueprint for next-session subagent implementation. It is
self-contained.

---

## 1. Decisions (binding for implementation)

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | **Rewrite `parser.py` + `transform.py`** from scratch. Keep `fetch.py`, `connector.py`, `__init__.py`. | ~70% of the parsing surface changed. Patching leaves dead branches and the core abstractions (header-set match, value-below-label, aggregate-by-symbol) don't map to the new structure. |
| D2 | Parse **3 sheets**. Open Positions → holdings/snapshot. Cash Operations → **all** CDC events. Closed Positions → **fee-enrichment lookup only**, keyed by Position ID; never emitted as its own event. | Cash Ops is the canonical cash ledger and carries trades (with qty/price in the comment) + deposits/interest/taxes/transfers. Closed Positions adds commission (fee lookup only). One event source ⇒ no double-count. |
| D3 | **Excel-serial dates** decoded to timezone-aware UTC datetimes in the parser via `openpyxl.utils.datetime.from_excel(serial)` (then attach `tzinfo=UTC`). Transform emits ISO datetimes so downstream analytics (`cdc_tables.py`) is unchanged. | New format stores all dates as Excel serials; old ISO-string path is dead. Confirmed by sample: 38718→2006-01-01, 46237.25→2026-08-03 06:00. `from_excel` handles the 1900 epoch and the 1900-02-29 leap-year bug. |
| D4 | Open Positions: use the **per-ticker aggregate rows** for holdings (the group-header row XTB emits per instrument — real name in `Instrument`, total `Value`, non-empty `Category`, empty `Type`); **skip the child lot rows** in the parser (never stored). Use **Ticker** as label/identifier; the aggregate's `Instrument` (real name) → `description`, `Category` → `asset_class`. | The snapshot schema is instrument-level (`label`=ticker; no lot/position-id/open-time/cost-basis column), so per-lot detail has nowhere to be stored — child rows would be redundant rows that sum to the same total. **Child rows have an empty `Category` and a numeric position ID in `Instrument`**, so using them would emit empty `asset_class` and a numeric-ID `description`; the aggregate carries both the real name and `Category`. Matches the old parser (`load_open_position_assets` summed by symbol → one `XtbPosition` per ticker) and IBKR (one Flex `OpenPosition` per symbol). CDC trades come from Cash Operations (D2), not Open Positions, so child lots aren't needed for CDC either. The aggregate is the group header for every ticker with open positions; a child without one is a malformed report. |
| D5 | Account currency = the Currency column of the Open Positions summary block (Product\|Metric\|Amount\|Currency), read in `parse_report`. All Cash Operations amounts are in this account currency; `security_ccy = account_ccy` (not a hardcoded literal). No per-row Currency column on Cash Ops. | Sample summary-block Currency reads `PLN`; no per-row Currency column on Cash Ops. |
| D6 | Updated event-type map: `Free funds interest`→INTEREST, `Free funds interest tax`→TAX, `Stock sell`/`Stock purchase`→TRADE, `Transfer`→TRANSFER, `Deposit`→DEPOSIT, `Withdrawal`→WITHDRAWAL, `Dividend`→DIVIDEND, `Fee`→FEE, `Correction`/`Profit/loss adjustment`→ADJUSTMENT. Unknown→UNKNOWN (kept, but should be empty after the map). | Current map misses `Free funds interest`, `Stock sell` (has `Stock sale`), `Subaccount transfer`. |
| D7 | **Filter out `Subaccount transfer`** rows entirely (internal moves between the trading and investment-plan subaccounts; net zero, clutter). **Keep currency-conversion `Transfer`** as a TRANSFER event with `target_fx_rate` left **null** — `normalize_currency` converts `cash_amount` (in account_ccy) to EUR via `CurrencyConverter` (normalize.py:151-155), same as T212/IBKR. Do **not** parse `Exchange rate:X`: it is the account_ccy→destination rate, which equals the pipeline's account_ccy→EUR `target_fx_rate` only when the destination is EUR; non-EUR destinations exist, so the broker rate is unsafe. `target_ccy` is always EUR (pipeline-set, never parsed). | User-confirmed: subaccount moves are internal noise; the currency-conversion transfer is a real outbound transfer (non-EUR destinations confirmed). `target_ccy` is always EUR (models.py:21) and the destination currency is never stored — the pipeline only needs `cash_amount` (account_ccy) + an account_ccy→EUR rate, which `CurrencyConverter` supplies correctly regardless of destination. Matches the existing XTB/IBKR/T212 pattern (all set `target_fx_rate` null). |
| D8 | Trade rows enriched from Closed Positions via Position ID: `Commission`→`fee_amount` on the **closing (`Stock sell`) row only**. Opening (`Stock purchase`) row gets no fee from the lookup. `Swap`/`Rollover`/`Margin`/`Open/Close Conversion Rate` are **dropped** — `fee_amount` = Commission only. | User-confirmed against a real report: commission appears only in Closed Positions (no separate Cash Ops commission row). One fee per round-trip, no double-count; matches "open positions have no commission" (commission recorded at close). The CDC schema (`cdc_events_normalized_schema`) has no columns for swap/rollover/margin/conversion rate, so there is nowhere to write them. They are not folded into `fee_amount` — that would conflate commission with financing/position costs and distort fee analytics. The raw layer still preserves the full xlsx bytes if a destination is added later. |
| D9 | **Open→closed lifecycle:** CDC dedup by Cash Ops `ID` at the transform (`dedup_cdc_events`, ADR 0105 parity). Snapshot stays latest **per account** (D18 — not `filter_latest_snapshot`, which collapses distinct accounts). Holdings (state) and CDC (event log) are distinct — a position moving from Open Positions to Closed Positions is correct lifecycle, not double-count. | Re-uploads are full-history; stable IDs prevent dup. Position leaving Open Positions drops from holdings; realized trade enters CDC with fee attached. |
| D10 | Exclude **Total rows** (Cash Operations `Total`, Closed Positions `Profit/loss` total, Open Positions summary block) from **CDC events**. The Cash Ops `Total` is still **read** by the parser into `XtbReport.free_cash` (D22) for the snapshot CASH holding — it is a summary, not an event. | Total row currently emitted as a bogus UNKNOWN operation. |
| D11 | Round `Value` (open), `Amount` (cash), and the closed-position `Commission`, `Purchase value`, `Sale value` to **2 decimals on read** to kill IEEE-754 artifacts (940.7399999999991 → 940.74). | Benign float noise in the source xlsx. |
| D12 | Identifier = **Ticker** (no ISIN available). ADR 0002 (broker-native identifiers) supports this. | New format has no ISIN column on any sheet. |
| D13 | **Adopt openpyxl** as the XLSX library for the parser rewrite. Load via `openpyxl.load_workbook(BytesIO(data))`, access sheets by name (`wb["Cash Operations"]`, etc.), iterate `ws.iter_rows(values_only=True)`. Drop the manual `read_shared_strings` / `read_sheet_rows` / `sheet_paths_by_name` zipfile-XML helpers. | Cleaner than raw ZIP/XML: native sheet/cell access, shared-strings handled internally, number/date cell handling, easier to maintain. Already added to `pyproject.toml` pipeline deps (`openpyxl==3.1.5`) and installed in the venv ahead of this rewrite. Decode dates at the parser boundary via `openpyxl.utils.datetime.from_excel` (+ `tzinfo=UTC`) — openpyxl handles the 1900 epoch and the 1900-02-29 leap-year bug, so no hand-rolled helper. openpyxl auto-converts date-*formatted* cells to `datetime`; for cells read as a raw numeric serial, pass them through `from_excel` explicitly. |
| D14 | **Remove the `cdc_supported` flag** outright — no intermediate "set True" step. Drop it from the `BrokerConnector` protocol (`base.py`) and all three connector classes; simplify `run.py:718` to `tables = [f"{name}_snapshot", f"{name}_cdc"]` (unconditional). Drop `test_cdc_supported_*` and the `Fake.cdc_supported` attr. Land after Stage 2. | The flag existed only to mark XTB `False`, but XTB CDC production was **broken**, not intentionally disabled — `fetch_cdc` is never invoked on the prod trigger path (D19) and `xtb_cdc` raw was never written, so `cdc_supported=False` masked a dead code path. D17 (shared bronze) + the transform rewrite make `transform_cdc` produce `xtb_cdc` for the first time, so unconditional validation is now correct. Per-connector validation is scoped to the invoked connector (whose transform just wrote in-process). |
| D15 | **Remove `_OPTIONAL_CDC_BROKERS`** from `pipeline/normalized/consolidate_cdc.py`. Derive the candidate broker set from the registry (`connectors.all()` → `c.name`) instead of a hardcoded list; retain only `_REQUIRED_CDC_BROKERS = ["ibkr","trading212"]` as the ADR 0087 required-non-empty quality gate. For each candidate, try to read `normalized/{name}_cdc`: if the broker is in `_REQUIRED_CDC_BROKERS`, raise on missing/empty (unchanged); otherwise skip missing/empty (log at DEBUG). Behavior is identical to today for ibkr/t212/xtb, but the `_OPTIONAL` list and the `"xtb"` literal are gone — the candidate set comes from the single source of truth (the registry). Verify no import cycle: `connectors.base` already imports `pipeline.normalized.consolidate` (for `Holding`), so `consolidate_cdc` → `connectors.registry` must not close a loop (import `registry` lazily inside the function if needed). Update `test_consolidate_skips_xtb_*` to assert the skip still happens via the registry path. | Removes the redundant parallel "XTB is special" encoding in the consolidate layer (DRY). `_OPTIONAL_CDC_BROKERS = ["xtb"]` is the only non-required entry; folding "non-required" into "skip if absent" via the registry deletes the list entirely. The required-vs-optional *policy* stays (`_REQUIRED_CDC_BROKERS`, the deliberate ADR 0087 gate) — what's removed is the duplicate candidate list. XTB stays non-required (skipped if absent/empty) because the prod daily-schedule run (`schedule_connectors = ["ibkr","trading212"]`) does not run XTB — so `xtb_cdc` may legitimately be missing or empty on that path. |
| D16 | **Complete ADR 0094's `*_ENABLED` cleanup** — sweep the two stale references it missed. (1) `tests/test_connector_registry.py:15`: drop `enabled_env_var = "FAKE_ENABLED"` from `FakeConnector` — the `BrokerConnector` protocol no longer declares `enabled_env_var` (ADR 0094 removed it) and nothing reads it, so the attr is dead. (2) `docs/configuration.md:62`: remove "Required environment variable: `XTB_ENABLED` (optional, enabled by default)." — the `XTB_ENABLED` env var no longer exists (ADR 0094), so the line documents a removed feature. | ADR 0094 deleted `IBKR_ENABLED`/`T212_ENABLED`/`XTB_ENABLED`, `is_enabled()`, and the `enabled_env_var` protocol attr, but left these two stragglers. The `configuration.md` line is XTB-specific waste; the `Fake.enabled_env_var` attr is a generic dead leftover. Both are one-line removals. No new ADR needed — this finishes 0094's execution, it is not a new decision. |
| D17 | **Shared bronze — transform CDC from the snapshot raw, not a separate CDC fetch.** One fetch per file writes a single raw row to `xtb_snapshot` raw with `source="XTB_REPORT"` carrying the full workbook (all 3 sheets). Delete `fetch.py:fetch_cdc`, `XtbConnector.fetch_cdc`/`fetch_cdc_kwargs`, and the `xtb_cdc` raw table + `xtb_cdc_raw_schema`. `transform_cdc` reads from the same `xtb_snapshot` raw. Add `cdc_raw_layer: str = "cdc"` to `BrokerConnector` (`base.py`); XTB overrides `cdc_raw_layer = "snapshot"` so `transform_connector` reads `get_raw_path(name, cdc_raw_layer)` for the CDC transform. | `fetch_snapshot` and `fetch_cdc` currently store **byte-identical** xlsx in two raw tables (same `payload`, same `payload_hash`, only `source` differs) — pure duplication. One xlsx carries all 3 sheets, so one bronze row is enough; both silvers derive from it. Eliminates the dead `fetch_cdc` path and makes CDC production real (fixes the D19 finding that CDC never reached gold). |
| D18 | **Multi-account transform semantics — do NOT use `filter_latest_snapshot` for XTB.** It keys on `source` alone and `account_id` is not in `RAW_SCHEMA` (only parsed from the payload), so it collapses distinct accounts (e.g. `PLN_123…` + `EUR_456…`) to the single latest row. Instead: `transform_snapshot` iterates **all** `source=="XTB_REPORT"` rows, parses each, keeps the latest `fetched_at` **per `account_id`**, and emits per-ticker aggregate holdings for every surviving account. `transform_cdc` iterates all rows and dedups by `event_id` (D9) — no latest-per-account (events are additive). | Multiple accounts must coexist; latest-per-account avoids stale-snapshot double-count of the same account while preserving every account. CDC needs no per-account filter since stable `event_id`s dedup across re-uploads/accounts. |
| D19 | **File-arrival trigger (prod path the overhaul must fit).** Prod: EventBridge S3 Object-Created on the XTB upload prefix (`pipeline/xtb_uploads/`, D20) → Step Functions `orchestrator` `RunConnectors` Map (`file_arrival_connectors = ["ibkr","trading212","xtb"]`, concurrency 3) → `ConsolidateAllocate` (`run-consolidate-analytics`). The trigger passes a **single** `--xtb-file` S3 URI per execution; multi-account accumulates across triggers into the shared `xtb_snapshot` raw table and is unioned per-account at transform (D18). The daily schedule (`schedule_connectors = ["ibkr","trading212"]`) excludes XTB, so `xtb_cdc` may be absent on that path (informs D15/D21). The `fetch_connector` XTB loop already iterates `--xtb-file` (supports N files in one CLI call too). | Documents the actual prod trigger and confirms CDC must come via shared bronze (D17) — the trigger never fetches CDC, so a separate CDC fetch is not how XTB CDC reaches gold. |
| D20 | **Fix the XTB upload landing-zone path so the EventBridge trigger fires (both envs).** Upload path becomes `{env_prefix}/xtb_uploads/<file>` (prod `pipeline/xtb_uploads/`, demo `pipeline_demo/xtb_uploads/`), replacing `{env_prefix}/{staging\|staging_demo}/xtb/`. `S3StorageConfig.staging_path` + `S3Backend.staging_path` (storage.py:115-120, 195-203) drop the `staging`/`staging_demo` segment and use `{prefix}/{connector}_uploads/{filename}`. Align `xtb_staging_prefix` → `pipeline/xtb_uploads/` in `terraform/prod/main.tf:547` and `pipeline_demo/xtb_uploads/` in `terraform/demo/main.tf:564`. No S3 migration (trigger never fired in either env; nothing consumed the old uploads). Ships in this overhaul PR. | The EventBridge rule filters `object.key` by prefix, but the rule prefix missed the env top-level segment (`S3_DEFAULT_PREFIX="pipeline"`, kept in prod at storage.py:294) that `S3Backend` always prepends — actual key `pipeline/staging/xtb/<file>` never matched rule prefix `staging/xtb/`. Identical bug in demo (`pipeline_demo/staging_demo/xtb/` vs `staging_demo/xtb/`). The `staging`/`staging_demo` segment also redundantly re-encoded the env (already carried by `pipeline`/`pipeline_demo`) and `staging` collided with `--mode staging`; dropping it + renaming `xtb`→`xtb_uploads` fixes the match in both envs and removes the collision. |
| D21 | **`xtb_cdc` stays excluded from `quality.py` `NON_EMPTY_REQUIRED`.** The prod daily schedule (`schedule_connectors = ["ibkr","trading212"]`) does not run XTB, so `xtb_cdc` may legitimately be absent or empty on that path; requiring it would break scheduled runs with no XTB activity. | Three distinct CDC-non-empty gates agree XTB CDC is never mandatory: (1) per-connector post-transform validation (D14 — unconditional, scoped to the invoked connector), (2) consolidate required/optional (D15 — `xtb_cdc` skip-if-absent), (3) `quality.py` `NON_EMPTY_REQUIRED` (this decision — `xtb_cdc` excluded). The prod daily schedule not running XTB is why `xtb_cdc` may legitimately be absent. |
| D22 | **Emit a CASH holding (per account) from the Cash Operations `Total` row, under a documented full-history-export constraint.** The parser reads the `Total` row's `Amount` into `XtbReport.free_cash` (2dp) before excluding it from `cash_operations` (it is a summary, not an event — D10); `transform_snapshot` emits one CASH row per account (`position_type=CASH`, `label="CASH {ccy}"`, `security_value=free_cash` encrypted, `security_ccy=account_ccy`) under the D18 latest-per-account filter. XTB has no API, so the Excel report is the only cash source. | `Total` = Σ cash ops over the report window; it equals the free-cash balance **only** under full-history export from a zero opening balance (sample 1583.92 verified). Partial exports silently understate cash and cannot be detected in-file, so the control is a documented XTB setup rule (`docs/brokers/xtb.md`: `Date from` = account opening), not a heuristic guard (YAGNI). Restores the CASH holding the old parser synthesized; preserves dashboard cash visibility. Invested value stays in Open Positions → account equity = CASH row + Σ open positions. Alternatives rejected: dropping CASH (dashboard regression), Σ CDC events (identical number to `Total`, more work, same constraint), Open Positions summary `Value` (wrong quantity — it is Σ open positions, not cash). |

---

## 2. Fixture corrections (APPLIED — sample is ready as a test fixture)

The anonymized sample had internal per-row inconsistencies; all were fixed in
pass 2 and verified. Summary of what changed:

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
- Build a new `tests/fixtures/xtb.py` that constructs a new-format workbook
  programmatically with **openpyxl** (D13) with known values and a closed
  position that has a **nonzero commission** (the sample has Commission=0, which
  hides fee handling). Include: one closed trade with commission, open positions
  with an aggregate+child group, a currency-conversion transfer, a subaccount
  transfer pair, free-funds interest + tax, a deposit.
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
    product: str            # "Investment Plan" | "My Trades" (group label; not mapped)
    instrument: str         # real instrument name on the aggregate row -> description
    ticker: str             # reliable identity key -> label / identifier
    category: str           # ETF on the aggregate row -> asset_class (empty on child lots)
    value: float            # account-ccy market value (aggregate row Value = per-ticker total), 2dp -> security_value

@dataclass(frozen=True)
class XtbClosedPosition:
    # Fee-enrichment lookup only (D2); never emitted as its own event.
    position_id: str        # join key to Cash Ops trade rows
    commission: float       # -> fee_amount on the closing (Stock sell) row
    purchase_value: float   # -> gross_amount (sale_value - purchase_value)
    sale_value: float
    close_time: datetime    # -> settle_date on the closing row (UTC)

@dataclass(frozen=True)
class XtbCashOperation:
    account_id: str
    operation_type: str     # raw "Type" text -> raw_event_type / event_type
    ticker: str             # populated on trade rows
    time: datetime          # UTC -> event_datetime
    amount: float           # account ccy, 2dp -> cash_amount
    operation_id: str       # -> event_id (CDC dedup key)
    comment: str            # -> description; carries trade qty/price + transfer FX details
    position_id: str        # join key to Closed Positions (trade rows only)

@dataclass(frozen=True)
class XtbReport:
    account_id: str
    account_ccy: str                            # summary-block Currency (D5)
    open_positions: list[XtbOpenPosition]      # per-ticker aggregate rows (child lots skipped)
    closed_positions: list[XtbClosedPosition]
    cash_operations: list[XtbCashOperation]    # Total/summary rows excluded from events
    free_cash: float | None                    # Cash Ops Total (R-last) -> snapshot CASH holding (D22); None if no Total row
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
- XLSX access via **openpyxl** (D13): `wb = openpyxl.load_workbook(BytesIO(data), data_only=True)`; select sheets by name; iterate `ws.iter_rows(values_only=True)`. The old low-level zipfile/XML helpers (`read_shared_strings`, `read_sheet_rows`, `sheet_paths_by_name`) are removed.
- Sheet discovery: `find_sheet_name` substring match survives ("OPEN POSITION"→"Open Positions", "CASH OPERATION"→"Cash Operations"); add "CLOSED POSITION"→"Closed Positions".
- Account ID: read from `Account number` (R1 of each sheet), not the dead `value_below_label("Account")`.
- Per-sheet parsers: `parse_open_positions(rows)`, `parse_cash_operations(rows)`, `parse_closed_positions(rows)`. Each extracts **only the fields on its dataclass** — do not populate columns with no normalized destination.
- Open Positions: read the account currency from the **summary block's Currency column** (`Product|Metric|Amount|Currency`) before skipping that block (the summary block is per-product, not holdings — D5). The detail header is `Product, Instrument/Position, Ticker, Category, Type, Volume, Value, Current price, Open price, Open time (UTC), …, Net Profit %, Net Profit, Gross Profit, …`. **Keep the per-ticker aggregate rows** (the group header XTB emits per instrument: empty `Type`, real name in `Instrument`, non-empty `Category`) as the holdings; **skip the child lot rows** (non-empty `Type` — per-position detail with no destination in the snapshot schema, and CDC trades come from Cash Operations). Extract only `product, instrument (real name), ticker, category, value`; ignore the rest. Round `value` to 2dp.
- Cash Operations: header `Type, Instrument, Ticker, Category, Time, Amount, ID, Comment, Product, Position ID`. Extract `Type, Ticker, Time, Amount, ID, Comment, Position ID` (drop `Instrument, Category, Product` — unused). Read the `Total` row's `Amount` into `XtbReport.free_cash` (2dp) before excluding it — it is a summary, not an event (D10); the snapshot transform emits it as the CASH holding (D22). Exclude rows where `Type` normalizes to `total`. Filter `Subaccount transfer` rows out here (D7) — single place (parser), so they never enter `XtbReport.cash_operations`.
- Closed Positions: header per dump; extract only `Position ID, Commission, Purchase value, Sale value, Profit/Loss, Close time` (the fee-enrichment fields). Exclude the `Profit/loss` total row.

**Acceptance:** `parse_report(fixture_bytes)` returns all three lists with only the
mapped fields populated, correct types, dates decoded via
`openpyxl.utils.datetime.from_excel`, `value`/`amount` rounded to 2dp, **per-ticker
aggregate rows kept as holdings (with `category` populated), child lot rows
skipped**, Total rows excluded from `cash_operations` but read into
`free_cash` (D22).

### Stage 2 — Rewrite `pipeline/connectors/xtb/transform.py`

Keep the same public signatures so the connector protocol is undisturbed:

```python
def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table
def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table
```

**`transform_snapshot` (Open Positions → snapshot_normalized_schema, multi-account — D18):**
- Do **NOT** call `filter_latest_snapshot` (it keys on `source` alone and collapses distinct accounts). Iterate **all** `source=="XTB_REPORT"` rows via `iter_raw_payloads`; parse each via `parse_report` (aggregates already skipped by the parser); keep the latest `fetched_at` **per `account_id`**.
- For each surviving account payload, map the per-ticker aggregate rows: `ticker`→`label`, `value`→`security_value` (encrypt), `account_ccy`→`security_ccy`, `category`→`asset_class` (with `position_type`=`"EQUITY"`), `instrument` (real name)→`description`. `identifier` = ticker (D12). One row per ticker per account (the aggregate is already the per-ticker total — do not also emit child lots).
- Emit one **CASH holding row per account** from `XtbReport.free_cash` (D22): `position_type`=`"CASH"`, `label`=`"CASH {account_ccy}"`, `asset_class`=`"CASH"`, `security_value`=`free_cash` (encrypt, 2dp), `security_ccy`=`account_ccy`, `isin`=`""`, `description`=`"Cash {account_ccy}"`. Skip if `free_cash` is `None` (no Total row). Account equity = free cash (this row) + Σ open positions (the aggregate rows).

**`transform_cdc` (shared bronze `xtb_snapshot` raw → cdc_events_normalized_schema, with Closed Positions fee enrichment — D17):**
- Reads `xtb_snapshot` raw (via `cdc_raw_layer="snapshot"`), not a separate CDC raw. Iterate **all** `source=="XTB_REPORT"` rows via `iter_raw_payloads`; parse each via `parse_report` (one payload carries all 3 sheets).
- Build a `position_id → XtbClosedPosition` lookup from `closed_positions` (per payload).
- For each cash operation (Total rows already excluded; subaccount transfers already filtered by the parser — D7):
  - `event_id = operation_id`, `event_datetime = time` (ISO), `cash_amount = amount` (encrypt), `security_ccy = account_ccy`, `broker = "XTB"`, `raw_event_type = operation_type`.
  - `event_type` via the D6 map.
  - **Trade rows** (`Stock sell`/`Stock purchase`): populate `ticker`, parse `quantity`/`price`/`side` from the comment (`"OPEN BUY 10.0001 @ 100.00"`). For the **sell** row, join via `position_id` to Closed Positions and set `fee_amount = commission`, `gross_amount = sale_value − purchase_value`, `settle_date = close_time`. Leave the **purchase** row's fee empty (D8).
  - **Currency-conversion Transfer**: keep as a TRANSFER event with `target_fx_rate` left **null** (do **not** parse `Exchange rate:X` — D7). `normalize_currency` converts `cash_amount` (account_ccy) to EUR via `CurrencyConverter`; `target_ccy` is always EUR (pipeline-set).
- `dedup_cdc_events` on `(event_type, event_id)` (D9, ADR 0105 parity).
- `encrypt_columns = ["cash_amount", "quantity", "price", "gross_amount", "fee_amount", "tax_amount", "target_fx_rate", "target_value"]` (mirror IBKR). The old `["cash_amount", "target_fx_rate", "target_value"]` left the new trade columns unencrypted; since they are `pa.binary()` in `cdc_events_normalized_schema`, `build_normalized_table`'s `cast(schema)` would fail on the plain floats. Full list required — see matrix below.

**Column mapping matrix** — the full, column-by-column population of both
normalized tables. Every schema column is accounted for. Encrypted columns
are Fernet-encrypted at the transform via `encrypt_columns`
(`build_normalized_table`).

**`xtb_snapshot_normalized`** (`snapshot_normalized_schema`) — one row per
Open Positions **per-ticker aggregate** row plus **one CASH row per account**
(Cash Ops `Total`, D22), per surviving account (latest `fetched_at` per
`account_id`, D18). Child lot rows are skipped by the parser (D4) before this
mapping runs; the aggregate carries the per-ticker total, real name, and
`Category`.

The two row-kinds (EQUITY aggregate rows + the per-account CASH row) share all 9
schema columns; the columns they draw from the same source are identical:

| Column | EQUITY (aggregate row) | CASH (D22) | Notes |
|---|---|---|---|
| `fetched_at` | raw row `fetched_at` | raw row `fetched_at` | UTC; latest per `account_id` (D18) |
| `account_id` | `XtbOpenPosition.account_id` | `XtbReport.account_id` | R1 `Account number` on each sheet |
| `position_type` | literal `"EQUITY"` | literal `"CASH"` | CASH mirrors IBKR's holding |
| `label` | `XtbOpenPosition.ticker` | `"CASH {account_ccy}"` | Ticker is the stable identifier (D4, D12) |
| `asset_class` | `XtbOpenPosition.category` | literal `"CASH"` | e.g. `ETF`; populated on the aggregate row (empty on child lots — why D4 uses aggregates); resolves `category`→`asset_class` |
| `security_value` | `XtbOpenPosition.value` | `XtbReport.free_cash` | Fernet-encrypted; per-ticker total from the aggregate row, account-ccy, rounded 2dp (D11) |
| `security_ccy` | `XtbReport.account_ccy` | `XtbReport.account_ccy` | summary-block Currency (D5); not a hardcoded literal |
| `isin` | `""` (empty) | `""` (empty) | no ISIN in the new format (D12); empty string, IBKR/T212 convention |
| `description` | `XtbOpenPosition.instrument` | `"Cash {account_ccy}"` | real instrument name on the aggregate row (e.g. `Core S&P 500`); child lots carry a numeric ID instead (D4) |

† **D22 — CASH from Cash Ops `Total` under a full-history constraint.** The
`Total` is the Σ of the cash ops over the report's `Date from → Date to`
window; it equals the account's free-cash balance **only when the export
covers the full account history from a zero opening balance** (sample:
`10000 − 12537.14 + 5040.05 + 100.01 − 19 − 1000 = 1583.92`, subaccount
transfers netting to 0). A partial-window export silently understates cash.
**XTB setup constraint (documented in `docs/brokers/xtb.md`): reports must be
exported with `Date from` set to the account opening date (full history).** No
in-file detection of a truncated export is reliable, so there is no heuristic
guard (YAGNI); the constraint is the control. Invested value stays in Open
Positions, so account equity = this CASH row + Σ open positions (the aggregate rows).

**`xtb_cdc_normalized`** (`cdc_events_normalized_schema`) — one row per Cash
Operations row (Total rows excluded, subaccount transfers filtered — D7/D10),
from the **shared bronze** `xtb_snapshot` raw (D17), all payloads (no
latest-per-account for CDC — D18). Trade rows are enriched from Closed
Positions via `position_id` (D8).

| Column | Populated from | Populated for | Notes |
|---|---|---|---|
| `fetched_at` | raw row `fetched_at` | all rows | from the shared-bronze `xtb_snapshot` raw row |
| `broker` | literal `"XTB"` | all rows | |
| `account_id` | `XtbCashOperation.account_id` | all rows | R1 `Account number` |
| `event_id` | `XtbCashOperation.operation_id` | all rows | CDC dedup key (D9) |
| `source` | raw row `source` | all rows | `"XTB_REPORT"` (D17) |
| `event_type` | D6 map of `operation_type` | all rows | INTEREST / TAX / TRADE / TRANSFER / DEPOSIT / … |
| `raw_event_type` | `XtbCashOperation.operation_type` | all rows | raw `Type` text |
| `event_datetime` | `XtbCashOperation.time` | all rows | ISO-8601 UTC string; Excel serial decoded via `from_excel` + UTC (D3) |
| `security_ccy` | `XtbReport.account_ccy` | all rows | all Cash Ops are in account ccy (D5) |
| `instrument_ccy` | `null` | all rows | XTB exposes no per-instrument trading currency |
| `cash_amount` | `XtbCashOperation.amount` | all rows | Fernet-encrypted; signed cash impact; rounded 2dp (D11) |
| `settle_date` | `XtbClosedPosition.close_time` | TRADE sell rows only | join via `position_id` (D8); ISO string; `null` otherwise |
| `ticker` | `XtbCashOperation.ticker` | trade rows | empty string on non-trade rows |
| `isin` | `""` (empty) | all rows | no ISIN (D12) |
| `description` | `XtbCashOperation.comment` | all rows | carries trade qty/price text + transfer FX details |
| `quantity` | parsed from `comment` | trade rows | comment `OPEN/CLOSE {side} {qty} @ {price}` → `{qty}`; Fernet-encrypted; `null` otherwise |
| `price` | parsed from `comment` | trade rows | `{price}` from the same comment pattern; Fernet-encrypted; `null` otherwise |
| `side` | parsed from `comment` | trade rows | the `{side}` token after OPEN/CLOSE (position direction: BUY/SELL); `null` otherwise |
| `gross_amount` | `sale_value − purchase_value` | TRADE sell rows only | from `XtbClosedPosition` (D8); Fernet-encrypted; `null` (incl. purchase rows) otherwise |
| `fee_amount` | `XtbClosedPosition.commission` | TRADE sell rows only | join via `position_id` (D8); Fernet-encrypted; `null` otherwise (purchase rows get no fee) |
| `tax_amount` | `null` | all rows | tax is its own TAX event (in `cash_amount`); no breakdown column; Fernet-encrypted null |
| `target_fx_rate` | `null` | all rows | do NOT parse `Exchange rate:X` (D7); `normalize_currency` fills it via `CurrencyConverter`; Fernet-encrypted null |
| `target_value` | `null` | all rows | filled by `normalize_currency` |
| `target_ccy` | `null` | all rows | set to `EUR` by `normalize_currency` |

**Acceptance:** snapshot produces one row per open position aggregate (per-ticker) **per account** plus one CASH row per account (latest payload per `account_id`; CASH from `free_cash`, D22); CDC produces one event per cash op (subaccount transfers excluded; Total row excluded from events but read into `free_cash`) from the shared bronze, trades carry qty/price/side/fee, currency transfer kept as TRANSFER with `target_fx_rate` null (D7), no Total/UNKNOWN events, re-running dedups by `event_id`.

### Stage 3 — `connector.py` + CDC-YAGNI removal (small edits)

- `extract_holdings`: use `ticker` as `identifier` and `Holding.identifier` (no ISIN). `currency = security_ccy` (account ccy from D5). Map `position_type`/`description` from the new fields.
- **Shared bronze (D17):** `fetch.py` — remove `fetch_cdc`; `fetch_snapshot` writes `source="XTB_REPORT"` (full workbook). Keep `fetch_kwargs`/`args.xtb_file` and the `fetch_connector` XTB loop (one raw row per file into `xtb_snapshot` raw; no CDC fetch). `connector.py` — remove `fetch_cdc`/`fetch_cdc_kwargs`; add `cdc_raw_layer = "snapshot"`. `base.py` — add `cdc_raw_layer: str = "cdc"` (default) so `transform_connector` reads `get_raw_path(name, connector.cdc_raw_layer)` for the CDC transform. `run.py` — `transform_connector` uses `connector.cdc_raw_layer` for the CDC layer's raw source; `cmd_run_connector` validation unconditional (D14).
- **Migration:** add `pipeline/migrations/` script to purge legacy XTB raw rows (`source` in `{"OPEN POSITION","CASH OPERATION"}`) — the new parser handles only the new format and transforms gate on `source=="XTB_REPORT"`, so legacy rows are skipped anyway; the migration is cleanup. The orphaned `xtb_cdc` raw table is no longer written or read (D17).
- **Remove the `cdc_supported` flag (D14, after Stage 2):** delete the field from `base.py` + all three connector classes, simplify `run.py:718` to `tables = [f"{connector.name}_snapshot", f"{connector.name}_cdc"]` (unconditional), drop the `test_cdc_supported_*` tests and the `Fake.cdc_supported` attr. No "set True" intermediate.
- **Remove `_OPTIONAL_CDC_BROKERS` (D15):** in `consolidate_cdc.py`, iterate `connectors.all()` for candidates, keep `_REQUIRED_CDC_BROKERS = ["ibkr","trading212"]` as the only hardcoded list (raise on missing/empty for those; skip others). Update `test_consolidate_skips_xtb_*`.
- **Complete ADR 0094's `*_ENABLED` cleanup (D16):** drop the dead `enabled_env_var = "FAKE_ENABLED"` attr from `FakeConnector` (`test_connector_registry.py:15`) alongside the `Fake.cdc_supported` removal above. The `docs/configuration.md:62` `XTB_ENABLED` line is removed in Stage 6 (docs).
- **Fix the upload landing-zone path (D20):** `pipeline/storage.py` — `S3Backend.staging_path` and `S3StorageConfig.staging_path` drop the `staging`/`staging_demo` segment, emit `{prefix}/{connector}_uploads/{filename}` (→ `pipeline/xtb_uploads/<file>` prod, `pipeline_demo/xtb_uploads/<file>` demo). Terraform — `xtb_staging_prefix = "pipeline/xtb_uploads/"` in `terraform/prod/main.tf:547`, `"pipeline_demo/xtb_uploads/"` in `terraform/demo/main.tf:564`. Update `tests/test_storage_config.py` path assertions (449/456/463/714/733/748/763) to the new scheme. No S3 migration.

### Stage 4 — Analytics date handling (verify, likely no change)

- The transform now emits ISO `event_datetime`, so `cdc_tables.py`'s strptime chain should work unchanged. **Add a regression test** that an XTB CDC row survives `_add_period_columns` (the old failure was the serial-string `"46236.875"`; the fix is emitting a real datetime).

### Stage 5 — Tests

- Update `tests/test_xtb_connector.py` and `tests/fixtures/xtb.py` to the new format (Stage 0 fixture).
- Cover: 3-sheet parsing, Excel-serial decoding (cash-op `time` + closed `close_time`; open positions carry no date field after the Stage 1 narrowing), aggregate-vs-child distinction, **per-ticker aggregate holdings (child lots skipped; `category` populated on holdings)**, **CASH holding from Cash Ops `Total` (D22)** — one CASH row per account (`position_type=CASH`, `security_value=free_cash`); absent when the sheet has no `Total` row; and Σ CDC `cash_amount` equals `free_cash` under full-history, Total-row exclusion from events, subaccount-transfer filtering, currency-conversion Transfer kept with `target_fx_rate` null (D7), trade enrichment (commission on sell row only), event-type map (interest/tax/sell), CDC dedup on re-upload, 2dp rounding, open→closed lifecycle (a position open in one snapshot, closed in the next, fee captured once). **Multi-account (D18):** two accounts in raw → both survive snapshot (latest payload per `account_id`); a re-upload of the same account supersedes the older snapshot; CDC unions across accounts with `event_id` dedup. **Shared bronze (D17):** CDC produced from `xtb_snapshot` raw with no `xtb_cdc` raw. Confirm no test references dropped fields (`net_profit`, `gross_profit`, `swap`, `rollover`, `margin`, `open_price`, `current_price`, `volume`, `side` on positions).
- Keep the integration test against the real xlsx sample.

### Stage 6 — Checks + docs

- Run `.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .`
- Run `.venv/Scripts/python -m pyright pipeline/ tests/`
- Run `.venv/Scripts/python -m pytest tests/ -q -rf` (re-run after lint auto-fixes)
- Update `docs/brokers/xtb.md` (remove the "only sample data verified" caveat; document the 3-sheet format, Excel-serial dates, account-currency-from-summary-block, subaccount-transfer filtering, ticker-as-identifier, no-ISIN limitation, shared-bronze CDC (one raw row → snapshot + CDC silvers), multi-account latest-per-account transform, EventBridge file-arrival trigger with a single `--xtb-file` per run, **CASH holding from Cash Ops `Total` (D22) and the full-history-export constraint (`Date from` = account opening)**).
- Remove the stale `docs/configuration.md:62` `XTB_ENABLED` line (D16 — ADR 0094 already removed the env var).
- **Record an ADR** (invoke `manage-adr` skill) for the rewrite: context = new XTB format, decision = rewrite parser/transform, 3-sheet model, Cash Ops as sole CDC source + Closed Positions fee lookup, subaccount-transfer filtering, ticker identifier, **shared bronze (D17)**, **multi-account latest-per-account transform (D18)**, **EventBridge file-arrival trigger (D19)**, **upload-path + trigger-prefix fix (D20)**, **CASH holding from Cash Ops `Total` under the full-history-export constraint (D22)**. Supersede any prior XTB ADRs whose decisions this reverses (check 0047, 0048, 0102's XTB instrument-ccy deferral). **Partially supersede ADR 0087**: its `cdc_supported` flag (decision #2) and `_OPTIONAL_CDC_BROKERS` optional-list (decision #3's mechanism) are removed by D14/D15 — per-connector CDC validation is now unconditional and consolidate derives candidates from the registry. **Partially supersede ADR 0100** for XTB: per-source `filter_latest_snapshot` is replaced by per-`account_id` latest (D18) because XTB raw lacks `account_id` and multiple accounts share one `source`. Carry forward 0087's surviving guarantees in the new ADR's Constraints: the required-non-empty gate for `_REQUIRED_CDC_BROKERS = ["ibkr","trading212"]` (raise on missing/empty) and `NON_EMPTY_REQUIRED` (unchanged; `xtb_cdc` stays excluded because the prod daily-schedule run may not produce it — D21).

---

## 4. Resolved verification items

All items below were promoted to binding decisions in §1 — nothing remains open:

| Former item | Resolved by |
|---|---|
| O1 — what `cdc_supported` gated | D14 (corrected rationale: CDC was broken, not disabled) |
| O2 — commission source | D8 (user-confirmed: Closed Positions only) |
| O3 — aggregate Net Profit % | D4 (map `Value` only, not `%`) |
| O4 — aggregate OpenPrice | D4 (use aggregate rows; `open_price` is a dropped field either way) |
| O5 — Cash Ops currency | D5 (all in account_ccy) |
| O6 — Excel epoch | D3 (1900, sample-confirmed) |
| O7 — `NON_EMPTY_REQUIRED` | D21 (`xtb_cdc` stays excluded) |
| O8 — transfer target currency | D7 (don't parse the rate; `CurrencyConverter` fallback) |

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
2. **Subagent B — Stage 2+3**: rewrite `transform.py` (multi-account latest-per-account snapshot + shared-bronze CDC, D17/D18), edit `connector.py`/`fetch.py`/`base.py`/`run.py`/`storage.py` + the two `xtb_staging_prefix` Terraform values (shared bronze + `cdc_raw_layer` + unconditional validation + upload-path fix, D14/D17/D20), add the legacy-raw migration, add transform tests. Depends on A.
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