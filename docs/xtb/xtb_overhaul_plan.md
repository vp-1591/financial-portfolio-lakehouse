# XTB Connector Overhaul Plan

**Goal:** Overhaul the XTB connector to parse the new XTB Excel report format
(3 sheets: Open Positions, Cash Operations, Closed Positions) and feed holdings
+ Change Data Capture (CDC) events into the medallion pipeline correctly.

**Inputs (this session's artifacts, tracked under `docs/xtb/`):**
- [xtb_sample_dump.txt](xtb_sample_dump.txt) — full cell dump of the anonymized sample.
- `dump_xtb_xlsx.py`, `verify_xtb.py` — extraction/verification scripts (kept to re-run the dump/checks).

**Sample file:** `docs/xtb/xtb-report-sample/PLN_12345678_2006-01-01_2026-08-03.xlsx`

This plan is the blueprint for next-session subagent implementation. It is
self-contained.

**How to use this plan:** §1 lists the binding decisions (what and why). §3
lists the implementation stages (how). §4 assigns stages to subagents. Read §1
for context, then your assigned stage in §3.

---

## 1. Decisions (binding for implementation)

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | **Rewrite `parser.py` + `transform.py`** from scratch. Keep `fetch.py`, `connector.py`, `__init__.py`. | ~70% of the parsing surface changed. Patching leaves dead branches and the core abstractions (header-set match, value-below-label, aggregate-by-symbol) don't map to the new structure. |
| D2 | Parse **3 sheets**. Open Positions → holdings/snapshot. Cash Operations → **all** CDC events. Closed Positions → **fee-enrichment lookup only**, keyed by Position ID; never emitted as its own event. | Cash Operations is the canonical cash ledger and carries trades (with qty/price in the comment) plus deposits/interest/taxes/transfers. Closed Positions adds commission only. One event source, so no double-count. |
| D3 | **Dates decoded to timezone-aware UTC datetimes** in the parser; transform emits ISO datetimes so downstream analytics (`cdc_tables.py`) is unchanged. | The sample's date cells are date-*formatted*, so openpyxl auto-converts them to naive `datetime` (verified on all 3 sheets — e.g. Cash Operations `Time` reads `datetime(2026, 8, 3, 6, 0)`, not a serial); attach `tzinfo=UTC` at the parser boundary. The old ISO-string code path is no longer reachable. |
| D4 | Open Positions: use the **per-ticker aggregate row** (the group-header row XTB's report includes per instrument — real name in `Instrument`, total `Value`, non-empty `Category`, empty `Type`) for holdings; **skip the child lot rows** (the per-position detail rows under each aggregate) in the parser (never stored). Use **Ticker** as label/identifier; the aggregate's `Instrument` (real name) → `description`, `Category` → `asset_class`. | The snapshot schema is instrument-level (`label`=ticker; no lot/position-id/cost-basis column), so child lots have nowhere to be stored and would duplicate the aggregate total. Child lots also carry an empty `Category` and a numeric ID in `Instrument`, so only the aggregate row supplies the real name and `asset_class`. Matches the old parser and IBKR; CDC trades come from Cash Operations (D2). |
| D5 | Account currency = the Currency column of the Open Positions summary block (Product\|Metric\|Amount\|Currency), read in `parse_report`. All Cash Operations amounts are in this account currency; `security_ccy = account_ccy` (not a hardcoded literal). No per-row Currency column on Cash Operations. | Sample summary-block Currency reads `PLN`; no per-row Currency column on Cash Operations. |
| D6 | Updated event-type map: `Free funds interest`→INTEREST, `Free funds interest tax`→TAX, `Stock sell`/`Stock purchase`→TRADE (with `Open position`/`Close position` kept as TRADE aliases — see Stage 1b guard 10), `Transfer`→TRANSFER, `Deposit`→DEPOSIT, `Withdrawal`→WITHDRAWAL, `Dividend`→DIVIDEND, `Fee`→FEE, `Correction`/`Profit/loss adjustment`→ADJUSTMENT. Unknown types map to UNKNOWN and are retained, but should never occur once the map is complete. | The current map misses `Free funds interest`; it has `Stock sale` but not `Stock sell`, and lacks `Subaccount transfer`. `Open position`/`Close position` are kept as harmless TRADE aliases because a report variant may emit them instead of `Stock purchase`/`Stock sell`; without the alias they would fall through to UNKNOWN and drop trades silently. |
| D7 | **Filter out `Subaccount transfer`** rows entirely (internal moves between the trading and investment-plan subaccounts; net zero, clutter). **Keep currency-conversion `Transfer`** as a TRANSFER event with `target_fx_rate` left **null** — `normalize_currency` converts `cash_amount` (in `account_ccy`) to EUR via `CurrencyConverter` (normalize.py:151-155), same as T212/IBKR. Do **not** parse `Exchange rate:X`. It is the `account_ccy`→destination rate, which equals the pipeline's `account_ccy`→EUR `target_fx_rate` only when the destination is EUR. Non-EUR destinations exist, so the broker rate is unsafe. `target_ccy` is always EUR (pipeline-set, never parsed). | User-confirmed: subaccount moves are internal noise; the currency-conversion transfer is a real outbound transfer (non-EUR destinations confirmed). `target_ccy` is always EUR and the destination currency is never stored, so the pipeline only needs `cash_amount` (in `account_ccy`) plus an `account_ccy`→EUR rate, which `CurrencyConverter` supplies regardless of destination. Matches the XTB/IBKR/T212 pattern (all set `target_fx_rate` null). |
| D8 | Trade rows enriched from Closed Positions via Position ID: `Commission`→`fee_amount` on the **closing (`Stock sell`) row only**. Opening (`Stock purchase`) row gets no fee from the lookup. `Swap`/`Rollover`/`Margin`/`Open/Close Conversion Rate` are **dropped** — `fee_amount` = Commission only. | User-confirmed: commission appears only in Closed Positions (no separate Cash Operations commission row). One fee per round-trip, no double-count; matches "open positions have no commission" (commission recorded at close). The CDC schema has no columns for swap/rollover/margin/conversion rate, so they are dropped, not folded into `fee_amount` (would conflate commission with financing/position costs). The raw layer preserves the full xlsx bytes. |
| D9 | **Open→closed lifecycle + CDC selection:** CDC takes the **latest payload per `account_id`** at the transform — the same selection as snapshot (D18, keyed on `fetched_at`), **not** a union of all uploaded payloads. Under the full-history export assumption (D22), the latest report per account contains that account's complete event log, so no cross-file union is needed and per-run work is O(accounts), not O(all uploaded files). `dedup_cdc_events` is retained as a safety net keyed on `(event_type, event_id, account_id)` (ADR 0105 parity; `account_id` added so same-ID events from different accounts are not collapsed). Snapshot stays latest **per account** (D18 — not `filter_latest_snapshot`, which collapses distinct accounts). Holdings (state) and CDC (event log) are distinct — a position moving from Open Positions to Closed Positions is correct lifecycle, not double-count. | Re-uploads are full-history, so latest-per-account supersedes the older payload (idempotent, no union needed); `account_id` in the dedup key keeps same-ID events from different accounts distinct. Latest keys on `fetched_at`, so a re-uploaded *older* report could supersede a newer one — a known staleness tradeoff shared with snapshot (keying on the report's `Date to (UTC)` content time would close it for both; deferred). |
| D10 | Exclude **Total rows** (Cash Operations `Total`, Closed Positions `Profit/loss` total, Open Positions summary block) from **CDC events**. The Cash Operations `Total` is still **read** by the parser into `XtbReport.free_cash` (D22) for the snapshot CASH holding — it is a summary, not an event. | Total rows were previously emitted as an UNKNOWN operation. |
| D11 | Round `Value` (open), `Amount` (cash), and the closed-position `Commission`, `Purchase value`, `Sale value` to **2 decimals on read** to eliminate IEEE-754 floating-point rounding artifacts (940.7399999999991 → 940.74). | Benign float noise in the source xlsx. |
| D12 | Identifier = **Ticker** (no ISIN available). ADR 0002 (broker-native identifiers) supports this. | New format has no ISIN column on any sheet. |
| D13 | **Adopt openpyxl** as the XLSX library for the parser rewrite. Load via `openpyxl.load_workbook(BytesIO(data), data_only=True)` (see Stage 1 helpers), access sheets by name (`wb["Cash Operations"]`, etc.), iterate `ws.iter_rows(values_only=True)`. **Risk (accepted, documented):** `data_only=True` returns `None` for formula cells lacking a cached value; if the XTB exporter ever writes the Cash Ops `Total` cell as a `=SUM(...)` formula without a cache, `free_cash` reads `None` and the CASH holding (D22) is skipped. The sample's `Total` cell is a literal value, so this is low-risk; if it occurs, fall back to summing `cash_operations` amounts in the parser. No code change now. Drop the manual `read_shared_strings` / `read_sheet_rows` / `sheet_paths_by_name` zipfile-XML helpers. | Cleaner than raw ZIP/XML: native sheet/cell access, shared-strings handled internally, number/date cell handling, easier to maintain. This plan assumes `openpyxl==3.1.5` is already added to `pyproject.toml` pipeline deps and installed in the venv. |
| D14 | **Remove the `cdc_supported` flag** outright — no intermediate "set True" step. Drop it from the `BrokerConnector` protocol (`base.py`) and all three connector classes; simplify `run.py:718` to `tables = [f"{name}_snapshot", f"{name}_cdc"]` (unconditional). Drop `test_cdc_supported_*` and the `Fake.cdc_supported` attr. Land after Stage 2. | The flag existed only to mark XTB `False`, but XTB CDC production was **broken**, not intentionally disabled — `fetch_cdc` is never invoked on the prod trigger path (D19) and `xtb_cdc` raw was never written, so `cdc_supported=False` masked a dead code path. D17 (shared bronze) plus the transform rewrite make `transform_cdc` produce `xtb_cdc` for the first time, so unconditional validation is now correct. |
| D15 | **Remove `_OPTIONAL_CDC_BROKERS`** from `pipeline/normalized/consolidate_cdc.py`. Derive the candidate broker set from the registry (`connectors.all()` → `c.name`) instead of a hardcoded list; retain only `_REQUIRED_CDC_BROKERS = ["ibkr","trading212"]` as the ADR 0087 required-non-empty quality gate. For each candidate, try to read `normalized/{name}_cdc`: if the broker is in `_REQUIRED_CDC_BROKERS`, raise on missing/empty (unchanged); otherwise skip missing/empty (log at DEBUG). Behavior is identical to today for ibkr/t212/xtb, but the `_OPTIONAL` list and the `"xtb"` literal are gone — the candidate set comes from the single source of truth (the registry). Verify no import cycle: `connectors.base` already imports `pipeline.normalized.consolidate` (for `Holding`), so `consolidate_cdc` → `connectors.registry` must not close a loop (import `registry` lazily inside the function if needed). Update `test_consolidate_skips_xtb_*` to assert the skip still happens via the registry path. | Removes the redundant parallel "XTB is special" encoding in the consolidate layer (Don't Repeat Yourself (DRY)). `_OPTIONAL_CDC_BROKERS = ["xtb"]` is the only non-required entry; folding "non-required" into "skip if absent" via the registry deletes the list. The required-vs-optional *policy* stays (`_REQUIRED_CDC_BROKERS`, the ADR 0087 gate). XTB stays non-required because the prod daily schedule does not run XTB, so `xtb_cdc` may legitimately be missing. |
| D16 | **Complete ADR 0094's `*_ENABLED` cleanup** — sweep the two stale references it missed. (1) `tests/test_connector_registry.py:15`: drop `enabled_env_var = "FAKE_ENABLED"` from `FakeConnector` — the `BrokerConnector` protocol no longer declares `enabled_env_var` (ADR 0094 removed it) and nothing reads it, so the attr is dead. (2) `docs/configuration.md:62`: remove "Required environment variable: `XTB_ENABLED` (optional, enabled by default)." — the `XTB_ENABLED` env var no longer exists (ADR 0094), so the line documents a removed feature. | ADR 0094 deleted the `*_ENABLED` env vars, `is_enabled()`, and the `enabled_env_var` protocol attr but left two stragglers: `test_connector_registry.py:15` and `docs/configuration.md:62`. Both are one-line removals; no new ADR — this finishes 0094's execution. |
| D17 | **Shared bronze — transform CDC from the snapshot raw, not a separate CDC fetch.** One fetch per file writes a single raw row to `xtb_snapshot` raw with `source="XTB_REPORT"` carrying the full workbook (all 3 sheets). Delete `fetch.py:fetch_cdc`, `XtbConnector.fetch_cdc`/`fetch_cdc_kwargs`, and the `xtb_cdc` raw table + `xtb_cdc_raw_schema`. `transform_cdc` reads from the same `xtb_snapshot` raw. Add `cdc_raw_layer: str = "cdc"` to `BrokerConnector` (`base.py`); XTB overrides `cdc_raw_layer = "snapshot"` so `transform_connector` reads `get_raw_path(name, cdc_raw_layer)` for the CDC transform. | `fetch_snapshot` and `fetch_cdc` currently store **byte-identical** xlsx in two raw tables (same `payload`, same `payload_hash`, only `source` differs) — pure duplication. One xlsx carries all 3 sheets, so one bronze row is enough; both silvers derive from it. Eliminates the dead `fetch_cdc` path and makes CDC production real (fixes the D19 finding that CDC never reached gold). |
| D18 | **Multi-account transform semantics — do NOT use `filter_latest_snapshot` for XTB.** It keys on `source` alone and `account_id` is not in `RAW_SCHEMA` (only parsed from the payload), so it collapses distinct accounts (e.g. `PLN_123…` + `EUR_456…`) to the single latest row. Instead: `transform_snapshot` iterates **all** `source=="XTB_REPORT"` rows, parses each, keeps the latest `fetched_at` **per `account_id`**, and emits per-ticker aggregate holdings for every surviving account. `transform_cdc` keeps the latest `fetched_at` per `account_id` too (D9) — same selection as snapshot, not a union of all payloads; `dedup_cdc_events` is a safety net keyed on `(event_type, event_id, account_id)`. | Multiple accounts must coexist; latest-per-account avoids stale-snapshot double-count of one account while preserving every account. CDC uses the same selection (D9) — under full-history (D22) the latest report per account is the complete event log, so unioning uploads is unnecessary; `account_id` in the dedup key keeps same-ID events from different accounts distinct. Both share the `fetched_at`-keyed staleness tradeoff noted in D9. |
| D19 | **File-arrival trigger (prod path the overhaul must fit).** Prod: EventBridge S3 Object-Created on the XTB upload prefix (`pipeline/xtb_uploads/`, D20) → Step Functions `orchestrator` `RunConnectors` Map (`file_arrival_connectors = ["ibkr","trading212","xtb"]`, concurrency 3) → `ConsolidateAllocate` (`run-consolidate-analytics`). The trigger passes a **single** `--xtb-file` S3 URI per execution; multi-account accumulates across triggers into the shared `xtb_snapshot` raw table and is unioned per-account at transform (D18). The daily schedule (`schedule_connectors = ["ibkr","trading212"]`) excludes XTB, so `xtb_cdc` may be absent on that path (informs D15/D21). The `fetch_connector` XTB loop already iterates `--xtb-file` (supports N files in one CLI call too). | Documents the actual prod trigger and confirms CDC must come via shared bronze (D17) — the trigger never fetches CDC, so a separate CDC fetch is not how XTB CDC reaches gold. |
| D20 | **Fix the XTB upload landing-zone path so the EventBridge trigger fires (both environments).** Upload path becomes `{env_prefix}/xtb_uploads/<file>` (prod `pipeline/xtb_uploads/`, demo `pipeline_demo/xtb_uploads/`), replacing `{env_prefix}/{staging\|staging_demo}/xtb/`. `S3StorageConfig.staging_path` + `S3Backend.staging_path` (storage.py:115-120, 195-203) drop the `staging`/`staging_demo` segment and use `{prefix}/{connector}_uploads/{filename}`. Align `xtb_staging_prefix` → `pipeline/xtb_uploads/` in `terraform/prod/main.tf:547` and `pipeline_demo/xtb_uploads/` in `terraform/staging/main.tf:564`. No S3 migration (trigger never fired in either environment; nothing consumed the old uploads). Ships in this overhaul PR. | The EventBridge rule filters `object.key` by prefix, but the rule prefix missed the environment top-level segment (`S3_DEFAULT_PREFIX="pipeline"`) that `S3Backend` always prepends — the actual key `pipeline/staging/xtb/<file>` never matched the rule prefix `staging/xtb/`. Identical bug in demo. The `staging`/`staging_demo` segment redundantly re-encoded the environment (already carried by `pipeline`/`pipeline_demo`). And `staging` collided with `--mode staging`. Dropping it and renaming `xtb`→`xtb_uploads` fixes the match in both environments and removes the collision. |
| D21 | **`xtb_cdc` stays excluded from `quality.py` `NON_EMPTY_REQUIRED`.** The prod daily schedule (`schedule_connectors = ["ibkr","trading212"]`) does not run XTB, so `xtb_cdc` may legitimately be absent or empty on that path; requiring it would break scheduled runs with no XTB activity. | Three distinct CDC-non-empty gates agree XTB CDC is never mandatory: per-connector post-transform validation (D14), consolidate required/optional (D15), and `quality.py` `NON_EMPTY_REQUIRED` (this decision). The prod daily schedule not running XTB is why `xtb_cdc` may legitimately be absent. |
| D22 | **Emit a CASH holding (per account) from the Cash Operations `Total` row.** The parser reads the `Total` row's `Amount` into `XtbReport.free_cash` (2dp) before excluding it from `cash_operations` (it is a summary, not an event — D10); `transform_snapshot` emits one CASH row per account (`position_type=CASH`, `label="CASH {ccy}"`, `security_value=free_cash` encrypted, `security_ccy=account_ccy`) under the D18 latest-per-account filter. XTB has no API, so the Excel report is the only cash source. **Setup constraint:** reports must be exported with `Date from` set to the account opening date (full history from a zero opening balance); a partial-window export silently understates cash and cannot be detected in-file. | The `Total` is the sum of cash operations over the report window; it equals the free-cash balance only under that full-history setup (sample: `10000 − 12537.14 + 5040.05 + 100.01 − 19 − 1000 = 1583.92`, subaccount transfers netting to 0). No in-file detection of a truncated export is reliable, so the control is the setup rule, not a heuristic guard (You Aren't Gonna Need It (YAGNI)). Restores the CASH holding the old parser synthesized; account equity = this CASH row + sum of open positions. Alternatives rejected: dropping CASH causes a dashboard regression. Summing CDC events yields the same number as `Total` with more effort and the same constraint. The Open Positions summary `Value` is the wrong quantity — it is the sum of open positions, not cash. |

*Traceability: O1–O8 resolved by D14/D8/D4/D4/D5/D3/D21/D7.*

---

## 2. Fixture corrections (APPLIED — sample is ready as a test fixture)

The anonymized sample had internal per-row inconsistencies; all were fixed in
pass 2 and verified. Summary of what changed:

1. **R012 Net Profit %** 8.31 → 8.32; **R011 aggregate** 29.76 → 29.77 (sum-of-children).
2. **R011 aggregate Open price** 105 → 106.36 (volume-weighted).
3. **Cash Operations ID order** — the 3 added open-order IDs made time-monotonic.
4. **Buy/sell reorder** — pos 1334567890 purchase moved 07-20 → 08-02 08:00, ID
   900035425 → 900045000; running balance no longer goes negative on a purchase.

Note: corrections 1–2 only matter if a test asserts on `Net Profit %`. Per D4 we
map `Value` only (not `%`, `Net Profit`, or `Gross Profit`), so they affect
neither processing nor tests — fixed for fixture correctness anyway. Correction 3
(Open price) matters only if a test asserts the aggregate Open price; correction 4
(ID order) matters if a test asserts Cash Operations IDs or relies on ID-time monotonicity.

---

## 3. Implementation stages (subagent-sized, sequential dependencies)

> Each stage lists files, the change, signatures, and acceptance criteria.
> Run the project checks after stages that touch code (see Stage 6).

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
    product: str  # "Investment Plan" | "My Trades" (group label; not mapped)
    instrument: str  # real instrument name on the aggregate row -> description
    ticker: str  # reliable identity key -> label / identifier
    category: str  # ETF on the aggregate row -> asset_class (empty on child lots)
    value: float  # account-currency market value (aggregate row Value = per-ticker total), 2dp -> security_value


@dataclass(frozen=True)
class XtbClosedPosition:
    # Fee-enrichment lookup only (D2); never emitted as its own event.
    position_id: str  # join key to Cash Operations trade rows
    commission: float  # -> fee_amount on the closing (Stock sell) row
    purchase_value: float  # -> gross_amount (sale_value - purchase_value)
    sale_value: float
    close_time: datetime  # -> settle_date on the closing row (UTC)


@dataclass(frozen=True)
class XtbCashOperation:
    account_id: str
    operation_type: str  # raw "Type" text -> raw_event_type / event_type
    ticker: str  # populated on trade rows
    time: datetime  # UTC -> event_datetime
    amount: float  # account currency, 2dp -> cash_amount
    operation_id: str  # -> event_id (CDC dedup key)
    comment: str  # -> description; carries trade qty/price + transfer FX details
    position_id: str  # join key to Closed Positions (trade rows only)


@dataclass(frozen=True)
class XtbReport:
    account_id: str
    account_ccy: str  # summary-block Currency (D5)
    open_positions: list[
        XtbOpenPosition
    ]  # per-ticker aggregate rows (child lots skipped)
    closed_positions: list[XtbClosedPosition]
    cash_operations: list[XtbCashOperation]  # Total/summary rows excluded from events
    free_cash: (
        float | None
    )  # Cash Operations Total (R-last) -> snapshot CASH holding (D22); None if no Total row
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
- Date decoding: the sample's date cells are date-*formatted*, so openpyxl auto-converts them to naive `datetime` — attach `tzinfo=UTC` at the parser boundary (the path every sample date takes). For a cell read as a raw numeric serial (not observed in the sample, but retained as a defensive branch), pass it through `openpyxl.utils.datetime.from_excel` (handles the 1900 epoch and the 1900-02-29 leap-year bug, so no hand-rolled `excel_serial_to_datetime`) then attach `tzinfo=UTC`.
- `parse_report(data: bytes, account_id_override: str | None = None) -> XtbReport` — top-level; reads all 3 sheets.
- XLSX access via **openpyxl** (D13): `wb = openpyxl.load_workbook(BytesIO(data), data_only=True)`; select sheets by name; iterate `ws.iter_rows(values_only=True)`. **Risk (accepted, documented — D13):** `data_only=True` returns `None` for formula cells lacking a cached value; if the XTB exporter ever writes the Cash Ops `Total` cell as a `=SUM(...)` formula without a cache, `free_cash` reads `None` and the CASH holding (D22) is skipped. The sample's `Total` cell is a literal value, so this is low-risk; if it occurs, fall back to summing `cash_operations` amounts in the parser. No code change now. The old low-level zipfile/XML helpers (`read_shared_strings`, `read_sheet_rows`, `sheet_paths_by_name`) are removed.
- Sheet discovery: `find_sheet_name` substring match survives ("OPEN POSITION"→"Open Positions", "CASH OPERATION"→"Cash Operations"); add "CLOSED POSITION"→"Closed Positions".
- Account ID: read from `Account number` (R1 of each sheet), not the dead `value_below_label("Account")`.
- Per-sheet parsers: `parse_open_positions(rows)`, `parse_cash_operations(rows)`, `parse_closed_positions(rows)`. Each extracts **only the fields on its dataclass** — do not populate columns with no normalized destination.
- Open Positions: read the account currency from the **summary block's Currency column** (`Product|Metric|Amount|Currency`) before skipping that block (the summary block is per-product, not holdings — D5). The detail header is `Product, Instrument/Position, Ticker, Category, Type, Volume, Value, Current price, Open price, Open time (UTC), …, Net Profit %, Net Profit, Gross Profit, …`. **Keep the per-ticker aggregate rows** (the group-header row XTB emits per instrument: empty `Type`, real name in `Instrument`, non-empty `Category`) as the holdings; **skip the child lot rows** (non-empty `Type` — per-position detail with no destination in the snapshot schema, and CDC trades come from Cash Operations). Extract only `product, instrument (real name), ticker, category, value`; ignore the rest. Round `value` to 2dp.
- Cash Operations: header `Type, Instrument, Ticker, Category, Time, Amount, ID, Comment, Product, Position ID`. Extract `Type, Ticker, Time, Amount, ID, Comment, Position ID` (drop `Instrument, Category, Product` — unused). Read the `Total` row's `Amount` into `XtbReport.free_cash` (2dp) before excluding it — it is a summary, not an event (D10); the snapshot transform emits it as the CASH holding (D22). Exclude rows where `Type` normalizes to `total`. Filter `Subaccount transfer` rows out here (D7) — single place (parser), so they never enter `XtbReport.cash_operations`.
- Closed Positions: header per dump; extract only `Position ID, Commission, Purchase value, Sale value, Profit/Loss, Close time` (the fee-enrichment fields). Exclude the `Profit/loss` total row.

**Acceptance:** `parse_report(fixture_bytes)` returns all three lists with only the
mapped fields populated, correct types, dates as timezone-aware UTC datetimes
(formatted cells auto-converted by openpyxl; numeric serials via `from_excel`),
`value`/`amount` rounded to 2dp, **per-ticker
aggregate rows kept as holdings (with `category` populated), child lot rows
skipped**, Total rows excluded from `cash_operations` but read into
`free_cash` (D22). Plus the Stage 1b guards.

### Stage 1b — Parser & transform guards (required)

These 10 guards are required behavior for the parser/transform rewrite.
Each is a one-line spec: a bold name, the concrete check, and a
parenthetical reason. Guards that generalize or complement an existing
decision reference it rather than duplicating it.

1. **`position_id` string coercion** — `str(cell).strip()` on both the Cash Operations and Closed Positions parsers. openpyxl may return `int` on one sheet and `str` on the other; without coercion the `position_id` join silently misses and fee/gross/settle are null on sell rows.
2. **Round all computed monetary outputs to 2dp** (generalizes D11) — explicitly include `gross_amount = round(sale_value − purchase_value, 2)` so the IEEE-754 artifact D11 targets doesn't reappear in the encrypted `gross_amount` column. (`gross_amount` is slated for future deletion per a separate gh issue — the rounding is correct while it exists; do NOT change its semantics or remove it.)
3. **Skip zero-value aggregate rows** — `if round(value, 2) == 0: continue`. Delisted/fully-sold instruments would otherwise emit a bogus 0-value holding; the old parser explicitly skipped `current_value==0`.
4. **Raise on empty/absent summary-block Currency** — if the Open Positions summary-block Currency is empty or the block is absent, raise an `XtbError` ("account currency missing from summary block"). An empty `account_ccy` breaks `normalize_currency` FX conversion downstream.
5. **Missing sheet → empty list, do not abort** — if one of the 3 sheets is absent (e.g. a new account with no Closed Positions), `parse_report` returns an empty list for that sheet rather than aborting the whole parse (which would fail snapshot AND CDC for a valid report).
6. **Warn on unparseable trade comment** — if a trade-row comment doesn't match `OPEN/CLOSE {side} {qty} @ {price}`, log a warning and leave `quantity`/`price`/`side` null. Do not silently emit a trade with nulls and no signal.
7. **Warn on sell row with no Closed Positions match** — if a `Stock sell` row's `position_id` has no match in the Closed Positions lookup (partial export), log a warning and leave `fee_amount`/`gross_amount`/`settle_date` null. Fee analytics are silently incomplete without the signal.
8. **Warn on duplicate `position_id` in Closed Positions** — if two Closed Positions rows share a `position_id`, log a warning (dict last-wins would pick an arbitrary commission, making `fee_amount` non-deterministic).
9. **Deterministic `fetched_at` tiebreaker for latest-per-account** — when two raw rows for the same `account_id` share the max `fetched_at`, break the tie deterministically (e.g. max `(fetched_at, source_file)` or stable row order) so both payloads don't survive and emit duplicate holdings for the same account/ticker. (This complements the latest-per-account selection in D9/D18.)
10. **Keep `Open position`/`Close position` as TRADE aliases in the D6 map** — the current transform maps these names; a report variant may emit them instead of `Stock purchase`/`Stock sell`. Keeping them as harmless TRADE aliases prevents UNKNOWN events from dropping trades silently. (This pairs with the D6 alias addition in §1.)

### Stage 2 — Rewrite `pipeline/connectors/xtb/transform.py`

Keep the same public signatures so the connector protocol is undisturbed:

```python
def transform_snapshot(raw: pa.Table, fernet_key: bytes) -> pa.Table
def transform_cdc(raw: pa.Table, fernet_key: bytes) -> pa.Table
```

**`transform_snapshot` (Open Positions → snapshot_normalized_schema, multi-account — D18):**
- Do **NOT** call `filter_latest_snapshot` (it keys on `source` alone and collapses distinct accounts). Iterate **all** `source=="XTB_REPORT"` rows via `iter_raw_payloads`; parse each via `parse_report` (aggregates already skipped by the parser); keep the latest `fetched_at` **per `account_id`**.
- For each surviving account payload, map the per-ticker aggregate rows: `ticker`→`label`, `value`→`security_value` (encrypt), `account_ccy`→`security_ccy`, `category`→`asset_class` (with `position_type`=`"EQUITY"`), `instrument` (real name)→`description`. `identifier` = ticker (D12). One row per ticker per account (the aggregate is already the per-ticker total — do not also emit child lots).
- Emit one **CASH holding row per account** from `XtbReport.free_cash` (D22): `position_type`=`"CASH"`, `label`=`"CASH {account_ccy}"`, `asset_class`=`"CASH"`, `security_value`=`free_cash` (encrypt, 2dp), `security_ccy`=`account_ccy`, `isin`=`""`, `description`=`"Cash {account_ccy}"`. Skip if `free_cash` is `None` (no Total row). Account equity = free cash (this row) + sum of open positions (the aggregate rows).

**`transform_cdc` (shared bronze `xtb_snapshot` raw → cdc_events_normalized_schema, with Closed Positions fee enrichment — D17):**
- Reads `xtb_snapshot` raw (via `cdc_raw_layer="snapshot"`), not a separate CDC raw. Iterate **all** `source=="XTB_REPORT"` rows via `iter_raw_payloads`; parse each via `parse_report` (one payload carries all 3 sheets); keep the latest `fetched_at` **per `account_id`** (same selection as `transform_snapshot`, D18/D9) and emit only the surviving accounts' cash operations — do **not** union all uploaded payloads.
- Build a `position_id → XtbClosedPosition` lookup from `closed_positions` (per payload).
- For each cash operation (Total rows already excluded; subaccount transfers already filtered by the parser — D7):
  - `event_id = operation_id`, `event_datetime = time` (ISO), `cash_amount = amount` (encrypt), `security_ccy = account_ccy`, `broker = "XTB"`, `raw_event_type = operation_type`.
  - `event_type` via the D6 map.
  - **Trade rows** (`Stock sell`/`Stock purchase`): populate `ticker`, parse `quantity`/`price`/`side` from the comment (`"OPEN BUY 10.0001 @ 100.00"`). For the **sell** row, join via `position_id` to Closed Positions and set `fee_amount = commission`, `gross_amount = sale_value − purchase_value`, `settle_date = close_time`. Leave the **purchase** row's fee empty (D8).
  - **Currency-conversion Transfer**: keep as a TRANSFER event with `target_fx_rate` left **null** (do **not** parse `Exchange rate:X` — D7). `normalize_currency` converts `cash_amount` (account_ccy) to EUR via `CurrencyConverter`; `target_ccy` is always EUR (pipeline-set).
- `dedup_cdc_events` on `(event_type, event_id, account_id)` (D9, ADR 0105 parity; `account_id` keeps same-ID events from different accounts distinct — a safety net, since latest-per-account already yields one payload per account).
- `encrypt_columns = ["cash_amount", "quantity", "price", "gross_amount", "fee_amount", "tax_amount", "target_fx_rate", "target_value"]` (mirror IBKR). The old `["cash_amount", "target_fx_rate", "target_value"]` left the new trade columns unencrypted; since they are `pa.binary()` in `cdc_events_normalized_schema`, `build_normalized_table`'s `cast(schema)` would fail on the plain floats. Full list required — see matrix below.

**Column mapping matrix** — the column-by-column population of both normalized
tables (every schema column accounted for; encrypted columns are
Fernet-encrypted at the transform via `encrypt_columns`/`build_normalized_table`).
See §1 D4/D8/D17/D18/D22 for the decisions these mappings implement.

**`xtb_snapshot_normalized`** (`snapshot_normalized_schema`) — one row per
Open Positions **per-ticker aggregate** row plus **one CASH row per account**
(Cash Operations `Total`, D22), per surviving account (latest `fetched_at` per
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
| `security_value` | `XtbOpenPosition.value` | `XtbReport.free_cash` | Fernet-encrypted; per-ticker total from the aggregate row, account currency, rounded 2dp (D11) |
| `security_ccy` | `XtbReport.account_ccy` | `XtbReport.account_ccy` | summary-block Currency (D5); not a hardcoded literal |
| `isin` | `""` (empty) | `""` (empty) | no ISIN in the new format (D12); empty string, IBKR/T212 convention |
| `description` | `XtbOpenPosition.instrument` | `"Cash {account_ccy}"` | real instrument name on the aggregate row (e.g. `Core S&P 500`); child lots carry a numeric ID instead (D4) |

CASH row (D22) is valid only under a full-history export — see D22.

**`xtb_cdc_normalized`** (`cdc_events_normalized_schema`) — one row per Cash
Operations row (Total rows excluded, subaccount transfers filtered — D7/D10),
from the **shared bronze** `xtb_snapshot` raw (D17), **latest payload per `account_id`** (D9/D18, keyed on `fetched_at` — same selection as snapshot, not a union of all uploads). Trade rows are enriched from Closed
Positions via `position_id` (D8).

| Column | Populated from | Populated for | Notes |
|---|---|---|---|
| `fetched_at` | raw row `fetched_at` | all rows | from the shared-bronze `xtb_snapshot` raw row |
| `broker` | literal `"XTB"` | all rows | |
| `account_id` | `XtbCashOperation.account_id` | all rows | R1 `Account number` |
| `event_id` | `XtbCashOperation.operation_id` | all rows | CDC dedup key, with `account_id` (D9) |
| `source` | raw row `source` | all rows | `"XTB_REPORT"` (D17) |
| `event_type` | D6 map of `operation_type` | all rows | INTEREST / TAX / TRADE / TRANSFER / DEPOSIT / … |
| `raw_event_type` | `XtbCashOperation.operation_type` | all rows | raw `Type` text |
| `event_datetime` | `XtbCashOperation.time` | all rows | ISO-8601 UTC string; date cell auto-converted by openpyxl (or `from_excel` for a raw serial) + UTC (D3) |
| `security_ccy` | `XtbReport.account_ccy` | all rows | all Cash Operations are in account currency (D5) |
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

**Acceptance:** snapshot produces one row per open position aggregate (per-ticker) **per account** plus one CASH row per account (latest payload per `account_id`; CASH from `free_cash`, D22); CDC produces one event per cash operation (subaccount transfers excluded; Total row excluded from events but read into `free_cash`) from the shared bronze, trades carry qty/price/side/fee, currency transfer kept as TRANSFER with `target_fx_rate` null (D7), no Total/UNKNOWN events, re-running is idempotent (latest payload per `account_id` supersedes; `dedup_cdc_events` on `(event_type, event_id, account_id)`), and cross-account same-ID events both survive. Plus the Stage 1b guards.

### Stage 3 — `connector.py` + CDC-YAGNI removal (small edits)

- `extract_holdings`: use `ticker` as `identifier` and `Holding.identifier` (no ISIN). `currency = security_ccy` (account currency from D5). Map `position_type`/`description` from the new fields.
- **Shared bronze (D17):** `fetch.py` — remove `fetch_cdc`; `fetch_snapshot` writes `source="XTB_REPORT"` (full workbook). Keep `fetch_kwargs`/`args.xtb_file` and the `fetch_connector` XTB loop (one raw row per file into `xtb_snapshot` raw; no CDC fetch). `connector.py` — remove `fetch_cdc`/`fetch_cdc_kwargs`; add `cdc_raw_layer = "snapshot"`. `base.py` — add `cdc_raw_layer: str = "cdc"` (default) so `transform_connector` reads `get_raw_path(name, connector.cdc_raw_layer)` for the CDC transform. `run.py` — `transform_connector` uses `connector.cdc_raw_layer` for the CDC layer's raw source; `cmd_run_connector` validation unconditional (D14).
- **Migration:** add `pipeline/migrations/` script to purge legacy XTB raw rows (`source` in `{"OPEN POSITION","CASH OPERATION"}`) — the new parser handles only the new format and transforms gate on `source=="XTB_REPORT"`, so legacy rows are skipped anyway; the migration is cleanup. The orphaned `xtb_cdc` raw table is no longer written or read (D17).
- **Remove the `cdc_supported` flag (D14, after Stage 2):** delete the field from `base.py` + all three connector classes, simplify `run.py:718` to `tables = [f"{connector.name}_snapshot", f"{connector.name}_cdc"]` (unconditional), drop the `test_cdc_supported_*` tests and the `Fake.cdc_supported` attr. No "set True" intermediate.
- **Remove `_OPTIONAL_CDC_BROKERS` (D15):** in `consolidate_cdc.py`, iterate `connectors.all()` for candidates, keep `_REQUIRED_CDC_BROKERS = ["ibkr","trading212"]` as the only hardcoded list (raise on missing/empty for those; skip others). Update `test_consolidate_skips_xtb_*`. Also add `account_id` to the consolidate dedup subset (`consolidate_cdc.py`: `["broker", "event_type", "event_id", "account_id"]`) so multi-account brokers (XTB, D18) don't silently drop same-ID events across accounts at consolidate — expected a no-op for IBKR/T212 (verify their event_ids are unique per account via tests).
- **Complete ADR 0094's `*_ENABLED` cleanup (D16):** drop the dead `enabled_env_var = "FAKE_ENABLED"` attr from `FakeConnector` (`test_connector_registry.py:15`) alongside the `Fake.cdc_supported` removal above. The `docs/configuration.md:62` `XTB_ENABLED` line is removed in Stage 6 (docs).
- **Fix the upload landing-zone path (D20):** `pipeline/storage.py` — `S3Backend.staging_path` and `S3StorageConfig.staging_path` drop the `staging`/`staging_demo` segment, emit `{prefix}/{connector}_uploads/{filename}` (→ `pipeline/xtb_uploads/<file>` prod, `pipeline_demo/xtb_uploads/<file>` demo). Terraform — `xtb_staging_prefix = "pipeline/xtb_uploads/"` in `terraform/prod/main.tf:547`, `"pipeline_demo/xtb_uploads/"` in `terraform/staging/main.tf:564`. Update `tests/test_storage_config.py` path assertions (449/456/463/714/733/748/763) to the new scheme. Add a precise literal-key test asserting the exact S3 object key is `s3://<bucket>/pipeline/xtb_uploads/<file>` (prod) and `s3://<bucket>/pipeline_demo/xtb_uploads/<file>` (demo) — with **no `xtb/` subfolder** between `xtb_uploads/` and the filename — so a refactor that re-introduces the connector segment (e.g. `pipeline/xtb_uploads/xtb/<file>`) fails loudly instead of silently re-breaking the EventBridge prefix match. (`LocalBackend` in `tests/local_backend.py` is test-only — there is no `--mode local` since ADR 0090, and local deployment uses MinIO via `S3Backend` — so the literal-key test targets `S3Backend`/`StorageConfig` only; the existing local-backend assertions at 714/733 force `LocalBackend.staging_path` to follow the same scheme so the test fake does not assert a stale path.) No S3 migration.

### Stage 4 — Analytics date handling (verify, likely no change)

- The transform now emits ISO `event_datetime`, so `cdc_tables.py`'s strptime chain should work unchanged. **Add a regression test** that an XTB CDC row survives `_add_period_columns` (the old failure was the serial-string `"46236.875"`; the fix is emitting a real datetime).

### Stage 5 — Tests

- Update `tests/test_xtb_connector.py` and `tests/fixtures/xtb.py` to the new format (Stage 0 fixture).
- Cover: 3-sheet parsing, Excel-serial decoding (cash-operation `time` + closed `close_time`; open positions carry no date field after the Stage 1 narrowing), aggregate-vs-child distinction, **per-ticker aggregate holdings (child lots skipped; `category` populated on holdings)**, **CASH holding from Cash Operations `Total` (D22)** — one CASH row per account (`position_type=CASH`, `security_value=free_cash`); absent when the sheet has no `Total` row; and sum of CDC `cash_amount` equals `free_cash` under full-history, Total-row exclusion from events, subaccount-transfer filtering, currency-conversion Transfer kept with `target_fx_rate` null (D7), trade enrichment (commission on sell row only), event-type map (interest/tax/sell), CDC latest-payload-per-account on re-upload (keyed on `fetched_at` like snapshot; `dedup_cdc_events` on `(event_type, event_id, account_id)`), 2dp rounding, open→closed lifecycle (a position open in one snapshot, closed in the next, fee captured once). **Multi-account (D18):** two accounts in raw → both survive snapshot (latest payload per `account_id`); a re-upload of the same account supersedes the older snapshot; CDC emits the latest payload per account (no union of all uploads), and cross-account same-ID events coexist (`account_id` in the dedup key at transform and consolidate). **Shared bronze (D17):** CDC produced from `xtb_snapshot` raw with no `xtb_cdc` raw. Confirm no test references dropped fields (`net_profit`, `gross_profit`, `swap`, `rollover`, `margin`, `open_price`, `current_price`, `volume`, `side` on positions).
- Keep the integration test against the real xlsx sample.

### Stage 6 — Checks + docs

- Run `.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .`
- Run `.venv/Scripts/python -m pyright pipeline/ tests/`
- Run `.venv/Scripts/python -m pytest tests/ -q -rf` (re-run after lint auto-fixes)
- Update `docs/brokers/xtb.md` (remove the "only sample data verified" caveat; document the 3-sheet format, Excel-serial dates, account-currency-from-summary-block, subaccount-transfer filtering, ticker-as-identifier, no-ISIN limitation, shared-bronze CDC (one raw row → snapshot + CDC silvers), multi-account latest-per-account transform, EventBridge file-arrival trigger with a single `--xtb-file` per run, **CASH holding from Cash Operations `Total` (D22) and the full-history-export constraint (`Date from` = account opening)**).
- Remove the stale `docs/configuration.md:62` `XTB_ENABLED` line (D16 — ADR 0094 already removed the environment variable).
- **Record an ADR** (invoke `manage-adr` skill) for the rewrite: context = new XTB format, decision = rewrite parser/transform, 3-sheet model, Cash Operations as sole CDC source + Closed Positions fee lookup, subaccount-transfer filtering, ticker identifier, **shared bronze (D17)**, **multi-account latest-per-account transform (D18)**, **EventBridge file-arrival trigger (D19)**, **upload-path + trigger-prefix fix (D20)**, **CASH holding from Cash Operations `Total` under the full-history-export constraint (D22)**. Supersede any prior XTB ADRs whose decisions this reverses (check 0047, 0048, 0102's XTB instrument-ccy deferral). **Partially supersede ADR 0087**: its `cdc_supported` flag (decision #2) and `_OPTIONAL_CDC_BROKERS` optional-list (decision #3's mechanism) are removed by D14/D15 — per-connector CDC validation is now unconditional and consolidate derives candidates from the registry. **Partially supersede ADR 0100** for XTB: per-source `filter_latest_snapshot` is replaced by per-`account_id` latest (D18) because XTB raw lacks `account_id` and multiple accounts share one `source`. Carry forward 0087's surviving guarantees in the new ADR's Constraints: the required-non-empty gate for `_REQUIRED_CDC_BROKERS = ["ibkr","trading212"]` (raise on missing/empty) and `NON_EMPTY_REQUIRED` (unchanged; `xtb_cdc` stays excluded because the prod daily-schedule run may not produce it — D21).

**Post-deploy verification** (query staging to confirm XTB rows land):

```bash
PYTHONIOENCODING=utf-8 .venv/Scripts/python -m pipeline.run query "SELECT * FROM xtb_snapshot LIMIT 5" --decrypt --mode staging
PYTHONIOENCODING=utf-8 .venv/Scripts/python -m pipeline.run query "SELECT * FROM xtb_cdc LIMIT 5" --decrypt --mode staging
```

---

## 4. Subagent orchestration for next session

Suggested handoff sequence (matches the user's staged-subagent + tmp-handoff
preference; keep main context lean):

1. **Subagent A — Stage 0+1**: build new fixture, rewrite `parser.py`, write parser unit tests. Hand off `tmp/` status.
2. **Subagent B — Stage 2+3**: rewrite `transform.py` (multi-account latest-per-account snapshot + shared-bronze CDC, D17/D18), edit `connector.py`/`fetch.py`/`base.py`/`run.py`/`storage.py` + the two `xtb_staging_prefix` Terraform values (shared bronze + `cdc_raw_layer` + unconditional validation + upload-path fix, D14/D17/D20), add the legacy-raw migration, add transform tests. Depends on A.
3. **Subagent C — Stage 4+5**: analytics date regression test, full test suite update, integration test against the real sample. Depends on B.
4. **Main — Stage 6**: run checks, update `docs/brokers/xtb.md`, invoke `manage-adr`.

Each subagent writes a short `tmp/xtb_stageN_report.md` on completion.

---

## 5. What NOT to do

- Closed Positions are fee-lookup only (D2); everything else follows project CLAUDE.md.