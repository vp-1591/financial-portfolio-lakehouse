# 0108 — XTB New-Format Connector Overhaul

## Context

XTB changed its Excel export to a new 3-sheet format — **Open Positions**,
**Cash Operations**, **Closed Positions** — with a fundamentally different
layout from the single-sheet reports the old connector parsed. The old
`parser.py` used raw zipfile/XML walking (`xml.etree.ElementTree` +
`read_shared_strings`), a header-set match (`find_open_positions_header`
looking for `{position, symbol, type, volume}`), and a "value-below-label"
account/currency reader. None of these map onto the new structure:

- The new Open Positions sheet leads with a per-product **summary block**
  (`Product | Metric | Amount | Currency`) and then a detail table whose rows
  are either per-ticker **aggregate** rows (empty `Type`, real instrument
  name in `Instrument`, non-empty `Category`) or per-position **child lot**
  rows (numeric position ID in `Instrument`, empty `Category`, non-empty
  `Type`). The old header-set match keys on `position`/`symbol`/`volume`,
  which the new header (`Product | Instrument/Position | Ticker | Category |
  Type | Volume | Value | …`) does not contain, so the old parser finds no
  header and produces nothing.
- The new Cash Operations sheet is the canonical cash ledger and carries
  trades (with quantity/price in the comment), deposits, interest, taxes,
  transfers, and a `Total` summary row. The old parser read a different
  cash-operations sheet with a `Currency` column per row.
- A new **Closed Positions** sheet carries per-trade commission — the fee
  source the old connector never had.

CDC production for XTB was also **broken, not disabled**: `fetch_cdc` was
never invoked on the production file-arrival trigger path (the EventBridge
trigger only runs `fetch_snapshot` with a single `--xtb-file`), so
`xtb_cdc` raw was never written and XTB CDC never reached gold. The
`cdc_supported = False` flag (ADR 0087) masked this dead code path rather
than describing an intentional choice. Separately, the EventBridge rule
prefix never matched the actual S3 upload key: `S3Backend` always prepends
the environment prefix (`S3_DEFAULT_PREFIX = "pipeline"`), so the real key
`pipeline/staging/xtb/<file>` never matched the rule prefix `staging/xtb/` —
the trigger never fired in either environment. The full overhaul plan
(`docs/xtb/xtb_overhaul_plan.md`) records the 22 binding decisions (D1–D22).

**Refinement (pre-merge two-pass review of this PR):** the initial D15/D21
framing treated XTB as optional — excluded from the daily schedule and from
`NON_EMPTY_REQUIRED`, with `xtb_cdc` allowed to be absent. Review surfaced two
problems with that framing while the PR was still open: (a) `_latest_per_account`
called `parse_report` **unguarded** on every historical `XTB_REPORT` raw row,
so one malformed row (text date cell, out-of-range Excel serial, empty-currency
fully-closed account) propagated through `transform_connector` and killed both
`transform_snapshot` and `transform_cdc` for all accounts until purged; and (b)
a required XTB is simpler and safer than an optional one — the daily run must
complete regardless, and `run-connector xtb` already returned `SKIPPED` with no
file. D15/D18/D21 below are the refined (shipped) form; the optionality framing
they replace was never merged.

## Decision

Rewrite `pipeline/connectors/xtb/parser.py` and `transform.py` from scratch
(keep `fetch.py`, `connector.py`, `__init__.py`), and adopt **openpyxl** as
the XLSX library (D13). The new parser produces a typed `XtbReport` from all
3 sheets; the transform consumes it.

1. **3-sheet model (D2).** Open Positions → snapshot holdings; Cash
   Operations → **all** CDC events; Closed Positions → **fee-enrichment
   lookup only**, keyed by Position ID — never emitted as its own event. One
   event source avoids double-counting; Closed Positions adds commission only.

2. **Dates → tz-aware UTC (D3).** The new date cells are date-*formatted*, so
   openpyxl auto-converts them to naive `datetime`; the parser attaches
   `tzinfo=UTC` at the boundary. A defensive numeric-serial branch uses
   `openpyxl.utils.datetime.from_excel` + UTC. The transform emits ISO-8601
   UTC strings, so downstream analytics (`cdc_tables.py`) is unchanged —
   the old failure (the serial string `"46236.875"` breaking
   `_add_period_columns`) is fixed at the source.

3. **Account currency from the Open Positions summary block (D5).** Read the
   `Currency` column of the `Product | Metric | Amount | Currency` block.
   All Cash Operations amounts are in this account currency; there is no
   per-row Currency column on Cash Operations. `security_ccy =
   account_ccy`, not a hardcoded literal.

4. **Per-ticker aggregate holdings (D4).** Keep the aggregate rows (empty
   `Type`, real name in `Instrument`, non-empty `Category`) as holdings;
   skip the child lot rows (the snapshot schema is instrument-level with no
   lot/position-id/cost-basis column, so child lots have nowhere to live and
   would duplicate the aggregate total). Use **Ticker** as `label`/
   identifier (D12 — no ISIN in the new format); the aggregate's `Instrument`
   → `description`, `Category` → `asset_class`.

5. **CASH holding from Cash Operations `Total` (D22).** The parser reads the
   `Total` row's `Amount` into `XtbReport.free_cash` (2dp) **before**
   excluding it from `cash_operations` (it is a summary, not an event — D10);
   `transform_snapshot` emits one CASH row per account. **Setup constraint:**
   reports must be exported with `Date from` set to the account opening date
   (full history from a zero opening balance); a partial-window export
   silently understates cash and cannot be detected in-file (the control is
   the setup rule, not a heuristic guard — YAGNI).

6. **Subaccount transfers filtered; currency-conversion Transfer kept
   (D7).** `Subaccount transfer` rows (internal moves between the trading and
   investment-plan subaccounts; net zero) are filtered out entirely in the
   parser — single place. Currency-conversion `Transfer` rows are kept as
   TRANSFER events with `target_fx_rate` left **null**: the broker's
   `Exchange rate:X` is the `account_ccy`→destination rate, which equals the
   pipeline's `account_ccy`→EUR `target_fx_rate` only when the destination is
   EUR; non-EUR destinations exist, so the broker rate is unsafe.
   `normalize_currency` fills `target_fx_rate` via `CurrencyConverter`
   regardless of destination (matches the IBKR/T212 pattern, which all set it
   null). `target_ccy` is always EUR (pipeline-set, never parsed).

7. **Trade enrichment from Closed Positions (D8).** The closing (`Stock
   sell`) row is joined via `Position ID` to Closed Positions:
   `fee_amount = Commission`, `gross_amount = sale_value − purchase_value`
   (2dp), `settle_date = Close time`. The opening (`Stock purchase`) row gets
   no fee (commission is recorded at close). `Swap`/`Rollover`/`Margin`/
   `Open`/`Close Conversion Rate` are dropped — the CDC schema has no columns
   for them, and folding them into `fee_amount` would conflate commission
   with financing/position costs. The raw layer preserves the full xlsx
   bytes.

8. **Shared bronze (D17).** One fetch per file writes a single raw row to
   `xtb_snapshot` raw with `source="XTB_REPORT"` carrying the full workbook
   (all 3 sheets). `fetch_cdc` and the `xtb_cdc` raw table are removed;
   `transform_cdc` reads from the same `xtb_snapshot` raw. A new
   `cdc_raw_layer: str = "cdc"` (default) on `BrokerConnector` lets
   `transform_connector` read `get_raw_path(name, cdc_raw_layer)` for the CDC
   transform; XTB overrides `cdc_raw_layer = "snapshot"`. This eliminates the
   dead `fetch_cdc` path and makes CDC production real.

9. **Multi-account latest-per-account (D18).** Do **not** use
   `filter_latest_snapshot` for XTB — it keys on `source` alone and
   `account_id` is not in `RAW_SCHEMA`, so it collapses distinct accounts
   (`PLN_123…` + `EUR_456…`) to the single latest row. Instead both
   `transform_snapshot` and `transform_cdc` group `source=="XTB_REPORT"` rows
   **by `account_id` derived from `source_file`** (filename pattern
   `{CCY}_{account_id}_{from}_{to}.xlsx`) **without parsing**, keep the latest
   `fetched_at` **per `account_id`** (deterministic `(fetched_at,
   source_file)` tiebreaker — guard 9), and parse only that one latest row per
   account. Each parse is **guarded**: a malformed latest row falls back to the
   previous good row for that account, and if all rows for an account fail the
   account is skipped with a warning — one bad historical row can no longer kill
   the connector. The report's R1 `account_id` is authoritative: a
   filename-vs-R1 mismatch is logged and R1 wins. Rows whose filename doesn't
   match the pattern fall back to a guarded parse for account-id discovery (no
   silent data loss). CDC does **not** union all uploaded payloads: under the
   full-history-export assumption (D22) the latest report per account is that
   account's complete event log. `dedup_cdc_events` on
   `(event_type, event_id, account_id)` is a safety net (ADR 0105 parity;
   `account_id` keeps same-ID events from different accounts distinct).

10. **Remove the `cdc_supported` flag (D14).** Per-connector CDC validation in
    `cmd_run_connector` is now unconditional:
    `tables = [f"{name}_snapshot", f"{name}_cdc"]`. The flag existed only to
    mark XTB `False`, but XTB CDC production was broken, not intentionally
    disabled. D17 + the transform rewrite make `transform_cdc` produce
    `xtb_cdc` for the first time, so unconditional validation is correct.

11. **Derive consolidate CDC candidates from the registry (D15).** Remove
    `_OPTIONAL_CDC_BROKERS = ["xtb"]` from `consolidate_cdc.py`; derive
    candidates from `connectors.all()` (lazy import to avoid the import
    cycle). `_REQUIRED_CDC_BROKERS = ["ibkr","trading212","xtb"]` is the
    ADR 0087 required-non-empty gate — XTB is now required (D21), so a
    missing/empty `xtb_cdc` raises at consolidate like any other broker.
    `account_id` is added to the consolidate dedup subset so multi-account
    brokers don't drop same-ID events across accounts.

12. **EventBridge file-arrival trigger (D19) + upload-path fix (D20).** Prod:
    EventBridge S3 Object-Created on `pipeline/xtb_uploads/` → Step Functions
    `RunConnectors` with `file_arrival_connectors = ["ibkr","trading212",
    "xtb"]`, passing a single `--xtb-file` S3 URI per execution; multi-account
    accumulates across triggers. The daily schedule **includes** XTB
    (`schedule_connectors = ["ibkr","trading212","xtb"]`); `run-connector xtb`
    skips gracefully (return 0) when no file has arrived yet, so the daily run
    completes and `xtb_cdc` is required (informs D14/D15/D21).
    The upload landing-zone path drops the `staging`/`staging_demo` segment
    and renames `xtb` → `xtb_uploads`: `S3Backend.staging_path(segment,
    filename)` emits `{prefix}/{segment}/{filename}` and
    `StorageConfig.staging_path(connector, filename)` passes
    `f"{connector}_uploads"` — so the actual key
    `pipeline/xtb_uploads/<file>` matches the EventBridge rule prefix. The
    `staging`/`staging_demo` segment redundantly re-encoded the environment
    (already carried by `pipeline`/`pipeline_demo`) and collided with
    `--mode staging`; both are removed. Terraform `xtb_staging_prefix` →
    `pipeline/xtb_uploads/` (prod), `pipeline_demo/xtb_uploads/` (demo).

13. **`xtb_cdc` is required in `NON_EMPTY_REQUIRED` (D21).** XTB is a required
    connector: the daily schedule includes XTB (skips when no file has arrived),
    and `xtb_cdc` must exist in the lake — stale is OK because CDC is
    cumulative. Three CDC-non-empty gates agree: per-connector validation (D14,
    unconditional), consolidate required-brokers (D15 — `xtb` in
    `_REQUIRED_CDC_BROKERS`), and `quality.py`
    `NON_EMPTY_REQUIRED = {cdc_events, ibkr_cdc, trading212_cdc, xtb_cdc}`.
    The full optionality removal — no hardcoded required list, gate derived
    from the registry — is a follow-up issue, not this PR.

## Constraints

- The raw layer still stores original `.xlsx` bytes unmodified and parsing
  happens in the transform (silver) layer — unchanged since ADR 0047. Account
  ID remains a silver-layer concept — now derived primarily from `source_file`
  (the filename pattern) for grouping, with the parsed workbook's R1
  `account_id` authoritative on mismatch; it is never added to `RAW_SCHEMA`.
- `_REQUIRED_CDC_BROKERS = ["ibkr","trading212","xtb"]` is the hardcoded
  CDC-required list; a missing/empty required broker CDC table still raises
  (the ADR 0087 required-non-empty gate, now extended to XTB per D21).
  `NON_EMPTY_REQUIRED = {cdc_events, ibkr_cdc, trading212_cdc, xtb_cdc}` —
  `xtb_cdc` is required (D21). Removing the hardcoded list in favor of a
  registry-derived gate is a follow-up issue, not this PR.
- T212 `fetch_cdc` raising `RuntimeError` on all-empty endpoints (ADR 0087
  decision #4) and `transform_connector`'s empty-raw WARNING (ADR 0087
  decision #5) remain in force — unchanged.
- IBKR and T212 CDC keep their separate CDC raw fetch (`cdc_raw_layer =
  "cdc"`); only XTB reads CDC from the snapshot raw (shared bronze).
- `filter_latest_snapshot` per-source dedup (ADR 0100 decision #1) and the
  T212 `security_value` encryption fix (ADR 0100 decision #2) remain in force
  for T212 and IBKR; only XTB no longer uses `filter_latest_snapshot`
  (replaced by per-`account_id` latest, D18).
- XTB snapshot `security_ccy` stays the account currency (the XLSX export
  exposes no per-position instrument currency — ADR 0102's XTB instrument-ccy
  deferral, unchanged in substance; the source is now the Open Positions
  summary block rather than the old `Currency` label). XTB currency-exposure
  grouping remains account-currency — a documented known limitation.
- `dedup_cdc_events` keeps `keep="first"` on a `fetched_at`-descending sort
  (ADR 0105) — the latest fetch wins; XTB adds `account_id` to the subset.
- No `instrument_ccy`/`instrument_value` columns on snapshots (ADR 0102);
  XTB CDC `instrument_ccy` stays null (XTB exposes no per-instrument trading
  currency).
- The old low-level zipfile/XML helpers and the legacy `load_*` parser
  functions are removed; no compatibility shim is kept.
- The orphaned `xtb_cdc` raw table is abandoned in place (D17 — never written
  or read again); a migration purges legacy `OPEN POSITION`/`CASH OPERATION`
  raw rows from `xtb_snapshot` so they don't accumulate (the new transforms
  already skip them — cleanup, not a correctness gate).

## Consequences

- **Positive:** XTB parses the new 3-sheet format; XTB CDC reaches gold for
  the first time (shared bronze, D17). Multi-account coexists (D18). The
  EventBridge trigger now fires (D20). openpyxl gives native sheet/cell
  access, shared-strings handling, and date conversion — cleaner to
  maintain than raw ZIP/XML.
- **Positive:** Removing `cdc_supported` and `_OPTIONAL_CDC_BROKERS` deletes
  the redundant parallel "XTB is special" encodings (DRY); the candidate set
  comes from the single source of truth (the registry).
- **Negative / known limitation:** The CASH holding (D22) is valid only under
  a full-history export (`Date from` = account opening). A truncated export
  silently understates cash and cannot be detected in-file — the control is
  a setup rule, not a guard. Documented in `docs/brokers/xtb.md`.
- **Negative / staleness tradeoff:** Latest-per-account keys on `fetched_at`,
  so a re-uploaded *older* report could supersede a newer one — a known
  tradeoff shared with snapshot (keying on the report's `Date to (UTC)`
  content time would close it for both; deferred).
- **Negative (accepted, documented):** `data_only=True` returns `None` for
  formula cells lacking a cached value; if the XTB exporter ever writes the
  Cash Ops `Total` as a `=SUM(...)` formula without a cache, `free_cash`
  reads `None` and the CASH holding (D22) is skipped. The sample's `Total` is
  a literal, so this is low-risk; the fallback would be summing
  `cash_operations` amounts in the parser — no code change now.
- **Dependency:** openpyxl==3.1.5 added to pipeline deps.
- **Migration:** `pipeline/migrations/migrate_xtb_purge_legacy_raw.py`
  purges legacy raw rows; run before deploying the shared-bronze transform.

This ADR **supersedes ADR 0048** (the upload-path `staging`/`staging_demo`
decision #4 is reversed by D20; Option B — S3 staging + EventBridge —
least-privilege, and ephemeral staging carry forward unchanged, originally
decided in ADR 0048 §Decision).

This ADR **partially supersedes ADR 0087**: its `cdc_supported` flag (decision
#2) and `_OPTIONAL_CDC_BROKERS` optional-list (decision #3's mechanism) are
removed by D14/D15 — per-connector CDC validation is now unconditional and
consolidate derives candidates from the registry. The `NON_EMPTY_REQUIRED`
set and `_REQUIRED_CDC_BROKERS` list (decision #1) are **extended** to include
`xtb_cdc`/`xtb` (D21) — XTB is now a required broker, not exempt. Carried
forward unchanged: the `check_non_empty` quality check mechanism, the
required-broker missing/empty `RuntimeError` (decision #3 behavior), T212
`fetch_cdc` raise on all-empty (decision #4), and `transform_connector`
empty-raw WARNING (decision #5) — see ADR 0087 §Decision.

This ADR **partially supersedes ADR 0100 for XTB**: per-source
`filter_latest_snapshot` is replaced by per-`account_id` latest (D18) because
XTB raw lacks `account_id` and multiple accounts share one `source`.
Carried forward unchanged for T212 and IBKR: per-source dedup (decision #1)
and the T212 `security_value` encryption fix (decision #2) — see ADR 0100
§Decision.

ADR 0047 (parse in silver, raw stores bytes, account_id derived in
transform), ADR 0094 (no `*_ENABLED` toggles — this overhaul completes its
two straggler cleanups: the dead `enabled_env_var` test attr and the stale
`XTB_ENABLED` doc line), and ADR 0102 (XTB `security_ccy` stays
account-currency) remain active and are not superseded.

## Validation

- Full suite: 791 tests pass; `ruff check --fix . && ruff format .` clean;
  `pyright pipeline/ tests/` 0 errors.
- `tests/test_xtb_connector.py` — `TestXtbParser` (26): 3-sheet parsing,
  dataclass field scope (YAGNI), aggregate-vs-child distinction (child lots
  skipped, `category` populated on holdings), zero-value aggregate skipped
  (guard 3), date decoding tz-aware UTC (cash + closed), `free_cash` from
  `Total` (D22), `free_cash` == sum of `cash_operations`, Total-row exclusion
  (D10), subaccount-transfer filtering (D7), currency-conversion Transfer
  kept, 2dp rounding, nonzero commission, Profit/loss total excluded,
  closed position matches Stock sell by Position ID, `position_id` string
  coercion (guard 1), missing sheet → empty list (guard 5), empty/absent
  summary currency raises (guard 4), real-sample round-trip. `TestXtbExcelSerialDecoding`:
  raw numeric serials in Time/Close time → tz-aware UTC via `from_excel`.
  `TestXtbOpenClosedLifecycle`: fee captured exactly once on the sell, open
  position does not reappear, purchase event not in CDC (latest-per-account
  supersedes). `TestTransformSnapshot` (12) + `TestTransformCDC` (11):
  multi-account D18, re-upload supersedes, guard 9 tiebreaker, legacy source
  skipped, event-type map, cash_sum == free_cash, currency transfer D7,
  sell-only fee enrichment D8, 2dp rounding, shared bronze, cross-account
  same-ID coexist. `TestXtbRealSampleTransformIntegration`: real anonymized
  sample through both transforms (2 EQUITY + 1 CASH; deposit/interest/sell;
  Commission=0 → fee_amount 0.0). `TestAccountIdFromFilename` (4): filename
  pattern `{CCY}_{account_id}_{from}_{to}.xlsx` → account_id; no-underscore,
  non-digit segment, empty → None. `TestLatestPerAccountGuarded` (5):
  filename-grouped two-account latest-only parse, malformed latest row
  falls back to the older good row, all-rows-fail skips the account, R1 wins
  on filename-vs-R1 mismatch (logged), non-matching filename uses the
  fallback parse path.
- `tests/test_cdc_analytics.py` — `TestXtbEventDatetimeRegression` (3):
  `_add_period_columns` accepts the XTB ISO `event_datetime`; end-to-end
  `transform_cdc` → `build_cash_flow_summary` keeps 2026-07 and 2026-08
  buckets (the old serial-string `"46236.875"` regression is fixed).
- `tests/test_storage_config.py` — `staging_path` assertions updated to
  `pipeline/xtb_uploads/<file>` / `pipeline_demo/xtb_uploads/<file>`; new
  `test_staging_path_no_xtb_subfolder` literal-key guard so a refactor
  re-introducing a connector segment fails loudly.
- `tests/test_consolidate_cdc.py` — XTB is now a **required** CDC broker:
  missing/empty `xtb_cdc` raises `RuntimeError` (the two former optional-skip
  tests flipped to assert the raise); `test_consolidate_merges_all_brokers`
  and `_overwrites_cdc_events_re_read` write `xtb_cdc`; dedup subset includes
  `account_id`.
- `tests/test_connector_registry.py` / `test_run_subcommands.py` —
  `cdc_supported`/`enabled_env_var` removed; `cdc_raw_layer = "cdc"` added;
  `test_cdc_supported_*` dropped; XTB validation unconditional.
  `test_xtb_without_file_returns_0`: `run-connector xtb` with no `--xtb-file`
  skips gracefully (return 0) — XTB is a required scheduled connector that
  skips when no file has arrived. `TestCmdFullSfnTrigger` stubs include the
  xtb connector ARN (`DEFAULT_CONNECTORS` now lists xtb).
- `tests/test_quality.py` — `test_non_empty_required_registry` asserts
  `xtb_cdc in NON_EMPTY_REQUIRED`; the three end-to-end tests
  (`test_returns_zero_on_all_pass`, `test_fail_on_warn_flag`,
  `test_write_and_read_results`) write `xtb_cdc`.
- `tests/test_sfn.py` — `test_queries_history_and_each_log_group` expects the
  four log groups (`ibkr`, `trading212`, `xtb`, `consolidate-allocate`) since
  `fetch_failure_details` iterates `[*DEFAULT_CONNECTORS, CONSOLIDATE_FAMILY]`.
- `tests/test_migrate_xtb_purge_legacy_raw.py` — idempotent, dry-run, skips
  absent tables (mirrors `migrate_snapshot_schema_unify.py`).
- Post-deploy (manual): `SELECT * FROM xtb_snapshot LIMIT 5` and
  `SELECT * FROM xtb_cdc LIMIT 5` via `pipeline.run query --decrypt --mode
  staging` to confirm XTB rows land.