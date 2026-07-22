# 0087 — Make CDC Mandatory and Fail on Empty Silver CDC Tables

## Context

The production pipeline failed at `run-consolidate-analytics` because `cdc_events` didn't exist. Root cause: CDC wasn't configured (no IBKR flex CDC query ID, no T212 CDC setup). But the code made this failure invisible and late:

- **ibkr vs t212 inconsistency**: IBKR's Flex response always carries an XML payload (≥1 row raw), so `transform_connector` always runs and writes `ibkr_cdc` (even with 0 events). Trading 212's `fetch_cdc` calls three endpoints with a per-endpoint `except Exception` at WARNING; if all fail or return empty, it produces a 0-row raw table, and `transform_connector` silently skips it (no log) → `trading212_cdc` is never written.
- **`consolidate_cdc_events()`** swallows all read errors at DEBUG level and returns `None` when no broker has rows, leaving stale/absent `cdc_events` on disk.
- **No non-empty check** in the quality framework — empty Delta tables pass every check (schema, nulls, freshness all pass on 0 rows).
- **XTB** has no CDC feed (`fetch_cdc_kwargs()` returns `{}`); its CDC path is dead code.

## Decision

Make CDC mandatory for IBKR and Trading 212. Empty silver CDC tables (`ibkr_cdc`, `trading212_cdc`, `cdc_events`) must fail the pipeline, because an empty table indicates either misconfiguration or a fully-blank account with nothing to process/analyze. XTB is exempt (file-based, no CDC feed).

Key changes:

1. **New `check_non_empty` quality check** with `NON_EMPTY_REQUIRED = {cdc_events, ibkr_cdc, trading212_cdc}`. Returns FAIL on 0 rows. Missing `NON_EMPTY_REQUIRED` tables also FAIL (not just WARN). Gold tables are not in the set — they'd double-fail downstream of `cdc_events`.

2. **`cdc_supported: bool`** on the `BrokerConnector` protocol. True for IBKR/T212, False for XTB. `cmd_run_connector` builds the validation table list conditionally: `["{name}_snapshot"]` plus `["{name}_cdc"]` only if `cdc_supported`.

3. **`consolidate_cdc_events()` raises `RuntimeError`** when a required broker CDC table is missing or empty. Optional brokers (XTB) are skipped silently. The function now returns `pa.Table` (no `None`). `_consolidate_cdc()` and `_normalize_cdc()` return `int` rc; callers propagate.

4. **`Trading212Connector.fetch_cdc()` raises `RuntimeError`** when all three CDC endpoints produce no data (blank account or total failure). `fetch_connector` already catches this and sets `error_occurred`.

5. **`transform_connector`** now logs at DEBUG when a raw table is absent and at WARNING when a raw table is empty (previously silent). `iter_raw_payloads` and `decrypt_cdc_payloads` count dropped rows and emit a WARNING summary.

6. **`cmd_analytics` comment** updated: CDC is mandatory, not optional. The `cdc_ok` guard is kept as defense-in-depth.

## Constraints

- XTB is exempt from mandatory CDC — `xtb_cdc` is never validated, never required to be non-empty.
- No `cdc_enabled` flag — CDC is mandatory for supported brokers.
- The non-empty check produces **FAIL** (not WARN), so `run_validation` returns 1 regardless of `fail_on_warn`.

## Consequences

- A blank account or misconfigured CDC credentials now fail the pipeline at the connector step (quality validation) or at `_consolidate_cdc` with a clear `RuntimeError`, instead of silently producing no analytics and failing late at `cmd_analytics` with a confusing `FileNotFoundError`.
- Stale `cdc_events` on disk can no longer persist — consolidation either overwrites with fresh data or raises.
- `cmd_run_connector` for XTB no longer validates `xtb_cdc` (which was always missing → WARN). XTB connector runs are cleaner.
- The T212 `fetch_cdc` raise on empty payloads means a misconfigured T212 API key (or a genuinely blank account) fails at fetch time with a clear message, rather than silently producing a 0-row raw table that gets dropped by `transform_connector`.

## Validation

- `TestCheckNonEmpty` in `tests/test_quality.py`: pass on rows, FAIL on 0 rows, `NON_EMPTY_REQUIRED` registry assertions.
- `TestConsolidateCdc` in `tests/test_consolidate_cdc.py`: `test_consolidate_raises_when_required_broker_missing`, `test_consolidate_raises_when_required_broker_empty`, `test_consolidate_skips_xtb_when_missing`, `test_consolidate_skips_xtb_when_empty`. The existing `test_consolidate_merges_all_brokers` still passes.
- `test_fetch_cdc_raises_when_all_endpoints_empty` in `tests/test_trading212_connector.py` (renamed from `test_fetch_cdc_empty_result_produces_table`).
- `test_cdc_supported_flag_values` in `tests/test_run_subcommands.py`: ibkr/T212 True, XTB False.
- `test_xtb_with_file_calls_fetch` updated: XTB validates `["xtb_snapshot"]` only (no `xtb_cdc`).
- `ruff check --fix .` and `ruff format .` clean.
- `.venv/Scripts/python -m pytest tests/ -v` green.
- `.venv/Scripts/python -m pyright` clean (new `cdc_supported` attribute and `int` return types).