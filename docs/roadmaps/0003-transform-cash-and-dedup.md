# Roadmap: Fix Bronze→Silver Deduplication and Cash Extraction for IBKR/T212

## Goal

Ensure the bronze→silver transform processes only the latest snapshot per broker (not every accumulated raw row), and that both IBKR and Trading 212 produce cash rows from demo and live data. This roadmap is for the pipeline maintainer who needs correct portfolio allocation in both environments.

## Current state

Three interconnected bugs in the pipeline:

1. **No date filtering in bronze→silver transform.** Every `transform` run reads the entire raw Delta table and processes all rows, regardless of `fetched_at`. Since raw tables accumulate via `mode="append"` (ingest.py:146), data from 08.07 and 09.07 are processed together and mixed into the normalized output. The normalized table is overwritten each run, but its contents reflect all historical fetches — not just the latest. This affects all three connectors (IBKR, T212, XTB). T212 has accidental last-row-wins semantics (transform.py:42–48) that mask the issue for positions but not for correctness.

2. **IBKR cash rows missing from demo.** The demo Flex response contains only a `BASE_SUMMARY` entry in the `<CashReport>` section — no per-currency entries like `<CashReportCurrency currency="EUR">`. The `parse_cash_report` function (client.py:209–231) correctly filters out `BASE_SUMMARY` via `_IS_CURRENCY_RE` (it's a subtotal, not a real balance). But with no per-currency entries, no cash rows are produced at all. In live accounts with multi-currency holdings, individual currency rows exist and cash works correctly. ADR 0035 removed the `cashBalance` fallback and noted that a future NLV-derived cash tier could address this; that tier was never implemented.

3. **T212 cash rows missing.** The Trading 212 API `/equity/account/summary` returns `cash` as a nested object: `{"availableToTrade": 10500.0, "reservedForOrders": 0, "inPies": 0}`. The `cash_value()` function (client.py:236–241) probes for scalar keys `"free"`, `"cash"`, `"availableFunds"`, `"available"`, `"totalCash"` in order. It finds `"cash"`, but the value is a dict, not a number. `as_float(dict_value)` returns `0.0`, so `if cash_balance:` on transform.py:94 is `False` and no cash row is created. The actual available cash (`10500.0 PLN`) is at `summary["cash"]["availableToTrade"]`.

### Relevant ADRs

- ADR 0003 — Medallion architecture (raw→normalized→analytics)
- ADR 0035 — Removed `cashBalance` fallback; noted unimplemented NLV-derived tier
- ADR 0045 — `build_normalized_table` helper; replaced list-append pattern
- ADR 0047 — Moved XLSX parsing to silver; removed `account_id` from raw schema

## Success criteria

- [ ] Running `transform` with multiple raw rows from different `fetched_at` dates produces a normalized table containing only rows from the latest `fetched_at` timestamp per broker
- [ ] IBKR demo data (which has only `BASE_SUMMARY` cash entries) produces cash rows in the normalized table with the correct base-currency value
- [ ] IBKR live data (which has per-currency cash entries) continues to produce the same cash rows as before — no regression
- [ ] T212 demo data (which returns `cash` as a nested dict) produces a cash row with `availableToTrade` as the value
- [ ] T212 live data (where `cash` is a scalar) continues to work — no regression
- [ ] All existing tests pass (`pytest tests/ -v`)
- [ ] New tests cover: (a) date filtering keeps only latest snapshot, (b) IBKR `BASE_SUMMARY` fallback, (c) T212 nested `cash` dict
- [ ] `ruff check --fix .` and `ruff format .` produce no errors

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Filter by date in the `run.py` orchestrator before calling `transform` | Would require changing the transform API to accept a `fetched_at` filter; each connector's transform would still need to handle the filter. Simpler to have each transform filter internally. |
| Dedup by position key (ISIN/ticker) keeping latest value | More complex and doesn't match the snapshot model. Raw data is per-fetch snapshots, not CDC. Keeping only the latest fetch is the correct semantics. |
| Add a separate "latest snapshot" materialisation step between raw and normalized | Adds a new pipeline stage and table. The transform step already reads from raw and writes to normalized — filtering by latest `fetched_at` within the existing transform is simpler. |
| Use `BASE_SUMMARY` cash as the primary cash source for IBKR | `BASE_SUMMARY` is a subtotal that can double-count multi-currency entries. It should only be a fallback when per-currency entries are absent. |
| NLV-derived cash (ADR 0006 tier 3) instead of `BASE_SUMMARY` fallback | Requires position values to be computed before cash, creating a circular dependency in the transform. `BASE_SUMMARY` as fallback is simpler and avoids this. |

## Phases

### Phase 1 — Filter latest snapshot in bronze→silver transform *[status: done]*

Add `fetched_at`-based filtering to each connector's `transform_snapshot` so that only rows from the latest `fetched_at` timestamp are processed. This applies to all three connectors (IBKR, T212, XTB).

**Scope:**
- [ ] Add a shared utility function (e.g., `filter_latest_snapshot(raw: pa.Table) -> pa.Table`) in `pipeline/connectors/transform_utils.py` that filters a raw table to only rows with the maximum `fetched_at` value. All rows from a single fetch share the exact same `fetched_at` (set once per fetch call), so equality comparison on the max timestamp correctly selects the entire latest batch
- [ ] Call `filter_latest_snapshot` at the start of each connector's `transform_snapshot` (IBKR, T212, XTB) before processing rows
- [ ] Call `filter_latest_snapshot` at the start of each connector's `transform_cdc` if applicable (T212 and XTB have CDC; IBKR raises `NotImplementedError`)
- [ ] Add tests for `filter_latest_snapshot` with: single timestamp, multiple timestamps (keeps only latest), empty table
- [ ] Verify that `transform` on demo S3 data produces normalized rows from only the latest fetch date

**Out of scope:**
- Changing the raw table write mode (still `append`)
- Adding a deduplication step to `consolidate` or `allocate`
- CDC deduplication (CDC data is inherently chronological, not snapshot-based)

**Files:** `pipeline/connectors/transform_utils.py`, `pipeline/connectors/ibkr/transform.py`, `pipeline/connectors/trading212/transform.py`, `pipeline/connectors/xtb/transform.py`, `tests/test_transform_utils.py`

**Links:** ADR 0003, ADR 0045

---

### Phase 2 — Fix IBKR cash extraction for single-currency demo accounts *[status: done]*

When the Flex response contains only a `BASE_SUMMARY` `<CashReportCurrency>` entry and no per-currency entries, use the summary row as a fallback to produce a cash row in the base currency.

**Scope:**
- [ ] Modify `parse_cash_report` in `pipeline/connectors/ibkr/client.py` to separate per-currency entries from `BASE_SUMMARY` entries, returning both
- [ ] Modify `transform_snapshot` in `pipeline/connectors/ibkr/transform.py` to use `BASE_SUMMARY.endingCash` as a fallback when no per-currency cash entries are found. The fallback produces exactly one cash row: `CASH {base_currency}` with `value = BASE_SUMMARY.endingCash` and `value_currency = base_currency`
- [ ] Add test fixture for Flex XML with only `BASE_SUMMARY` cash entry
- [ ] Add test for single-currency IBKR account producing a cash row
- [ ] Add test for multi-currency IBKR account (with per-currency entries) not using `BASE_SUMMARY` — no regression
- [ ] Add test for `BASE_SUMMARY` being skipped when per-currency entries exist — `BASE_SUMMARY` should not double-count
- [ ] Verify against demo S3 data that IBKR now produces cash rows

**Out of scope:**
- NLV-derived cash (ADR 0006 tier 3) — circular dependency with position values
- Multi-currency `BASE_SUMMARY` handling — `BASE_SUMMARY` is always in the base currency, so the fallback is always a single row
- Changing the Flex Query configuration (the fix works regardless of query config)

**Files:** `pipeline/connectors/ibkr/client.py`, `pipeline/connectors/ibkr/transform.py`, `tests/test_ibkr_connector.py`, `tests/fixtures/ibkr.py`

**Links:** ADR 0035, ADR 0006

---

### Phase 3 — Fix T212 cash extraction for nested dict API response *[status: done]*

Update `cash_value()` to handle the nested `cash` dict structure returned by the Trading 212 demo API, where `cash` is `{"availableToTrade": N, "reservedForOrders": 0, "inPies": 0}`.

**Scope:**
- [ ] Modify `cash_value()` in `pipeline/connectors/trading212/client.py` to detect when `summary["cash"]` is a dict and drill into `availableToTrade` (or `free` as fallback within the dict)
- [ ] Add tests for: (a) `cash_value` with nested dict (`{"cash": {"availableToTrade": 10500.0, ...}}`), (b) `cash_value` with scalar `cash` key (regression), (c) `cash_value` with `free` key (regression), (d) `cash_value` with no cash keys (returns 0.0)
- [ ] Verify against demo S3 data that T212 now produces a cash row with the correct PLN value

**Out of scope:**
- Changing the T212 API client to return different endpoint data
- Multi-currency cash support (T212 API only provides a single cash figure)
- Modifying `account_currency()` or other T212 parsing functions

**Files:** `pipeline/connectors/trading212/client.py`, `tests/test_trading212_connector.py`

**Links:** None