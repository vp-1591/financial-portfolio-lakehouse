# 0035 — Remove dead `cashBalance` and `startingCash` fallbacks from IBKR transform

## Context

ADR 0006 described a three-tier cash resolution strategy for IBKR Flex data:

1. **Cash Report** (per-currency `endingCash`)
2. **AccountInformation `cashBalance`**
3. **Derived from NLV minus positions**

In the code, only tiers 1 and 2 were actually implemented. Tier 2 lived at
`pipeline/connectors/ibkr/transform.py:208-241` and read the `cashBalance`
attribute off `<AccountInformation>`. Tier 3 (NLV-derived) was never
implemented, and the "warning on missing FX rate" mentioned in ADR 0006 was
also never implemented — FX rates fall back to 1:1 silently.

The `cashBalance` attribute is **not exposed by the IBKR Activity Flex Query**.
A real Flex report from a configured Activity Flex Query returns
`<AccountInformation>` rows with only `accountId` and `currency` (and
optionally `netLiquidationValue`, which is also unused). The tier-2 fallback
was therefore dead code in production — but it was reachable in tests, because
the test fixture at `tests/fixtures/ibkr.py` and three assertions in
`tests/test_ibkr_connector.py` all injected a synthetic `cashBalance` value.
The tests were passing against a value that real XML would never carry.

Additionally, the Cash Report parser had a `startingCash` fallback: when
`endingCash` was 0, it would substitute `startingCash` from the same
`<CashReportCurrency>` entry. This fallback is methodologically incorrect —
starting cash is not a valid substitute for ending cash (it represents the
balance at the beginning of the period, not the end). When `endingCash` is
genuinely 0, the entry should be skipped rather than replaced with an unrelated
value.

The companion user-facing doc `docs/ibkr/flex-query-required-fields.md` had
been written assuming `cashBalance` was real; the field was removed from that
doc first, which surfaced the dead-code inconsistency this ADR resolves.

## Decision

Delete the tier-2 `cashBalance` fallback in
`pipeline/connectors/ibkr/transform.py` and remove the synthetic `cashBalance`
attribute from the tests and the shared fixture. Also remove the
`startingCash` fallback from the Cash Report parser — when `endingCash` is 0,
the entry is now skipped entirely rather than replaced with `startingCash`.
Update ADR 0006 to reflect the actually-implemented two-tier behaviour (Cash
Report only, with silent 1:1 FX fallback). The NLV-derived tier (tier 3) and
the missing-FX-rate warning remain unimplemented and out of scope for this
change.

Specifically:

- **`pipeline/connectors/ibkr/transform.py`**: delete the entire
  `if not cash_from_report and account_infos:` block and the
  `cash_from_report` flag (init at line 166, set at line 206, read in the
  deleted block). Also delete the `startingCash` fallback (`if ending_cash == 0:
  ending_cash = as_float(entry.get("startingCash"))`).
- **`tests/test_ibkr_connector.py`**: drop the `cashBalance="5000.00"`
  attribute and the `accounts[0]["cashBalance"]` assertion in
  `test_parse_account_info_extracts_attributes`; drop the
  `cashBalance="500.0"` attribute from
  `test_transform_produces_equity_and_cash_rows`.
- **`tests/fixtures/ibkr.py`**: drop the `cashBalance="2000.00"` attribute
  from the default `<AccountInformation>` element. Keep
  `netLiquidationValue` as a parser-faithfulness signal.
- **`docs/adr/0006-ibkr-flex-web-service.md`**: rewrite the "Cash balances
  resolved through 3-source priority" bullet list to describe the
  single-source reality, and drop the "warning on missing rate" claim.

## Consequences

- Cash rows are produced only when the **Cash Report** section of the
  Activity Flex Query is enabled and returns at least one `<CashReportCurrency>`
  row with a non-zero `endingCash`. Entries where `endingCash` is 0 are now
  skipped rather than falling back to `startingCash` — starting cash is not a
  valid substitute for ending cash.
- If the Flex Query is misconfigured (Cash Report section off, or all cash
  balances are zero), the pipeline produces **no cash rows** for that
  account. The dashboard will show zero cash — a clear signal that the
  Flex Query needs to be fixed. Previously, the dead `cashBalance` fallback
  would have silently produced no cash either (because the field is never
  present in real XML), so the user-visible behaviour is unchanged for
  correctly-configured accounts.
- The test suite no longer asserts against a value that production XML will
  never carry, eliminating a false-positive signal.
- The NLV-derived cash fallback (ADR 0006 tier 3) remains unimplemented. If
  a future change wants to re-introduce cash as a safety net when the Cash
  Report section is unavailable, it should use `netLiquidationValue -
  sum(position values)` to derive a single CASH row in the account's base
  currency. That is a separate decision and warrants its own ADR — do not
  bundle it with this cleanup.
- The missing-FX-rate warning described in ADR 0006 is also still
  unimplemented. Currently, a non-base-currency cash entry with no available
  FX rate falls back to a 1:1 rate silently. This matches the pre-existing
  behaviour from before this ADR and is not changed by it.

## Validation

- `tests/test_ibkr_connector.py` passes after the edits.
- The full test suite (`pytest tests/ -v`) is green.
- `ruff check --fix .` and `ruff format .` produce no further changes.
- After auto-fixes, the test suite is re-run and remains green.
- `docs/ibkr/flex-query-required-fields.md` is consistent with the new code
  (no mention of `cashBalance` anywhere).
- `docs/adr/0006-ibkr-flex-web-service.md` is consistent with the new code
  (single-source cash resolution, no tier-2 or tier-3 references).
- For accounts whose Flex Query has the Cash Report section enabled (the
  recommended configuration in the user-facing doc), end-to-end fetch +
  transform produces identical cash rows to before this change. The
  deletion only affects the unreachable fallback path.
