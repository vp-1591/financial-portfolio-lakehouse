# 0071: IBKR Demo Initial Deposit Injection

## Context

IBKR demo accounts start with approximately $1M in cash, but the Flex Web Service CDC
API does not return a `CashTransaction` for this initial funding. As a result, the
"Cash flow breakdown" chart in the portfolio report shows trades, dividends, fees, and
interest for IBKR demo accounts but no deposit to explain where the money came from.

This makes the demo report look incorrect and confusing — it appears as though the
account has expenses without any income or funding source.

The injection must be:

- **Demo-only**: Production accounts should never receive synthetic deposits.
- **IBKR-specific**: The initial deposit amount and format are particular to IBKR demo
  accounts; other brokers handle deposits differently.
- **Date-safe**: The synthetic deposit must appear before any other operation so that
  the cash flow chart makes chronological sense.
- **Idempotent**: Re-running the pipeline must not create duplicate deposits.

## Decision

Add a `_inject_demo_deposit()` helper in `pipeline/connectors/ibkr/transform.py` that
injects a synthetic DEPOSIT event for each IBKR account when `is_demo=True`.

### Key design choices

1. **Injection point**: In `transform_cdc()`, after XML parsing and before
   `build_normalized_table()`. This places the deposit in the silver (normalized) layer,
   before consolidation and analytics. The synthetic record goes through the same
   encryption and deduplication as real records.

2. **Date calculation**: The deposit date is set to one day before the earliest
   `event_datetime` found in the parsed records. If no records exist, the function
   returns early (no accounts to create deposits for). The time is zeroed to midnight
   UTC.

3. **Amount**: 1,000,000.0 in the account's base currency (matching IBKR's demo
  starting balance).

4. **Event ID**: Uses `_deterministic_event_id("CashTransaction", acct_id,
   "DEMO_INITIAL_DEPOSIT")` to produce a stable hash. This ensures idempotency —
   the same event ID is generated every run, and the existing dedup logic in
   `transform_cdc()` keeps only one copy.

5. **Record shape**: Mimics the exact dict shape returned by
   `_process_ibkr_cash_transaction()` — only CashTransaction fields are included
   (no `quantity`, `price`, `side`, `gross_amount`, `fee_amount`, `tax_amount`,
   `net_amount`).

6. **Demo detection**: The `is_demo` flag is passed from `IbkrConnector.transform_cdc()`
   via `pipeline.secrets.is_demo()`, following the same pattern as Trading 212's
   demo URL switching.

## Constraints

- Only activates when `DEMO=true` — production data is never modified.
- Only applies to IBKR — each broker connector manages its own demo logic.
- The `event_id` must remain stable across pipeline runs to prevent duplicate deposits.
- The deposit date must be earlier than any real event in the dataset.

## Consequences

- IBKR demo reports will show a $1M initial deposit in the cash flow breakdown, making
  the chart accurate and intuitive.
- The synthetic deposit flows through the entire pipeline like any real deposit — it
  appears in `cash_flow_summary` and the cash flow breakdown chart.
- If IBKR ever adds a real deposit CashTransaction to the demo API response, the
  synthetic deposit will still exist alongside it (different `event_id`). This is
  acceptable because having two deposits is better than having none.
- The `_inject_demo_deposit()` function is a private helper, not part of the public
  API. Future connectors can follow the same pattern if needed.

## Validation

- `tests/test_ibkr_connector.py::TestInjectDemoDeposit`: 8 tests covering
  no-injection when not demo, injection when demo, date calculation (one day before
  earliest event), multi-account deposits, idempotent event IDs, and the integration
  with `transform_cdc()`.
- `tests/test_ibkr_connector.py::TestCdcTransform`: Existing tests still pass with
  `is_demo=False` (the default).
- Full test suite: 568 tests pass with no regressions.