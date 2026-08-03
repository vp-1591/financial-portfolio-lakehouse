# 0101 — Fix IBKR Flex Attribute Names for CDC event_id Dedup

## Context

`pipeline/connectors/ibkr/transform.py` read the wrong Flex attribute names from
IBKR Flex Web Service payloads:

- OpenPosition: `assetClass` and `quantity` instead of the real `assetCategory`
  and `position`.
- Trade: `ibExecutionId` / `tradeId` instead of the real `ibExecID` / `tradeID`.
- CashTransaction and Transfer: `transactionId` instead of the real
  `transactionID`.

These names were never what IBKR Flex emits, so `dict.get(...)` always missed and
fell through to defaults. The most serious consequence interacted with
[ADR 0069](./0069-fix-ibkr-cdc-triplication-and-date-parsing.md), which decided
to deduplicate CDC events by `event_id`. Because the canonical broker IDs were
never read, every Trade/CashTransaction `event_id` fell back to
`_deterministic_event_id(...)` — a hash of account + dateTime + amount. A
*corrected* trade carrying the same `ibExecID` but a changed `dateTime` or
`quantity` computed a **new** hash, so 0069's `unique(subset=['event_id'])`
treated the correction as a brand-new event: the original was kept and the
correction was duplicated, so the correction was effectively lost.

Two further latent effects: non-STK OpenPositions were misclassified as `STK`
(the `assetClass` miss defaulted to `"STK"`), and OpenPositions with
`positionValue == 0` but a non-zero `position` and `markPrice` were silently
dropped (the zero-value fallback read `quantity`, which also missed, yielding
`0 * markPrice == 0`).

This surfaced during a test-suite audit (round1-fixture-reality): the IBKR
fixtures used the same wrong attribute names as the transform, so the suite
validated shapes that real demo bronze data never produces. Aligning the
fixtures to real demo data required fixing the transform to read the real names
— a source change, not just a test change.

## Decision

Read the real Flex attribute names in the IBKR transform:

- OpenPosition: `assetCategory` (asset class) and `position` (share count).
  The zero-`positionValue` fallback now computes `position * markPrice`, so
  non-zero holdings with a zero reported positionValue are retained instead of
  dropped.
- Trade: `ibExecID` / `tradeID`.
- CashTransaction and Transfer: `transactionID`.

Keep the `_deterministic_event_id(...)` fallback **only** when the canonical
broker ID is genuinely absent — not as the default path. This is what makes
0069's per-`event_id` dedup actually deduplicate corrections: a corrected trade
keeps its `ibExecID`, keeps its `event_id`, and replaces the prior row instead
of being appended as a new event.

A redundant inline `base_currency_override` block in the BASE_SUMMARY cash
fallback was removed: it masked the main override block (which populates
`base_currency_by_account`), making the currency override untestable.

### Alternatives considered

- **Always hash, ignore canonical IDs** (the prior behavior): rejected — it is
  exactly what defeated 0069's dedup for corrections. Stable only across
  identical payloads, not across corrections.
- **Strict mode — raise if the canonical ID is missing**: rejected — real Flex
  payloads can legitimately omit IDs on some record types, and raising would
  break the pipeline on otherwise-valid data. The deterministic fallback remains
  the safety net for those records.
- **Fix only the tests, leave the transform wrong**: rejected — the fixtures
  would then diverge from the transform's actual output, so the test-fidelity
  fix required the source fix.

## Constraints

- Other brokers (Trading 212, XTB) are unaffected; the change is IBKR-Flex-only.
- The deterministic fallback must remain for records that genuinely lack a
  canonical ID — it cannot be removed.
- Existing connectors and the normalized/analytics layers must keep working with
  no schema migration (the normalized table is overwritten each run, per 0069).
- XTB remains deferred/unsupported; nothing in this ADR depends on XTB.

## Consequences

- IBKR CDC corrections are now deduplicated correctly: a corrected trade
  replaces the original instead of duplicating it. 0069's dedup works as
  intended for the correction case it could not previously handle.
- Non-STK OpenPositions (OPT/FUT/BOND) classify correctly; zero-positionValue
  holdings with non-zero `position` are no longer dropped. Both were latent
  (current demo data has no such positions) but would surface on real OPT/FUT/
  BOND data.
- The IBKR fixtures now use the real Flex attribute names, so the test suite
  exercises the paths production runs — and golden tests can assert against
  real demo bronze output rather than fantasy shapes.
- Downside: the deterministic fallback is still used for any genuinely-ID-less
  record, so dedup for those records remains hash-based and is not stable across
  corrections to such records. This is accepted because such records are rare
  and raising would be worse.

## Validation

- `tests/test_ibkr_connector.py`: `TestIbkrExtractHoldingsValue` asserts
  decrypted `value` via `pytest.approx` (5000/3000/500/2500/2000); the canonical
  `event_id` test asserts a Trade `event_id == "e001"` (a real canonical ID, not
  a deterministic hash); W3/W4 assert a non-empty parseable payload and that the
  currency override changes `base_currency_by_account`.
- `tests/test_transform_pipeline.py`: `TestIBKRTransform` asserts decrypted
  `security_value` per row; `TestTransformSnapshotGolden` is a golden test at
  the `transform_snapshot` boundary with hand-verified plaintext expected values
  (not SUT snapshots), sorted by a stable key. The OPT fixture position is
  classified `"OPT"`; the zero-`positionValue`/non-zero-`position` BOND fixture
  position is retained.
- Mutation checks (each applied to source, then reverted): `security_value = 1.0`
  and `value = 0.0` fail the IBKR value tests; `decrypt_float → 1.0` fails the
  golden and targeted value tests.
- Full test suite: 725 passed; ruff and pyright clean.