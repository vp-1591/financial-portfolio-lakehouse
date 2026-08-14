# 0105 — Fix T212 CDC Dedup and Cross-Endpoint Concat Type Mismatch

## Context

T212 CDC duplicated trades in `cdc_events_normalized`, double-counting cash
sums in analytics. The duplication is structural, not a regression:

1. **T212 CDC fetches the full order/dividend/transaction history on every
   run** (`/equity/history/orders` with no date range). Every fetch after the
   first is a 100% date-range overlap with the previous.
2. The raw layer is **append-only**, and `dedup_raw` dedups only on the
   whole-page SHA-256 (`payload_hash`). T212 paginates by `nextPagePath`; when
   a new order is added between runs, every page's content shifts and its hash
   changes, so `dedup_raw` re-appends all pages. Even without a new order, any
   response nondeterminism (field ordering, a changed `updatedAt`) re-appends.
3. The T212 CDC transform — unlike IBKR — had **no `event_id` dedup**, and
   `consolidate_cdc_events` just `pa.concat_tables` with no dedup. Duplicates
   flowed straight into `cdc_events` and `cdc_events_normalized`.

ADR 0069 added `event_id` dedup to the **IBKR** CDC transform and explicitly
recorded the constraint: *"Other brokers (T212, XTB) have truly incremental
CDC data and must not be affected. The dedup is IBKR-specific."* That
assumption is wrong for T212: T212 CDC is full-history, the same shape as
IBKR. The prior snapshot-dedup work (ADR 0057, ADR 0100) correctly excluded
CDC, so the gap was never closed for T212 CDC.

While adding the dedup, a **second latent bug** surfaced: the T212 per-endpoint
transforms set `instrument_ccy=pl.lit(None)` (orders, transactions) producing
Null-typed columns, while dividends set `instrument_ccy=pl.col("tickerCurrency")`
(String). `pl.concat(dfs)` runs with the default vertical strategy, and
production fetch order is orders → dividends → transactions, so the first
frame fixes the expected schema to Null and the dividends String column is
rejected (`type String is incompatible with expected type Null`). No existing
test concatenated mixed endpoints in a single `transform_cdc` call, so the bug
was uncaught and would crash any T212 run with both orders and dividends.

## Decision

- **Add `event_id` dedup to the T212 CDC transform**, mirroring the IBKR
  pattern (ADR 0069). After `pl.concat(dfs)` and before `finalize_table`,
  sort by `fetched_at` descending and `.unique(subset=["event_type",
  "event_id"], keep="first")`, keeping the latest-fetched version, then
  re-sort for deterministic order. `keep="first"` is required because
  `unique()`'s default `keep="any"` is non-deterministic and may drop the
  newest version despite the descending sort. The subset is
  `["event_type", "event_id"]` — **not**
  `["event_id"]` alone as in IBKR — because T212 `order.id` is an integer cast
  to string while dividend/transaction `reference` is a separate string ID
  space; a dividend `reference` of `"12345"` could collide with order id
  `12345`. `event_type` scopes uniqueness to each ID space at no cost. `broker`
  is constant `"Trading 212"` at the transform layer, so it is omitted there.
  IBKR event_ids are constructed globally unique across types, so IBKR's
  `["event_id"]`-only subset remains correct and is unchanged.

- **Add a defense-in-depth dedup at `consolidate_cdc_events`** on
  `["broker", "event_type", "event_id"]` (broker varies across the concat).
  Each broker's transform already dedups its own events, but this boundary
  check catches the full-history re-fetch class of bug regardless of which
  broker forgot transform-level dedup, and guards XTB and future brokers. This
  is a boundary check at a different layer (cross-broker correctness), not
  duplicated logic.

- **Fix the `instrument_ccy` concat type mismatch** by concatenating the
  per-endpoint frames with `pl.concat(dfs, how="vertical_relaxed")`, which
  promotes mismatched column types at the concat boundary (Null → String for
  `instrument_ccy`, which is `pl.lit(None)` for orders/transactions but
  `pl.col("tickerCurrency")` for dividends). This replaces per-endpoint
  `pl.lit(None).cast(pl.Utf8)` casts with a single concat-site setting and
  protects against future columns that are Null in one endpoint and typed in
  another.

- **Share the dedup recipe via `dedup_cdc_events`** in
  `pipeline/connectors/transform_utils.py`. The "sort by `fetched_at`
  descending → `unique(keep="first")` → log → optional re-sort" pattern is
  used by both the T212 transform and the `consolidate_cdc_events` boundary
  check; a single helper keeps them consistent so a future policy change
  (subset, keep, sort) propagates from one place. IBKR's inline dedup is
  carried forward unchanged for now (see Constraints); adopting the helper
  there is a follow-up.

- **No downstream duplicate-`event_id` quality check.** With two active
  `unique()` filters (transform + consolidate) running in the same pipeline, a
  check sitting after them is tautological — it can only fail if both filters
  are broken, and the unit tests guard the dedup logic in CI. Active filtering
  is preferred over a passive observer that can observe nothing.

- **No schema migration.** The fix changes transform behavior only; `{broker}_cdc`
  and `cdc_events` are overwritten each run, so the next run auto-corrects. Raw
  duplicates remain (raw is append-only) but transform dedup handles them every run.

ADR 0069's IBKR `event_datetime` normalization and IBKR `event_id` dedup
decisions remain valid and are carried forward unchanged (originally decided
in ADR 0069, §Decision); only the "T212/XTB are truly incremental" constraint
is superseded.

## Constraints

- Must not change `cdc_events_normalized_schema` (no column type changes, no
  added/removed columns) — the fix is behavior-only, so no migration.
- Must not apply `filter_latest_snapshot` to CDC (unchanged since ADR 0057);
  the correct fix is event-level dedup, not snapshot filtering, which would
  drop unique old orders.
- Must not break the IBKR CDC dedup or its `["event_id"]`-only subset — IBKR
  event_ids are globally unique and need no `event_type` scoping.
- XTB remains optional and file-based; the consolidate boundary dedup must not
  require XTB to be present.
- The dedup must keep the latest-`fetched_at` version of a duplicated event,
  not an arbitrary one, so field corrections in later fetches win.

## Consequences

- T212 CDC events appear once per `(event_type, event_id)`; analytics cash sums
  and `event_count` are no longer inflated by duplicates.
- The `vertical_relaxed` concat unblocks `transform_cdc` for T212 accounts that
  have both orders and dividends (previously a latent crash).
- The consolidate boundary dedup is currently a no-op for IBKR and T212 (both
  dedup at transform) but earns its keep as a regression catch and XTB/future
  guard; cost is one `unique()` per consolidate run.
- A genuine downside accepted: there is no production-state quality check that
  would halt a deploy if a future change silently regresses both dedup layers
  and slips past CI. This is traded against the tautology of a post-filter
  check; the unit tests are the chosen regression guard instead.
- Raw-layer duplicate pages are NOT removed — they accumulate in the append-only
  raw table. This is accepted (raw is immutable history); transform dedup
  handles them on every run. A future raw-compaction effort is out of scope.

## Validation

- `tests/test_trading212_connector.py::TestCdcTransform::test_transform_cdc_deduplicates_across_payloads`
  — `pa.concat_tables([raw, raw])` of an order payload yields one row with
  unique `(event_type, event_id)` keys.
- `tests/test_trading212_connector.py::TestCdcTransform::test_transform_cdc_dedup_scopes_by_event_type`
  — an order (`id=12345` → `TRADE`/`"12345"`) and a dividend
  (`reference="12345"` → `DIVIDEND`/`"12345"`) survive as two distinct rows;
  this is the load-bearing proof that `event_type` belongs in the subset (it
  would fail under `["event_id"]` alone) and that mixed-endpoint concat now
  succeeds (it would have hit the Null/String SchemaError before the
  `vertical_relaxed` concat).
- `tests/test_trading212_connector.py::TestCdcTransform::test_transform_cdc_dedup_keeps_latest_fetched_version`
  — two payloads with the same order id but different `fetched_at` and a
  corrected `walletImpact.netValue`; asserts the latest-fetched
  `cash_amount` wins. Verified to fail with `keep="last"`, guarding the
  `keep="first"` contract.
- `tests/test_consolidate_cdc.py` — existing consolidate tests still pass
  (boundary dedup is a no-op when inputs are already unique).
- `tests/test_ibkr_connector.py::TestCdcTransform::test_transform_cdc_deduplicates_across_payloads`
  — unchanged, still passes.
- Full suite: 759 tests pass; `ruff check`/`ruff format` clean; `pyright
  pipeline/ tests/` 0 errors.
