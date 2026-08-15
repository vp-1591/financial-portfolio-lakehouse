# 0102 — Standardize Snapshot Schemas and Fix T212 security_ccy to Instrument Currency

## Context

The Trading 212 snapshot `security_ccy` showed PLN (the wallet/account currency) where it should
show the instrument's trading currency (EUR/USD). This was a deliberate decision, not an
accident: [ADR 0095](./0095-fix-t212-snapshot-ccy-gbx-rate.md) (Bug 1) set snapshot
`security_ccy = position_currency()` (wallet currency) to match `security_value =
walletImpact.currentValue`, which is denominated in the wallet currency. The goal was to avoid a
value↔currency mismatch that inflated the portfolio ~3.3× (labeling a wallet-PLN value with
instrument EUR). But this made `security_ccy` mean different things across brokers — T212 = wallet
currency, IBKR = instrument currency — which conflicts with [ADR 0073](./0073-currency-exposure-donut-chart.md):
the Currency Exposure donut groups `portfolio_holdings` by `security_ccy` on the assumption it
carries the instrument's native trading currency. ADR 0095 is already marked superseded by
[ADR 0097](./0097-remove-yahoo-finance-fx-provider.md), but 0097 only removed the Yahoo FX
fallback; the snapshot `security_ccy` = wallet-currency decision remained in force.

Investigation on staging confirmed the T212 `/equity/positions` payload already exposes the
instrument-currency value directly. The raw `trading212_snapshot_raw` table was queried and
decrypted, the `/equity/positions` payload located, and the first position's structure inspected —
`currentPrice` (instrument currency, e.g. EUR) × `quantity` = the position's market value in the
instrument currency, with `walletImpact` carrying `currentValue` (wallet currency) and the `fxRate`
used to convert — **without any FX fetch**. Verified: `currentPrice × quantity ≈ 463.93 EUR`, and
`463.93 × ~4.31 (PLN/EUR) ≈ 1999.4 PLN = walletImpact.currentValue`. Switching
`security_value` to the instrument value makes `security_ccy` = instrument currency consistent
with IBKR, while the value↔currency pair stays consistent — so the 3.3× inflation ADR 0095 feared
does not occur (we are not relabeling a wallet value with instrument ccy; we are switching the
value itself to instrument currency). The portfolio total is value-preserving for real data: a
wallet-PLN value × the PLN→EUR rate equals the instrument-EUR value, by the triangular-arbitrage
identity among consistent FX rates.

A second, structural problem enabled this bug to go uncaught: the snapshot path had **three
separate `pa.schema` objects** (`ibkr_`/`trading212_`/`xtb_snapshot_normalized_schema`) that
differed on the display column (`description` vs `name`) and field ordering.
`pipeline/analytics/quality.py` checked each table against its own schema, so nothing prevented
the three from drifting apart. The CDC path already avoided this by sharing one
`cdc_events_normalized_schema` across all brokers.

## Decision

1. **T212 snapshot uses instrument currency.** In `transform_snapshot()`, equity
   `security_value`/`security_ccy` are read directly from the `/equity/positions` payload —
   `security_value` = `currentPrice × quantity`, `security_ccy` = the embedded
   `instrument.currency` — both assumed present, with no fallback. This is backed by the schema
   evidence: all 72 positions across the staging snapshots carry the embedded `instrument` dict
   with `ticker`, `name`, `isin`, `currency`, and there is a single stable key-set per endpoint
   (cross-checked against the vendored OpenAPI spec). The `/metadata/instruments` fetch is dropped
   (the instrument dict is already embedded in each position, so no separate reference is needed).
   A future absent field now surfaces loudly instead of silently degrading — accepted as strictly
   safer, since one removed fallback ladder would silently have used `workingScheduleId` (an int
   ID) as a currency code. The instrument identity keys (`ticker`/`name`/`isin`/`currency`) are
   read directly and fast-fail with a clear `KeyError` if absent; a null `currentPrice` or
   `quantity` fast-fails with a `ValueError` rather than being silently skipped. Both fields are
   present and non-null on every position across all staging snapshots (72/72, verified), so a
   null is not a normal API state (a suspended instrument still carries both) but a
   corrupted/truncated payload — surfacing it loudly at the transform is strictly safer than
   silently dropping a position from the portfolio. A genuinely zero-value position (both fields
   present, product 0) is still skipped quietly. CASH rows keep wallet currency (cash is held
   in the wallet currency; `security_ccy = currency` is already correct). This **supersedes the
   snapshot `security_ccy` = wallet-currency decision (Bug 1) of ADR 0095**. GBX handling via
   `MINOR_CURRENCY_UNITS` (0095 Bug 2) remains unchanged (originally decided in ADR 0095 §Decision).
   ADR 0097 (Yahoo removal) is unchanged.

2. **One shared snapshot schema.** The three per-broker snapshot schemas collapse into a single
   `snapshot_normalized_schema` (field order: `fetched_at, account_id, position_type, label,
   asset_class, security_value(binary), security_ccy, isin, description`) so drift is
   structurally impossible. The display column is `description` (matches downstream
   `Holding.description`, `consolidated_holdings.description`, `portfolio_holdings.description`;
   charts never read it). T212/XTB rename `name`→`description`; IBKR already used `description`.
   `quality.py` `TABLE_SCHEMAS` points all three snapshot keys at the shared schema;
   `REQUIRED_FIELDS` is unchanged.

3. **XTB is a no-op semantically.** XTB keeps account-currency `security_ccy` (PLN) and only
   conforms structurally (`name`→`description`). The XTB XLSX export exposes no per-position
   instrument currency, so instrument-ccy for XTB is deferred; the Currency Exposure grouping for
   XTB remains account-currency — a documented known limitation.

4. **No `instrument_ccy` column on snapshots.** Per the project's one-column-name-one-meaning
   principle, no `instrument_ccy` (or `instrument_value`) column is added to the snapshot schemas
   — it would carry different semantic weight across the snapshot vs CDC paths. `instrument_ccy`
   remains CDC-only (ADR 0080).

This **reconciles ADR 0073**: the chart's assumption that `security_ccy` is the instrument trading
currency now holds for IBKR + T212. It does not change ADR 0095 Bug 2 (GBX), ADR 0097 (Yahoo), or
ADR 0080 (`instrument_ccy` CDC-only).

## Constraints

- No `instrument_ccy` or `instrument_value` columns added to snapshot/consolidated schemas
  (one-column-name-one-meaning).
- CDC path is unchanged (already shares one schema; trades already use instrument currency).
- XTB instrument-ccy is deferred (no per-position instrument currency in the XLSX export).
- A migration script `pipeline/migrations/migrate_snapshot_schema_unify.py` renames
  `name`→`description` in existing `trading212_snapshot` and `xtb_snapshot` normalized Delta tables
  so a deploy's `validate` does not fail on a stale `name`-column table before the next transform.
  It does NOT recompute T212 `security_ccy`/`security_value` — that happens automatically when
  `transform_snapshot` re-runs (`mode="overwrite"`) on the next pipeline run. It is idempotent
  (skips absent and already-migrated tables) and supports `--dry-run`. After this PR the prior
  one-time migration scripts (`migrate_001_encrypt_gold_values`, `migrate_drop_account_id`,
  `migrate_drop_conid`, `migrate_rename_currency_columns`, `migrate_phase2_phase3_schema`) and the
  `run-migration` CLI subcommand were removed once confirmed applied to all environments, leaving
  `migrate_snapshot_schema_unify.py` as the only script in `pipeline/migrations/`.

## Consequences

- **Positive:** `security_ccy` means one thing (instrument trading currency) across IBKR + T212
  snapshots; ADR 0073's chart grouping is correct for both. Drift between broker snapshot schemas
  is now structurally impossible (one shared schema checked by `quality.py`).
- **Positive:** The fix needs no FX fetch — `currentPrice × quantity` is already in the payload.
- **Negative / known limitation:** XTB snapshot `security_ccy` stays account currency, so the
  currency-exposure grouping for XTB is still wrong until XLSX instrument-currency data is
  available.
- **Negative (sub-cent rounding):** `walletImpact.currentValue` is 2-dp PLN; `currentPrice ×
  quantity` carries more precision in the instrument currency, so the silver `security_value` is no
  longer bit-identical to the wallet value. The portfolio total is value-preserving for real data
  (within FX-rate consistency); fixture totals change because the fixtures use round numbers where
  the wallet-PLN value numerically equals the instrument-EUR value.

## Validation

- Full suite: 741 tests pass; `pyright pipeline/ tests/` reports 0 errors; `ruff check` clean.
- `tests/test_trading212_connector.py` covers the direct-read transform: equity
  `security_value` = `currentPrice × quantity` paired with the embedded `instrument.currency`,
  and CASH rows keep wallet currency. The wallet-currency fallback was removed (no longer needed
  — 72/72 positions carry the embedded instrument dict), so the fallback tests
  (`test_transform_snapshot_falls_back_to_wallet_ccy_*`,
  `test_position_security_currency_returns_none_when_unresolvable`) and the `position_*` /
  `position_security_*` unit tests for the deleted helpers were removed. New coverage:
  `test_transform_snapshot_works_without_instruments_source` (transform produces correct rows
  when `/metadata/instruments` is absent from the raw table) and
  `TestSnapshotFetch.test_fetch_snapshot_does_not_request_instruments` (the snapshot fetch calls
  `account_summary` + `positions` but not `/metadata/instruments`). The fast-fail
  coverage `test_transform_snapshot_raises_on_null_price` (null `currentPrice`/`quantity`
  raises `ValueError`) and `test_transform_snapshot_skips_zero_value_quietly` (a zero-value
  position with both fields present is skipped, not raised) was added; the null-field case was
  verified absent across all 72 staging positions by decrypting every `/equity/positions` payload
  in the raw staging table and checking each position.
- `test_snapshot_security_ccy_uses_wallet_currency` is renamed to
  `test_snapshot_security_ccy_uses_instrument_currency`; equity assertions flipped to instrument
  currency, CASH stays wallet currency.
- FX expectations recomputed in `tests/test_consolidate_pipeline.py`: VWCEl_EQ EUR→EUR `2500.0`
  (was `625.0`); AAPLu_EQ USD→EUR `1620.0` (was `450.0`); CASH PLN→EUR `375.0` (unchanged). In
  `tests/test_portfolio_holdings.py` the `total_target` is recomputed to `17145.0` and all
  percentage expectations updated.
- Migration `pipeline/migrations/migrate_snapshot_schema_unify.py` is covered by
  `tests/test_migrate_snapshot_schema_unify.py` (4 cases: migrate + order-sensitive schema check,
  idempotent-already-migrated, missing-table, dry-run). The order-sensitive `schema.equals`
  assertion would have caught the column-reorder bug fixed in commit `3d2271d`. `--dry-run`
  supported, idempotent, skips absent/already-migrated tables.
- Staging check (manual, env populated): `SELECT label, security_ccy FROM
  trading212_snapshot_normalized` should show instrument currency (EUR/USD) for equities and PLN
  for CASH after re-running `pipeline transform` for trading212; before the fix this was PLN for
  every row.