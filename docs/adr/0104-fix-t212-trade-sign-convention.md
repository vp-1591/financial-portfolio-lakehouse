# 0104 — Fix T212 Trade `cash_amount`/`gross_amount` Sign Convention

> **Superseded by [ADR 0112](./0112-remove-yagni-gross-amount-column.md)** — the `gross_amount` half of the decision is removed together with the column; the `cash_amount` sign convention carries forward unchanged, see 0112 §Decision.

## Context

ADR 0058 (superseded by the active ADR 0077) established the CDC events sign
convention: **positive = inflow, negative = outflow**. The convention is
carried into the active schema and the column comment at
`pipeline/normalized/models.py` (`cash_amount` — "signed cash impact in
security_ccy").

IBKR already conforms: `_process_ibkr_trade` sets `cash_amount = netCash` and
`gross_amount = proceeds`, both signed (BUY → negative, SELL → positive).

T212 trades did **not** conform. `_transform_orders` stored
`cash_amount = walletImpact.netValue × fx_rate` and
`gross_amount = filledValue × fx_rate` as **unsigned magnitudes** for both BUY
and SELL, carrying direction only in the `side` field. T212 transactions
(deposits/withdrawals) and dividends already conformed (withdrawal negative,
deposit/dividend positive), so the mismatch was isolated to trades.

Impact: the Cash Flow Breakdown chart (`pipeline/report/charts.py`,
`cash_flow_breakdown`) groups by `event_type` + `period_month` and sums the
signed `target_value`/`cash_amount`. Within the `TRADE` event type, unsigned
T212 magnitudes (always positive) were summed against signed IBKR values (buys
negative), so inflows and outflows algebraically canceled and T212 buys were
not represented as outflows. The `TRADE` bar was meaningless. This also blocks
roadmap 0007 Phase 3, which reverse-applies signed CDC cash flows (including
trade cash legs) to reconstruct historical cash balance and
`cumulative_invested_capital`.

## Decision

In `_transform_orders()` (`pipeline/connectors/trading212/transform.py`),
apply a direction multiplier before storing the trade amounts:

```python
direction = pl.when(side == "BUY").then(-1.0).otherwise(1.0)
cash_amount = net_value * fx_rate * direction
gross_amount = filledValue * fx_rate * direction
```

BUY → negative (outflow), SELL → positive (inflow). This makes T212 trades
conform to the ADR 0058/0077 convention and match IBKR.

`fee_amount` and `tax_amount` remain **positive magnitudes**, matching IBKR
(`fee_amount = abs(ibCommission)`). They are stored as magnitudes consistently
across brokers and do not reach the chart as a separate direction; signing
them is out of scope.

The fix is applied at the **transform layer** (the source), not at the chart
or analytics layer. It then propagates automatically: `normalize_currency()`
multiplies by a positive FX rate (preserving sign) → gold
`cash_flow_summary` → chart. No chart or gold change is needed.

**Alternatives considered:**

- *Fix at the chart layer* (negate based on `side` there). Rejected: the chart
  regroups already-aggregated gold values and drops `broker`/`side`, and the
  gold layer sums by `broker + event_type + month` before the chart sees the
  data — by then `side`/direction is irrecoverably lost. The transform layer
  is the only place where direction is still known, and fixing there makes
  every downstream consumer (chart, roadmap 0007 cash reconstruction,
  invested-capital line) correct without per-consumer fixes.
- *Split `TRADE` into `TRADE_BUY`/`TRADE_SELL` event types*. Rejected as a
  larger schema change; the signed convention already encodes direction and
  is sufficient for the chart and roadmap 0007.

## Constraints

- **No schema change.** `cash_amount`/`gross_amount` columns and types are
  unchanged; only the sign of their values for T212 trades.
- **IBKR connector unchanged** — it already conforms.
- **T212 dividends and transactions unchanged** — `_transform_dividends` and
  `_transform_transactions` already conform.
- **`fee_amount`/`tax_amount` sign not changed** — they remain positive
  magnitudes in both brokers.
- **Unknown/missing `side`** (anything not `"BUY"`) falls to `+1`, preserving
  prior behavior; the fix only affects explicit BUYs.

## Consequences

- **Positive:** the Cash Flow Breakdown `TRADE` bar now shows net trade cash
  flow (buys negative, sells positive), consistent across brokers. Roadmap
  0007 Phase 3 cash-balance reconstruction and `cumulative_invested_capital`
  receive correctly signed T212 trade cash legs.
- **Positive:** T212 and IBKR trade `cash_amount`/`gross_amount` are now
  directly comparable (both signed, same convention).
- **Negative:** existing `normalized/trading212_cdc`, `cdc_events`, and gold
  `cash_flow_summary` tables that contain T212 trades hold stale **unsigned**
  values. They must be rebuilt from raw data (re-run CDC ingest → normalize →
  analytics) for the chart and analytics to reflect the fix on historical data.
  New ingests produce correct values automatically.
- **Negative (pre-existing, unchanged):** within the `TRADE` event type the
  gold layer still nets buys against sells per broker per month before the
  chart sees them, so the chart shows **net** trade cash flow, not gross
  buys/sells separately. Showing gross money-in/money-out is a separate,
  deferred decision (would require splitting inflow/outflow at the gold
  layer before summing).

## Validation

- `test_transform_cdc_orders_produces_trade_events` asserts a BUY produces
  `cash_amount` and `gross_amount` of `-1500.0`.
- `test_transform_cdc_order_sell_side` asserts a SELL keeps `cash_amount` and
  `gross_amount` at `+1500.0`.
- `test_transform_cdc_order_cross_currency_fx_rate`,
  `test_transform_cdc_order_gbx_security_currency`,
  `test_transform_cdc_order_fee_tax_converted_to_security_ccy`, and
  `test_transform_cdc_order_missing_optional_fields` assert negated
  `cash_amount`/`gross_amount` for BUY while `fee_amount`/`tax_amount` stay
  positive.
- All 756 tests pass; `ruff check --fix .` + `ruff format .` and
  `pyright pipeline/ tests/` are clean.
- Manual (deferred to the user): regenerate the report and confirm the Cash
  Flow Breakdown `TRADE` bar reflects signed net trade cash flow.