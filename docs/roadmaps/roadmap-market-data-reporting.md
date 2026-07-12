# Roadmap: Market Data Integration and Performance Reporting

## Goal

Reconstruct historical portfolio value over time by integrating external market
data, and add the charts that depend on it: the performance chart (portfolio
value vs. invested capital), asset allocation over time, and finer-grained
position-type classification. This roadmap builds on the reporting baseline
roadmap, which delivered current-state reporting from snapshots and CDC events.

Who it's for: the portfolio owner who wants to see how their portfolio has
performed over time — not just what it holds now, but whether it has grown
relative to the capital invested.

## Current state

- **Reporting baseline roadmap** delivers a current-state HTML report with
  allocation, passive income, and cash flow charts from snapshots and CDC.
- **CDC events** (`cdc_events`) record trades, deposits, withdrawals,
  dividends, interest, fees, taxes — the full cash-flow and trade history.
- **Current snapshot** (`consolidated_holdings` + per-broker snapshot tables)
  gives ground-truth quantities held right now.
- **No market data integration** — no price history, no external instrument
  metadata.
- **No position history** — we know current holdings and individual trades, but
  not "what was held on date X" as a reconstructed table.
- **No performance chart** — the "holy grail" from `DATA_ANALYST_DATA_MODELLING.md`
  (portfolio value vs. net invested capital) is not possible without either
  accumulated snapshots or market data.

The core problem from `IBKR_DATA_MODELLING.md`: CDC records cash flows, not
market value changes. Unrealized gains/losses are invisible in CDC. To plot
portfolio value over time, we need `date → sum(quantity_held × price_on_date)`
for equities plus the cash balance on that date. Market data provides the
prices; CDC trades + the current snapshot provide the quantities; CDC cash
flows + the current cash balance provide historical cash.

This roadmap uses Approach C from `IBKR_DATA_MODELLING.md` (external market
data) rather than Approach A (snapshot accumulation), per the explicit
preference to avoid hoarding snapshots. Market data fills gaps for dates when
the pipeline did not run, and avoids dependence on continuous snapshot
collection.

## Success criteria

- [ ] A `position_history` table exists, reconstructing quantity held per
  security per date from CDC trades anchored to the current snapshot —
  verifiable by checking that reconstructed quantities on the latest date
  match the current snapshot
- [ ] A `price_history` table exists with historical close prices for each
  security ever held, fetched from an external market data source
- [ ] A `portfolio_value_history` table exists with daily portfolio value
  (equities at market price + reconstructed cash balance), converted to the
  account base currency
- [ ] The HTML report (or a performance report variant) includes:
  - a time-series line chart of total portfolio value over time
  - a second line for cumulative net invested capital (deposits minus
    withdrawals), so the gap visualizes total P&L
- [ ] Position-type classification tags positions beyond EQUITY/CASH (e.g.,
  ETF, stock, bond) using instrument metadata from the market data source
- [ ] All new code has tests; `ruff check --fix .` and `ruff format .` pass
- [ ] Running the report on demo data produces a visible performance chart
  without errors (accounting for the demo data gap — see Phase 3)

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| **Approach A: Regular snapshot accumulation** | Requires the pipeline to run on every date we want data for; gaps when it doesn't run cannot be backfilled. The user explicitly prefers market data over hoarding snapshots. Kept as an optional validation anchor, not the primary source. |
| **Approach B: Snapshot anchors + CDC interpolation** | Only approximates between anchors — unrealized gains are invisible. Market data gives accurate valuations for every date, so interpolation is unnecessary. |
| **LLM-based position-type classification** | Adds a dependency on an LLM API and non-deterministic categorization. Instrument metadata from market data APIs (asset type, sector, category) is deterministic and comes for free with price fetches. LLM classification can be a fallback for instruments lacking metadata. |
| **Paid market data API (Alpha Vantage, Financial Modeling Prep)** | Adds API key management and rate limits/costs for a personal-use tool. Yahoo Finance via `yfinance` is free and sufficient for daily close prices. Can swap the provider behind an interface if `yfinance` becomes unreliable. |
| **Reconstructing positions forward from the first trade** | Errors accumulate forward and the demo account has no deposit events, so the initial balance is unknown. Reconstructing backward from the current snapshot (ground truth) avoids both problems. |

## Phases

### Phase 1 — Position history reconstruction *[status: planned]*

Reconstruct the quantity of each security held on each historical date, by
working backward from the current snapshot and applying CDC trades in reverse.
This is the foundation: market data provides prices, but we must know
*quantities held* on each date to value the portfolio.

**Scope:**
- [ ] `position_history` table: one row per `(date, security)` with quantity
  held, derived from current snapshot quantities minus reverse-applied trades
- [ ] Handle buys (subtract quantity when going backward) and sells (add
  quantity when going backward), including TRANSFER events that move securities
- [ ] Date granularity: daily series from the earliest CDC trade date to the
  latest snapshot date (forward-fill quantities between trade dates)
- [ ] Validation: reconstructed quantities on the latest date match the current
  snapshot exactly (assertion-based test)
- [ ] Handle the demo data gap: when no DEPOSIT events exist, flag the account
  so downstream cash reconstruction can inject a synthetic opening balance
  (see `IBKR_DATA_MODELLING.md` demo data trap)

**Out of scope:**
- Historical prices — Phase 2
- Cash balance reconstruction — Phase 3 (cash has no unrealized gains, so it
  is reconstructed separately from equity positions)
- Multi-currency conversion of quantities — quantities are currency-agnostic;
  FX conversion happens in Phase 3 at valuation time

**Links:** ADR 0058 (broker-neutral CDC schema), `IBKR_DATA_MODELLING.md`

---

### Phase 2 — Market data integration *[status: planned]*

Fetch historical close prices for each security ever held, plus instrument
metadata for position-type classification. Cache results to avoid re-fetching.

**Scope:**
- [ ] `MarketDataProvider` interface with an initial `YFinanceProvider`
  implementation (fetch daily close prices by ticker and date range)
- [ ] `price_history` table: one row per `(date, ticker)` with close price in
  the security's currency
- [ ] `instrument_metadata` table: one row per security with asset type
  (ETF/stock/bond/etc.), sector, industry, and category — sourced from the
  market data provider
- [ ] Caching: only fetch dates not already in `price_history`; backfill on
  subsequent runs
- [ ] Broker-to-market-data ticker mapping (IBKR conids/ISINs, T212 ISINs, XTB
  symbols → market data ticker) with a fallback using ISIN lookup
- [ ] Rate-limit handling and graceful degradation: missing prices for a
  security do not fail the whole run; that security is excluded from valuation
  with a logged warning

**Out of scope:**
- Intraday prices — daily close is sufficient for portfolio valuation
- Real-time quotes — this is historical reporting, not live monitoring
- Fundamental data (earnings, ratios) — not needed for the planned charts
- FX rate history — the existing `fx_rate_to_base` from broker data and CDC
  is used; fetching historical FX rates is deferred unless valuation accuracy
  requires it

**Links:** ADR 0003 (medallion architecture), `DATA_ANALYST_DATA_MODELLING.md`

---

### Phase 3 — Historical portfolio valuation *[status: planned]*

Combine position history, market prices, and CDC cash flows into a daily
portfolio value series converted to the account base currency.

**Scope:**
- [ ] `portfolio_value_history` table: one row per date with equity value
  (`sum(quantity_held × price_on_date)`), cash balance, and total value, all in
  base currency
- [ ] Cash balance reconstruction: current cash from snapshot, minus
  reverse-applied CDC cash flows (deposits, withdrawals, dividends, interest,
  fees, taxes, trade cash legs) — cash has no unrealized gains, so CDC is
  sufficient
- [ ] FX conversion: equity and cash values converted to base currency using
  `fx_rate_to_base` from CDC/broker data (or a static rate where historical FX
  is unavailable, with a logged assumption)
- [ ] Synthetic opening balance for demo accounts: inject a "Day 0" deposit
  equal to the reconstructed opening cash balance so charts do not start at
  zero (per the demo data trap in `IBKR_DATA_MODELLING.md`)
- [ ] `cumulative_invested_capital` field: running sum of DEPOSIT minus
  WITHDRAWAL events, for the performance chart's capital line
- [ ] Validation: portfolio value on the latest snapshot date is within a
  tolerance of the current snapshot total value (accounts for minor FX/timing
  differences)

**Out of scope:**
- Returns attribution (which securities drove gains) — future work
- Tax-lot-level cost basis — requires trade lot tracking beyond this roadmap
- Benchmark comparison (e.g., vs. S&P 500) — can be added later using the same
  market data provider

**Links:** `IBKR_DATA_MODELLING.md` (Approach C), ADR 0058

---

### Phase 4 — Performance reporting *[status: planned]*

Extend the HTML report with the charts that depend on market data and position
history. Position-type classification uses instrument metadata from Phase 2.

**Scope:**
- [ ] Performance chart: time-series line chart with two lines — total
  portfolio value and cumulative net invested capital — so the gap visualizes
  total lifetime P&L (realized + unrealized)
- [ ] Asset allocation over time: 100% stacked area chart of EQUITY vs. CASH
  (and finer position types once classified) over time
- [ ] Position-type breakdown chart: pie/stacked chart using instrument
  metadata (ETF, stock, bond, etc.) replacing the binary EQUITY/CASH split
  from the baseline report
- [ ] Extend the existing `pipeline report` subcommand (or add a `--performance`
  flag) to include these sections when `portfolio_value_history` is populated
- [ ] Graceful degradation: if market data is missing or Phase 1–3 tables are
  empty, the report falls back to the baseline (current-state) sections without
  errors
- [ ] Tests for each chart's data preparation logic

**Out of scope:**
- Email delivery and scheduling — productionization step 5
- Interactive dashboard (Streamlit/Dash) — the self-contained HTML report
  remains the format
- Custom date-range selection in the UI — the report covers the full available
  history; ad-hoc queries use `pipeline query`
- LLM-based classification fallback — deferred unless instrument metadata is
  insufficient for a meaningful number of holdings

**Links:** Reporting baseline roadmap, ADR 0045 (transform utilities)

---

## Future

The following are explicitly out of scope and belong in later roadmaps:

### Returns attribution

- Break down performance by security, sector, and asset class
- Separate realized vs. unrealized gains
- Benchmark comparison (e.g., portfolio vs. S&P 500 or a custom benchmark)
- Risk metrics (volatility, max drawdown, Sharpe ratio)

### Delivery and automation (productionization step 5)

- Email delivery of the generated report
- Scheduled execution via Step Functions
- Run metadata and error visibility