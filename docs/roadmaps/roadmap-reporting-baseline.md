# Roadmap: Reporting Baseline with Current Data

## Goal

Deliver a single self-contained HTML report that proves the pipeline produces
useful output, using data available right now — the latest snapshot for current
portfolio state and CDC events for historical cash flows. This roadmap also
includes lightweight data quality checks that validate the data the report
consumes, resolving the chicken-and-egg between quality gates and a deliverable.

Who it's for: the portfolio owner — a single place to see portfolio composition,
passive income, investment friction, and cash flow patterns.

## Current state

- **Analytics layer** has one table (`portfolio_allocation`) that is overwritten
  each run — no time-series history.
- **CDC events** are unified in `cdc_events` across all brokers with event types:
  TRADE, DEPOSIT, WITHDRAWAL, DIVIDEND, INTEREST, FEE, TAX, TRANSFER.
- **Snapshot data** exists per-broker in normalized tables (`ibkr_snapshot`,
  `trading212_snapshot`, `xtb_snapshot`) with `fetched_at` timestamps, plus the
  consolidated `consolidated_holdings` table.
- **Position classification** is binary: EQUITY or CASH. No finer-grained
  classification (ETF, stock, bond, gold) exists yet.
- **No reporting code** — zero charting libraries, HTML templates, or report
  subcommands.
- **No data quality framework** — productionization step 3 (staging quality gates)
  is not implemented.
- **Market data integration** does not exist. Historical portfolio value over
  time requires either accumulated snapshots or external price data — neither is
  available. This roadmap intentionally defers market data to a future roadmap.

The core tension from `IBKR_DATA_MODELLING.md`: CDC events alone cannot show
unrealized gains/losses, so a performance chart (portfolio value vs. invested
capital) is not possible without market data. This roadmap focuses on what CDC
*can* show: cash flows categorized by type.

## Success criteria

- [ ] `pipeline report` subcommand generates a self-contained HTML file that
  opens in a browser and displays at least: portfolio allocation summary,
  passive income timeline, and cash flow breakdown
- [ ] Data quality checks run after `allocate` and produce a
  PASS/FAIL/WARN summary covering schema validation, null checks on required
  fields, row count stability, freshness, and reconciliation
- [ ] Report includes a data quality section showing validation results
- [ ] All new code has tests; `ruff check --fix .` and `ruff format .` pass
- [ ] Running `pipeline report` on demo data produces a visible report without
  errors
- [ ] New analytics tables (`dividend_income`, `cash_flow_summary`) are queryable
  via the existing `pipeline query` subcommand

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| **Accumulating snapshots for time-series** | The user explicitly prefers market data over hoarding snapshots. Snapshot accumulation gives gaps when the pipeline skips runs; market data fills those gaps. Deferred to the market data roadmap. |
| **Streamlit/Dash web dashboard** | Adds a running server dependency. A self-contained HTML file is simpler to generate, store, and share — no process to keep alive. May revisit if interactive exploration is needed later. |
| **Building the report without data quality checks** | Quality checks and the report are mutually validating — the report proves the data is present, and the checks prove it's correct. Skipping one weakens confidence in the other. |
| **Matplotlib for charts** | Produces static images; no hover/tooltips. Plotly produces self-contained interactive charts that embed directly in HTML. |
| **Deferring data quality to a separate roadmap** | The productionization roadmap puts quality gates before reporting, but you need a deliverable to validate quality against. Merging them resolves the dependency. |

## Phases

### Phase 1 — Data quality framework *[status: planned]*

Add lightweight validation that runs after the `allocate` step. Produces a
structured quality result (pass/fail per check) that the report can display and
the pipeline can use as a gate.

**Severity model:**

Quality checks produce one of two statuses:

| Status | When | Pipeline behavior |
|--------|------|-------------------|
| **FAIL** | Schema mismatch, required nulls in critical fields | `pipeline validate` exits non-zero → Step Function marks execution FAILED → existing CI visibility (ADR 0062) catches it |
| **WARN** | Row count drop >50%, stale data, reconciliation mismatch >5% | Pipeline continues, logs warning |

Quality checks are **diagnostic** — they report problems, they don't silently
drop or filter data.

**Communication:** `pipeline validate` prints a human-readable summary to
stdout. FAIL causes non-zero exit code (Step Function failure, visible in CI).
WARN logs a message and continues. Both statuses are stored in the
`data_quality` table and displayed in the report's data quality section.
Email/SNS alerting is deferred to productionization step 5.

**Scope:**
- [ ] Schema validation: each Delta table has expected columns and types
- [ ] Null checks: no unexpected nulls in required fields (e.g., `value` in
  holdings, `cash_amount` in CDC events) — **FAIL** on null in required fields
- [ ] Row count stability: row counts don't drop unexpectedly vs. previous run
  (threshold: >50% drop → **WARN**)
- [ ] Freshness check: latest `fetched_at` is within a configurable window —
  **WARN** if stale
- [ ] Basic reconciliation: sum of position values ≈ net liquidation value
  (where available from broker data) — **WARN** if mismatch >5%
- [ ] Quality results stored as a Delta table (`data_quality`) with timestamp,
  check name, status (PASS/FAIL/WARN), and details
- [ ] `pipeline validate` subcommand that runs checks and exits non-zero on
  **FAIL** status; prints summary to stdout; stores results in `data_quality`
  table

**Out of scope:**
- Idempotency checks (running pipeline twice produces identical output) —
  requires snapshot history comparison, deferred to when accumulation exists
- Cross-broker reconciliation (e.g., T212 cash balance matches IBKR cash
  balance) — not meaningful until multi-broker aggregation is validated
- Alerting or notification on quality failures — deferred to productionization
  step 5
- Dropping or filtering bad data — checks are diagnostic, not corrective

**Links:** ADR 0003 (medallion architecture), productionization roadmap step 3

---

### Phase 2 — CDC analytics tables *[status: in progress]*

Create gold analytics tables from CDC events. These power the cash-flow-based
charts in the report and are queryable independently via `pipeline query`.

**Scope:**
- [ ] `dividend_income` table: dividends grouped by period (month/quarter),
  broker, and security — includes `amount_base` for cross-currency comparison
- [ ] `interest_income` table: interest received/paid grouped by period and
  broker
- [ ] `cash_flow_summary` table: all CDC events aggregated by period and type —
  deposits, withdrawals, dividends, interest, fees, taxes, trades
- [ ] New `pipeline report-data` (or equivalent) step that generates these
  tables from `cdc_events` and current snapshot
- [ ] Unit tests for each table's transformation logic

**Out of scope:**
- Historical portfolio value or returns — requires market data (roadmap 2)
- Position-level P&L or cost basis — requires market data
- Currency conversion beyond `amount_base` already present in CDC schema

**Links:** ADR 0058 (broker-neutral CDC schema)

---

### Phase 3 — Report generation *[status: planned]*

Build the self-contained HTML report using Jinja2 templates and Plotly charts.
Includes portfolio summary from the current snapshot and CDC-based charts.

**Scope:**
- [ ] Add `plotly` and `jinja2` to project dependencies
- [ ] HTML report template with sections:
  - **Portfolio summary**: current total value, allocation by broker, by
    currency, by position type (EQUITY/CASH)
  - **Passive income timeline**: stacked bar chart of dividends + interest by
    month, powered by `dividend_income` and `interest_income` tables
  - **Cash flow breakdown**: bar chart of deposits, withdrawals, dividends,
    fees, taxes by month, powered by `cash_flow_summary`
  - **Data quality section**: pass/fail summary from Phase 1 validation
- [ ] `pipeline report` subcommand that generates the HTML file
- [ ] Report output path configurable (default: `data/report.html` or similar)
- [ ] All chart data derived from analytics Delta tables (no raw/normalized
  table queries in the report — gold layer is the single source of truth)

**Out of scope:**
- Portfolio value over time chart — requires market data (future roadmap)
- Performance chart (value vs. invested capital) — requires market data
- Asset allocation over time — requires snapshot history or market data
- Position-type enrichment beyond EQUITY/CASH — deferred to market data roadmap
- Email delivery — deferred to productionization step 5
- PDF or image export — HTML-only for now

**Links:** ADR 0045 (transform utilities), ADR 0058 (CDC schema)

---

## Future

The following are explicitly out of scope for this roadmap and belong in future
roadmaps:

### Market data integration roadmap (roadmap 2)

- Integrate an external price data source (e.g., Yahoo Finance, Alpha Vantage)
- Reconstruct historical portfolio value over time:
  `date → sum(quantity_held × price_on_date)` for equities + cash balance
- Build the "holy grail" performance chart: portfolio value vs. invested capital
- Asset allocation over time (100% stacked area chart)
- Position-type classification enrichment (ETF, stock, bond, gold) — possibly
  using market data metadata or LLM-assisted categorization

### Delivery and automation (productionization step 5)

- Email delivery of the generated report
- Manual trigger support for pipeline and report generation
- Scheduled execution via Step Functions
- Run metadata and basic error visibility