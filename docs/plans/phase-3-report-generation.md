# Phase 3 — Report Generation (Implementation Plan)

See ADR 0066 for the full context, decisions, and validation.

## What was implemented

1. **Dependencies** — `jinja2>=3.1.0` and `plotly==6.2.0` added to `pyproject.toml`
2. **New gold table `portfolio_holdings`** — joins `consolidated_holdings` (base value)
   with per-broker snapshots (native value, currency, position_type). Schema in
   `pipeline/analytics/models.py`. Builder in `pipeline/analytics/holdings.py`.
3. **Report module** — `pipeline/report/` package with loader, charts, renderer,
   and Jinja2 template. Self-contained HTML with Plotly JS inlined once.
4. **`pipeline report` subcommand** — added to `run.py` with `--output`,
   `--base-currency`, `--open` flags.
5. **Tests** — `tests/test_portfolio_holdings.py` (8 tests) and
   `tests/test_report.py` (6 integration tests). All 536 tests pass.
6. **ADR 0066** — documents the `portfolio_holdings` gold table decision and
   report generation architecture.
7. **Roadmap** — Phase 3 bullets checked off, success criteria checked off,
   note added about `portfolio_holdings` being Phase 2-adjacent.