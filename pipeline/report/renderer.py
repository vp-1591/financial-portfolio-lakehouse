"""Render the portfolio report: load data, build charts, produce HTML.

:func:`render_report` is the core function that orchestrates everything and
returns the final HTML string.  :func:`generate_report` is the top-level
entry point called by the CLI subcommand — it also writes the file and
optionally opens a browser.
"""

from __future__ import annotations

import html
import logging
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

import plotly.io as pio
from jinja2 import Environment, FileSystemLoader, select_autoescape

from pipeline.report.charts import (
    allocation_by_broker,
    allocation_by_currency,
    allocation_by_position_type,
    cash_flow_breakdown,
    data_quality_chart,
    passive_income_timeline,
)
from pipeline.report.loader import load_all

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _figures_to_html(figures: list) -> list[str]:
    """Convert Plotly figures to HTML div strings.

    The first figure inlines the plotly.js bundle (~3.5 MB).
    All subsequent figures use ``include_plotlyjs=False`` to avoid
    duplicating the bundle.
    """
    parts: list[str] = []
    for i, fig in enumerate(figures):
        include = "inline" if i == 0 else False
        parts.append(pio.to_html(fig, include_plotlyjs=include, full_html=False))
    return parts


def _summary_table(holdings) -> str:
    """Build an HTML summary table from portfolio holdings data.

    Returns a string of HTML (safe to inject via |safe in the template).
    """
    import polars as pl

    if holdings.is_empty():
        return ""

    base_currency = holdings["base_currency"][0]
    total_value = holdings["value_base"].sum()

    rows: list[str] = []
    # By broker
    broker_agg = (
        holdings.group_by("broker")
        .agg(pl.col("value_base").sum().alias("total"))
        .sort("total", descending=True)
    )
    for row in broker_agg.iter_rows(named=True):
        pct = (row["total"] / total_value * 100) if total_value else 0
        rows.append(
            f"<tr><td>{row['broker']}</td><td>{row['total']:,.2f} {base_currency}</td>"
            f"<td>{pct:.1f}%</td></tr>"
        )

    # By position type
    type_agg = (
        holdings.group_by("position_type")
        .agg(pl.col("value_base").sum().alias("total"))
        .sort("total", descending=True)
    )
    type_rows: list[str] = []
    for row in type_agg.iter_rows(named=True):
        pct = (row["total"] / total_value * 100) if total_value else 0
        type_rows.append(
            f"<tr><td>{row['position_type']}</td><td>{row['total']:,.2f} {base_currency}</td>"
            f"<td>{pct:.1f}%</td></tr>"
        )

    html = (
        "<h4>By Broker</h4><table><tr><th>Broker</th><th>Value</th><th>%</th></tr>"
        + "".join(rows)
        + "</table>"
        "<h4>By Position Type</h4><table><tr><th>Type</th><th>Value</th><th>%</th></tr>"
        + "".join(type_rows)
        + "</table>"
    )
    return html


def _passive_income_table(dividends, interest) -> str:
    """Build an HTML table summarizing passive income totals."""
    parts: list[str] = []

    if not dividends.is_empty():
        total_div = dividends["amount_base"].sum()
        base_curr = (
            dividends["base_currency"][0]
            if "base_currency" in dividends.columns
            else ""
        )
        parts.append(
            f"<p><strong>Total Dividends:</strong> {total_div:,.2f} {base_curr}</p>"
        )

    if not interest.is_empty():
        total_int = interest["amount_base"].sum()
        base_curr = (
            interest["base_currency"][0] if "base_currency" in interest.columns else ""
        )
        parts.append(
            f"<p><strong>Total Interest:</strong> {total_int:,.2f} {base_curr}</p>"
        )

    return "".join(parts)


def _dq_summary(dq) -> tuple[str, str]:
    """Build data quality summary HTML and detailed results table.

    Returns (summary_html, table_html).
    """
    if dq.is_empty():
        return "", ""

    status_counts = dq.group_by("status").len().sort("status")
    badge_class = {"PASS": "badge-pass", "WARN": "badge-warn", "FAIL": "badge-fail"}

    # Summary badges
    badges: list[str] = []
    for row in status_counts.iter_rows(named=True):
        cls = badge_class.get(row["status"], "")
        badges.append(
            f'<span class="badge {cls}">{row["status"]}: {row["len"]}</span> '
        )
    summary_html = '<div class="dq-badges">' + " ".join(badges) + "</div>"

    # Detailed results table
    rows: list[str] = []
    for row in dq.sort("checked_at", descending=True).iter_rows(named=True):
        cls = badge_class.get(row["status"], "")
        rows.append(
            f"<tr><td>{row['checked_at']}</td>"
            f"<td>{html.escape(str(row['table_name']))}</td>"
            f"<td>{row['check_name']}</td>"
            f"<td><span class='badge {cls}'>{row['status']}</span></td>"
            f"<td>{html.escape(str(row['details']))}</td>"
            f"<td>{row.get('threshold', '') or ''}</td>"
            f"<td>{row.get('actual', '') or ''}</td></tr>"
        )

    table_html = (
        "<table><tr><th>Checked At</th><th>Table</th><th>Check</th>"
        "<th>Status</th><th>Details</th><th>Threshold</th><th>Actual</th></tr>"
        + "".join(rows)
        + "</table>"
    )

    return summary_html, table_html


def _dq_failed_tables(dq) -> set[str]:
    """Return set of table names that have at least one FAIL in *dq*.

    Returns an empty set if *dq* is empty.
    """
    import polars as pl

    if dq.is_empty():
        return set()
    failed = dq.filter(pl.col("status") == "FAIL")
    return set(failed["table_name"].to_list())


def render_report(output_path: str, *, base_currency: str | None = None) -> str:
    """Load gold tables, build charts, render HTML, write to *output_path*.

    Returns the HTML string.

    Sections whose dependency tables are empty or have FAIL-level DQ checks
    are hidden from the report.  The Data Quality section is always shown.
    """
    tables = load_all()
    holdings = tables["portfolio_holdings"]
    allocation = tables["portfolio_allocation"]
    dividends = tables["dividend_income"]
    interest = tables["interest_income"]
    cash_flow = tables["cash_flow_summary"]
    dq = tables["data_quality"]

    # Determine which sections are visible based on data presence and DQ status.
    # A section is hidden when ALL its dependency tables are empty OR when any
    # dependency table has a FAIL in the DQ results.
    failed = _dq_failed_tables(dq)
    show_portfolio = (not holdings.is_empty() or not allocation.is_empty()) and not (
        failed & {"portfolio_holdings", "portfolio_allocation"}
    )
    show_passive = (not dividends.is_empty() or not interest.is_empty()) and not (
        failed & {"dividend_income", "interest_income"}
    )
    show_cash_flow = not cash_flow.is_empty() and "cash_flow_summary" not in failed

    # Raise only if there is truly nothing to show (no data, no DQ)
    if not any([show_portfolio, show_passive, show_cash_flow]) and dq.is_empty():
        raise RuntimeError(
            "No analytics tables found. Run 'pipeline analytics' and "
            "'pipeline validate' first to populate gold tables."
        )

    # --- Portfolio summary ---
    summary_cards: list[dict[str, str]] = []
    summary_table_html = ""
    base_cur = base_currency

    if show_portfolio:
        use_holdings = not holdings.is_empty()
        if use_holdings and not base_cur:
            base_cur = holdings["base_currency"][0]

        if use_holdings:
            total = holdings["value_base"].sum()
            summary_cards.append(
                {"label": "Total Value", "value": f"{total:,.2f} {base_cur}"}
            )

        if use_holdings:
            summary_table_html = _summary_table(holdings)
        elif not allocation.is_empty():
            summary_table_html = "<p><em>Portfolio holdings not available; showing allocation percentages only.</em></p>"
            rows_html = []
            for row in allocation.sort("percentage", descending=True).iter_rows(
                named=True
            ):
                rows_html.append(
                    f"<tr><td>{row['ticker']}</td><td>{row['broker']}</td>"
                    f"<td>{row['percentage']:.2f}%</td></tr>"
                )
            summary_table_html += (
                "<table><tr><th>Ticker</th><th>Broker</th><th>%</th></tr>"
                + "".join(rows_html)
                + "</table>"
            )

    # --- Charts ---
    fig_allocation_broker = (
        allocation_by_broker(holdings)
        if show_portfolio and not holdings.is_empty()
        else None
    )
    fig_allocation_type = (
        allocation_by_position_type(holdings)
        if show_portfolio and not holdings.is_empty()
        else None
    )
    fig_allocation_currency = (
        allocation_by_currency(holdings)
        if show_portfolio and not holdings.is_empty()
        else None
    )
    fig_passive = passive_income_timeline(dividends, interest) if show_passive else None
    fig_cash_flow = cash_flow_breakdown(cash_flow) if show_cash_flow else None
    fig_dq = data_quality_chart(dq)

    # Collect non-None figures for inline-plotlyjs handling
    all_figs = [
        f
        for f in [
            fig_allocation_broker,
            fig_allocation_type,
            fig_allocation_currency,
            fig_passive,
            fig_cash_flow,
            fig_dq,
        ]
        if f is not None
    ]
    chart_htmls = _figures_to_html(all_figs)

    # Map chart names to HTML, distributing the inline plotly.js correctly
    chart_names = [
        "allocation_broker",
        "allocation_position_type",
        "allocation_currency",
        "passive_income",
        "cash_flow",
        "data_quality",
    ]
    figs_by_name = {
        "allocation_broker": fig_allocation_broker,
        "allocation_position_type": fig_allocation_type,
        "allocation_currency": fig_allocation_currency,
        "passive_income": fig_passive,
        "cash_flow": fig_cash_flow,
        "data_quality": fig_dq,
    }

    # Build charts dict: name → html string or None
    charts: dict[str, str | None] = {}
    html_idx = 0
    for name in chart_names:
        fig = figs_by_name[name]
        if fig is not None:
            charts[name] = chart_htmls[html_idx]
            html_idx += 1
        else:
            charts[name] = None

    # --- Passive income table ---
    passive_income_table_html = (
        _passive_income_table(dividends, interest) if show_passive else ""
    )

    # --- Data quality (always shown) ---
    dq_summary_html, dq_table_html = _dq_summary(dq)

    # --- Render template ---
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("report.html")

    html = template.render(
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        summary_cards=summary_cards,
        summary_table_html=summary_table_html,
        charts=charts,
        passive_income_table_html=passive_income_table_html,
        dq_summary_html=dq_summary_html,
        dq_table_html=dq_table_html,
        show_portfolio=show_portfolio,
        show_passive=show_passive,
        show_cash_flow=show_cash_flow,
    )

    # Write to file
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("Report written to %s", out)
    return html


def generate_report(
    output_path: str = "data/report.html",
    *,
    base_currency: str | None = None,
    open_browser: bool = False,
) -> int:
    """Top-level entry point for the ``pipeline report`` CLI subcommand.

    Returns 0 on success, 1 on fatal error.
    """
    try:
        render_report(output_path, base_currency=base_currency)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error generating report: {exc}", file=sys.stderr)
        return 1

    print(f"Report written to {Path(output_path).resolve()}")

    if open_browser:
        webbrowser.open(Path(output_path).resolve().as_uri())

    return 0
