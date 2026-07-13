"""Plotly figure builders for the portfolio report.

Each function takes one or more Polars DataFrames and returns a
``plotly.graph_objects.Figure``.  No I/O is performed here — only
chart construction.
"""

from __future__ import annotations

import polars as pl
import plotly.graph_objects as go


def allocation_by_broker(holdings: pl.DataFrame) -> go.Figure:
    """Pie chart: portfolio value by broker."""
    if holdings.is_empty():
        return _empty_figure("Portfolio Allocation by Broker")
    agg = (
        holdings.group_by("broker")
        .agg(pl.col("value_base").sum())
        .sort("value_base", descending=True)
    )
    fig = go.Figure(
        data=[
            go.Pie(
                labels=agg["broker"].to_list(),
                values=agg["value_base"].to_list(),
                textinfo="label+percent",
                hole=0.4,
            )
        ],
        layout=go.Layout(
            title="Portfolio Allocation by Broker",
            margin=dict(l=20, r=20, t=40, b=20),
        ),
    )
    return fig


def allocation_by_position_type(holdings: pl.DataFrame) -> go.Figure:
    """Pie chart: EQUITY vs CASH by value."""
    if holdings.is_empty():
        return _empty_figure("Allocation by Position Type")
    agg = (
        holdings.group_by("position_type")
        .agg(pl.col("value_base").sum())
        .sort("value_base", descending=True)
    )
    fig = go.Figure(
        data=[
            go.Pie(
                labels=agg["position_type"].to_list(),
                values=agg["value_base"].to_list(),
                textinfo="label+percent",
                hole=0.4,
            )
        ],
        layout=go.Layout(
            title="Allocation by Position Type",
            margin=dict(l=20, r=20, t=40, b=20),
        ),
    )
    return fig


def allocation_by_currency(holdings: pl.DataFrame) -> go.Figure:
    """Bar chart: portfolio value by native currency."""
    if holdings.is_empty():
        return _empty_figure("Allocation by Currency")
    agg = (
        holdings.group_by("currency")
        .agg(pl.col("value_base").sum())
        .sort("value_base", descending=True)
    )
    fig = go.Figure(
        data=[
            go.Bar(
                x=agg["currency"].to_list(),
                y=agg["value_base"].to_list(),
                text=agg["value_base"].round(2).to_list(),
                textposition="auto",
            )
        ],
        layout=go.Layout(
            title="Allocation by Currency",
            xaxis_title="Currency",
            yaxis_title="Value (base)",
            margin=dict(l=20, r=20, t=40, b=40),
        ),
    )
    return fig


def passive_income_timeline(
    dividends: pl.DataFrame,
    interest: pl.DataFrame,
) -> go.Figure:
    """Stacked bar chart: dividends + interest by month."""
    traces: list[go.Bar] = []

    if not dividends.is_empty():
        div_agg = (
            dividends.group_by("period_month")
            .agg(pl.col("amount_base").sum().alias("total"))
            .sort("period_month")
        )
        value_col = "total"
        traces.append(
            go.Bar(
                x=div_agg["period_month"].to_list(),
                y=div_agg[value_col].to_list(),
                name="Dividends",
            )
        )

    if not interest.is_empty():
        int_agg = (
            interest.group_by("period_month")
            .agg(pl.col("amount_base").sum().alias("total"))
            .sort("period_month")
        )
        value_col = "total"
        traces.append(
            go.Bar(
                x=int_agg["period_month"].to_list(),
                y=int_agg[value_col].to_list(),
                name="Interest",
            )
        )

    fig = go.Figure(
        data=traces,
        layout=go.Layout(
            title="Passive Income Timeline",
            barmode="stack",
            xaxis_title="Month",
            yaxis_title="Amount (base currency)",
            margin=dict(l=20, r=20, t=40, b=40),
        ),
    )
    return fig


def cash_flow_breakdown(cash_flow: pl.DataFrame) -> go.Figure:
    """Grouped bar chart: cash flow by month and event type."""
    if cash_flow.is_empty():
        return _empty_figure("Cash Flow Breakdown")

    # Use amount_base if available, fall back to cash_amount
    if cash_flow["amount_base"].null_count() < cash_flow.height:
        value_col = "amount_base"
    else:
        value_col = "cash_amount"

    event_types = sorted(cash_flow["event_type"].unique().to_list())
    months = sorted(cash_flow["period_month"].unique().to_list())

    traces: list[go.Bar] = []
    for et in event_types:
        et_data = cash_flow.filter(pl.col("event_type") == et)
        agg = (
            et_data.group_by("period_month")
            .agg(pl.col(value_col).sum())
            .sort("period_month")
        )
        # Fill in missing months with 0
        month_vals = {m: 0.0 for m in months}
        for m, v in zip(agg["period_month"].to_list(), agg[value_col].to_list()):
            month_vals[m] = v
        traces.append(
            go.Bar(
                x=months,
                y=[month_vals[m] for m in months],
                name=et,
            )
        )

    fig = go.Figure(
        data=traces,
        layout=go.Layout(
            title="Cash Flow Breakdown",
            barmode="group",
            xaxis_title="Month",
            yaxis_title="Amount (base currency)",
            margin=dict(l=20, r=20, t=40, b=40),
        ),
    )
    return fig


def data_quality_chart(dq: pl.DataFrame) -> go.Figure | None:
    """Bar chart: check counts by status (PASS/WARN/FAIL).

    Returns ``None`` if the DataFrame is empty (no validation results).
    """
    if dq.is_empty():
        return None

    agg = dq.group_by("status").len().sort("status")
    colors = {"PASS": "#2ecc71", "WARN": "#f39c12", "FAIL": "#e74c3c"}
    bar_colors = [colors.get(s, "#95a5a6") for s in agg["status"].to_list()]

    fig = go.Figure(
        data=[
            go.Bar(
                x=agg["status"].to_list(),
                y=agg["len"].to_list(),
                marker_color=bar_colors,
                text=agg["len"].to_list(),
                textposition="auto",
            )
        ],
        layout=go.Layout(
            title="Data Quality Summary",
            xaxis_title="Status",
            yaxis_title="Count",
            margin=dict(l=20, r=20, t=40, b=40),
        ),
    )
    return fig


def _empty_figure(title: str) -> go.Figure:
    """Return a figure with a 'no data' annotation."""
    fig = go.Figure()
    fig.update_layout(
        title=title,
        annotations=[
            dict(
                text="No data available",
                showarrow=False,
                font=dict(size=16),
            )
        ],
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig
