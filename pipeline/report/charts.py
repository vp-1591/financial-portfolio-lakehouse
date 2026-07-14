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
    """Donut chart: portfolio value by instrument trading currency."""
    if holdings.is_empty():
        return _empty_figure("Currency Exposure")
    agg = (
        holdings.group_by("security_currency")
        .agg(pl.col("value_base").sum())
        .sort("value_base", descending=True)
    )
    fig = go.Figure(
        data=[
            go.Pie(
                labels=agg["security_currency"].to_list(),
                values=agg["value_base"].to_list(),
                textinfo="label+percent",
                hole=0.4,
            )
        ],
        layout=go.Layout(
            title="Currency Exposure",
            margin=dict(l=20, r=20, t=40, b=20),
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
        traces.append(
            go.Bar(
                x=div_agg["period_month"].to_list(),
                y=div_agg["total"].to_list(),
                name="Dividends",
                hovertemplate="Dividends<extra></extra><br>%{y:,.2f}",
            )
        )

    if not interest.is_empty():
        int_agg = (
            interest.group_by("period_month")
            .agg(pl.col("amount_base").sum().alias("total"))
            .sort("period_month")
        )
        traces.append(
            go.Bar(
                x=int_agg["period_month"].to_list(),
                y=int_agg["total"].to_list(),
                name="Interest",
                hovertemplate="Interest<extra></extra><br>%{y:,.2f}",
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
    """Grouped bar chart: cash flow by month and event type.

    When outlier event types exist (peak monthly value > 10× the median
    peak), an interactive toggle lets the reader hide them so smaller
    flows — interest, fees, trades — become visible.
    """
    if cash_flow.is_empty():
        return _empty_figure("Cash Flow Breakdown")

    # Use amount_base if available, fall back to cash_amount
    if cash_flow["amount_base"].null_count() < cash_flow.height:
        value_col = "amount_base"
    else:
        value_col = "cash_amount"

    event_types = sorted(cash_flow["event_type"].unique().to_list())
    months = sorted(cash_flow["period_month"].unique().to_list())

    # Build trace data and compute per-type peak for outlier detection
    trace_data: list[dict] = []
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
        y_values = [month_vals[m] for m in months]
        peak_abs = max(abs(v) for v in y_values) if y_values else 0.0
        trace_data.append(
            {"name": et, "x": months, "y": y_values, "peak_abs": peak_abs}
        )

    # Identify outlier event types whose peak dwarfs the median peak
    peaks = [td["peak_abs"] for td in trace_data]
    is_outlier = _classify_outliers(peaks, ratio=10)

    traces = [go.Bar(x=td["x"], y=td["y"], name=td["name"]) for td in trace_data]

    layout_kwargs: dict = dict(
        title="Cash Flow Breakdown",
        barmode="group",
        xaxis_title="Month",
        yaxis_title="Amount (base currency)",
        margin=dict(l=20, r=20, t=40, b=40),
    )

    if any(is_outlier):
        outlier_names = sorted(
            trace_data[i]["name"] for i, o in enumerate(is_outlier) if o
        )
        all_visible = [True] * len(traces)
        ex_outlier_visible = [not o for o in is_outlier]

        layout_kwargs["updatemenus"] = [
            dict(
                type="buttons",
                direction="left",
                buttons=[
                    dict(
                        label="All Events",
                        method="update",
                        args=[
                            {"visible": all_visible},
                            {"title": "Cash Flow Breakdown"},
                        ],
                    ),
                    dict(
                        label="Hide Outliers",
                        method="update",
                        args=[
                            {"visible": ex_outlier_visible},
                            {"title": f"Cash Flow (excl. {', '.join(outlier_names)})"},
                        ],
                    ),
                ],
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.0,
                xanchor="left",
                y=1.15,
            )
        ]
        layout_kwargs["margin"]["t"] = 60  # room for buttons

    fig = go.Figure(data=traces, layout=go.Layout(**layout_kwargs))
    return fig


def data_quality_chart(dq: pl.DataFrame) -> go.Figure | None:
    """Bar chart: check counts by status (PASS/WARN/FAIL).

    Returns ``None`` if the DataFrame is empty or if every check passed
    (no WARN or FAIL rows).  An all-pass chart is just a big green bar
    with no actionable information.
    """
    if dq.is_empty():
        return None

    # Skip the chart when there's nothing to flag — all-pass is not useful.
    statuses = set(dq["status"].unique().to_list())
    if statuses <= {"PASS"}:
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


def _classify_outliers(peaks: list[float], ratio: float = 10) -> list[bool]:
    """Flag values whose peak exceeds *ratio* × the median of the other peaks.

    Each peak is compared to the median of all *other* peaks so that a
    single extreme value cannot inflate the baseline.  Returns a list
    of booleans, one per entry.  An empty input or a zero baseline
    produces no outliers.
    """
    if len(peaks) < 2:
        return [False] * len(peaks)
    result: list[bool] = []
    for i, peak in enumerate(peaks):
        others = sorted(peaks[j] for j in range(len(peaks)) if j != i)
        mid = len(others) // 2
        baseline = (
            others[mid] if len(others) % 2 == 1 else (others[mid - 1] + others[mid]) / 2
        )
        result.append(peak > ratio * baseline if baseline > 0 else False)
    return result


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
