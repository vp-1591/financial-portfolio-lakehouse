"""Unit tests for portfolio report chart builders."""

from __future__ import annotations

import polars as pl
import plotly.graph_objects as go

from pipeline.report.charts import (
    allocation_by_currency,
    cash_flow_breakdown,
    _classify_outliers,
    data_quality_chart,
    passive_income_timeline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _holdings(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal holdings DataFrame for chart tests."""
    return pl.DataFrame(rows)


def _cash_flow(rows: list[list]) -> pl.DataFrame:
    """Build a minimal cash_flow_summary DataFrame for chart tests."""
    return pl.DataFrame(
        rows,
        schema={
            "period_month": pl.String,
            "event_type": pl.String,
            "cash_amount": pl.Float64,
            "amount_base": pl.Float64,
        },
        orient="row",
    )


# ---------------------------------------------------------------------------
# allocation_by_currency – donut chart (security_currency grouping)
# ---------------------------------------------------------------------------


class TestAllocationByCurrency:
    """Tests for the currency exposure donut chart."""

    def test_donut_shape(self) -> None:
        """Chart is a go.Pie with hole=0.4 and textinfo=label+percent."""
        df = _holdings(
            [
                {"security_currency": "USD", "value_base": 1000.0},
                {"security_currency": "EUR", "value_base": 500.0},
            ]
        )
        fig = allocation_by_currency(df)

        trace = fig.data[0]
        assert isinstance(trace, go.Pie)
        assert trace.hole == 0.4
        assert trace.textinfo == "label+percent"

    def test_title(self) -> None:
        """Chart title is 'Currency Exposure'."""
        df = _holdings(
            [
                {"security_currency": "USD", "value_base": 1000.0},
            ]
        )
        fig = allocation_by_currency(df)

        assert fig.layout.title.text == "Currency Exposure"

    def test_groups_by_security_currency_not_wallet_currency(self) -> None:
        """T212-style positions with security_currency != currency
        appear under security_currency, not wallet currency.

        Regression guard: a GBX instrument on a PLN-denominated
        account must appear under 'GBX', not 'PLN'.
        """
        df = _holdings(
            [
                {"security_currency": "GBX", "value_base": 800.0},
                {"security_currency": "USD", "value_base": 200.0},
            ]
        )
        fig = allocation_by_currency(df)

        trace = fig.data[0]
        assert isinstance(trace, go.Pie)
        labels = list(trace.labels)
        assert "GBX" in labels
        assert "PLN" not in labels

    def test_aggregation_sums_same_currency(self) -> None:
        """Two rows with the same security_currency sum into one slice."""
        df = _holdings(
            [
                {"security_currency": "USD", "value_base": 300.0},
                {"security_currency": "USD", "value_base": 700.0},
                {"security_currency": "EUR", "value_base": 500.0},
            ]
        )
        fig = allocation_by_currency(df)

        trace = fig.data[0]
        # After aggregation: USD=1000, EUR=500, sorted descending
        assert list(trace.labels) == ["USD", "EUR"]
        assert list(trace.values) == [1000.0, 500.0]

    def test_empty_input(self) -> None:
        """Empty DataFrame returns _empty_figure with correct title."""
        df = pl.DataFrame(
            {
                "security_currency": pl.Series([], dtype=pl.String),
                "value_base": pl.Series([], dtype=pl.Float64),
            }
        )
        fig = allocation_by_currency(df)

        assert fig.layout.title.text == "Currency Exposure"


# ---------------------------------------------------------------------------
# cash_flow_breakdown – grouped bar chart with outlier toggle
# ---------------------------------------------------------------------------


class TestCashFlowBreakdown:
    """Tests for the cash flow grouped bar chart."""

    def test_basic_chart_structure(self) -> None:
        """Returns a grouped bar chart with one trace per event type."""
        df = _cash_flow(
            [
                ["2026-01", "DEPOSIT", 1000.0, 1000.0],
                ["2026-01", "INTEREST", 50.0, 50.0],
                ["2026-02", "DEPOSIT", 500.0, 500.0],
            ]
        )
        fig = cash_flow_breakdown(df)

        trace_names = sorted(t.name for t in fig.data)
        assert trace_names == ["DEPOSIT", "INTEREST"]
        assert fig.layout.barmode == "group"

    def test_empty_input(self) -> None:
        """Empty DataFrame returns _empty_figure."""
        df = pl.DataFrame(
            {
                "period_month": pl.Series([], dtype=pl.String),
                "event_type": pl.Series([], dtype=pl.String),
                "cash_amount": pl.Series([], dtype=pl.Float64),
                "amount_base": pl.Series([], dtype=pl.Float64),
            }
        )
        fig = cash_flow_breakdown(df)

        assert fig.layout.title.text == "Cash Flow Breakdown"
        assert len(fig.data) == 0  # empty figure, no traces

    def test_prefers_amount_base_over_cash_amount(self) -> None:
        """Uses amount_base column when it has non-null values."""
        df = _cash_flow(
            [
                ["2026-01", "INTEREST", 50.0, 45.0],
            ]
        )
        fig = cash_flow_breakdown(df)

        # amount_base=45.0 should be used, not cash_amount=50.0
        trace = next(t for t in fig.data if t.name == "INTEREST")
        assert list(trace.y) == [45.0]

    def test_falls_back_to_cash_amount(self) -> None:
        """Falls back to cash_amount when amount_base is all null."""
        df = pl.DataFrame(
            {
                "period_month": ["2026-01"],
                "event_type": ["INTEREST"],
                "cash_amount": [50.0],
                "amount_base": [None],
            },
            schema={
                "period_month": pl.String,
                "event_type": pl.String,
                "cash_amount": pl.Float64,
                "amount_base": pl.Float64,
            },
        )
        fig = cash_flow_breakdown(df)

        trace = next(t for t in fig.data if t.name == "INTEREST")
        assert list(trace.y) == [50.0]

    def test_no_toggle_when_no_outliers(self) -> None:
        """No updatemenus when all event types have similar peaks."""
        df = _cash_flow(
            [
                ["2026-01", "DEPOSIT", 1000.0, 1000.0],
                ["2026-01", "INTEREST", 500.0, 500.0],
                ["2026-02", "DEPOSIT", 800.0, 800.0],
                ["2026-02", "INTEREST", 400.0, 400.0],
            ]
        )
        fig = cash_flow_breakdown(df)

        # Peaks: DEPOSIT=1000, INTEREST=500 → ratio=2, below threshold
        assert not fig.layout.updatemenus  # empty tuple when no toggle

    def test_toggle_appears_when_outliers_exist(self) -> None:
        """updatemenus added when one event type is an outlier."""
        df = _cash_flow(
            [
                ["2026-01", "DEPOSIT", 1_000_000.0, 1_000_000.0],
                ["2026-01", "INTEREST", 50.0, 50.0],
                ["2026-01", "TRADE", 200.0, 200.0],
                ["2026-02", "DEPOSIT", 500_000.0, 500_000.0],
                ["2026-02", "INTEREST", 30.0, 30.0],
                ["2026-02", "TRADE", 150.0, 150.0],
            ]
        )
        fig = cash_flow_breakdown(df)

        # DEPOSIT peak=1M, INTEREST=50, TRADE=200 → median=200, 1M > 10*200
        menus = fig.layout.updatemenus
        assert menus is not None
        assert len(menus) == 1

        buttons = menus[0].buttons
        assert len(buttons) == 2
        assert buttons[0].label == "All Events"
        assert buttons[1].label == "Hide Outliers"

        # "All Events" shows everything
        all_vis = buttons[0].args[0]["visible"]
        assert all_vis == [True, True, True]

        # "Hide Outliers" hides DEPOSIT
        ex_vis = buttons[1].args[0]["visible"]
        # DEPOSIT is first alphabetically; its peak is the outlier
        assert ex_vis[0] is False  # DEPOSIT hidden
        assert ex_vis[1] is True  # INTEREST visible
        assert ex_vis[2] is True  # TRADE visible

    def test_toggle_title_changes(self) -> None:
        """The ex-outliers button updates the chart title."""
        df = _cash_flow(
            [
                ["2026-01", "DEPOSIT", 1_000_000.0, 1_000_000.0],
                ["2026-01", "INTEREST", 50.0, 50.0],
            ]
        )
        fig = cash_flow_breakdown(df)

        buttons = fig.layout.updatemenus[0].buttons
        # "All Events" keeps original title
        assert buttons[0].args[1]["title"] == "Cash Flow Breakdown"
        # "Hide Outliers" shows what's excluded
        assert "DEPOSIT" in buttons[1].args[1]["title"]

    def test_months_filled_for_missing_event_types(self) -> None:
        """Months with no data for an event type show 0."""
        df = _cash_flow(
            [
                ["2026-01", "INTEREST", 100.0, 100.0],
                ["2026-02", "DEPOSIT", 500.0, 500.0],
            ]
        )
        fig = cash_flow_breakdown(df)

        deposit = next(t for t in fig.data if t.name == "DEPOSIT")
        interest = next(t for t in fig.data if t.name == "INTEREST")
        # Both traces have both months
        assert len(deposit.x) == 2
        assert len(interest.x) == 2
        # DEPOSIT has 0 for 2026-01
        assert deposit.y[deposit.x.index("2026-01")] == 0.0


# ---------------------------------------------------------------------------
# _classify_outliers helper
# ---------------------------------------------------------------------------


class TestClassifyOutliers:
    """Tests for the outlier detection helper."""

    def test_no_outliers_when_peaks_similar(self) -> None:
        """Peaks within 10× of the other-peaks median are not outliers."""
        # Each peak's "others" median is comparable → no outliers
        assert _classify_outliers([100, 200, 300]) == [False, False, False]

    def test_flags_extreme_outlier(self) -> None:
        """A peak > 10× the median of the other peaks is an outlier."""
        # 1M's others = [50, 200], median = 125; 1M > 10*125 = 1250
        result = _classify_outliers([50, 200, 1_000_000])
        assert result == [False, False, True]

    def test_flags_outlier_with_two_values(self) -> None:
        """Even with only two event types, extreme ratio is detected."""
        # 1M's others = [50], baseline = 50; 1M > 10*50 = 500
        result = _classify_outliers([1_000_000, 50])
        assert result == [True, False]

    def test_custom_ratio(self) -> None:
        """A lower ratio flags more events as outliers."""
        # 5000's others = [100, 500], median = 300; ratio=5 → 5*300=1500; 5K > 1500
        result = _classify_outliers([100, 500, 5000], ratio=5)
        assert result == [False, False, True]

    def test_single_value_is_never_outlier(self) -> None:
        """With only one event type, no outlier detection is possible."""
        assert _classify_outliers([1_000_000]) == [False]

    def test_zero_baseline_produces_no_outliers(self) -> None:
        """If the other peaks' median is zero, no events are flagged."""
        # 500's others = [0, 0], baseline = 0 → skip
        assert _classify_outliers([0, 0, 500]) == [False, False, False]

    def test_empty_input(self) -> None:
        """Empty list returns empty result."""
        assert _classify_outliers([]) == []

    def test_all_zero_peaks(self) -> None:
        """All-zero peaks produce no outliers."""
        assert _classify_outliers([0, 0, 0]) == [False, False, False]


# ---------------------------------------------------------------------------
# passive_income_timeline – tooltip shows values, not just months
# ---------------------------------------------------------------------------


def _dividends(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal dividend_income DataFrame for chart tests."""
    return pl.DataFrame(rows)


def _interest(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal interest_income DataFrame for chart tests."""
    return pl.DataFrame(rows)


class TestPassiveIncomeTimeline:
    """Tests for the passive income timeline bar chart."""

    def test_hovertemplate_shows_value(self) -> None:
        """Bar hovertemplate formats the numeric value, not the month label."""
        div = _dividends(
            [
                {"period_month": "2026-01", "amount_base": 123.45},
                {"period_month": "2026-02", "amount_base": 67.89},
            ]
        )
        interest = pl.DataFrame(
            {
                "period_month": pl.Series([], dtype=pl.String),
                "amount_base": pl.Series([], dtype=pl.Float64),
            }
        )
        fig = passive_income_timeline(div, interest)

        bar = fig.data[0]
        assert bar.name == "Dividends"
        # hovertemplate must contain the value format specifier
        assert "%{y:,.2f}" in bar.hovertemplate

    def test_interest_hovertemplate_shows_value(self) -> None:
        """Interest bar hovertemplate also formats the numeric value."""
        interest = _interest(
            [
                {"period_month": "2026-01", "amount_base": 10.50},
            ]
        )
        div = pl.DataFrame(
            {
                "period_month": pl.Series([], dtype=pl.String),
                "amount_base": pl.Series([], dtype=pl.Float64),
            }
        )
        fig = passive_income_timeline(div, interest)

        bar = fig.data[0]
        assert bar.name == "Interest"
        assert "%{y:,.2f}" in bar.hovertemplate

    def test_both_traces_have_hovertemplate(self) -> None:
        """When both dividends and interest exist, both have hovertemplates."""
        div = _dividends([{"period_month": "2026-01", "amount_base": 50.0}])
        interest = _interest([{"period_month": "2026-01", "amount_base": 25.0}])
        fig = passive_income_timeline(div, interest)

        assert len(fig.data) == 2
        for bar in fig.data:
            assert bar.hovertemplate is not None
            assert "%{y:,.2f}" in bar.hovertemplate

    def test_stacked_barmode(self) -> None:
        """Chart uses stacked bar mode."""
        div = _dividends([{"period_month": "2026-01", "amount_base": 50.0}])
        interest = _interest([{"period_month": "2026-01", "amount_base": 25.0}])
        fig = passive_income_timeline(div, interest)

        assert fig.layout.barmode == "stack"


# ---------------------------------------------------------------------------
# data_quality_chart – hidden when all checks pass
# ---------------------------------------------------------------------------


def _dq(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal data_quality DataFrame for chart tests."""
    return pl.DataFrame(rows)


class TestDataQualityChart:
    """Tests for the data quality bar chart."""

    def test_returns_none_when_all_pass(self) -> None:
        """All-pass DQ data returns None (no chart rendered)."""
        df = _dq(
            [
                {"status": "PASS", "table_name": "t1", "check_name": "c1"},
                {"status": "PASS", "table_name": "t2", "check_name": "c2"},
            ]
        )
        assert data_quality_chart(df) is None

    def test_returns_none_when_empty(self) -> None:
        """Empty DQ DataFrame returns None."""
        df = pl.DataFrame(
            {
                "status": pl.Series([], dtype=pl.String),
                "table_name": pl.Series([], dtype=pl.String),
                "check_name": pl.Series([], dtype=pl.String),
            }
        )
        assert data_quality_chart(df) is None

    def test_returns_chart_when_warn_present(self) -> None:
        """DQ data with a WARN status returns a chart."""
        df = _dq(
            [
                {"status": "PASS", "table_name": "t1", "check_name": "c1"},
                {"status": "WARN", "table_name": "t2", "check_name": "c2"},
            ]
        )
        fig = data_quality_chart(df)
        assert fig is not None

    def test_returns_chart_when_fail_present(self) -> None:
        """DQ data with a FAIL status returns a chart."""
        df = _dq(
            [
                {"status": "FAIL", "table_name": "t1", "check_name": "c1"},
            ]
        )
        fig = data_quality_chart(df)
        assert fig is not None
