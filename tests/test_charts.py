"""Unit tests for portfolio report chart builders."""

from __future__ import annotations

import polars as pl
import plotly.graph_objects as go

from pipeline.report.charts import allocation_by_currency


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _holdings(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal holdings DataFrame for chart tests."""
    return pl.DataFrame(rows)


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
