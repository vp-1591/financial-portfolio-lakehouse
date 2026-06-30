"""Tests for the consolidation and allocation modules."""

from __future__ import annotations

import pytest

from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    PortfolioConnectorError,
    PortfolioRow,
    aggregate_percentages,
    format_identifier,
    normalize_trading212_ticker,
    real_currency,
)


class TestNormalizeTrading212Ticker:
    def test_removes_eq_suffix(self) -> None:
        assert normalize_trading212_ticker("IS3Nd_EQ") == "IS3N"

    def test_removes_etf_and_market_suffix(self) -> None:
        assert normalize_trading212_ticker("VWCE_DE_ETF") == "VWCE"

    def test_removes_market_suffix(self) -> None:
        assert normalize_trading212_ticker("VWCE_DE_EQ") == "VWCE"

    def test_preserves_cash_prefix(self) -> None:
        assert normalize_trading212_ticker("CASH EUR") == "CASH EUR"

    def test_preserves_already_lowercase(self) -> None:
        assert normalize_trading212_ticker("alreadylower") == "alreadylower"


class TestFormatIdentifier:
    def test_formats_with_prefix(self) -> None:
        assert format_identifier("ISIN", "IE00BK5BQT80") == "ISIN:IE00BK5BQT80"

    def test_returns_empty_for_empty_value(self) -> None:
        assert format_identifier("ISIN", "") == ""


class TestRealCurrency:
    def test_returns_real_currency(self) -> None:
        assert real_currency("USD", "EUR") == "USD"

    def test_falls_back_for_base_placeholder(self) -> None:
        assert real_currency("BASE", "EUR") == "EUR"

    def test_falls_back_for_empty(self) -> None:
        assert real_currency(None, "EUR") == "EUR"


class TestAggregatePercentages:
    def test_converts_and_groups_by_ticker_and_broker(self) -> None:
        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.8, "PLN": 0.25})

        rows = aggregate_percentages(
            [
                Holding("Trading 212", "VWCE", "USD", 100.0),
                Holding(
                    "Trading 212",
                    "VWCE",
                    "USD",
                    50.0,
                    identifier="ISIN:IE00BK5BQT80",
                    security_currency="USD",
                    description="Vanguard FTSE All-World UCITS ETF",
                ),
                Holding("XTB", "CASH PLN", "PLN", 80.0),
                Holding("IBKR", "AAPL", "EUR", 100.0),
            ],
            converter,
        )

        assert rows == [
            PortfolioRow(
                "VWCE",
                50.0,
                "Trading 212",
                "ISIN:IE00BK5BQT80",
                "USD",
                "Vanguard FTSE All-World UCITS ETF",
            ),
            PortfolioRow("AAPL", 41.66666666666667, "IBKR", "-", "-", "-"),
            PortfolioRow("CASH PLN", 8.333333333333332, "XTB", "-", "-", "-"),
        ]

    def test_fills_missing_isin_from_override_map(self) -> None:
        converter = CurrencyConverter("EUR")

        rows = aggregate_percentages(
            [
                Holding(
                    "XTB",
                    "SXR8.DE",
                    "EUR",
                    100.0,
                    description="SXR8.DE",
                ),
            ],
            converter,
            isin_overrides={"SXR8.DE": "IE00B5BMR087"},
        )

        assert rows == [
            PortfolioRow(
                "SXR8.DE",
                100.0,
                "XTB",
                "ISIN:IE00B5BMR087",
                "-",
                "SXR8.DE",
            )
        ]

    def test_zero_net_worth_raises_error(self) -> None:
        converter = CurrencyConverter("EUR")
        with pytest.raises(PortfolioConnectorError, match="Net worth is zero"):
            aggregate_percentages([], converter)


class TestCurrencyConverter:
    def test_manual_rates_used_directly(self) -> None:
        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})
        assert converter.convert(100.0, "USD") == pytest.approx(90.0)

    def test_same_currency_returns_same_value(self) -> None:
        converter = CurrencyConverter("EUR")
        assert converter.convert(100.0, "EUR") == 100.0

    def test_empty_currency_returns_same_value(self) -> None:
        converter = CurrencyConverter("EUR")
        assert converter.convert(100.0, "") == 100.0
