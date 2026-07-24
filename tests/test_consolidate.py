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
)

# Mark tests that call live external APIs (Yahoo Finance, Frankfurter).
# Run with:  pytest -m integration  (to run only integration tests)
# Run with:  pytest -m "not integration"  (to skip them)
integration = pytest.mark.integration


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

    def test_gbx_converts_via_gbp_divided_by_100(self) -> None:
        """GBX (British pence) should be converted by fetching GBP rate and dividing by 100."""
        converter = CurrencyConverter("EUR", manual_rates={"GBP": 1.17})
        # 100 GBX = 1 GBP = 1.17 EUR, so 100 GBX should be 1.17 EUR
        assert converter.convert(100.0, "GBX") == pytest.approx(1.17)

    def test_gbp_unaffected_by_gbx_mapping(self) -> None:
        """GBP conversion should not be affected by the GBX minor unit mapping."""
        converter = CurrencyConverter("EUR", manual_rates={"GBP": 1.17})
        assert converter.convert(100.0, "GBP") == pytest.approx(117.0)

    def test_gbx_rate_cached_after_first_convert(self) -> None:
        """After converting GBX, the derived rate should be cached."""
        converter = CurrencyConverter("EUR", manual_rates={"GBP": 1.17})
        converter.convert(100.0, "GBX")
        # GBX rate should be cached: 1.17 / 100 = 0.0117
        assert "GBX" in converter._rates
        assert converter._rates["GBX"] == pytest.approx(0.0117)

    def test_yahoo_rejects_normalised_symbol(self) -> None:
        """Yahoo normalising GBX→GBP in its response must raise an error.

        When Yahoo silently changes the requested symbol (e.g. GBXEUR=X
        to GBPEUR=X), the returned rate would be 100× wrong.  The converter
        must detect this mismatch and raise PortfolioConnectorError.
        """
        converter = CurrencyConverter("EUR")
        converter.request_json = lambda url: {  # type: ignore[method-assign]
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "GBPEUR=X",  # Yahoo normalised GBX→GBP
                            "regularMarketPrice": 1.17,
                        }
                    }
                ]
            }
        }

        with pytest.raises(
            PortfolioConnectorError, match="Yahoo normalised GBXEUR=X to GBPEUR=X"
        ):
            converter.fetch_yahoo_rate("GBX")

    def test_yahoo_accepts_matching_symbol(self) -> None:
        """When Yahoo echoes back the same symbol, the rate is returned."""
        converter = CurrencyConverter("EUR")
        converter.request_json = lambda url: {  # type: ignore[method-assign]
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "USDEUR=X",
                            "regularMarketPrice": 0.92,
                        }
                    }
                ]
            }
        }

        assert converter.fetch_yahoo_rate("USD") == pytest.approx(0.92)

    def test_yahoo_accepts_response_without_symbol(self) -> None:
        """When Yahoo omits the symbol from meta, the rate is still returned.

        Some Yahoo Finance responses may not include a 'symbol' key.
        In that case we cannot validate and fall through to returning the
        rate, matching the pre-validation behaviour.
        """
        converter = CurrencyConverter("EUR")
        converter.request_json = lambda url: {  # type: ignore[method-assign]
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 0.92,
                        }
                    }
                ]
            }
        }

        assert converter.fetch_yahoo_rate("USD") == pytest.approx(0.92)


@integration
class TestCurrencyConverterIntegration:
    """Integration tests that call live FX rate APIs.

    These hit real external services and may fail due to network issues
    or API downtime.  Run with:  pytest -m integration
    """

    def test_yahoo_returns_matching_symbol_for_valid_currency(self) -> None:
        """Live Yahoo Finance response includes a 'symbol' that matches the request.

        This validates that the symbol-validation logic works against real
        responses — not just mocked ones.
        """
        converter = CurrencyConverter("EUR")
        # USD→EUR is a major pair that Yahoo Finance always serves.
        rate = converter.fetch_yahoo_rate("USD")
        # The rate should be a positive number (USD/EUR is typically ~0.9–1.1)
        assert rate > 0, f"Expected positive rate, got {rate}"

    def test_frankfurter_returns_valid_rate(self) -> None:
        """Live Frankfurter API returns a valid rate for a major currency pair."""
        converter = CurrencyConverter("EUR")
        rate = converter.fetch_frankfurter_rate("USD")
        assert rate > 0, f"Expected positive rate, got {rate}"

    def test_gbx_live_conversion_via_gbp(self) -> None:
        """GBX conversion works end-to-end using the live GBP→EUR rate.

        The GBX→EUR rate should be roughly GBP→EUR / 100.
        This exercises the full MINOR_CURRENCY_UNITS path against real APIs.
        """
        converter = CurrencyConverter("EUR")
        # First get the GBP rate (from manual_rates or live fetch)
        gbp_rate = converter.fetch_rate("GBP")
        gbx_rate = converter.convert(1.0, "GBX")
        # 1 GBX = GBP_rate/100 EUR, so gbx_rate should equal gbp_rate / 100
        assert gbx_rate == pytest.approx(gbp_rate / 100, rel=0.05), (
            f"GBX rate {gbx_rate} doesn't match GBP/100 ({gbp_rate / 100})"
        )

    def test_unknown_currency_raises_error(self) -> None:
        """An unknown currency code not in MINOR_CURRENCY_UNITS must raise.

        Uses "XYZ" (not a real ISO 4217 code).  Frankfurter should reject it
        with a 400, and if Yahoo tries to normalise it, the symbol validation
        should catch the mismatch.  Either way, no silent wrong rate.
        """
        converter = CurrencyConverter("EUR")
        with pytest.raises(PortfolioConnectorError):
            converter.convert(100.0, "XYZ")
