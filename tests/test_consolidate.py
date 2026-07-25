"""Tests for the consolidation and allocation modules."""

from __future__ import annotations

import http.client
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    PortfolioConnectorError,
    PortfolioRow,
    TransientHttpError,
    aggregate_percentages,
    format_identifier,
    normalize_trading212_ticker,
)

# Mark tests that call live external APIs (Frankfurter).
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


class TestRequestJsonRetry:
    """Unit tests for tenacity retry logic in CurrencyConverter.request_json."""

    def _make_converter(self, **kwargs):  # type: ignore[no-untyped-def]
        """Create a CurrencyConverter with manual_rates to avoid real HTTP calls."""
        return CurrencyConverter("EUR", manual_rates={"USD": 0.9}, **kwargs)

    @staticmethod
    def _http_error(code: int, reason: str) -> urllib.error.HTTPError:
        """Create an HTTPError with a properly-typed hdrs argument."""
        return urllib.error.HTTPError(
            "https://example.com",
            code,
            reason,
            http.client.HTTPMessage(),  # type: ignore[arg-type]
            None,
        )

    def _mock_ok_response(self, data: dict | None = None) -> MagicMock:
        """Create a mock urllib response that returns JSON data."""
        import json

        response = MagicMock()
        response.read.return_value = json.dumps(
            data or {"rates": {"EUR": 1.0}}
        ).encode()
        response.__enter__ = MagicMock(return_value=response)
        response.__exit__ = MagicMock(return_value=False)
        return response

    def test_success_on_first_attempt(self) -> None:
        """No retry needed when the first request succeeds."""
        converter = self._make_converter()
        with patch("urllib.request.urlopen", return_value=self._mock_ok_response()):
            result = converter.request_json("https://example.com/api")
        assert result == {"rates": {"EUR": 1.0}}

    def test_retries_on_timeout_error(self) -> None:
        """TimeoutError triggers retry; succeeds on second attempt."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        ok_response = self._mock_ok_response()
        with patch(
            "urllib.request.urlopen",
            side_effect=[TimeoutError("timed out"), ok_response],
        ) as mock_urlopen:
            result = converter.request_json("https://example.com/api")
        assert result == {"rates": {"EUR": 1.0}}
        assert mock_urlopen.call_count == 2

    def test_retries_on_url_error(self) -> None:
        """URLError (network error) triggers retry; succeeds on second attempt."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        ok_response = self._mock_ok_response()
        with patch(
            "urllib.request.urlopen",
            side_effect=[urllib.error.URLError("Connection refused"), ok_response],
        ) as mock_urlopen:
            result = converter.request_json("https://example.com/api")
        assert result == {"rates": {"EUR": 1.0}}
        assert mock_urlopen.call_count == 2

    def test_retries_on_5xx_error(self) -> None:
        """5xx HTTP errors trigger retry; succeeds on second attempt."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        ok_response = self._mock_ok_response()
        with patch(
            "urllib.request.urlopen",
            side_effect=[
                self._http_error(503, "Service Unavailable"),
                ok_response,
            ],
        ) as mock_urlopen:
            result = converter.request_json("https://example.com/api")
        assert result == {"rates": {"EUR": 1.0}}
        assert mock_urlopen.call_count == 2

    def test_no_retry_on_4xx_error(self) -> None:
        """4xx HTTP errors are NOT retried; raises immediately."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        with (
            patch(
                "urllib.request.urlopen",
                side_effect=[
                    self._http_error(400, "Bad Request"),
                ],
            ) as mock_urlopen,
            pytest.raises(PortfolioConnectorError, match="HTTP 400"),
        ):
            converter.request_json("https://example.com/api")
        assert mock_urlopen.call_count == 1

    def test_no_retry_on_json_decode_error(self) -> None:
        """JSONDecodeError is NOT retried; raises immediately."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        bad_response = MagicMock()
        bad_response.read.return_value = b"not json"
        bad_response.__enter__ = MagicMock(return_value=bad_response)
        bad_response.__exit__ = MagicMock(return_value=False)
        with (
            patch("urllib.request.urlopen", return_value=bad_response) as mock_urlopen,
            pytest.raises(PortfolioConnectorError, match="non-JSON response"),
        ):
            converter.request_json("https://example.com/api")
        assert mock_urlopen.call_count == 1

    def test_no_retry_on_non_dict_response(self) -> None:
        """Non-dict JSON response is NOT retried; raises immediately."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        list_response = MagicMock()
        list_response.read.return_value = b"[1, 2, 3]"
        list_response.__enter__ = MagicMock(return_value=list_response)
        list_response.__exit__ = MagicMock(return_value=False)
        with (
            patch("urllib.request.urlopen", return_value=list_response) as mock_urlopen,
            pytest.raises(PortfolioConnectorError, match="unexpected response"),
        ):
            converter.request_json("https://example.com/api")
        assert mock_urlopen.call_count == 1

    def test_raises_after_all_retries_exhausted(self) -> None:
        """Raises TransientHttpError after all retries are exhausted."""
        converter = self._make_converter(retries=2, retry_delay=0.01)
        with (
            patch(
                "urllib.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ) as mock_urlopen,
            pytest.raises(TransientHttpError, match="timed out"),
        ):
            converter.request_json("https://example.com/api")
        # 1 initial + 2 retries = 3 total attempts
        assert mock_urlopen.call_count == 3

    def test_zero_retries_no_retry(self) -> None:
        """With retries=0, no retry occurs on transient errors."""
        converter = self._make_converter(retries=0)
        with (
            patch(
                "urllib.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ) as mock_urlopen,
            pytest.raises(TransientHttpError, match="timed out"),
        ):
            converter.request_json("https://example.com/api")
        assert mock_urlopen.call_count == 1

    def test_default_retry_parameters(self) -> None:
        """Default constructor provides retries=2 and retry_delay=1.0."""
        converter = CurrencyConverter("EUR")
        assert converter.retries == 2
        assert converter.retry_delay == 1.0


@integration
class TestCurrencyConverterIntegration:
    """Integration tests that call live FX rate APIs.

    These hit real external services and may fail due to network issues
    or API downtime.  Run with:  pytest -m integration
    """

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

        Uses "XYZ" (not a real ISO 4217 code).  Frankfurter rejects it
        with a 400 error.  No silent wrong rate is possible.
        """
        converter = CurrencyConverter("EUR")
        with pytest.raises(PortfolioConnectorError):
            converter.convert(100.0, "XYZ")
