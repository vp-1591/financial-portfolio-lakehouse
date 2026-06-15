from __future__ import annotations

from pathlib import Path
import sys
import urllib.error


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import portfolio_connectors as connectors
import portfolio_percentages


class FakeIbkrClientWithBasePlaceholder:
    def __init__(self, base_url: str, verify_tls: bool, timeout: float) -> None:
        self.base_url = base_url
        self.verify_tls = verify_tls
        self.timeout = timeout

    def accounts(self) -> list[dict[str, str]]:
        return [{"accountId": "U123"}]

    def positions(self, account_id: str) -> list[dict[str, object]]:
        return [
            {
                "contractDesc": "VWCE",
                "currency": "BASE",
                "mktValue": 100.0,
            }
        ]

    def ledger(self, account_id: str) -> dict[str, dict[str, object]]:
        return {
            "BASE": {
                "currency": "BASE",
                "netliquidationvalue": 100.0,
                "exchangerate": 1.0,
            }
        }


def test_aggregate_percentages_converts_and_groups_by_ticker_and_broker() -> None:
    converter = connectors.CurrencyConverter(
        "EUR",
        manual_rates={"USD": 0.8, "PLN": 0.25},
    )

    rows = connectors.aggregate_percentages(
        [
            connectors.Holding("Trading 212", "VWCE", "USD", 100.0),
            connectors.Holding(
                "Trading 212",
                "VWCE",
                "USD",
                50.0,
                isin="IE00BK5BQT80",
                name="Vanguard FTSE All-World UCITS ETF",
            ),
            connectors.Holding("XTB", "CASH PLN", "PLN", 80.0),
            connectors.Holding("IBKR", "AAPL", "EUR", 100.0),
        ],
        converter,
    )

    assert rows == [
        (
            "VWCE",
            50.0,
            "Trading 212",
            "IE00BK5BQT80",
            "Vanguard FTSE All-World UCITS ETF",
        ),
        ("AAPL", 41.66666666666667, "IBKR", "", ""),
        ("CASH PLN", 8.333333333333332, "XTB", "", ""),
    ]


def test_aggregate_percentages_fills_missing_isin_from_override_map() -> None:
    converter = connectors.CurrencyConverter("EUR")

    rows = connectors.aggregate_percentages(
        [
            connectors.Holding(
                "XTB",
                "SXR8.DE",
                "EUR",
                100.0,
                name="SXR8.DE",
            ),
        ],
        converter,
        isin_overrides={"SXR8.DE": "IE00B5BMR087"},
    )

    assert rows == [("SXR8.DE", 100.0, "XTB", "IE00B5BMR087", "SXR8.DE")]


def test_load_isin_map_reads_ticker_and_isin_columns() -> None:
    path = ROOT / ".tmp-tests" / "isins.csv"
    path.parent.mkdir(exist_ok=True)
    path.write_text("ticker,isin\nSXR8.DE,IE00B5BMR087\n", encoding="utf-8")

    try:
        assert portfolio_percentages.load_isin_map(path) == {
            "SXR8.DE": "IE00B5BMR087",
        }
    finally:
        path.unlink(missing_ok=True)


def test_portfolio_output_contains_requested_ai_context_columns(capsys) -> None:
    portfolio_percentages.print_rows(
        [
            (
                "VWCE",
                75.0,
                "Trading 212",
                "IE00BK5BQT80",
                "Vanguard FTSE All-World UCITS ETF",
            ),
            ("AAPL", 25.0, "IBKR", "US0378331005", "Apple Inc."),
        ]
    )

    output = capsys.readouterr().out

    assert "Ticker" in output
    assert "%" in output
    assert "Broker" in output
    assert "ISIN" in output
    assert "Name" in output
    assert "Value" not in output
    assert "Currency" not in output
    assert "Net worth" not in output
    assert "VWCE" in output
    assert "75.00%" in output
    assert "IE00BK5BQT80" in output
    assert "Vanguard FTSE All-World UCITS ETF" in output


def test_normalize_trading212_ticker_removes_broker_suffixes() -> None:
    assert connectors.normalize_trading212_ticker("IS3Nd_EQ") == "IS3N"
    assert connectors.normalize_trading212_ticker("VWCE_DE_EQ") == "VWCE"
    assert connectors.normalize_trading212_ticker("CASH EUR") == "CASH EUR"
    assert connectors.normalize_trading212_ticker("alreadylower") == "alreadylower"


def test_ibkr_base_placeholder_requires_explicit_base_currency(monkeypatch) -> None:
    monkeypatch.setattr(connectors.ibkr, "IbkrClient", FakeIbkrClientWithBasePlaceholder)

    try:
        connectors.load_ibkr_holdings(
            base_url="https://localhost:5000/v1/api",
            account=None,
            verify_tls=False,
            timeout=20.0,
            skip_auth_check=True,
            require_brokerage_session=False,
        )
    except connectors.PortfolioConnectorError as exc:
        message = str(exc)
    else:
        raise AssertionError("BASE placeholder should require explicit base currency")

    assert "--ibkr-base-currency" in message


def test_ibkr_base_placeholder_uses_explicit_base_currency(monkeypatch) -> None:
    monkeypatch.setattr(connectors.ibkr, "IbkrClient", FakeIbkrClientWithBasePlaceholder)

    holdings = connectors.load_ibkr_holdings(
        base_url="https://localhost:5000/v1/api",
        account=None,
        verify_tls=False,
        timeout=20.0,
        skip_auth_check=True,
        require_brokerage_session=False,
        base_currency_override="EUR",
    )

    assert holdings == [
        connectors.Holding("IBKR", "VWCE", "EUR", 100.0, name="VWCE")
    ]


def test_currency_converter_uses_frankfurter_rate(monkeypatch) -> None:
    requested_urls: list[str] = []

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"rates":{"EUR":0.25}}'

    def fake_urlopen(request, timeout: float) -> FakeResponse:
        requested_urls.append(request.full_url)
        return FakeResponse()

    monkeypatch.setattr(connectors.urllib.request, "urlopen", fake_urlopen)

    converter = connectors.CurrencyConverter("EUR")

    assert converter.convert(100.0, "PLN") == 25.0
    assert requested_urls == [
        "https://api.frankfurter.app/latest?from=PLN&to=EUR",
    ]


def test_currency_converter_falls_back_to_yahoo_when_frankfurter_fails(
    monkeypatch,
) -> None:
    requested_urls: list[str] = []

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"chart":{"result":[{"meta":{"regularMarketPrice":0.23}}]}}'

    def fake_urlopen(request, timeout: float) -> FakeResponse:
        url = request.full_url
        requested_urls.append(url)
        if "frankfurter" in url:
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)
        return FakeResponse()

    monkeypatch.setattr(connectors.urllib.request, "urlopen", fake_urlopen)

    converter = connectors.CurrencyConverter("EUR")

    assert converter.convert(100.0, "PLN") == 23.0
    assert requested_urls == [
        "https://api.frankfurter.app/latest?from=PLN&to=EUR",
        "https://query1.finance.yahoo.com/v8/finance/chart/PLNEUR%3DX?range=1d&interval=1d",
    ]


def test_currency_converter_reports_all_provider_failures(monkeypatch) -> None:
    def fake_urlopen(request, timeout: float) -> object:
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(connectors.urllib.request, "urlopen", fake_urlopen)

    converter = connectors.CurrencyConverter("EUR")

    try:
        converter.convert(100.0, "PLN")
    except connectors.PortfolioConnectorError as exc:
        message = str(exc)
    else:
        raise AssertionError("provider failures should raise")

    assert "Could not fetch FX rate PLN->EUR" in message
    assert "Frankfurter: HTTP 403 Forbidden" in message
    assert "Yahoo: HTTP 403 Forbidden" in message
    assert "Pass --fx-rate PLN=RATE" in message
