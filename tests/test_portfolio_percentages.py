from __future__ import annotations

from pathlib import Path
import sys
import urllib.error


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import portfolio_connectors as connectors  # noqa: E402
import portfolio_percentages  # noqa: E402


class FakeIbkrFlexClient:
    """Fake Flex client that returns a fixed XML response."""

    def __init__(
        self,
        token: str = "test-token",
        query_id: str = "1554188",
        base_url: str = "https://example.test",
        timeout: float = 30.0,
    ) -> None:
        self.token = token
        self.query_id = query_id
        self.base_url = base_url
        self.timeout = timeout

    def request_report(self) -> str:
        return "12345678"

    def fetch_report(
        self, reference_code: str, retries: int = 6, delay: float = 3.0
    ) -> object:
        import xml.etree.ElementTree as ET

        return ET.fromstring(FLEX_XML_BASIC)


FLEX_XML_BASIC = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U123" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard FTSE All-World"
                      conid="1234567" isin="IE00BK5BQT80"
                      quantity="100" markPrice="50.0" positionValue="5000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U123" currency="EUR"
                           netLiquidationValue="8000.00" cashBalance="3000.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""

FLEX_XML_CONID = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U123" currency="USD" fxRateToBase="0.9"
                      assetClass="STK" symbol="GOOGL" description="Alphabet Inc Class A"
                      conid="208813719" isin="US0378331005"
                      quantity="50" markPrice="100.0" positionValue="5000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U123" currency="EUR"
                           netLiquidationValue="8000.00" cashBalance="3000.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


class FakeTrading212Client:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        timeout: float,
        user_agent: str,
    ) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout
        self.user_agent = user_agent


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
                identifier="ISIN:IE00BK5BQT80",
                security_currency="USD",
                description="Vanguard FTSE All-World UCITS ETF",
            ),
            connectors.Holding("XTB", "CASH PLN", "PLN", 80.0),
            connectors.Holding("IBKR", "AAPL", "EUR", 100.0),
        ],
        converter,
    )

    assert rows == [
        connectors.PortfolioRow(
            "VWCE",
            50.0,
            "Trading 212",
            "ISIN:IE00BK5BQT80",
            "USD",
            "Vanguard FTSE All-World UCITS ETF",
        ),
        connectors.PortfolioRow("AAPL", 41.66666666666667, "IBKR", "-", "-", "-"),
        connectors.PortfolioRow("CASH PLN", 8.333333333333332, "XTB", "-", "-", "-"),
    ]


def test_trading212_holdings_display_security_currency_not_wallet_currency(
    monkeypatch,
) -> None:
    def fake_load_assets(
        client: FakeTrading212Client,
        account_id_value: str,
        include_metadata: bool,
    ) -> tuple[list[object], float]:
        assert include_metadata is True
        return (
            [
                connectors.trading212.Asset(
                    "T212",
                    "IS3Nd_EQ",
                    "iShares Core MSCI World UCITS ETF",
                    "EQUITY",
                    "PLN",
                    100.0,
                    isin="IE00B4L5Y983",
                    security_currency="EUR",
                )
            ],
            100.0,
        )

    monkeypatch.setattr(
        connectors.trading212,
        "Trading212Client",
        FakeTrading212Client,
    )
    monkeypatch.setattr(connectors.trading212, "load_assets", fake_load_assets)

    holdings = connectors.load_trading212_holdings(
        api_key="key",
        api_secret="secret",
        account_id="T212",
        base_url="https://example.test",
        timeout=20.0,
        user_agent="agent",
        include_metadata=True,
    )

    assert holdings == [
        connectors.Holding(
            "Trading 212",
            "IS3N",
            "PLN",
            100.0,
            identifier="ISIN:IE00B4L5Y983",
            security_currency="EUR",
            description="iShares Core MSCI World UCITS ETF",
        )
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
                description="SXR8.DE",
            ),
        ],
        converter,
        isin_overrides={"SXR8.DE": "IE00B5BMR087"},
    )

    assert rows == [
        connectors.PortfolioRow(
            "SXR8.DE",
            100.0,
            "XTB",
            "ISIN:IE00B5BMR087",
            "-",
            "SXR8.DE",
        )
    ]


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


def test_portfolio_output_contains_requested_identifier_context_columns(capsys) -> None:
    portfolio_percentages.print_rows(
        [
            connectors.PortfolioRow(
                "VWCE",
                75.0,
                "Trading 212",
                "ISIN:IE00BK5BQT80",
                "EUR",
                "Vanguard FTSE All-World UCITS ETF",
            ),
            connectors.PortfolioRow(
                "AAPL",
                25.0,
                "IBKR",
                "IBKR:265598",
                "USD",
                "Apple Inc.",
            ),
        ]
    )

    output = capsys.readouterr().out

    assert "Ticker" in output
    assert "%" in output
    assert "Broker" in output
    assert "Identifier" in output
    assert "Ccy" in output
    assert "Description" in output
    assert "Name" not in output
    assert "Value" not in output
    assert "Net worth" not in output
    assert "VWCE" in output
    assert "75.00%" in output
    assert "ISIN:IE00BK5BQT80" in output
    assert "IBKR:265598" in output
    assert "Vanguard FTSE All-World UCITS ETF" in output


def test_normalize_trading212_ticker_removes_broker_suffixes() -> None:
    assert connectors.normalize_trading212_ticker("IS3Nd_EQ") == "IS3N"
    assert connectors.normalize_trading212_ticker("VWCE_DE_EQ") == "VWCE"
    assert connectors.normalize_trading212_ticker("CASH EUR") == "CASH EUR"
    assert connectors.normalize_trading212_ticker("alreadylower") == "alreadylower"


def test_ibkr_flex_holdings_include_positions_and_cash(monkeypatch) -> None:
    monkeypatch.setattr(connectors.ibkr, "IbkrFlexClient", FakeIbkrFlexClient)

    holdings = connectors.load_ibkr_holdings(flex_token="test-token")

    # Should have one equity position (VWCE) and one cash entry (EUR)
    equity = [h for h in holdings if h.ticker == "VWCE"][0]
    assert equity.broker == "IBKR"
    assert equity.identifier == "IBKR:1234567"
    assert equity.security_currency == "EUR"
    assert equity.description == "Vanguard FTSE All-World"

    cash = [h for h in holdings if h.ticker == "CASH EUR"][0]
    assert cash.broker == "IBKR"
    assert cash.value == 3000.0


def test_ibkr_flex_uses_fx_rate_and_provides_isin(monkeypatch) -> None:
    """Flex provides conid, isin, and description directly — no contract_info call needed."""
    import xml.etree.ElementTree as ET

    client = FakeIbkrFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(
        FLEX_XML_CONID
    )  # type: ignore[assignment]
    monkeypatch.setattr(connectors.ibkr, "IbkrFlexClient", lambda **kwargs: client)

    holdings = connectors.load_ibkr_holdings(flex_token="test-token")

    # GOOGL position: positionValue=5000, fxRateToBase=0.9 → 4500 in EUR base
    equity = [h for h in holdings if h.ticker == "GOOGL"][0]
    assert equity.identifier == "IBKR:208813719"
    assert equity.description == "Alphabet Inc Class A"
    # FX rate to base: 5000 * 0.9 = 4500
    assert equity.value == 4500.0
    assert equity.security_currency == "USD"


def test_ibkr_flex_identifier_prefers_conid_over_isin(monkeypatch) -> None:
    """When conid is available, use IBKR:<conid> as identifier."""
    monkeypatch.setattr(connectors.ibkr, "IbkrFlexClient", FakeIbkrFlexClient)

    holdings = connectors.load_ibkr_holdings(flex_token="test-token")
    equity = [h for h in holdings if h.ticker == "VWCE"][0]
    # conid is present, so identifier is IBKR:1234567 (not ISIN:IE00BK5BQT80)
    assert equity.identifier == "IBKR:1234567"


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
