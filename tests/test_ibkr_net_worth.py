from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import ibkr_net_worth as ibkr


FLEX_RESPONSE_XML = """\
<FlexQueryResponse queryName="get-open-positions" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U123" currency="EUR" fxRateToBase="1.2"
                      assetClass="STK" symbol="EUR ETF" description="iShares Core MSCI World"
                      conid="1234567" isin="IE00BK5BQT80" listingExchange="XETRA"
                      reportDate="20260625" quantity="100" markPrice="50.0"
                      positionValue="5000.0" costBasisPrice="40.0"
                      costBasisMoney="4000.0" percentOfNAV="5.0"
                      unrealizedPnl="1000.0" side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U123" currency="USD"
                           netLiquidationValue="78000.00" cashBalance="5000.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


class FakeFlexClient:
    """Fake Flex client that returns a fixed XML response."""

    def __init__(self) -> None:
        self.request_report_calls: int = 0
        self.fetch_report_calls: int = 0

    def request_report(self) -> str:
        self.request_report_calls += 1
        return "12345678"

    def fetch_report(self, reference_code: str, retries: int = 6, delay: float = 3.0) -> ET.Element:
        self.fetch_report_calls += 1
        assert reference_code == "12345678"
        return ET.fromstring(FLEX_RESPONSE_XML)


def test_as_float_handles_various_types() -> None:
    assert ibkr.as_float(100) == 100.0
    assert ibkr.as_float("50.5") == 50.5
    assert ibkr.as_float(None) == 0.0
    assert ibkr.as_float("") == 0.0
    assert ibkr.as_float("abc", 99.0) == 99.0


def test_position_helpers_parse_flex_attributes() -> None:
    pos = {
        "symbol": "AAPL",
        "description": "Apple Inc",
        "conid": "265598",
        "isin": "US0378331005",
        "assetClass": "STK",
    }
    assert ibkr.position_label(pos) == "AAPL"
    assert ibkr.position_conid(pos) == "265598"
    assert ibkr.position_isin(pos) == "US0378331005"
    assert ibkr.position_description(pos) == "Apple Inc"


def test_position_label_falls_back_to_description() -> None:
    assert ibkr.position_label({"description": "My Fund"}) == "My Fund"
    assert ibkr.position_label({"conid": "999"}) == "999"
    assert ibkr.position_label({}) == "UNKNOWN"


def test_position_isin_returns_empty_when_missing() -> None:
    assert ibkr.position_isin({}) == ""
    assert ibkr.position_isin({"isin": ""}) == ""


def test_position_conid_returns_empty_when_missing() -> None:
    assert ibkr.position_conid({}) == ""
    assert ibkr.position_conid({"conid": None}) == ""


def test_parse_positions_extracts_open_position_attributes() -> None:
    root = ET.fromstring(FLEX_RESPONSE_XML)
    positions = ibkr.parse_positions(root)
    assert len(positions) == 1
    assert positions[0]["symbol"] == "EUR ETF"
    assert positions[0]["isin"] == "IE00BK5BQT80"
    assert positions[0]["conid"] == "1234567"


def test_parse_account_info_extracts_attributes() -> None:
    root = ET.fromstring(FLEX_RESPONSE_XML)
    accounts = ibkr.parse_account_info(root)
    assert len(accounts) == 1
    assert accounts[0]["accountId"] == "U123"
    assert accounts[0]["netLiquidationValue"] == "78000.00"
    assert accounts[0]["cashBalance"] == "5000.00"


def test_load_assets_from_flex_response() -> None:
    client = FakeFlexClient()
    assets, net_worth = ibkr.load_assets(client)

    assert client.request_report_calls == 1
    assert client.fetch_report_calls == 1
    assert net_worth == 78000.0

    # One equity position + one cash entry
    assert len(assets) == 2

    equity = [a for a in assets if a.asset_class != "CASH"][0]
    assert equity.label == "EUR ETF"
    assert equity.isin == "IE00BK5BQT80"
    assert equity.conid == "1234567"
    assert equity.description == "iShares Core MSCI World"
    # EUR ETF: positionValue=5000, fxRateToBase=1.2 → 6000 in base currency
    assert equity.value == 6000.0

    cash = [a for a in assets if a.asset_class == "CASH"][0]
    assert cash.label == "CASH USD"
    assert cash.value == 5000.0


def test_load_assets_net_worth_percentages_sum_to_100() -> None:
    """When positions and cash fully account for net worth, percentages sum to 100%."""
    xml_balanced = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U456" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U456" currency="USD" fxRateToBase="1.0"
                      assetClass="STK" symbol="AAPL" description="Apple Inc"
                      conid="265598" isin="US0378331005"
                      quantity="100" markPrice="150.0" positionValue="15000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U456" currency="USD"
                           netLiquidationValue="20000.00" cashBalance="5000.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml_balanced)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    assert net_worth == 20000.0
    total_pct = sum(a.value / net_worth * 100 for a in assets)
    # 15000 (AAPL) + 5000 (cash) = 20000 = 100%
    assert abs(total_pct - 100.0) < 0.01


def test_load_assets_falls_back_to_sum_when_no_account_info() -> None:
    """When AccountInformation has no netLiquidationValue, net worth is the sum of asset values."""
    xml_no_nlv = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U123" currency="USD" fxRateToBase="1.0"
                      assetClass="STK" symbol="AAPL" description="Apple"
                      conid="265598" isin="US0378331005"
                      quantity="10" markPrice="150.0" positionValue="1500.0"
                      side="Long"/>
      </OpenPositions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    # Override fetch_report to return this specific XML
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml_no_nlv)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    assert net_worth == 1500.0
    assert len(assets) == 1
    assert assets[0].label == "AAPL"


def test_load_assets_derives_cash_from_nlv_minus_positions() -> None:
    """When cashBalance field is absent, derive cash = NLV - sum(positions in base currency)."""
    xml_no_cash_field = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U789" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U789" currency="USD" fxRateToBase="0.9"
                      assetClass="STK" symbol="GOOGL" description="Alphabet"
                      conid="208813719" isin="US02079K3059"
                      quantity="50" markPrice="100.0" positionValue="5000.0"
                      side="Long"/>
        <OpenPosition accountId="U789" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="1234567" isin="IE00BK5BQT80"
                      quantity="10" markPrice="100.0" positionValue="1000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U789" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml_no_cash_field)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    assert net_worth == 10000.0

    # GOOGL: 5000 USD * 0.9 = 4500 EUR
    # VWCE: 1000 EUR * 1.0 = 1000 EUR
    # Total positions: 5500 EUR
    # Cash = 10000 - 5500 = 4500 EUR
    cash = [a for a in assets if a.asset_class == "CASH"]
    assert len(cash) == 1
    assert cash[0].label == "CASH EUR"
    assert cash[0].value == 4500.0

    total_pct = sum(a.value / net_worth * 100 for a in assets)
    assert abs(total_pct - 100.0) < 0.01


def test_load_assets_no_derived_cash_when_nlv_equals_positions() -> None:
    """When NLV equals the sum of positions, no CASH row is added (avoids zero-cash noise)."""
    xml_no_cash = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U999" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U999" currency="USD" fxRateToBase="1.0"
                      assetClass="STK" symbol="AAPL" description="Apple"
                      conid="265598" isin="US0378331005"
                      quantity="10" markPrice="150.0" positionValue="1500.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U999" currency="USD"
                           netLiquidationValue="1500.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml_no_cash)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    assert net_worth == 1500.0
    assert all(a.asset_class != "CASH" for a in assets)


def test_parse_cash_report_extracts_ending_cash_per_currency() -> None:
    """parse_cash_report returns per-currency endingCash entries."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <CashReport>
        <CashReportCurrency accountId="U123" currency="USD"
                    endingCash="5000.00" startingCash="4800.00"/>
        <CashReportCurrency accountId="U123" currency="EUR"
                    endingCash="2000.00" startingCash="1800.00"/>
      </CashReport>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    root = ET.fromstring(xml)
    entries = ibkr.parse_cash_report(root)
    assert len(entries) == 2
    usd = [e for e in entries if e["currency"] == "USD"][0]
    assert usd["accountId"] == "U123"
    assert usd["endingCash"] == "5000.00"
    eur = [e for e in entries if e["currency"] == "EUR"][0]
    assert eur["endingCash"] == "2000.00"


def test_parse_cash_report_ignores_section_wrapper() -> None:
    """The outer <CashReport> section element (without accountId) is not a data row."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <CashReport>
        <CashReportCurrency accountId="U123" currency="USD"
                    endingCash="3000.00"/>
      </CashReport>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    root = ET.fromstring(xml)
    entries = ibkr.parse_cash_report(root)
    # Only the CashReportCurrency element is a data row
    assert len(entries) == 1
    assert entries[0]["accountId"] == "U123"
    assert entries[0]["currency"] == "USD"


def test_parse_cash_report_filters_summary_rows() -> None:
    """Summary rows like 'BASE SUMMARY' and 'Total' are excluded to avoid double-counting."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <CashReport>
        <CashReportCurrency accountId="U123" currency="EUR"
                    endingCash="3000.00"/>
        <CashReportCurrency accountId="U123" currency="PLN"
                    endingCash="20000.00"/>
        <CashReportCurrency accountId="U123" currency="BASE SUMMARY"
                    endingCash="4700.00"/>
      </CashReport>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    root = ET.fromstring(xml)
    entries = ibkr.parse_cash_report(root)
    currencies = [e["currency"] for e in entries]
    assert "EUR" in currencies
    assert "PLN" in currencies
    assert "BASE SUMMARY" not in currencies
    assert len(entries) == 2


def test_parse_conversion_rates() -> None:
    """parse_conversion_rates extracts currency -> rate mapping."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <ConversionRates>
        <ConversionRate fromCurrency="EUR" toCurrency="USD" rate="1.1"/>
        <ConversionRate fromCurrency="CHF" toCurrency="USD" rate="1.15"/>
        <ConversionRate fromCurrency="USD" toCurrency="USD" rate="1.0"/>
      </ConversionRates>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    root = ET.fromstring(xml)
    rates = ibkr.parse_conversion_rates(root)
    assert rates["EUR"] == 1.1
    assert rates["CHF"] == 1.15
    assert rates["USD"] == 1.0


def test_load_assets_uses_cash_report_for_per_currency_cash() -> None:
    """When Cash Report section is present, per-currency endingCash is used.

    CashReport doesn't include fxRateToBase — FX rates come from OpenPositions
    for matching (account, currency) pairs.
    """
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U123" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard FTSE All-World"
                      conid="1234567" isin="IE00BK5BQT80"
                      quantity="100" markPrice="50.0" positionValue="5000.0"
                      side="Long"/>
        <OpenPosition accountId="U123" currency="USD" fxRateToBase="0.9"
                      assetClass="STK" symbol="GOOGL" description="Alphabet"
                      conid="208813719" isin="US02079K3059"
                      quantity="10" markPrice="100.0" positionValue="1000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U123" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
      <CashReport>
        <CashReportCurrency accountId="U123" currency="EUR"
                    endingCash="3500.00" startingCash="3000.00"/>
        <CashReportCurrency accountId="U123" currency="USD"
                    endingCash="1500.00" startingCash="1000.00"/>
      </CashReport>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    assert net_worth == 10000.0

    # Positions: VWCE=5000 EUR, GOOGL=1000 USD * 0.9 = 900 EUR
    # Cash from Cash Report: EUR 3500, USD 1500 * 0.9 (from GOOGL position fxRate) = 1350 EUR
    cash_entries = [a for a in assets if a.asset_class == "CASH"]
    assert len(cash_entries) == 2

    cash_eur = [c for c in cash_entries if c.label == "CASH EUR"][0]
    assert cash_eur.value == 3500.0
    assert cash_eur.security_currency == "EUR"

    cash_usd = [c for c in cash_entries if c.label == "CASH USD"][0]
    assert cash_usd.value == 1500.0 * 0.9  # 1350 EUR (USD fxRateToBase=0.9 from GOOGL)
    assert cash_usd.security_currency == "USD"


def test_load_assets_cash_report_overrides_derived_cash() -> None:
    """When Cash Report is present, it takes priority over derived cash from NLV.

    FX rates come from OpenPositions (CashReport has no fxRateToBase field).
    """
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U456" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U456" currency="USD" fxRateToBase="1.0"
                      assetClass="STK" symbol="AAPL" description="Apple Inc"
                      conid="265598" isin="US0378331005"
                      quantity="100" markPrice="150.0" positionValue="15000.0"
                      side="Long"/>
        <OpenPosition accountId="U456" currency="EUR" fxRateToBase="1.1"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="999" isin="IE00BK5BQT80"
                      quantity="50" markPrice="80.0" positionValue="4000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U456" currency="USD"
                           netLiquidationValue="25000.00"/>
      </AccountInformation>
      <CashReport>
        <CashReportCurrency accountId="U456" currency="USD"
                    endingCash="8000.00" startingCash="7000.00"/>
        <CashReportCurrency accountId="U456" currency="EUR"
                    endingCash="2000.00" startingCash="1500.00"/>
      </CashReport>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)

    # Cash from Cash Report:
    #   USD: 8000 (same currency as base, fxRate=1.0 from AAPL position)
    #   EUR: 2000 * 1.1 (fxRateToBase from VWCE position) = 2200 USD
    cash_entries = [a for a in assets if a.asset_class == "CASH"]
    assert len(cash_entries) == 2

    cash_usd = [c for c in cash_entries if c.label == "CASH USD"][0]
    assert cash_usd.value == 8000.0
    assert cash_usd.security_currency == "USD"

    cash_eur = [c for c in cash_entries if c.label == "CASH EUR"][0]
    assert cash_eur.value == 2000.0 * 1.1  # 2200 USD
    assert cash_eur.security_currency == "EUR"

    # No derived CASH entry (only the two from Cash Report)
    assert not any(c.label == "CASH USD" and c.security_currency == "" for c in cash_entries)


def test_load_assets_cash_report_no_fx_rate_defaults_to_one() -> None:
    """Cash in a currency with no matching OpenPosition uses fxRate=1.0 (same as base)."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U789" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U789" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="123" isin="IE00BK5BQT80"
                      quantity="100" markPrice="50.0" positionValue="5000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U789" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
      <CashReport>
        <CashReportCurrency accountId="U789" currency="EUR"
                    endingCash="3000.00"/>
        <CashReportCurrency accountId="U789" currency="CHF"
                    endingCash="500.00"/>
      </CashReport>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)

    cash_eur = [a for a in assets if a.label == "CASH EUR"][0]
    assert cash_eur.value == 3000.0
    assert cash_eur.security_currency == "EUR"

    # CHF has no position to provide fxRateToBase, so it defaults to 1.0
    # and is treated as if it were the base currency (EUR)
    cash_chf = [a for a in assets if a.label == "CASH CHF"][0]
    assert cash_chf.value == 500.0  # No conversion, fxRate defaults to 1.0
    assert cash_chf.security_currency == "CHF"


def test_load_assets_cash_report_uses_conversion_rates_for_missing_fx() -> None:
    """ConversionRates section provides FX rates for currencies with no OpenPosition."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U789" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U789" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="123" isin="IE00BK5BQT80"
                      quantity="100" markPrice="50.0" positionValue="5000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U789" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
      <CashReport>
        <CashReportCurrency accountId="U789" currency="EUR"
                    endingCash="3000.00"/>
        <CashReportCurrency accountId="U789" currency="CHF"
                    endingCash="500.00"/>
      </CashReport>
      <ConversionRates>
        <ConversionRate fromCurrency="EUR" toCurrency="EUR" rate="1.0"/>
        <ConversionRate fromCurrency="CHF" toCurrency="EUR" rate="0.95"/>
      </ConversionRates>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)

    cash_eur = [a for a in assets if a.label == "CASH EUR"][0]
    assert cash_eur.value == 3000.0
    assert cash_eur.security_currency == "EUR"

    # CHF has no position, but ConversionRate provides rate=0.95
    cash_chf = [a for a in assets if a.label == "CASH CHF"][0]
    assert cash_chf.value == 500.0 * 0.95  # 475 EUR
    assert cash_chf.security_currency == "CHF"


def test_load_assets_cash_report_excludes_base_summary() -> None:
    """BASE SUMMARY rows are excluded to prevent double-counting per-currency cash."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U456" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U456" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="123" isin="IE00BK5BQT80"
                      quantity="100" markPrice="50.0" positionValue="5000.0"
                      side="Long"/>
        <OpenPosition accountId="U456" currency="USD" fxRateToBase="0.9"
                      assetClass="STK" symbol="GOOGL" description="Alphabet"
                      conid="456" isin="US02079K3059"
                      quantity="10" markPrice="100.0" positionValue="1000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U456" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
      <CashReport>
        <CashReportCurrency accountId="U456" currency="EUR"
                    endingCash="3500.00"/>
        <CashReportCurrency accountId="U456" currency="USD"
                    endingCash="1500.00"/>
        <CashReportCurrency accountId="U456" currency="PLN"
                    endingCash="20000.00"/>
        <CashReportCurrency accountId="U456" currency="BASE SUMMARY"
                    endingCash="8700.00"/>
      </CashReport>
      <ConversionRates>
        <ConversionRate fromCurrency="EUR" toCurrency="EUR" rate="1.0"/>
        <ConversionRate fromCurrency="USD" toCurrency="EUR" rate="0.9"/>
        <ConversionRate fromCurrency="PLN" toCurrency="EUR" rate="0.235"/>
      </ConversionRates>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    cash_entries = [a for a in assets if a.asset_class == "CASH"]
    cash_labels = [c.label for c in cash_entries]

    # BASE SUMMARY must NOT appear — it's a subtotal of per-currency rows
    assert "CASH BASE SUMMARY" not in cash_labels

    # Per-currency cash should be present and correctly converted
    cash_eur = [c for c in cash_entries if c.label == "CASH EUR"][0]
    assert cash_eur.value == 3500.0

    cash_usd = [c for c in cash_entries if c.label == "CASH USD"][0]
    assert cash_usd.value == 1500.0 * 0.9  # 1350 EUR

    cash_pln = [c for c in cash_entries if c.label == "CASH PLN"][0]
    assert cash_pln.value == 20000.0 * 0.235  # 4700 EUR


def test_load_assets_cash_report_warns_on_missing_fx_rate(capsys) -> None:
    """A warning is printed when a non-base currency cash entry has no FX rate."""
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U789" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U789" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="123" isin="IE00BK5BQT80"
                      quantity="100" markPrice="50.0" positionValue="5000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U789" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
      <CashReport>
        <CashReportCurrency accountId="U789" currency="EUR"
                    endingCash="3000.00"/>
        <CashReportCurrency accountId="U789" currency="PLN"
                    endingCash="20000.00"/>
      </CashReport>
      <!-- No ConversionRates section, and no PLN position to provide fxRateToBase -->
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    captured = capsys.readouterr()

    cash_pln = [a for a in assets if a.label == "CASH PLN"][0]
    # Without an FX rate, PLN amount is treated as-is (wrong, but graceful)
    assert cash_pln.value == 20000.0
    assert cash_pln.security_currency == "PLN"
    # A warning should have been printed to stderr
    assert "No FX rate for PLN" in captured.err


    """When no Cash Report and no cashBalance, cash is derived from NLV minus positions."""
    # This is the same test as test_load_assets_derives_cash_from_nlv_minus_positions,
    # verifying the fallback chain still works.
    xml = """\
<FlexQueryResponse queryName="test" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U789" fromDate="20260601" toDate="20260625">
      <OpenPositions>
        <OpenPosition accountId="U789" currency="USD" fxRateToBase="0.9"
                      assetClass="STK" symbol="GOOGL" description="Alphabet"
                      conid="208813719" isin="US02079K3059"
                      quantity="50" markPrice="100.0" positionValue="5000.0"
                      side="Long"/>
        <OpenPosition accountId="U789" currency="EUR" fxRateToBase="1.0"
                      assetClass="STK" symbol="VWCE" description="Vanguard"
                      conid="1234567" isin="IE00BK5BQT80"
                      quantity="10" markPrice="100.0" positionValue="1000.0"
                      side="Long"/>
      </OpenPositions>
      <AccountInformation>
        <AccountInformation accountId="U789" currency="EUR"
                           netLiquidationValue="10000.00"/>
      </AccountInformation>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    client = FakeFlexClient()
    client.fetch_report = lambda ref, retries=6, delay=3.0: ET.fromstring(xml)  # type: ignore[assignment]

    assets, net_worth = ibkr.load_assets(client)
    assert net_worth == 10000.0
    cash = [a for a in assets if a.asset_class == "CASH"]
    assert len(cash) == 1
    assert cash[0].label == "CASH EUR"
    # 10000 - (5000*0.9 + 1000) = 10000 - 5500 = 4500
    assert cash[0].value == 4500.0


def test_ibkr_flex_client_request_report_parses_reference_code() -> None:
    """Verify request_report parses the SendRequest XML response."""
    import unittest.mock

    response_xml = """\
<FlexStatementResponse>
  <Status>Success</Status>
  <ReferenceCode>98765432</ReferenceCode>
</FlexStatementResponse>
"""
    client = ibkr.IbkrFlexClient(token="test-token", query_id="1554188")
    client._request = lambda path, params: response_xml  # type: ignore[assignment]

    ref_code = client.request_report()
    assert ref_code == "98765432"


def test_ibkr_flex_client_request_report_raises_on_error() -> None:
    """Verify request_report raises IbkrError on failure status."""
    response_xml = """\
<FlexStatementResponse>
  <Status>Fail</Status>
  <ErrorCode>1003</ErrorCode>
  <ErrorMessage>Invalid token</ErrorMessage>
</FlexStatementResponse>
"""
    client = ibkr.IbkrFlexClient(token="bad-token", query_id="1554188")
    client._request = lambda path, params: response_xml  # type: ignore[assignment]

    try:
        client.request_report()
        assert False, "Expected IbkrError"
    except ibkr.IbkrError as exc:
        assert "Invalid token" in str(exc)