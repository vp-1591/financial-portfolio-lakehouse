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