from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import ibkr_net_worth as ibkr


class FakeClient:
    def accounts(self) -> list[dict[str, str]]:
        return [{"accountId": "U123"}]

    def positions(self, account_id: str) -> list[dict[str, object]]:
        assert account_id == "U123"
        return [
            {
                "contractDesc": "EUR ETF",
                "assetClass": "STK",
                "currency": "EUR",
                "mktValue": 100.0,
                "secIdType": "ISIN",
                "secId": "IE00BK5BQT80",
            }
        ]

    def ledger(self, account_id: str) -> dict[str, dict[str, object]]:
        assert account_id == "U123"
        return {
            "BASE": {"currency": "USD", "netliquidationvalue": 240.0, "exchangerate": 1.0},
            "EUR": {"currency": "EUR", "cashbalance": 100.0, "exchangerate": 1.2},
        }


class FakeSessionClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def sso_validate(self) -> dict[str, object]:
        self.calls.append("sso_validate")
        return {"RESULT": True}

    def auth_status(self) -> dict[str, object]:
        self.calls.append("auth_status")
        return {"authenticated": True}


def test_to_base_currency_uses_exchange_rate_as_base_value_per_unit() -> None:
    assert ibkr.to_base_currency(100.0, "EUR", {"EUR": 1.2}) == 120.0


def test_net_liquidation_value_without_base_converts_each_currency_to_base() -> None:
    ledger = {
        "USD": {"currency": "USD", "netliquidationvalue": 50.0, "exchangerate": 1.0},
        "EUR": {"currency": "EUR", "netliquidationvalue": 100.0, "exchangerate": 1.2},
    }

    assert ibkr.net_liquidation_value(ledger) == 170.0


def test_load_assets_percentages_sum_to_net_worth_after_currency_conversion() -> None:
    assets, net_worth = ibkr.load_assets(FakeClient(), selected_account=None)

    assert net_worth == 240.0
    assert [(asset.label, asset.value) for asset in assets] == [
        ("EUR ETF", 120.0),
        ("CASH EUR", 120.0),
    ]
    assert assets[0].isin == "IE00BK5BQT80"
    assert sum(asset.value / net_worth * 100 for asset in assets) == 100.0


def test_position_isin_reads_common_ibkr_fields() -> None:
    assert ibkr.position_isin({"isin": "US0378331005"}) == "US0378331005"
    assert (
        ibkr.position_isin({"secIdType": "ISIN", "secId": "IE00BK5BQT80"})
        == "IE00BK5BQT80"
    )
    assert ibkr.position_isin({"secIdType": "CUSIP", "secId": "037833100"}) == ""


def test_position_identifier_and_description_helpers() -> None:
    position = {
        "conid": 208813719,
        "contractDesc": "GOOGL",
        "currency": "USD",
    }

    assert ibkr.position_conid(position) == "208813719"
    assert (
        ibkr.position_description(position, {"companyName": "Alphabet Inc Class A"})
        == "Alphabet Inc Class A"
    )
    assert ibkr.position_description(position) == "GOOGL"


def test_validate_gateway_session_uses_sso_for_portfolio_reads_by_default() -> None:
    client = FakeSessionClient()

    ibkr.validate_gateway_session(client, require_brokerage_session=False)

    assert client.calls == ["sso_validate"]


def test_validate_gateway_session_can_require_brokerage_session() -> None:
    client = FakeSessionClient()

    ibkr.validate_gateway_session(client, require_brokerage_session=True)

    assert client.calls == ["auth_status"]
