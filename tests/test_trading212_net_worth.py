from __future__ import annotations

from pathlib import Path
import sys
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import trading212_net_worth as trading212


class FakeClient:
    def account_summary(self) -> dict[str, object]:
        return {"currencyCode": "EUR", "free": 25.0, "total": 225.0}

    def positions(self) -> list[dict[str, object]]:
        return [
            {
                "ticker": "VUAA",
                "quantity": 2,
                "currentPrice": 100.0,
            },
            {
                "ticker": "ZERO",
                "quantity": 0,
                "currentPrice": 100.0,
            },
        ]

    def instruments(self) -> list[dict[str, object]]:
        return [{"ticker": "VUAA", "currencyCode": "USD", "name": "Vanguard ETF"}]


class FakeClientWithWalletCurrency:
    def account_summary(self) -> dict[str, object]:
        return {"currencyCode": "EUR", "free": 0.0, "total": 100.0}

    def positions(self) -> list[dict[str, object]]:
        return [
            {
                "instrument": {
                    "ticker": "IS3Nd_EQ",
                    "currencyCode": "EUR",
                    "name": "iShares Core MSCI World UCITS ETF",
                    "isin": "IE00B4L5Y983",
                },
                "walletImpact": {"currency": "PLN", "currentValue": 100.0},
            }
        ]

    def instruments(self) -> list[dict[str, object]]:
        return [
            {
                "ticker": "IS3Nd_EQ",
                "currencyCode": "EUR",
                "name": "iShares Core MSCI World UCITS ETF",
                "isin": "IE00B4L5Y983",
            }
        ]


class FakeClientWithMarketValue:
    def account_summary(self) -> dict[str, object]:
        return {"currencyCode": "GBP", "free": 30.0}

    def positions(self) -> list[dict[str, object]]:
        return [
            {
                "ticker": "ABC",
                "quantity": 2,
                "currentPrice": 100.0,
                "marketValue": 150.0,
                "currencyCode": "GBP",
            }
        ]

    def instruments(self) -> list[dict[str, object]]:
        raise AssertionError("metadata should not be loaded when disabled")


def test_load_assets_builds_same_table_shape_from_positions_and_cash() -> None:
    assets, net_worth = trading212.load_assets(
        FakeClient(),
        account_id_value="T212-1",
        include_metadata=True,
    )

    assert net_worth == 225.0
    assert assets == [
        trading212.Asset(
            "T212-1",
            "VUAA",
            "Vanguard ETF",
            "EQUITY",
            "USD",
            200.0,
            security_currency="USD",
        ),
        trading212.Asset(
            "T212-1",
            "CASH EUR",
            "Cash EUR",
            "CASH",
            "EUR",
            25.0,
            security_currency="EUR",
        ),
    ]


def test_position_value_prefers_api_market_value_when_available() -> None:
    assets, net_worth = trading212.load_assets(
        FakeClientWithMarketValue(),
        account_id_value="T212-2",
        include_metadata=False,
    )

    assert net_worth == 180.0
    assert assets == [
        trading212.Asset(
            "T212-2",
            "ABC",
            "ABC",
            "EQUITY",
            "GBP",
            150.0,
            security_currency="GBP",
        ),
        trading212.Asset(
            "T212-2",
            "CASH GBP",
            "Cash GBP",
            "CASH",
            "GBP",
            30.0,
            security_currency="GBP",
        ),
    ]


def test_load_assets_keeps_wallet_currency_separate_from_security_currency() -> None:
    assets, _net_worth = trading212.load_assets(
        FakeClientWithWalletCurrency(),
        account_id_value="T212-3",
        include_metadata=True,
    )

    assert assets == [
        trading212.Asset(
            "T212-3",
            "IS3Nd_EQ",
            "iShares Core MSCI World UCITS ETF",
            "EQUITY",
            "PLN",
            100.0,
            isin="IE00B4L5Y983",
            security_currency="EUR",
        )
    ]


def test_position_mapping_uses_documented_nested_position_schema() -> None:
    position = {
        "instrument": {
            "ticker": "VWCE_DE_EQ",
            "currencyCode": "EUR",
            "name": "VWCE ETF",
            "isin": "IE00BK5BQT80",
        },
        "quantity": 3,
        "currentPrice": 100.0,
        "walletImpact": {"currency": "PLN", "currentValue": 1290.0},
    }

    assert trading212.position_label(position) == "VWCE_DE_EQ"
    assert trading212.position_name(position, {}) == "VWCE ETF"
    assert trading212.position_isin(position, {}) == "IE00BK5BQT80"
    assert trading212.position_value(position) == 1290.0
    assert trading212.position_currency(position, {}, "EUR") == "PLN"
    assert trading212.position_security_currency(position, {}, "EUR") == "EUR"


def test_position_isin_uses_instrument_metadata_lookup() -> None:
    position = {"ticker": "VWCE_DE_EQ"}

    assert (
        trading212.position_isin(position, {"VWCE_DE_EQ": "IE00BK5BQT80"})
        == "IE00BK5BQT80"
    )


def test_print_assets_includes_asset_name_column(capsys) -> None:
    trading212.print_assets(
        [
            trading212.Asset(
                "T212-1",
                "VWCE_DE_EQ",
                "Vanguard FTSE All-World UCITS ETF",
                "EQUITY",
                "PLN",
                100.0,
            )
        ],
        net_worth=200.0,
    )

    output = capsys.readouterr().out

    assert "Asset Name" in output
    assert "Vanguard FTSE All-World UCITS ETF" in output


def test_demo_flag_overrides_default_base_url(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "trading212_net_worth.py",
            "--api-key",
            "secret",
            "--api-secret",
            "api-secret",
            "--account-id",
            "T212-1",
            "--demo",
        ],
    )

    args = trading212.parse_args()

    assert args.api_key == "secret"
    assert args.api_secret == "api-secret"
    assert args.account_id == "T212-1"
    assert args.demo is True


def test_client_sends_configurable_user_agent(monkeypatch) -> None:
    captured_headers: dict[str, str] = {}

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"currencyCode":"EUR"}'

    def fake_urlopen(
        request: urllib.request.Request,
        timeout: float,
    ) -> FakeResponse:
        assert timeout == 10.0
        captured_headers.update(dict(request.header_items()))
        return FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = trading212.Trading212Client(
        "https://live.trading212.com/api/v0",
        api_key=" api-key ",
        api_secret=" api-secret ",
        timeout=10.0,
        user_agent="custom-agent",
    )

    assert client.account_summary() == {"currencyCode": "EUR"}
    assert captured_headers["Authorization"] == "Basic YXBpLWtleTphcGktc2VjcmV0"
    assert captured_headers["User-agent"] == "custom-agent"


def test_access_denied_html_gets_actionable_error() -> None:
    error = trading212.Trading212HttpError(
        "GET",
        "https://live.trading212.com/api/v0/equity/account/info",
        403,
        "<html><h1>Access denied</h1></html>",
    )

    assert "access denied by Trading 212" in str(error)
    assert "--user-agent" in str(error)


def test_unauthorized_error_is_not_padded_with_guesses() -> None:
    error = trading212.Trading212HttpError(
        "GET",
        "https://live.trading212.com/api/v0/equity/account/summary",
        401,
        '{"error":"API key is invalid"}',
    )

    assert str(error) == (
        'GET https://live.trading212.com/api/v0/equity/account/summary '
        'failed: HTTP 401 {"error": "API key is invalid"}'
    )


def test_concise_details_returns_plain_text_body() -> None:
    assert trading212.concise_details("unauthorized") == "unauthorized"


def test_basic_auth_header_uses_key_as_username_and_secret_as_password() -> None:
    assert (
        trading212.basic_auth_header(" api-key ", " api-secret ")
        == "Basic YXBpLWtleTphcGktc2VjcmV0"
    )
