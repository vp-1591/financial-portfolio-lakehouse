#!/usr/bin/env python3
"""Print Trading 212 portfolio assets as percentages of account net worth."""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_URL = "https://live.trading212.com/api/v0"
DEMO_BASE_URL = "https://demo.trading212.com/api/v0"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


class Trading212Error(RuntimeError):
    pass


class Trading212HttpError(Trading212Error):
    def __init__(self, method: str, url: str, code: int, details: str) -> None:
        self.method = method
        self.url = url
        self.code = code
        self.details = details
        if is_access_denied_html(details):
            message = (
                f"{method} {url} failed: HTTP {code} access denied by Trading 212. "
                "Try again with a different --user-agent value, and verify your "
                "network/IP is allowed by Trading 212."
            )
        else:
            reason = concise_details(details)
            message = f"{method} {url} failed: HTTP {code}"
            if reason:
                message = f"{message} {reason}"
        super().__init__(message)


@dataclass(frozen=True)
class Asset:
    account_id: str
    label: str
    name: str
    asset_class: str
    currency: str
    value: float


class Trading212Client:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        timeout: float,
        user_agent: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.timeout = timeout
        self.user_agent = user_agent

    def request(self, method: str, path: str) -> Any:
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": basic_auth_header(self.api_key, self.api_secret),
                "User-Agent": self.user_agent,
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise Trading212HttpError(method, url, exc.code, details) from exc
        except urllib.error.URLError as exc:
            raise Trading212Error(f"{method} {url} failed: {exc.reason}") from exc

        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            if is_access_denied_html(raw):
                raise Trading212Error(
                    f"{method} {url} returned an access denied page from Trading 212. "
                    "Try again with a different --user-agent value, and verify your "
                    "network/IP is allowed by Trading 212."
                ) from exc
            raise Trading212Error(
                f"{method} {url} returned non-JSON response: {raw[:200]}"
            ) from exc

    def account_summary(self) -> dict[str, Any]:
        summary = self.request("GET", "/equity/account/summary")
        if not isinstance(summary, dict):
            raise Trading212Error("Unexpected account summary response.")
        return summary

    def positions(self) -> list[dict[str, Any]]:
        positions = self.request("GET", "/equity/positions")
        if not isinstance(positions, list):
            raise Trading212Error("Unexpected positions response.")
        return positions

    def instruments(self) -> list[dict[str, Any]]:
        instruments = self.request("GET", "/equity/metadata/instruments")
        if not isinstance(instruments, list):
            raise Trading212Error("Unexpected instruments metadata response.")
        return instruments


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def is_access_denied_html(details: str) -> bool:
    lowered = details.lower()
    return "<html" in lowered and "access denied" in lowered


def concise_details(details: str, limit: int = 500) -> str:
    stripped = details.strip()
    if not stripped:
        return ""
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return stripped[:limit]
    return json.dumps(parsed, ensure_ascii=True)[:limit]


def basic_auth_header(api_key: str, api_secret: str) -> str:
    credentials = f"{api_key.strip()}:{api_secret.strip()}".encode("utf-8")
    encoded_credentials = base64.b64encode(credentials).decode("ascii")
    return f"Basic {encoded_credentials}"


def first_value(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def nested_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def account_currency(summary: dict[str, Any]) -> str:
    value = first_value(
        summary,
        (
            "currencyCode",
            "currency",
            "baseCurrency",
            "accountCurrency",
        ),
    )
    return str(value) if value else ""


def cash_value(summary: dict[str, Any]) -> float:
    for key in ("free", "cash", "availableFunds", "available", "totalCash"):
        value = first_value(summary, (key,))
        if value is not None:
            return as_float(value)
    return 0.0


def net_worth_value(summary: dict[str, Any], fallback: float) -> float:
    for key in (
        "total",
        "totalValue",
        "accountValue",
        "netAssetValue",
        "portfolioValue",
    ):
        value = first_value(summary, (key,))
        if value is not None:
            return as_float(value)
    return fallback


def instrument_currency_by_ticker(instruments: list[dict[str, Any]]) -> dict[str, str]:
    currencies: dict[str, str] = {}
    for instrument in instruments:
        ticker = first_value(instrument, ("ticker", "shortName", "isin"))
        currency = first_value(
            instrument,
            ("currencyCode", "currency", "workingScheduleId"),
        )
        if ticker and currency:
            currencies[str(ticker)] = str(currency)
    return currencies


def instrument_name_by_ticker(instruments: list[dict[str, Any]]) -> dict[str, str]:
    names: dict[str, str] = {}
    for instrument in instruments:
        ticker = first_value(instrument, ("ticker",))
        name = first_value(instrument, ("name", "shortName", "isin"))
        if ticker and name:
            names[str(ticker)] = str(name)
    return names


def position_label(position: dict[str, Any]) -> str:
    instrument = nested_dict(position, "instrument")
    value = first_value(instrument, ("ticker", "name", "shortName", "isin"))
    if value is None:
        value = first_value(position, ("ticker", "name", "shortName", "isin"))
    return str(value) if value else "UNKNOWN"


def position_name(position: dict[str, Any], instrument_names: dict[str, str]) -> str:
    instrument = nested_dict(position, "instrument")
    value = first_value(instrument, ("name", "shortName", "isin"))
    if value:
        return str(value)

    ticker = first_value(instrument, ("ticker",))
    if ticker and str(ticker) in instrument_names:
        return instrument_names[str(ticker)]

    ticker = first_value(position, ("ticker",))
    if ticker and str(ticker) in instrument_names:
        return instrument_names[str(ticker)]

    return position_label(position)


def position_value(position: dict[str, Any]) -> float:
    wallet_impact = nested_dict(position, "walletImpact")
    wallet_value = first_value(wallet_impact, ("currentValue",))
    if wallet_value is not None:
        return as_float(wallet_value)

    direct_value = first_value(
        position,
        ("marketValue", "currentValue", "investedValue"),
    )
    if direct_value is not None:
        return as_float(direct_value)

    quantity = as_float(first_value(position, ("quantity", "ownedQuantity")))
    price = as_float(first_value(position, ("currentPrice", "price")))
    return quantity * price


def position_currency(
    position: dict[str, Any],
    instrument_currencies: dict[str, str],
    fallback: str,
) -> str:
    wallet_impact = nested_dict(position, "walletImpact")
    currency = first_value(wallet_impact, ("currency",))
    if currency:
        return str(currency)

    currency = first_value(position, ("currencyCode", "currency"))
    if currency:
        return str(currency)

    instrument = nested_dict(position, "instrument")
    ticker = first_value(instrument, ("ticker",))
    if ticker and str(ticker) in instrument_currencies:
        return instrument_currencies[str(ticker)]

    ticker = first_value(position, ("ticker",))
    if ticker and str(ticker) in instrument_currencies:
        return instrument_currencies[str(ticker)]

    return fallback


def load_assets(
    client: Trading212Client,
    account_id_value: str,
    include_metadata: bool,
) -> tuple[list[Asset], float]:
    summary = client.account_summary()
    positions = client.positions()
    currency = account_currency(summary)
    instruments = client.instruments() if include_metadata else []
    instrument_currencies = instrument_currency_by_ticker(instruments)
    instrument_names = instrument_name_by_ticker(instruments)

    assets: list[Asset] = []
    for position in positions:
        value = position_value(position)
        if value == 0:
            continue
        assets.append(
            Asset(
                account_id=account_id_value,
                label=position_label(position),
                name=position_name(position, instrument_names),
                asset_class="EQUITY",
                currency=position_currency(position, instrument_currencies, currency),
                value=value,
            )
        )

    cash_balance = cash_value(summary)
    if cash_balance:
        assets.append(
            Asset(
                account_id=account_id_value,
                label=f"CASH {currency}".rstrip(),
                name=f"Cash {currency}".rstrip(),
                asset_class="CASH",
                currency=currency,
                value=cash_balance,
            )
        )

    assets_total = sum(asset.value for asset in assets)
    net_worth = net_worth_value(summary, fallback=assets_total)
    return assets, net_worth


def print_assets(assets: list[Asset], net_worth: float) -> None:
    if net_worth == 0:
        raise Trading212Error("Net worth is zero; cannot calculate percentages.")

    rows = sorted(assets, key=lambda asset: abs(asset.value), reverse=True)
    print(f"Net worth: {net_worth:,.2f}")
    print()
    print(
        f"{'Account':<14} "
        f"{'Asset':<18} "
        f"{'Asset Name':<34} "
        f"{'Class':<8} "
        f"{'Currency':<8} "
        f"{'Value':>16} "
        f"{'Net Worth %':>12}"
    )
    print("-" * 122)
    for asset in rows:
        percentage = asset.value / net_worth * 100
        print(
            f"{asset.account_id:<14} "
            f"{asset.label[:18]:<18} "
            f"{asset.name[:34]:<34} "
            f"{asset.asset_class[:8]:<8} "
            f"{asset.currency[:8]:<8} "
            f"{asset.value:>16,.2f} "
            f"{percentage:>11.2f}%"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print Trading 212 positions and cash as net worth percentages."
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="Trading 212 API key. Required so it does not need to be stored on disk.",
    )
    parser.add_argument(
        "--api-secret",
        required=True,
        help="Trading 212 API secret. Required so it does not need to be stored on disk.",
    )
    parser.add_argument(
        "--account-id",
        required=True,
        help="Trading 212 account id or label to display in the Account column.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Trading 212 API base URL. Default: {DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help=f"Use the Trading 212 demo API base URL: {DEMO_BASE_URL}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP request timeout in seconds. Default: 20",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip instruments metadata lookup and use account currency for positions.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=(
            "HTTP User-Agent header. Override this if Trading 212 blocks the "
            "default client identity."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = DEMO_BASE_URL if args.demo else args.base_url
    client = Trading212Client(
        base_url,
        api_key=args.api_key,
        api_secret=args.api_secret,
        timeout=args.timeout,
        user_agent=args.user_agent,
    )

    try:
        started = time.monotonic()
        assets, net_worth = load_assets(
            client,
            account_id_value=args.account_id,
            include_metadata=not args.skip_metadata,
        )
        print_assets(assets, net_worth)
        print(f"\nFetched in {time.monotonic() - started:.1f}s")
        return 0
    except Trading212Error as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
