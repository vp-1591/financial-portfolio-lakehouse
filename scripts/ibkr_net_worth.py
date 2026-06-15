#!/usr/bin/env python3
"""Print IBKR portfolio assets as percentages of account net worth.

This script uses the Interactive Brokers Client Portal Web API through the
local Client Portal Gateway. Start the gateway and authenticate in the browser
before running it.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_URL = "https://localhost:5000/v1/api"


class IbkrError(RuntimeError):
    pass


class IbkrHttpError(IbkrError):
    def __init__(self, method: str, url: str, code: int, details: str) -> None:
        self.method = method
        self.url = url
        self.code = code
        self.details = details
        super().__init__(f"{method} {url} failed: HTTP {code} {details}")


@dataclass(frozen=True)
class Asset:
    account_id: str
    label: str
    asset_class: str
    currency: str
    value: float
    isin: str = ""
    conid: str = ""
    description: str = ""


class IbkrClient:
    def __init__(self, base_url: str, verify_tls: bool, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.ssl_context = None if verify_tls else ssl._create_unverified_context()

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                req, timeout=self.timeout, context=self.ssl_context
            ) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise IbkrHttpError(method, url, exc.code, details) from exc
        except urllib.error.URLError as exc:
            raise IbkrError(
                f"{method} {url} failed: {exc.reason}. Is the IBKR Client Portal "
                "Gateway running and authenticated?"
            ) from exc

        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise IbkrError(f"{method} {url} returned non-JSON response: {raw[:200]}") from exc

    def auth_status(self) -> dict[str, Any]:
        try:
            status = self.request("POST", "/iserver/auth/status", {})
        except IbkrHttpError as exc:
            if exc.code == 401:
                raise IbkrError(
                    "IBKR brokerage session is not authenticated. This endpoint "
                    "requires the single active brokerage session for your username; "
                    "log out of TWS/Client Portal/IBKR Mobile or approve resetting "
                    "the other session, then reauthenticate the gateway."
                ) from exc
            raise
        if not isinstance(status, dict):
            raise IbkrError("Unexpected authentication status response.")
        return status

    def sso_validate(self) -> dict[str, Any]:
        try:
            status = self.request("GET", "/sso/validate")
        except IbkrHttpError as exc:
            if exc.code == 401:
                raise IbkrError(
                    "IBKR gateway is not logged in. Open https://localhost:5000 "
                    "on this machine, complete login, and wait for the page to show "
                    "'Client login succeeds' before running the script."
                ) from exc
            raise
        if not isinstance(status, dict):
            raise IbkrError("Unexpected SSO validation response.")
        return status

    def accounts(self) -> list[dict[str, Any]]:
        accounts = self.request("GET", "/portfolio/accounts")
        if not isinstance(accounts, list):
            raise IbkrError("Unexpected accounts response.")
        return accounts

    def positions(self, account_id: str) -> list[dict[str, Any]]:
        path_account_id = urllib.parse.quote(account_id, safe="")
        positions = self.request(
            "GET", f"/portfolio2/{path_account_id}/positions?sort=position&direction=d"
        )
        if not isinstance(positions, list):
            raise IbkrError(f"Unexpected positions response for account {account_id}.")
        return positions

    def ledger(self, account_id: str) -> dict[str, Any]:
        path_account_id = urllib.parse.quote(account_id, safe="")
        ledger = self.request("GET", f"/portfolio/{path_account_id}/ledger")
        if not isinstance(ledger, dict):
            raise IbkrError(f"Unexpected ledger response for account {account_id}.")
        return ledger

    def contract_info(self, conid: object) -> dict[str, Any]:
        path_conid = urllib.parse.quote(str(conid), safe="")
        details = self.request("GET", f"/iserver/contract/{path_conid}/info")
        if not isinstance(details, dict):
            raise IbkrError(f"Unexpected contract info response for conid {conid}.")
        return details


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def account_id(account: dict[str, Any]) -> str:
    for key in ("accountId", "id", "accountVan"):
        value = account.get(key)
        if value:
            return str(value)
    raise IbkrError(f"Could not determine account id from account object: {account}")


def position_value(position: dict[str, Any]) -> float:
    return as_float(
        position.get("mktValue", position.get("marketValue", position.get("value")))
    )


def position_label(position: dict[str, Any]) -> str:
    for key in ("contractDesc", "description", "ticker", "symbol", "conid"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)
    return "UNKNOWN"


def position_conid(position: dict[str, Any]) -> str:
    value = position.get("conid")
    return str(value) if value not in (None, "") else ""


def first_value(data: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def position_description(
    position: dict[str, Any],
    contract_info: dict[str, Any] | None = None,
) -> str:
    contract_info = contract_info or {}
    return (
        first_value(
            contract_info,
            (
                "companyName",
                "companyHeader",
                "description",
                "contractDesc",
                "symbol",
            ),
        )
        or first_value(
            position,
            ("companyName", "description", "fullName", "contractDesc", "symbol"),
        )
        or position_label(position)
    )


def position_isin(position: dict[str, Any]) -> str:
    for key in ("isin", "ISIN", "securityId", "securityID"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)

    sec_id_type = str(position.get("secIdType") or position.get("secidType") or "")
    sec_id = position.get("secId") or position.get("secid")
    if sec_id_type.upper() == "ISIN" and sec_id not in (None, ""):
        return str(sec_id)

    return ""


def net_liquidation_value(ledger: dict[str, Any]) -> float:
    base = ledger.get("BASE")
    if isinstance(base, dict):
        value = as_float(base.get("netliquidationvalue"))
        if value:
            return value

    return sum(
        to_base_currency(
            as_float(entry.get("netliquidationvalue")),
            str(entry.get("currency") or currency),
            exchange_rates(ledger),
        )
        for entry in ledger.values()
        if isinstance(entry, dict)
    )


def exchange_rates(ledger: dict[str, Any]) -> dict[str, float]:
    rates: dict[str, float] = {}
    for currency, entry in ledger.items():
        if not isinstance(entry, dict):
            continue
        currency_code = str(entry.get("currency") or currency)
        rates[currency_code] = as_float(entry.get("exchangerate"), 1.0)
    return rates


def to_base_currency(value: float, currency: str, rates: dict[str, float]) -> float:
    rate = rates.get(currency, 1.0)
    if rate == 0:
        return value
    return value * rate


def cash_assets(account_id_value: str, ledger: dict[str, Any]) -> list[Asset]:
    assets: list[Asset] = []
    rates = exchange_rates(ledger)
    for currency, entry in ledger.items():
        if currency == "BASE" or not isinstance(entry, dict):
            continue
        cash_balance = as_float(entry.get("cashbalance"))
        currency_code = str(entry.get("currency") or currency)
        if cash_balance == 0:
            continue
        assets.append(
            Asset(
                account_id=account_id_value,
                label=f"CASH {currency_code}",
                asset_class="CASH",
                currency=currency_code,
                value=to_base_currency(cash_balance, currency_code, rates),
            )
        )
    return assets


def load_assets(client: IbkrClient, selected_account: str | None) -> tuple[list[Asset], float]:
    accounts = client.accounts()
    if selected_account:
        account_ids = [selected_account]
    else:
        account_ids = [account_id(account) for account in accounts]

    assets: list[Asset] = []
    net_worth = 0.0
    for account_id_value in account_ids:
        positions = client.positions(account_id_value)
        ledger = client.ledger(account_id_value)
        rates = exchange_rates(ledger)
        net_worth += net_liquidation_value(ledger)

        for position in positions:
            value = position_value(position)
            if value == 0:
                continue
            currency = str(position.get("currency") or "")
            assets.append(
                Asset(
                    account_id=account_id_value,
                    label=position_label(position),
                    asset_class=str(
                        position.get("assetClass")
                        or position.get("secType")
                        or "UNKNOWN"
                    ),
                    currency=currency,
                    value=to_base_currency(value, currency, rates),
                    isin=position_isin(position),
                    conid=position_conid(position),
                    description=position_description(position),
                )
            )
        assets.extend(cash_assets(account_id_value, ledger))

    return assets, net_worth


def print_assets(assets: list[Asset], net_worth: float) -> None:
    if net_worth == 0:
        raise IbkrError("Net worth is zero; cannot calculate percentages.")

    rows = sorted(assets, key=lambda asset: abs(asset.value), reverse=True)
    print(f"Net worth: {net_worth:,.2f}")
    print()
    print(f"{'Account':<14} {'Asset':<24} {'Class':<8} {'Currency':<8} {'Value':>16} {'Net Worth %':>12}")
    print("-" * 90)
    for asset in rows:
        percentage = asset.value / net_worth * 100
        print(
            f"{asset.account_id:<14} "
            f"{asset.label[:24]:<24} "
            f"{asset.asset_class[:8]:<8} "
            f"{asset.currency[:8]:<8} "
            f"{asset.value:>16,.2f} "
            f"{percentage:>11.2f}%"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print IBKR account positions and cash as net worth percentages."
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Client Portal Gateway API base URL. Default: {DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--account",
        help="Optional IBKR account id. By default all portfolio accounts are included.",
    )
    parser.add_argument(
        "--verify-tls",
        action="store_true",
        help="Verify gateway TLS certificate. Off by default for the local self-signed gateway.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP request timeout in seconds. Default: 20",
    )
    parser.add_argument(
        "--skip-auth-check",
        action="store_true",
        help="Skip gateway session validation before reading portfolio data.",
    )
    parser.add_argument(
        "--require-brokerage-session",
        action="store_true",
        help=(
            "Require /iserver/auth/status before reading portfolio data. "
            "This can conflict with TWS, Client Portal, or IBKR Mobile because "
            "IBKR allows one brokerage session per username."
        ),
    )
    return parser.parse_args()


def validate_gateway_session(client: IbkrClient, require_brokerage_session: bool) -> None:
    if require_brokerage_session:
        status = client.auth_status()
        if not status.get("authenticated"):
            message = status.get("message") or "not authenticated"
            raise IbkrError(
                f"IBKR brokerage session is not authenticated ({message}). "
                "Open the gateway in your browser and sign in first."
            )
        return

    status = client.sso_validate()
    if not status.get("RESULT"):
        raise IbkrError(
            "IBKR gateway SSO session is not valid. Open the gateway in your "
            "browser and sign in first."
        )


def main() -> int:
    args = parse_args()
    client = IbkrClient(args.base_url, verify_tls=args.verify_tls, timeout=args.timeout)

    try:
        if not args.skip_auth_check:
            validate_gateway_session(client, args.require_brokerage_session)

        started = time.monotonic()
        assets, net_worth = load_assets(client, args.account)
        print_assets(assets, net_worth)
        print(f"\nFetched in {time.monotonic() - started:.1f}s")
        return 0
    except IbkrError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
