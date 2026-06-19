"""IBKR API client — moved from scripts/ibkr_net_worth.py.

This module preserves the original client logic and adds raw response
interception for pipeline ingestion.
"""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class IbkrError(RuntimeError):
    pass


class IbkrHttpError(IbkrError):
    def __init__(self, method: str, url: str, code: int, details: str) -> None:
        self.method = method
        self.url = url
        self.code = code
        self.details = details
        super().__init__(f"{method} {url} failed: HTTP {code} {details}")


class IbkrClient:
    """HTTP client for the IBKR Client Portal Web API.

    Parameters
    ----------
    base_url:
        API base URL, e.g. ``https://localhost:5000/v1/api``.
    verify_tls:
        Whether to verify the gateway TLS certificate.
    timeout:
        HTTP request timeout in seconds.
    capture_raw:
        When *True*, every successful ``request()`` call appends the
        raw response bytes to :attr:`captured_responses` as ``(path,
        raw_bytes)`` tuples.  This enables the pipeline connector to
        store exact API payloads without re-fetching.
    """

    def __init__(
        self,
        base_url: str,
        verify_tls: bool = False,
        timeout: float = 20.0,
        capture_raw: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.ssl_context = None if verify_tls else ssl._create_unverified_context()
        self.capture_raw = capture_raw
        self.captured_responses: list[tuple[str, bytes]] = []

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        """Make an HTTP request and return the parsed JSON response."""
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
                raw_bytes = response.read()
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise IbkrHttpError(method, url, exc.code, details) from exc
        except urllib.error.URLError as exc:
            raise IbkrError(
                f"{method} {url} failed: {exc.reason}. Is the IBKR Client Portal "
                "Gateway running and authenticated?"
            ) from exc

        if self.capture_raw:
            self.captured_responses.append((path, raw_bytes))

        raw = raw_bytes.decode("utf-8")
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


# --- Parsing helpers (preserved from scripts/ibkr_net_worth.py) ---

DEFAULT_BASE_URL = "https://localhost:5000/v1/api"


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
        for currency, entry in ledger.items()
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


def cash_assets(account_id_value: str, ledger: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a list of cash asset dicts from the IBKR ledger."""
    assets: list[dict[str, Any]] = []
    rates = exchange_rates(ledger)
    for currency, entry in ledger.items():
        if currency == "BASE" or not isinstance(entry, dict):
            continue
        cash_balance = as_float(entry.get("cashbalance"))
        currency_code = str(entry.get("currency") or currency)
        if cash_balance == 0:
            continue
        assets.append({
            "account_id": account_id_value,
            "label": f"CASH {currency_code}",
            "asset_class": "CASH",
            "currency": currency_code,
            "value": to_base_currency(cash_balance, currency_code, rates),
            "isin": "",
            "conid": "",
            "description": f"Cash {currency_code}",
        })
    return assets