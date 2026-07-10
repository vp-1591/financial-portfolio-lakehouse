"""Trading 212 API client.

This module provides the Trading 212 API client with raw response
interception for pipeline ingestion, plus CDC endpoint methods.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
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
                "Verify your API credentials and network access."
            )
        else:
            reason = concise_details(details)
            message = f"{method} {url} failed: HTTP {code}"
            if reason:
                message = f"{message} {reason}"
        super().__init__(message)


class Trading212Client:
    """HTTP client for the Trading 212 public API.

    Parameters
    ----------
    base_url:
        API base URL.
    api_key:
        Trading 212 API key.
    api_secret:
        Trading 212 API secret (used for HTTP Basic Authentication).
    timeout:
        HTTP request timeout in seconds.
    capture_raw:
        When *True*, every successful ``request()`` call appends the
        raw response bytes to :attr:`captured_responses`.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str = "",
        timeout: float = 20.0,
        capture_raw: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip() if api_secret else ""
        self.timeout = timeout
        self.capture_raw = capture_raw
        self.captured_responses: list[tuple[str, bytes]] = []

    def request(self, method: str, path: str) -> Any:
        """Make an HTTP request and return the parsed JSON response."""
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": basic_auth_header(self.api_key, self.api_secret),
                "User-Agent": DEFAULT_USER_AGENT,
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                raw_bytes = response.read()
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise Trading212HttpError(method, url, exc.code, details) from exc
        except urllib.error.URLError as exc:
            raise Trading212Error(f"{method} {url} failed: {exc.reason}") from exc

        if self.capture_raw:
            self.captured_responses.append((path, raw_bytes))

        raw = raw_bytes.decode("utf-8")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            if is_access_denied_html(raw):
                raise Trading212Error(
                    f"{method} {url} returned an access denied page from Trading 212. "
                    "Verify your API credentials and network access."
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

    # --- CDC (historical) endpoints ---

    def _fetch_paginated(self, path: str) -> list[dict[str, Any]]:
        """Fetch all pages from a paginated T212 API endpoint.

        The T212 API returns either a bare JSON list (legacy/unpaginated)
        or a dict with ``{"items": [...], "nextPagePath": "..."}``. This
        method handles both formats and follows ``nextPagePath`` links
        until no more pages exist.
        """
        all_items: list[dict[str, Any]] = []
        current_path = path

        while current_path:
            result = self.request("GET", current_path)

            if isinstance(result, list):
                # Legacy/unpaginated response: bare list of items
                return result

            if isinstance(result, dict):
                items = result.get("items")
                if not isinstance(items, list):
                    raise Trading212Error(
                        f"Paginated response from {current_path} missing 'items' list."
                    )
                all_items.extend(items)
                next_page = result.get("nextPagePath")
                current_path = str(next_page) if next_page else ""
            else:
                raise Trading212Error(
                    f"Unexpected response type from {current_path}: {type(result).__name__}"
                )

        return all_items

    def orders(self) -> list[dict[str, Any]]:
        """Fetch all historical orders, following pagination."""
        return self._fetch_paginated("/equity/history/orders")

    def dividends(self) -> list[dict[str, Any]]:
        """Fetch all historical dividends, following pagination."""
        return self._fetch_paginated("/equity/history/dividends")

    def transactions(self) -> list[dict[str, Any]]:
        """Fetch all historical transactions, following pagination."""
        return self._fetch_paginated("/equity/history/transactions")


# --- Parsing helpers ---


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
    """Return a Basic authorization header for the Trading 212 API.

    Trading 212 API v0 requires HTTP Basic Authentication where the
    API Key is the username and the API Secret is the password.
    """
    credentials = f"{api_key.strip()}:{api_secret.strip()}"
    encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
    return f"Basic {encoded}"


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
    """Extract the available cash balance from a T212 account summary.

    The live API returns ``cash`` as a scalar float.
    The demo API returns ``cash`` as a nested dict like
    ``{"availableToTrade": 10500.0, "reservedForOrders": 0, "inPies": 0}``.

    When ``cash`` is a dict, drills into ``availableToTrade`` to get the
    numeric amount.  Returns 0.0 when ``cash`` is absent.
    """
    raw = summary.get("cash")
    if raw is None:
        return 0.0
    if isinstance(raw, dict):
        inner = raw.get("availableToTrade")
        if inner is not None:
            return as_float(inner)
        return 0.0
    return as_float(raw)


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


def instrument_isin_by_ticker(instruments: list[dict[str, Any]]) -> dict[str, str]:
    isins: dict[str, str] = {}
    for instrument in instruments:
        ticker = first_value(instrument, ("ticker",))
        isin = first_value(instrument, ("isin",))
        if ticker and isin:
            isins[str(ticker)] = str(isin)
    return isins


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


def position_isin(position: dict[str, Any], instrument_isins: dict[str, str]) -> str:
    instrument = nested_dict(position, "instrument")
    value = first_value(instrument, ("isin",))
    if value:
        return str(value)

    ticker = first_value(instrument, ("ticker",))
    if ticker and str(ticker) in instrument_isins:
        return instrument_isins[str(ticker)]

    ticker = first_value(position, ("ticker",))
    if ticker and str(ticker) in instrument_isins:
        return instrument_isins[str(ticker)]

    value = first_value(position, ("isin",))
    return str(value) if value else ""


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


def position_security_currency(
    position: dict[str, Any],
    instrument_currencies: dict[str, str],
    fallback: str,
) -> str:
    instrument = nested_dict(position, "instrument")
    currency = first_value(instrument, ("currencyCode", "currency"))
    if currency:
        return str(currency)

    ticker = first_value(instrument, ("ticker",))
    if ticker and str(ticker) in instrument_currencies:
        return instrument_currencies[str(ticker)]

    currency = first_value(position, ("currencyCode", "currency"))
    if currency:
        return str(currency)

    ticker = first_value(position, ("ticker",))
    if ticker and str(ticker) in instrument_currencies:
        return instrument_currencies[str(ticker)]

    return fallback
