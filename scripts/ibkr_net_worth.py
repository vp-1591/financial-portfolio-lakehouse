#!/usr/bin/env python3
"""Print IBKR portfolio assets as percentages of account net worth.

This script uses the IBKR Flex Web Service API to fetch positions and account
data. It requires a Flex Query (Activity Flex Query with Open Positions and
Account Information sections) and a Flex Web Service token. No local gateway
or browser login is needed.

The data has a 15–30 minute delay compared to real-time positions from the
Client Portal Gateway.
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any


DEFAULT_FLEX_BASE_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
DEFAULT_QUERY_ID = "1554188"


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
    security_currency: str = ""


class IbkrFlexClient:
    """Client for the IBKR Flex Web Service API.

    The Flex Web Service uses a two-step process:
    1. Send a request to generate a report, receiving a reference code.
    2. Poll for the report using the reference code until it is ready.
    """

    def __init__(
        self,
        token: str,
        query_id: str = DEFAULT_QUERY_ID,
        base_url: str = DEFAULT_FLEX_BASE_URL,
        timeout: float = 30.0,
    ) -> None:
        self.token = token
        self.query_id = query_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, path: str, params: dict[str, str]) -> str:
        """Make an HTTP GET request and return the response body as text."""
        query_string = urllib.parse.urlencode(params)
        url = f"{self.base_url}/{path}?{query_string}"
        req = urllib.request.Request(url, headers={"Accept": "application/xml"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise IbkrHttpError("GET", url, exc.code, details) from exc
        except urllib.error.URLError as exc:
            raise IbkrError(
                f"GET {url} failed: {exc.reason}. Check your network connection "
                "and the Flex Web Service base URL."
            ) from exc

    def request_report(self) -> str:
        """Submit a Flex Query request and return the reference code.

        Raises IbkrError if the response indicates an error (e.g. invalid token
        or query ID).
        """
        params = {"t": self.token, "q": self.query_id, "v": "3"}
        body = self._request("SendRequest", params)
        root = ET.fromstring(body)

        status = root.findtext("Status")
        if status and status.strip().upper() != "SUCCESS":
            error_msg = root.findtext("ErrorMessage") or root.findtext("Message") or ""
            error_code = root.findtext("ErrorCode") or ""
            raise IbkrError(
                f"Flex request failed (status={status}, code={error_code}): {error_msg}".strip()
            )

        ref_code = root.findtext("ReferenceCode")
        if not ref_code:
            raise IbkrError(
                f"Flex request returned no reference code. Response: {body[:500]}"
            )
        return ref_code

    def fetch_report(
        self,
        reference_code: str,
        retries: int = 6,
        delay: float = 3.0,
    ) -> ET.Element:
        """Poll for a Flex report until it is ready.

        Reports take a few seconds to generate. This method retries up to
        *retries* times, waiting *delay* seconds between attempts.

        Returns the root XML element of the FlexQueryResponse.
        """
        params = {"t": self.token, "q": reference_code, "v": "3"}
        last_error: str = ""

        for attempt in range(1, retries + 1):
            body = self._request("GetStatement", params)
            root = ET.fromstring(body)

            # Successful report: root tag is FlexQueryResponse or
            # FlexStatementResponse with Status=Success/Warn
            tag = root.tag.lower() if root.tag else ""
            if "flexqueryresponse" in tag or "flexstatementresponse" in tag:
                status_elem = root.find("Status")
                if status_elem is not None and status_elem.text:
                    status_text = status_elem.text.strip().upper()
                    if status_text in ("SUCCESS", "WARN"):
                        return root
                    if status_text == "FAIL":
                        error_msg = (
                            root.findtext("ErrorMessage")
                            or root.findtext("Message")
                            or ""
                        )
                        error_code = root.findtext("ErrorCode") or ""
                        raise IbkrError(
                            f"Flex report generation failed (code={error_code}): "
                            f"{error_msg}".strip()
                        )
                    # Status might be "Processing" — treat as not ready
                    last_error = f"status={status_text}"
                else:
                    # If there are FlexStatements children, the data is ready
                    if root.find(".//FlexStatement") is not None:
                        return root
                    last_error = "no Status element and no FlexStatement found"

            # Error or still processing
            error_code = root.findtext("ErrorCode")
            if error_code:
                last_error = f"code={error_code}, message={root.findtext('ErrorMessage') or root.findtext('Message') or ''}"
                # Error code 1018 = not ready yet
                if error_code.strip() == "1018" and attempt < retries:
                    time.sleep(delay)
                    continue
                # Other error codes are fatal
                if error_code.strip() != "1018":
                    raise IbkrError(
                        f"Flex report error ({last_error}). Response: {body[:500]}"
                    )

            if attempt < retries:
                time.sleep(delay)

        raise IbkrError(
            f"Flex report not ready after {retries} retries ({delay}s each). "
            f"Last status: {last_error}"
        )


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_positions(root: ET.Element) -> list[dict[str, Any]]:
    """Parse <OpenPosition> elements from a Flex XML response."""
    positions: list[dict[str, Any]] = []
    for pos in root.iter("OpenPosition"):
        positions.append(dict(pos.attrib))
    return positions


def parse_account_info(root: ET.Element) -> list[dict[str, Any]]:
    """Parse <AccountInformation> elements from a Flex XML response.

    The XML has an outer <AccountInformation> section element (with no
    attributes) wrapping inner <AccountInformation> elements that contain
    the actual data. We skip the outer section element.
    """
    accounts: list[dict[str, Any]] = []
    for info in root.iter("AccountInformation"):
        if info.get("accountId"):
            accounts.append(dict(info.attrib))
    return accounts


def position_label(position: dict[str, Any]) -> str:
    for key in ("symbol", "description", "conid"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)
    return "UNKNOWN"


def position_conid(position: dict[str, Any]) -> str:
    value = position.get("conid")
    return str(value) if value not in (None, "") else ""


def position_isin(position: dict[str, Any]) -> str:
    value = position.get("isin")
    if value not in (None, ""):
        return str(value)
    return ""


def position_description(position: dict[str, Any]) -> str:
    for key in ("description", "symbol", "companyName"):
        value = position.get(key)
        if value not in (None, ""):
            return str(value)
    return position_label(position)


def load_assets(
    client: IbkrFlexClient,
    retries: int = 6,
    delay: float = 3.0,
) -> tuple[list[Asset], float]:
    """Fetch IBKR positions and cash via Flex Web Service and return assets with net worth."""
    ref_code = client.request_report()
    root = client.fetch_report(ref_code, retries=retries, delay=delay)

    positions = parse_positions(root)
    account_infos = parse_account_info(root)

    # Build a lookup of account_id -> net liquidation value and base currency
    # from AccountInformation elements
    net_liq_by_account: dict[str, float] = {}
    base_currency_by_account: dict[str, str] = {}
    cash_by_account_currency: dict[tuple[str, str], float] = {}

    for info in account_infos:
        acct_id = str(info.get("accountId", ""))
        nlv = as_float(info.get("netLiquidationValue"))
        if nlv:
            net_liq_by_account[acct_id] = nlv
        currency = str(info.get("currency", "") or "").upper()
        if currency and currency != "BASE":
            base_currency_by_account[acct_id] = currency
        elif not base_currency_by_account.get(acct_id):
            base_currency_by_account[acct_id] = "USD"
        # Track cash balance per account
        cash_balance = as_float(info.get("cashBalance"))
        if cash_balance:
            cash_currency = str(info.get("currency", currency) or currency)
            if cash_currency:
                key = (acct_id, cash_currency)
                cash_by_account_currency[key] = cash_by_account_currency.get(key, 0.0) + cash_balance

    # If no AccountInformation, try to infer base currency from positions' fxRateToBase
    assets: list[Asset] = []
    net_worth = 0.0

    # Process positions
    for pos in positions:
        acct_id = str(pos.get("accountId", ""))
        value = as_float(pos.get("positionValue"))
        if value == 0:
            # Fall back to quantity * markPrice
            quantity = as_float(pos.get("quantity"))
            mark_price = as_float(pos.get("markPrice"))
            value = quantity * mark_price
        if value == 0:
            continue

        currency = str(pos.get("currency", "") or "").upper()
        fx_rate = as_float(pos.get("fxRateToBase"), 1.0)
        base_currency = base_currency_by_account.get(acct_id, currency)

        # Convert to base currency
        if currency and currency != base_currency and fx_rate and fx_rate != 0:
            base_value = value * fx_rate
        else:
            base_value = value

        label = position_label(pos)
        asset_class = str(pos.get("assetClass", "") or "STK").upper()

        assets.append(Asset(
            account_id=acct_id,
            label=label,
            asset_class=asset_class,
            currency=base_currency,
            value=base_value,
            isin=position_isin(pos),
            conid=position_conid(pos),
            description=position_description(pos),
            security_currency=currency,
        ))

    # Process cash from AccountInformation
    for (acct_id, cash_currency), cash_balance in cash_by_account_currency.items():
        base_currency = base_currency_by_account.get(acct_id, cash_currency)
        fx_rate = 1.0
        # Check if we have fxRateToBase from positions for this currency
        for pos in positions:
            pos_acct = str(pos.get("accountId", ""))
            pos_currency = str(pos.get("currency", "") or "").upper()
            if pos_acct == acct_id and pos_currency == cash_currency:
                fx_rate = as_float(pos.get("fxRateToBase"), 1.0)
                break

        base_value = cash_balance * fx_rate if cash_currency != base_currency else cash_balance
        if base_value != 0:
            assets.append(Asset(
                account_id=acct_id,
                label=f"CASH {cash_currency}",
                asset_class="CASH",
                currency=base_currency,
                value=base_value,
            ))

    # Calculate net worth
    for acct_id, nlv in net_liq_by_account.items():
        net_worth += nlv

    # Fallback: sum asset values if no AccountInformation
    if net_worth == 0:
        net_worth = sum(asset.value for asset in assets)

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
        description="Print IBKR account positions and cash as net worth percentages using Flex Web Service."
    )
    parser.add_argument(
        "--ibkr-flex-token",
        required=True,
        help="IBKR Flex Web Service token (generated in Client Portal → Flex Queries).",
    )
    parser.add_argument(
        "--ibkr-flex-query-id",
        default=DEFAULT_QUERY_ID,
        help=f"IBKR Flex Query ID. Default: {DEFAULT_QUERY_ID}",
    )
    parser.add_argument(
        "--ibkr-flex-base-url",
        default=DEFAULT_FLEX_BASE_URL,
        help=f"Flex Web Service base URL. Default: {DEFAULT_FLEX_BASE_URL}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP request timeout in seconds. Default: 30",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=6,
        help="Number of retries when polling for the Flex report. Default: 6",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=3.0,
        help="Seconds to wait between retries. Default: 3",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = IbkrFlexClient(
        token=args.ibkr_flex_token,
        query_id=args.ibkr_flex_query_id,
        base_url=args.ibkr_flex_base_url,
        timeout=args.timeout,
    )

    try:
        started = time.monotonic()
        assets, net_worth = load_assets(client, retries=args.retries, delay=args.retry_delay)
        print_assets(assets, net_worth)
        print(f"\nFetched in {time.monotonic() - started:.1f}s")
        return 0
    except IbkrError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())