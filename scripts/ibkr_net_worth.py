#!/usr/bin/env python3
"""Print IBKR portfolio assets as percentages of account net worth.

This script uses the IBKR Flex Web Service API to fetch positions and account
data. It requires a Flex Query (Activity Flex Query with Open Positions,
Account Information, and optionally Cash Report sections) and a Flex Web
Service token. No local gateway or browser login is needed.

When the Cash Report section is included in the Flex Query, per-currency
cash balances (endingCash) are used directly. Otherwise, it falls back to
AccountInformation.cashBalance if available.

The data has a 15–30 minute delay compared to real-time positions from the
Client Portal Gateway.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

# ISO 4217 currency codes are exactly 3 uppercase letters.
_IS_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


DEFAULT_FLEX_BASE_URL = (
    "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
)
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


def parse_cash_report(root: ET.Element) -> list[dict[str, Any]]:
    """Parse cash report entries from a Flex XML response.

    IBKR uses <CashReportCurrency> elements inside the <CashReport> section.
    Each element represents a currency row with fields like endingCash,
    startingCash, and currency. The Cash Report section must be included
    in the Flex Query configuration for these elements to appear.

    Summary rows (e.g. currency="BASE SUMMARY") are excluded — they are
    subtotals that would double-count per-currency entries.

    Returns a list of attribute dicts, one per per-currency <CashReportCurrency>.
    """
    entries: list[dict[str, Any]] = []
    for cr in root.iter("CashReportCurrency"):
        attribs = dict(cr.attrib)
        currency = str(attribs.get("currency", "") or "").upper()
        # Skip summary/total rows: IBKR includes rows like "BASE SUMMARY",
        # "Total", etc. that are subtotals, not actual currency balances.
        if currency and not _IS_CURRENCY_RE.match(currency):
            continue
        entries.append(attribs)
    return entries


def parse_conversion_rates(root: ET.Element) -> dict[str, float]:
    """Parse <ConversionRate> elements from a Flex XML response.

    Returns a dict mapping from currency code to the conversion rate
    (e.g. {"EUR": 1.1, "USD": 1.0}) where the rate converts from that
    currency to the account's base currency.
    """
    rates: dict[str, float] = {}
    for cr in root.iter("ConversionRate"):
        from_ccy = str(cr.get("fromCurrency", "") or "").upper()
        rate = as_float(cr.get("rate"))
        if from_ccy and rate:
            rates[from_ccy] = rate
    return rates


def infer_base_currency_from_rates(
    conversion_rates: dict[str, float],
    account_ids: set[str],
) -> dict[str, str]:
    """Infer the base currency from ConversionRate entries.

    The base currency has a rate of exactly 1.0 (or is absent from the
    rates dict, meaning it converts 1:1 to itself). If no rate equals 1.0,
    falls back to the most common currency across positions and cash entries.
    Returns a dict mapping account_id -> base_currency.
    """
    # Find the currency with rate=1.0 — that's the base
    for ccy, rate in conversion_rates.items():
        if rate == 1.0:
            return {acct_id: ccy for acct_id in account_ids}

    # No rate=1.0 found — can't determine base currency
    return {}


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
    root: ET.Element | None = None,
) -> tuple[list[Asset], float]:
    """Fetch IBKR positions and cash via Flex Web Service and return assets with net worth.

    If *root* is provided (a pre-parsed XML element), it is used directly instead of
    making a new Flex request. This avoids duplicate requests when the caller has
    already fetched the report (e.g. for debug inspection).
    """
    if root is None:
        ref_code = client.request_report()
        root = client.fetch_report(ref_code, retries=retries, delay=delay)

    positions = parse_positions(root)
    account_infos = parse_account_info(root)
    cash_report_entries = parse_cash_report(root)
    conversion_rates = parse_conversion_rates(root)

    # Build a lookup of account_id -> base currency from AccountInformation elements
    base_currency_by_account: dict[str, str] = {}

    for info in account_infos:
        acct_id = str(info.get("accountId", ""))
        currency = str(info.get("currency", "") or "").upper()
        if currency and currency != "BASE":
            base_currency_by_account[acct_id] = currency
        elif not base_currency_by_account.get(acct_id):
            base_currency_by_account[acct_id] = "USD"

    # If no AccountInformation, try to infer base currency from positions' fxRateToBase
    assets: list[Asset] = []
    net_worth = 0.0

    # Build FX rate lookup from OpenPositions and ConversionRates.
    # The Cash Report section does not include fxRateToBase, so we reuse rates
    # from position data and the <ConversionRate> section for the same currencies.
    fx_rate_lookup: dict[tuple[str, str], float] = {}
    for pos in positions:
        pos_acct = str(pos.get("accountId", ""))
        pos_currency = str(pos.get("currency", "") or "").upper()
        if pos_acct and pos_currency:
            fx_rate_lookup[(pos_acct, pos_currency)] = as_float(
                pos.get("fxRateToBase"), 1.0
            )

    # Supplement with global conversion rates (currency -> rate) from <ConversionRate>.
    # These apply across all accounts and cover currencies not in OpenPositions.
    for acct_id in base_currency_by_account:
        for ccy, rate in conversion_rates.items():
            key = (acct_id, ccy)
            if key not in fx_rate_lookup:
                fx_rate_lookup[key] = rate

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

        assets.append(
            Asset(
                account_id=acct_id,
                label=label,
                asset_class=asset_class,
                currency=base_currency,
                value=base_value,
                isin=position_isin(pos),
                conid=position_conid(pos),
                description=position_description(pos),
                security_currency=currency,
            )
        )

    # --- Cash processing (3 sources in priority order) ---
    #
    # 1. Cash Report (per-currency endingCash) — most precise, gives each
    #    currency's cash balance individually (e.g. CASH EUR 500, CASH USD 200).
    #    Requires the "Cash Report" section in the Flex Query configuration.
    #
    # 2. AccountInformation.cashBalance — single-currency field from the
    #    Account Information section. Less precise: only the base-currency
    #    balance.
    #
    # 3. Derived from NLV minus positions — fallback when neither Cash Report
    #    nor cashBalance are present. Produces a single CASH entry in base
    #    currency per account.

    cash_from_report = False  # Whether we got cash from Cash Report

    # Source 1: Cash Report (per-currency endingCash)
    if cash_report_entries:
        for entry in cash_report_entries:
            acct_id = str(entry.get("accountId", ""))
            currency = str(entry.get("currency", "") or "").upper()
            ending_cash = as_float(entry.get("endingCash"))
            if ending_cash == 0:
                # Also try startingCash if endingCash is zero/missing
                ending_cash = as_float(entry.get("startingCash"))

            if not currency or ending_cash == 0:
                continue

            base_currency = base_currency_by_account.get(acct_id, currency)
            # CashReport doesn't include fxRateToBase — use the rate from
            # OpenPositions for the same account+currency, or from ConversionRates.
            fx_rate = fx_rate_lookup.get((acct_id, currency))
            if fx_rate is None:
                # No FX rate found — check if currency IS the base currency
                # (no conversion needed) or if this is a missing-rate situation.
                if currency != base_currency:
                    print(
                        f"Warning: No FX rate for {currency}→{base_currency}; "
                        f"treating {ending_cash:,.2f} {currency} as {base_currency}. "
                        f"Add OpenPositions or ConversionRates data for {currency}.",
                        file=sys.stderr,
                    )
                fx_rate = 1.0

            # Convert to base currency
            if currency != base_currency and fx_rate and fx_rate != 0:
                base_value = ending_cash * fx_rate
            else:
                base_value = ending_cash

            if base_value != 0:
                assets.append(
                    Asset(
                        account_id=acct_id,
                        label=f"CASH {currency}",
                        asset_class="CASH",
                        currency=base_currency,
                        value=base_value,
                        security_currency=currency,
                    )
                )
                cash_from_report = True

    # Source 2: AccountInformation.cashBalance
    if not cash_from_report:
        cash_by_account_currency: dict[tuple[str, str], float] = {}
        for info in account_infos:
            acct_id = str(info.get("accountId", ""))
            cash_balance = as_float(info.get("cashBalance"))
            if cash_balance:
                currency = str(info.get("currency", "") or "").upper()
                if currency:
                    key = (acct_id, currency)
                    cash_by_account_currency[key] = (
                        cash_by_account_currency.get(key, 0.0) + cash_balance
                    )

        for (acct_id, cash_currency), cash_balance in cash_by_account_currency.items():
            base_currency = base_currency_by_account.get(acct_id, cash_currency)
            fx_rate = fx_rate_lookup.get((acct_id, cash_currency), 1.0)

            base_value = (
                cash_balance * fx_rate
                if cash_currency != base_currency
                else cash_balance
            )
            if base_value != 0:
                assets.append(
                    Asset(
                        account_id=acct_id,
                        label=f"CASH {cash_currency}",
                        asset_class="CASH",
                        currency=base_currency,
                        value=base_value,
                        security_currency=cash_currency,
                    )
                )

    # Calculate net worth by summing all asset values
    net_worth = sum(asset.value for asset in assets)

    return assets, net_worth


def print_assets(assets: list[Asset], net_worth: float) -> None:
    if net_worth == 0:
        raise IbkrError("Net worth is zero; cannot calculate percentages.")

    rows = sorted(assets, key=lambda asset: abs(asset.value), reverse=True)
    print(f"Net worth: {net_worth:,.2f}")
    print()
    print(
        f"{'Account':<14} {'Asset':<24} {'Class':<8} {'Currency':<8} {'Value':>16} {'Net Worth %':>12}"
    )
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


def dump_xml_structure(root: ET.Element, max_depth: int = 5) -> None:
    """Print a summary of the XML structure for debugging."""
    print("=== XML Structure Debug ===")

    def _walk(elem: ET.Element, depth: int) -> None:
        if depth > max_depth:
            return
        attrs = dict(elem.attrib)
        for k, v in attrs.items():
            if len(v) > 60:
                attrs[k] = v[:57] + "..."
        if attrs:
            attr_str = " " + " ".join(f'{k}="{v}"' for k, v in attrs.items())
        else:
            attr_str = ""
        children = list(elem)
        child_tags = [c.tag for c in children]
        indent = "  " * depth
        child_info = (
            f"  [{len(children)} children: {', '.join(child_tags[:5])}]"
            if children
            else ""
        )
        print(f"{indent}<{elem.tag}{attr_str}>{child_info}")
        for child in children:
            _walk(child, depth + 1)

    _walk(root, 0)
    print("=== End XML Structure ===\n")


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
    parser.add_argument(
        "--debug-xml",
        action="store_true",
        help="Print the XML structure from the Flex response for debugging.",
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
        ref_code = client.request_report()
        root = client.fetch_report(
            ref_code, retries=args.retries, delay=args.retry_delay
        )

        if args.debug_xml:
            dump_xml_structure(root)

        assets, net_worth = load_assets(
            client, retries=args.retries, delay=args.retry_delay, root=root
        )
        print_assets(assets, net_worth)
        print(f"\nFetched in {time.monotonic() - started:.1f}s")
        return 0
    except IbkrError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
