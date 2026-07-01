"""IBKR API client for the Flex Web Service.

This module provides the ``IbkrFlexClient`` for fetching data via the IBKR
Flex Web Service API, plus parsing helpers for Flex XML responses.

Previously, this module also contained the Client Portal Gateway client
(``IbkrClient``) and associated gateway-only helpers. Those were removed
because the pipeline now exclusively uses the Flex Web Service API.
"""

from __future__ import annotations

import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

# ISO 4217 currency codes are exactly 3 uppercase letters.
_IS_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

DEFAULT_FLEX_BASE_URL = (
    "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
)


class IbkrError(RuntimeError):
    pass


class IbkrHttpError(IbkrError):
    def __init__(self, method: str, url: str, code: int, details: str) -> None:
        self.method = method
        self.url = url
        self.code = code
        self.details = details
        super().__init__(f"{method} {url} failed: HTTP {code} {details}")


class IbkrFlexClient:
    """Client for the IBKR Flex Web Service API.

    The Flex Web Service uses a two-step process:
    1. Send a request to generate a report, receiving a reference code.
    2. Poll for the report using the reference code until it is ready.
    """

    def __init__(
        self,
        token: str,
        query_id: str,
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


# --- Parsing helpers ---


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
