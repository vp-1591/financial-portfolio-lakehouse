"""Consolidate normalized snapshots into a unified holdings table.

Provides currency conversion, ticker normalization, and percentage
aggregation, operating on normalized Delta tables.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

import pyarrow as pa
from deltalake import write_deltalake
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from pipeline.normalized.models import consolidated_holdings_schema

FRANKFURTER_BASE_URL = "https://api.frankfurter.app"
DEFAULT_USER_AGENT = "Mozilla/5.0 financial-portfolio-lakehouse/2.0"


@dataclass(frozen=True)
class Holding:
    broker: str
    ticker: str
    currency: str
    value: float
    identifier: str = ""
    security_currency: str = ""
    description: str = ""
    position_type: str = "EQUITY"  # EQUITY | CASH


@dataclass(frozen=True)
class PortfolioRow:
    ticker: str
    percentage: float
    broker: str
    identifier: str
    security_currency: str
    description: str


class CurrencyConverter:
    # Minor currency units: code -> (major_currency, sub_unit_factor).
    # GBX (British pence) = GBP / 100.  When the converter encounters a
    # minor unit, it fetches the major-unit rate and divides by the factor.
    # Unknown minor currencies are not mapped here — they fall through to
    # Frankfurter, which rejects them with a 400 error.
    MINOR_CURRENCY_UNITS: ClassVar[dict[str, tuple[str, int]]] = {
        "GBX": ("GBP", 100),
    }

    def __init__(
        self,
        target_currency: str,
        manual_rates: dict[str, float] | None = None,
        base_url: str = FRANKFURTER_BASE_URL,
        timeout: float = 20.0,
        retries: int = 2,
        retry_delay: float = 1.0,
    ) -> None:
        self.target_currency = target_currency.upper()
        self.manual_rates = {
            currency.upper(): rate for currency, rate in (manual_rates or {}).items()
        }
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self._rates: dict[str, float] = {self.target_currency: 1.0}
        self._rates.update(self.manual_rates)

    def convert(self, value: float, currency: str) -> float:
        source_currency = currency.upper()
        if not source_currency or source_currency == self.target_currency:
            return value

        rate = self._rates.get(source_currency)
        if rate is None:
            rate = self.fetch_rate(source_currency)
            self._rates[source_currency] = rate

        return value * rate

    def fetch_rate(self, source_currency: str) -> float:
        # Handle minor currency units (e.g., GBX -> GBP / 100)
        # Decision: docs/adr/0097-remove-yahoo-finance-fx-provider.md
        # (GBX handling via MINOR_CURRENCY_UNITS originally from ADR 0095)
        if source_currency in self.MINOR_CURRENCY_UNITS:
            major_currency, factor = self.MINOR_CURRENCY_UNITS[source_currency]
            major_rate = self._rates.get(major_currency)
            if major_rate is None:
                major_rate = self.fetch_rate(major_currency)
            rate = major_rate / factor
            self._rates[source_currency] = rate
            return rate

        errors: list[str] = []
        for provider_name, fetcher in (("Frankfurter", self.fetch_frankfurter_rate),):
            try:
                return fetcher(source_currency)
            except PortfolioConnectorError as exc:
                errors.append(f"{provider_name}: {exc}")

        details = "; ".join(errors)
        raise PortfolioConnectorError(
            f"Could not fetch FX rate {source_currency}->{self.target_currency}. "
            f"{details}. Pass --fx-rate {source_currency}=RATE to provide it."
        )

    def request_json(self, url: str) -> dict[str, object]:
        """Fetch a URL and return the parsed JSON response.

        Retries on transient errors (timeouts, network failures, 5xx server
        errors) using tenacity.  Client errors (4xx) and response-format
        errors (non-JSON, non-dict) are not retried.
        """
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": DEFAULT_USER_AGENT,
            },
        )

        # Decision: docs/adr/0098-add-retry-logic-to-currency-converter-request-json.md
        @retry(
            retry=retry_if_exception_type(TransientHttpError),
            stop=stop_after_attempt(1 + self.retries),
            wait=wait_fixed(self.retry_delay),
            reraise=True,
        )
        def _do_request() -> dict[str, object]:
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
            except urllib.error.HTTPError as exc:
                # 5xx server errors are transient; 4xx client errors are permanent.
                if exc.code >= 500:
                    raise TransientHttpError(f"HTTP {exc.code} {exc.reason}") from exc
                raise PortfolioConnectorError(f"HTTP {exc.code} {exc.reason}") from exc
            except urllib.error.URLError as exc:
                raise TransientHttpError(str(exc.reason)) from exc
            except TimeoutError as exc:
                raise TransientHttpError("The read operation timed out") from exc

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise PortfolioConnectorError(
                    f"non-JSON response: {raw[:200]}"
                ) from exc
            if not isinstance(parsed, dict):
                raise PortfolioConnectorError(f"unexpected response: {raw[:200]}")
            return parsed

        return _do_request()

    def fetch_frankfurter_rate(self, source_currency: str) -> float:
        query = urllib.parse.urlencode(
            {"from": source_currency, "to": self.target_currency}
        )
        url = f"{self.base_url}/latest?{query}"
        data = self.request_json(url)
        try:
            rate = float(data["rates"][self.target_currency])
        except (KeyError, TypeError, ValueError) as exc:
            raise PortfolioConnectorError(f"unexpected response: {data}") from exc

        if rate == 0:
            raise PortfolioConnectorError(
                f"FX rate {source_currency}->{self.target_currency} is zero."
            )
        return rate


class PortfolioConnectorError(RuntimeError):
    pass


class TransientHttpError(PortfolioConnectorError):
    """HTTP/network error that may succeed on retry (timeout, 5xx, DNS failure)."""


def normalize_trading212_ticker(ticker: str) -> str:
    if ticker.startswith("CASH "):
        return ticker

    removed_broker_suffix = False
    for suffix in ("_EQ", "_ETF"):
        if ticker.endswith(suffix):
            ticker = ticker[: -len(suffix)]
            removed_broker_suffix = True
            break

    market_suffix = re.fullmatch(r"(.+)_([A-Z]{2})", ticker)
    if market_suffix:
        return market_suffix.group(1)

    lowercase_exchange_suffix = re.fullmatch(r"(.+)[a-z]", ticker)
    if removed_broker_suffix and lowercase_exchange_suffix:
        return lowercase_exchange_suffix.group(1)

    return ticker


def aggregate_percentages(
    holdings: Iterable[Holding],
    converter: CurrencyConverter,
) -> list[PortfolioRow]:
    totals: dict[tuple[str, str], float] = {}
    metadata: dict[tuple[str, str], tuple[str, str, str]] = {}
    for holding in holdings:
        converted_value = converter.convert(holding.value, holding.currency)
        key = (holding.ticker, holding.broker)
        totals[key] = totals.get(key, 0.0) + converted_value
        current_identifier, current_currency, current_description = metadata.get(
            key,
            ("", "", ""),
        )
        metadata[key] = (
            current_identifier or holding.identifier,
            current_currency or holding.security_currency,
            current_description or holding.description,
        )

    net_worth = sum(totals.values())
    if net_worth == 0:
        raise PortfolioConnectorError(
            "Net worth is zero; cannot calculate percentages."
        )

    rows = [
        PortfolioRow(
            ticker=ticker,
            percentage=value / net_worth * 100,
            broker=broker,
            identifier=metadata.get((ticker, broker), ("", "", ""))[0] or "-",
            security_currency=metadata.get((ticker, broker), ("", "", ""))[1] or "-",
            description=metadata.get((ticker, broker), ("", "", ""))[2] or "-",
        )
        for (ticker, broker), value in totals.items()
        if value != 0
    ]
    return sorted(rows, key=lambda row: abs(row.percentage), reverse=True)


def consolidate_holdings(
    holdings: list[Holding],
    fernet_key: bytes,
    converter: CurrencyConverter,
    table_path: str | None = None,
) -> pa.Table:
    """Consolidate holdings into the normalized holdings Delta table.

    Parameters
    ----------
    holdings:
        List of normalized holdings from all brokers.
    fernet_key:
        Fernet key for encrypting value columns.
    converter:
        Currency converter for FX rate calculations.
    table_path:
        Delta table path to write to. Defaults to
        ``NORMALIZED_CONSOLIDATED_HOLDINGS``.
    """
    from pipeline.crypto import encrypt_float

    if table_path is None:
        from pipeline.storage import get_storage

        table_path = get_storage().normalized_path("consolidated_holdings")

    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options
    get_storage().backend.ensure_parent(table_path)

    now = datetime.now(UTC)

    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    tickers: list[str] = []
    security_values: list[bytes] = []  # encrypted native-currency value
    security_ccys: list[str] = []
    target_ccys: list[str] = []
    values: list[bytes] = []  # encrypted target_value
    identifiers: list[str] = []
    descriptions: list[str] = []
    position_types: list[str] = []

    for holding in holdings:
        converted_value = converter.convert(holding.value, holding.currency)
        # Decision: docs/adr/0109-remove-isin-override-cli-feature.md
        identifier = holding.identifier

        fetched_ats.append(now)
        brokers.append(holding.broker)
        tickers.append(holding.ticker)
        security_values.append(encrypt_float(holding.value, fernet_key))
        security_ccys.append(holding.security_currency or "-")
        target_ccys.append(converter.target_currency)
        values.append(encrypt_float(converted_value, fernet_key))
        identifiers.append(identifier or "-")
        descriptions.append(holding.description or "-")
        position_types.append(holding.position_type)

    table = pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "ticker": tickers,
            "security_value": security_values,
            "security_ccy": security_ccys,
            "target_ccy": target_ccys,
            "target_value": values,
            "identifier": identifiers,
            "description": descriptions,
            "position_type": position_types,
        },
        schema=consolidated_holdings_schema,
    )

    write_deltalake(table_path, table, mode="overwrite", storage_options=storage_opts)
    return table
