"""Consolidate normalized snapshots into a unified holdings table.

This module replaces the logic in ``scripts/portfolio_connectors.py``
for currency conversion, ticker normalization, ISIN override, and
percentage aggregation, operating on normalized Delta tables instead
of live API calls.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.crypto import decrypt_float, decrypt_string, load_key
from pipeline.normalized.models import consolidated_holdings_schema


FRANKFURTER_BASE_URL = "https://api.frankfurter.app"
YAHOO_FINANCE_BASE_URL = "https://query1.finance.yahoo.com"
DEFAULT_USER_AGENT = "Mozilla/5.0 investment-portfolio-dashboard/2.0"


@dataclass(frozen=True)
class Holding:
    broker: str
    ticker: str
    currency: str
    value: float
    identifier: str = ""
    security_currency: str = ""
    description: str = ""


@dataclass(frozen=True)
class PortfolioRow:
    ticker: str
    percentage: float
    broker: str
    identifier: str
    security_currency: str
    description: str


class CurrencyConverter:
    def __init__(
        self,
        target_currency: str,
        manual_rates: dict[str, float] | None = None,
        base_url: str = FRANKFURTER_BASE_URL,
        yahoo_base_url: str = YAHOO_FINANCE_BASE_URL,
        timeout: float = 20.0,
    ) -> None:
        self.target_currency = target_currency.upper()
        self.manual_rates = {
            currency.upper(): rate for currency, rate in (manual_rates or {}).items()
        }
        self.base_url = base_url.rstrip("/")
        self.yahoo_base_url = yahoo_base_url.rstrip("/")
        self.timeout = timeout
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
        errors: list[str] = []
        for provider_name, fetcher in (
            ("Frankfurter", self.fetch_frankfurter_rate),
            ("Yahoo", self.fetch_yahoo_rate),
        ):
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
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": DEFAULT_USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise PortfolioConnectorError(f"HTTP {exc.code} {exc.reason}") from exc
        except urllib.error.URLError as exc:
            raise PortfolioConnectorError(str(exc.reason)) from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PortfolioConnectorError(f"non-JSON response: {raw[:200]}") from exc
        if not isinstance(parsed, dict):
            raise PortfolioConnectorError(f"unexpected response: {raw[:200]}")
        return parsed

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

    def fetch_yahoo_rate(self, source_currency: str) -> float:
        symbol = f"{source_currency}{self.target_currency}=X"
        encoded_symbol = urllib.parse.quote(symbol, safe="")
        url = f"{self.yahoo_base_url}/v8/finance/chart/{encoded_symbol}?range=1d&interval=1d"
        data = self.request_json(url)
        try:
            result = data["chart"]["result"][0]
            meta = result["meta"]
            rate = float(meta["regularMarketPrice"])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise PortfolioConnectorError(f"unexpected response: {data}") from exc

        if rate == 0:
            raise PortfolioConnectorError(
                f"FX rate {source_currency}->{self.target_currency} is zero."
            )
        return rate


class PortfolioConnectorError(RuntimeError):
    pass


def format_identifier(kind: str, value: str) -> str:
    return f"{kind}:{value}" if value else ""


def real_currency(value: object, fallback: str) -> str:
    currency = str(value or "").upper()
    if not currency or currency == "BASE":
        return fallback
    return currency


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


def normalize_isin_lookup_key(ticker: str) -> str:
    return ticker.strip().upper()


def aggregate_percentages(
    holdings: Iterable[Holding],
    converter: CurrencyConverter,
    isin_overrides: dict[str, str] | None = None,
) -> list[PortfolioRow]:
    normalized_isin_overrides = {
        normalize_isin_lookup_key(ticker): format_identifier("ISIN", isin)
        for ticker, isin in (isin_overrides or {}).items()
    }
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
        override_isin = normalized_isin_overrides.get(
            normalize_isin_lookup_key(holding.ticker),
            "",
        )
        metadata[key] = (
            current_identifier or holding.identifier or override_isin,
            current_currency or holding.security_currency,
            current_description or holding.description,
        )

    net_worth = sum(totals.values())
    if net_worth == 0:
        raise PortfolioConnectorError("Net worth is zero; cannot calculate percentages.")

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
    isin_overrides: dict[str, str] | None = None,
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
    isin_overrides:
        Manual ISIN overrides by ticker.
    table_path:
        Delta table path to write to. Defaults to
        ``NORMALIZED_CONSOLIDATED_HOLDINGS``.
    """
    from pipeline.crypto import encrypt_float

    if table_path is None:
        from pipeline.storage import get_storage
        table_path = str(get_storage().normalized_dir / "consolidated_holdings")

    from pathlib import Path
    Path(table_path).parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    normalized_isin_overrides = {
        normalize_isin_lookup_key(ticker): isin
        for ticker, isin in (isin_overrides or {}).items()
    }

    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    tickers: list[str] = []
    currencies: list[str] = []
    values: list[bytes] = []
    identifiers: list[str] = []
    security_currencies: list[str] = []
    descriptions: list[str] = []

    for holding in holdings:
        converted_value = converter.convert(holding.value, holding.currency)
        override_isin = normalized_isin_overrides.get(
            normalize_isin_lookup_key(holding.ticker), ""
        )
        identifier = holding.identifier or format_identifier("ISIN", override_isin) if override_isin else holding.identifier

        fetched_ats.append(now)
        brokers.append(holding.broker)
        tickers.append(holding.ticker)
        currencies.append(holding.security_currency or holding.currency)
        values.append(encrypt_float(converted_value, fernet_key))
        identifiers.append(identifier or "-")
        security_currencies.append(holding.security_currency or "-")
        descriptions.append(holding.description or "-")

    table = pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "ticker": tickers,
            "currency": currencies,
            "value": values,
            "identifier": identifiers,
            "security_currency": security_currencies,
            "description": descriptions,
        },
        schema=consolidated_holdings_schema,
    )

    write_deltalake(table_path, table, mode="overwrite")
    return table