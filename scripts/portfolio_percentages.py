#!/usr/bin/env python3
"""Print consolidated portfolio percentages by ticker and broker."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import portfolio_connectors as connectors
import trading212_net_worth as trading212
import ibkr_net_worth as ibkr


def parse_fx_rate(value: str) -> tuple[str, float]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("FX rates must use CURRENCY=RATE format.")
    currency, raw_rate = value.split("=", 1)
    currency = currency.strip().upper()
    if not currency:
        raise argparse.ArgumentTypeError("FX rate currency cannot be empty.")
    try:
        rate = float(raw_rate)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid FX rate: {raw_rate}") from exc
    if rate <= 0:
        raise argparse.ArgumentTypeError("FX rate must be greater than zero.")
    return currency, rate


def parse_isin_override(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("ISIN overrides must use TICKER=ISIN format.")
    ticker, isin = value.split("=", 1)
    ticker = ticker.strip()
    isin = isin.strip().upper()
    if not ticker:
        raise argparse.ArgumentTypeError("ISIN override ticker cannot be empty.")
    if not isin:
        raise argparse.ArgumentTypeError("ISIN override value cannot be empty.")
    return ticker, isin


def load_isin_map(path: Path) -> dict[str, str]:
    if not path.exists():
        raise connectors.PortfolioConnectorError(f"ISIN map file does not exist: {path}")

    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    isin_map: dict[str, str] = {}
    for row in rows:
        ticker = (row.get("ticker") or row.get("Ticker") or "").strip()
        isin = (row.get("isin") or row.get("ISIN") or "").strip().upper()
        if ticker and isin:
            isin_map[ticker] = isin
    return isin_map


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print consolidated ticker percentages across Trading 212, XTB, and IBKR."
    )
    parser.add_argument(
        "--target-currency",
        default="EUR",
        help="Currency used for consolidated percentage calculations. Default: EUR",
    )
    parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        metavar="CURRENCY=RATE",
        help=(
            "Manual FX rate where one CURRENCY unit equals RATE target-currency units. "
            "Can be passed multiple times."
        ),
    )
    parser.add_argument(
        "--fx-base-url",
        default=connectors.FRANKFURTER_BASE_URL,
        help=f"FX API base URL. Default: {connectors.FRANKFURTER_BASE_URL}",
    )
    parser.add_argument(
        "--yahoo-fx-base-url",
        default=connectors.YAHOO_FINANCE_BASE_URL,
        help=f"Yahoo FX API base URL. Default: {connectors.YAHOO_FINANCE_BASE_URL}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP request timeout in seconds. Default: 20",
    )
    parser.add_argument(
        "--isin",
        action="append",
        type=parse_isin_override,
        default=[],
        metavar="TICKER=ISIN",
        help=(
            "Fill a missing ISIN for a displayed ticker. Can be passed multiple "
            "times, for example --isin SXR8.DE=IE00B5BMR087."
        ),
    )
    parser.add_argument(
        "--isin-map-file",
        action="append",
        type=Path,
        default=[],
        help="CSV file with ticker and isin columns. Can be passed multiple times.",
    )

    parser.add_argument("--trading212-api-key", required=True)
    parser.add_argument("--trading212-api-secret", required=True)
    parser.add_argument("--trading212-account-id", required=True)
    parser.add_argument(
        "--trading212-base-url",
        default=trading212.DEFAULT_BASE_URL,
        help=f"Trading 212 API base URL. Default: {trading212.DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--trading212-demo",
        action="store_true",
        help=f"Use the Trading 212 demo API base URL: {trading212.DEMO_BASE_URL}",
    )
    parser.add_argument(
        "--trading212-skip-metadata",
        action="store_true",
        help="Skip Trading 212 instruments metadata lookup.",
    )
    parser.add_argument(
        "--trading212-user-agent",
        default=trading212.DEFAULT_USER_AGENT,
        help="Trading 212 HTTP User-Agent header.",
    )

    parser.add_argument(
        "--xtb-file",
        required=True,
        action="append",
        type=Path,
        help="Absolute path to an XTB .xlsx report. Can be passed multiple times.",
    )

    parser.add_argument(
        "--ibkr-base-url",
        default=ibkr.DEFAULT_BASE_URL,
        help=f"Client Portal Gateway API base URL. Default: {ibkr.DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--ibkr-account",
        help="Optional IBKR account id. By default all portfolio accounts are included.",
    )
    parser.add_argument(
        "--ibkr-base-currency",
        help=(
            "IBKR account base currency. Use this when the Client Portal ledger "
            "returns the placeholder currency BASE."
        ),
    )
    parser.add_argument(
        "--ibkr-verify-tls",
        action="store_true",
        help="Verify gateway TLS certificate.",
    )
    parser.add_argument(
        "--ibkr-skip-auth-check",
        action="store_true",
        help="Skip IBKR gateway session validation before reading portfolio data.",
    )
    parser.add_argument(
        "--ibkr-require-brokerage-session",
        action="store_true",
        help="Require /iserver/auth/status before reading IBKR portfolio data.",
    )
    return parser.parse_args()


def print_rows(rows: list[connectors.PortfolioRow]) -> None:
    print(
        f"{'Ticker':<12} {'%':>8} {'Broker':<12} "
        f"{'Identifier':<20} {'Ccy':<4} {'Description':<40}"
    )
    print("-" * 104)
    for row in rows:
        print(
            f"{row.ticker[:12]:<12} "
            f"{row.percentage:>7.2f}% "
            f"{row.broker:<12} "
            f"{row.identifier[:20]:<20} "
            f"{row.security_currency[:4]:<4} "
            f"{row.description[:40]:<40}"
        )


def main() -> int:
    args = parse_args()
    trading212_base_url = (
        trading212.DEMO_BASE_URL if args.trading212_demo else args.trading212_base_url
    )
    converter = connectors.CurrencyConverter(
        args.target_currency,
        manual_rates=dict(args.fx_rate),
        base_url=args.fx_base_url,
        yahoo_base_url=args.yahoo_fx_base_url,
        timeout=args.timeout,
    )

    try:
        isin_overrides = {}
        for isin_map_file in args.isin_map_file:
            isin_overrides.update(load_isin_map(isin_map_file))
        isin_overrides.update(dict(args.isin))

        holdings = []
        holdings.extend(
            connectors.load_trading212_holdings(
                api_key=args.trading212_api_key,
                api_secret=args.trading212_api_secret,
                account_id=args.trading212_account_id,
                base_url=trading212_base_url,
                timeout=args.timeout,
                user_agent=args.trading212_user_agent,
                include_metadata=not args.trading212_skip_metadata,
            )
        )
        holdings.extend(connectors.load_xtb_holdings(path.resolve() for path in args.xtb_file))
        holdings.extend(
            connectors.load_ibkr_holdings(
                base_url=args.ibkr_base_url,
                account=args.ibkr_account,
                verify_tls=args.ibkr_verify_tls,
                timeout=args.timeout,
                skip_auth_check=args.ibkr_skip_auth_check,
                require_brokerage_session=args.ibkr_require_brokerage_session,
                base_currency_override=args.ibkr_base_currency,
            )
        )
        print_rows(
            connectors.aggregate_percentages(
                holdings,
                converter,
                isin_overrides=isin_overrides,
            )
        )
        return 0
    except (
        connectors.PortfolioConnectorError,
        trading212.Trading212Error,
        ibkr.IbkrError,
    ) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
