"""Unified CLI for the investment portfolio pipeline.

Usage::

    python -m pipeline.run fetch --ibkr [--xtb-file report.xlsx] [--t212-api-key KEY]
    python -m pipeline.run fetch --ibkr-flex-token TOKEN [--ibkr-flex-query-id ID]
    python -m pipeline.run transform
    python -m pipeline.run consolidate --target-currency EUR [--isin-map-file isins.csv]
    python -m pipeline.run allocate --target-currency EUR [--isin-map-file isins.csv]
    python -m pipeline.run full --ibkr [--xtb-file report.xlsx] [--t212-api-key KEY]
    python -m pipeline.run keygen
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.connectors.registry import all, get
from pipeline.crypto import generate_key, load_key
from pipeline.storage import get_storage


def add_ibkr_args(parser: argparse._ArgumentGroup) -> None:
    parser.add_argument("--ibkr", action="store_true", help="Enable IBKR connector (connects to the default Client Portal Gateway URL)")
    parser.add_argument("--ibkr-base-url", default="https://localhost:5000/v1/api")
    parser.add_argument("--ibkr-account", default=None)
    parser.add_argument("--ibkr-base-currency", default=None)
    parser.add_argument("--ibkr-verify-tls", action="store_true")
    parser.add_argument("--ibkr-skip-auth-check", action="store_true")
    parser.add_argument("--ibkr-require-brokerage-session", action="store_true")
    parser.add_argument("--ibkr-flex-token", default=None, help="Use Flex Web Service instead of Client Portal Gateway (no local gateway needed)")
    parser.add_argument("--ibkr-flex-query-id", default="1554188", help="Flex Query ID (default: 1554188)")
    parser.add_argument("--ibkr-flex-base-url", default="https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService", help="Flex Web Service base URL")


def add_trading212_args(parser: argparse._ArgumentGroup) -> None:
    parser.add_argument("--t212-api-key", default=None)
    parser.add_argument("--t212-api-secret", default=None)
    parser.add_argument("--t212-account-id", default=None)
    parser.add_argument("--t212-base-url", default=None)
    parser.add_argument("--t212-demo", action="store_true")
    parser.add_argument("--t212-skip-metadata", action="store_true")
    parser.add_argument("--t212-user-agent", default=None)


def add_xtb_args(parser: argparse._ArgumentGroup) -> None:
    parser.add_argument("--xtb-file", action="append", type=str, default=None)
    parser.add_argument("--xtb-account-id", default=None)


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


def cmd_keygen(args: argparse.Namespace) -> int:
    """Generate a Fernet encryption key."""
    config = get_storage()
    config.secrets_dir.mkdir(parents=True, exist_ok=True)
    if config.encryption_key_file.exists():
        print(f"Encryption key already exists at {config.encryption_key_file}")
        return 0
    key = generate_key()
    config.encryption_key_file.write_bytes(key)
    print(f"Encryption key written to {config.encryption_key_file}")
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    """Fetch data from brokers and write to raw Delta tables."""
    from pipeline.raw.ingest import ingest_raw, build_raw_table

    fernet_key = load_key()
    connectors = all()

    for connector in connectors:
        snapshot_kwargs: dict = {}
        cdc_kwargs: dict = {}
        if connector.name == "ibkr":
            if not args.ibkr and not args.ibkr_flex_token:
                print(f"Skipping {connector.display_name}: use --ibkr or --ibkr-flex-token to enable")
                continue
            snapshot_kwargs = cdc_kwargs = {
                "base_url": args.ibkr_base_url,
                "account": args.ibkr_account,
                "verify_tls": args.ibkr_verify_tls,
                "skip_auth_check": args.ibkr_skip_auth_check,
                "require_brokerage_session": args.ibkr_require_brokerage_session,
            }
            if args.ibkr_flex_token:
                snapshot_kwargs = {
                    "flex_token": args.ibkr_flex_token,
                    "flex_query_id": args.ibkr_flex_query_id,
                    "flex_base_url": args.ibkr_flex_base_url,
                }
                cdc_kwargs = {}
        elif connector.name == "trading212":
            if not args.t212_api_key:
                print(f"Skipping {connector.display_name}: no API key provided")
                continue
            base_url = "https://demo.trading212.com/api/v0" if args.t212_demo else (args.t212_base_url or "https://live.trading212.com/api/v0")
            common = {
                "api_key": args.t212_api_key,
                "api_secret": args.t212_api_secret or "",
                "account_id": args.t212_account_id or "",
                "base_url": base_url,
                "user_agent": args.t212_user_agent or "Mozilla/5.0",
            }
            snapshot_kwargs = {**common, "include_metadata": not args.t212_skip_metadata}
            cdc_kwargs = common
        elif connector.name == "xtb":
            if not args.xtb_file:
                print(f"Skipping {connector.display_name}: no --xtb-file provided")
                continue
            for xtb_file in args.xtb_file:
                xtb_kwargs = {
                    "file_path": xtb_file,
                    "account_id": args.xtb_account_id,
                }
                try:
                    raw = connector.fetch_snapshot(**xtb_kwargs)
                    table_path = str(get_raw_path(connector.name, "snapshot"))
                    Path(table_path).parent.mkdir(parents=True, exist_ok=True)
                    count = ingest_raw(raw, table_path, fernet_key)
                    print(f"  {connector.display_name} snapshot: {count} rows written")
                except Exception as exc:
                    print(f"  Error fetching {connector.display_name} snapshot: {exc}", file=sys.stderr)
            continue

        try:
            raw = connector.fetch_snapshot(**snapshot_kwargs)
            table_path = str(get_raw_path(connector.name, "snapshot"))
            Path(table_path).parent.mkdir(parents=True, exist_ok=True)
            count = ingest_raw(raw, table_path, fernet_key)
            print(f"  {connector.display_name} snapshot: {count} rows written")
        except NotImplementedError:
            print(f"  {connector.display_name} snapshot: not implemented")
        except Exception as exc:
            print(f"  Error fetching {connector.display_name} snapshot: {exc}", file=sys.stderr)

        # Try CDC
        try:
            raw_cdc = connector.fetch_cdc(**cdc_kwargs)
            cdc_path = str(get_raw_path(connector.name, "cdc"))
            Path(cdc_path).parent.mkdir(parents=True, exist_ok=True)
            count = ingest_raw(raw_cdc, cdc_path, fernet_key)
            print(f"  {connector.display_name} CDC: {count} rows written")
        except NotImplementedError:
            print(f"  {connector.display_name} CDC: not implemented")
        except Exception as exc:
            print(f"  Error fetching {connector.display_name} CDC: {exc}", file=sys.stderr)

    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    """Transform raw data into normalized Delta tables."""
    from deltalake import DeltaTable

    from pipeline.crypto import load_key
    from pipeline.raw.ingest import encrypt_raw_payloads

    fernet_key = load_key()
    connectors_list = all()

    for connector in connectors_list:
        for layer in ("snapshot", "cdc"):
            raw_path = get_raw_path(connector.name, layer)
            try:
                dt = DeltaTable(str(raw_path))
            except Exception:
                continue

            raw_table = dt.to_pyarrow_table()
            if raw_table.num_rows == 0:
                continue

            try:
                if layer == "snapshot":
                    normalized = connector.transform_snapshot(raw_table, fernet_key)
                else:
                    normalized = connector.transform_cdc(raw_table, fernet_key)

                from pathlib import Path
                config = get_storage()
                norm_path = config.normalized_path(f"{connector.name}_{layer}")
                Path(norm_path).parent.mkdir(parents=True, exist_ok=True)
                from deltalake import write_deltalake
                if normalized.num_rows == 0:
                    print(f"  {connector.display_name} {layer}: no data to transform")
                    continue
                write_deltalake(norm_path, normalized, mode="overwrite")
                print(f"  {connector.display_name} {layer}: {normalized.num_rows} rows transformed")
            except NotImplementedError:
                print(f"  {connector.display_name} {layer} transform: not implemented")

    return 0


def cmd_consolidate(args: argparse.Namespace) -> int:
    """Consolidate normalized broker snapshots into the holdings table."""
    import csv
    from pathlib import Path

    from deltalake import DeltaTable

    from pipeline.crypto import load_key
    from pipeline.normalized.consolidate import (
        CurrencyConverter,
        Holding,
        consolidate_holdings,
    )
    from pipeline.normalized.extract import extract_holdings

    fernet_key = load_key()

    # Load ISIN overrides
    isin_overrides: dict[str, str] = {}
    if args.isin:
        isin_overrides.update(dict(args.isin))
    if args.isin_map_file:
        for map_file in args.isin_map_file:
            path = Path(map_file)
            if not path.exists():
                print(f"ISIN map file does not exist: {path}", file=sys.stderr)
                return 1
            with path.open(newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    ticker = (row.get("ticker") or row.get("Ticker") or "").strip()
                    isin = (row.get("isin") or row.get("ISIN") or "").strip().upper()
                    if ticker and isin:
                        isin_overrides[ticker] = isin

    # Manual FX rates
    manual_rates: dict[str, float] = {}
    if args.fx_rate:
        manual_rates.update(dict(args.fx_rate))

    target_currency = args.target_currency.upper()
    converter = CurrencyConverter(
        target_currency=target_currency,
        manual_rates=manual_rates,
    )

    all_holdings: list[Holding] = []
    connectors_list = all()

    for connector in connectors_list:
        config = get_storage()
        snapshot_path = Path(config.normalized_path(f"{connector.name}_snapshot"))
        try:
            DeltaTable(str(snapshot_path))
        except Exception:
            print(f"  Skipping {connector.display_name}: no normalized snapshot data")
            continue

        holdings = extract_holdings(connector.name, str(snapshot_path), fernet_key)
        print(f"  {connector.display_name}: {len(holdings)} holdings extracted")
        all_holdings.extend(holdings)

    if not all_holdings:
        print("No holdings found. Run the transform step first.", file=sys.stderr)
        return 1

    table = consolidate_holdings(
        holdings=all_holdings,
        fernet_key=fernet_key,
        converter=converter,
        isin_overrides=isin_overrides,
    )
    print(f"  Consolidated: {table.num_rows} rows written")
    return 0


def cmd_allocate(args: argparse.Namespace) -> int:
    """Calculate portfolio allocation from normalized data."""
    import csv
    from pathlib import Path

    from pipeline.analytics.allocation import allocate_percentages
    from pipeline.crypto import load_key

    fernet_key = load_key()

    # Load ISIN overrides
    isin_overrides: dict[str, str] = {}
    if args.isin:
        isin_overrides.update(dict(args.isin))
    if args.isin_map_file:
        for map_file in args.isin_map_file:
            path = Path(map_file)
            if not path.exists():
                print(f"ISIN map file does not exist: {path}", file=sys.stderr)
                return 1
            with path.open(newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    ticker = (row.get("ticker") or row.get("Ticker") or "").strip()
                    isin = (row.get("isin") or row.get("ISIN") or "").strip().upper()
                    if ticker and isin:
                        isin_overrides[ticker] = isin

    try:
        result = allocate_percentages(fernet_key=fernet_key)
        print_allocation(result)
        return 0
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def print_allocation(table: "pa.Table") -> None:
    """Print portfolio allocation table in the same format as the old CLI."""
    print(
        f"{'Ticker':<12} {'%':>8} {'Broker':<12} "
        f"{'Identifier':<20} {'Ccy':<4} {'Description':<40}"
    )
    print("-" * 104)
    for i in range(table.num_rows):
        ticker = table.column("ticker")[i].as_py()
        percentage = table.column("percentage")[i].as_py()
        broker = table.column("broker")[i].as_py()
        identifier = table.column("identifier")[i].as_py()
        security_currency = table.column("security_currency")[i].as_py()
        description = table.column("description")[i].as_py()
        print(
            f"{ticker[:12]:<12} "
            f"{percentage:>7.2f}% "
            f"{broker:<12} "
            f"{identifier[:20]:<20} "
            f"{security_currency[:4]:<4} "
            f"{description[:40]:<40}"
        )


def get_raw_path(connector_name: str, layer: str) -> "Path":
    """Get the raw data path for a connector and layer."""
    config = get_storage()
    return Path(config.raw_path(f"{connector_name}_{layer}"))


def cmd_full(args: argparse.Namespace) -> int:
    """Run the full pipeline: fetch → transform → consolidate → allocate."""
    result = cmd_fetch(args)
    if result != 0:
        return result
    result = cmd_transform(args)
    if result != 0:
        return result
    result = cmd_consolidate(args)
    if result != 0:
        return result
    return cmd_allocate(args)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Investment portfolio data pipeline"
    )
    parser.add_argument(
        "--env",
        choices=["prod", "dev"],
        default=None,
        help="Pipeline environment: 'prod' (default, data/) or 'dev' (data-dev/). "
        "Can also be set via PIPELINE_ENV env var.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # keygen
    subparsers.add_parser("keygen", help="Generate encryption key")

    # fetch
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from brokers")
    ibkr_group = fetch_parser.add_argument_group("IBKR")
    add_ibkr_args(ibkr_group)
    t212_group = fetch_parser.add_argument_group("Trading 212")
    add_trading212_args(t212_group)
    xtb_group = fetch_parser.add_argument_group("XTB")
    add_xtb_args(xtb_group)

    # transform
    subparsers.add_parser("transform", help="Transform raw data into normalized tables")

    # consolidate
    consolidate_parser = subparsers.add_parser("consolidate", help="Consolidate normalized snapshots into holdings table")
    consolidate_parser.add_argument("--target-currency", default="EUR")
    consolidate_parser.add_argument("--fx-rate", action="append", type=parse_fx_rate, default=[])
    consolidate_parser.add_argument("--isin", action="append", type=parse_isin_override, default=[])
    consolidate_parser.add_argument("--isin-map-file", action="append", type=str, default=[])

    # allocate
    allocate_parser = subparsers.add_parser("allocate", help="Calculate portfolio allocation")
    allocate_parser.add_argument("--target-currency", default="EUR")
    allocate_parser.add_argument("--fx-rate", action="append", type=parse_fx_rate, default=[])
    allocate_parser.add_argument("--isin", action="append", type=parse_isin_override, default=[])
    allocate_parser.add_argument("--isin-map-file", action="append", type=str, default=[])

    # full
    full_parser = subparsers.add_parser("full", help="Run full pipeline")
    ibkr_full = full_parser.add_argument_group("IBKR")
    add_ibkr_args(ibkr_full)
    t212_full = full_parser.add_argument_group("Trading 212")
    add_trading212_args(t212_full)
    xtb_full = full_parser.add_argument_group("XTB")
    add_xtb_args(xtb_full)
    full_parser.add_argument("--target-currency", default="EUR")
    full_parser.add_argument("--fx-rate", action="append", type=parse_fx_rate, default=[])
    full_parser.add_argument("--isin", action="append", type=parse_isin_override, default=[])
    full_parser.add_argument("--isin-map-file", action="append", type=str, default=[])

    args = parser.parse_args()

    # Resolve storage configuration before any path access
    from pipeline.storage import resolve_storage
    resolve_storage(args.env)

    commands = {
        "keygen": cmd_keygen,
        "fetch": cmd_fetch,
        "transform": cmd_transform,
        "consolidate": cmd_consolidate,
        "allocate": cmd_allocate,
        "full": cmd_full,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())