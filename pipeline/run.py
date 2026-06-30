"""Unified CLI for the investment portfolio pipeline.

Usage::

    python -m pipeline.run fetch
    python -m pipeline.run transform
    python -m pipeline.run consolidate --target-currency EUR
    python -m pipeline.run allocate --target-currency EUR
    python -m pipeline.run full
    python -m pipeline.run keygen

Connector enable/disable is controlled by environment variables:

- ``IBKR_ENABLED`` — set to ``0``, ``false``, or ``no`` to disable IBKR
- ``T212_ENABLED`` — set to ``0``, ``false``, or ``no`` to disable Trading 212
- ``XTB_ENABLED`` — set to ``0``, ``false``, or ``no`` to disable XTB

All connectors are **enabled by default**.  Secrets come from environment
variables (set by GitHub Actions via GitHub Secrets, or locally via ``.env``).
"""

from __future__ import annotations


import argparse
import os
import sys

from pipeline.connectors.registry import all
from pipeline.crypto import load_key
from pipeline.keygen import main as keygen_main
from pipeline.secrets import get_config, inject_secrets, is_enabled, parse_bool
from pipeline.storage import get_storage


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
    keygen_main()
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    """Fetch data from brokers and write to raw Delta tables."""
    from pipeline.raw.ingest import ingest_raw

    fernet_key = load_key()
    connectors = all()

    for connector in connectors:
        if connector.name == "ibkr" and not is_enabled("IBKR_ENABLED"):
            print(f"Skipping {connector.display_name}: IBKR_ENABLED is false")
            continue
        if connector.name == "trading212" and not is_enabled("T212_ENABLED"):
            print(f"Skipping {connector.display_name}: T212_ENABLED is false")
            continue
        if connector.name == "xtb" and not is_enabled("XTB_ENABLED"):
            print(f"Skipping {connector.display_name}: XTB_ENABLED is false")
            continue

        snapshot_kwargs: dict = {}
        cdc_kwargs: dict = {}

        if connector.name == "ibkr":
            ibkr_flex_token = os.environ.get("IBKR_FLEX_TOKEN")
            if ibkr_flex_token:
                snapshot_kwargs = {
                    "flex_token": ibkr_flex_token,
                    "flex_query_id": get_config("IBKR_FLEX_QUERY_ID", "1554188"),
                    "flex_base_url": get_config(
                        "IBKR_FLEX_BASE_URL",
                        "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
                    ),
                }
                cdc_kwargs = {}
            else:
                snapshot_kwargs = cdc_kwargs = {
                    "base_url": get_config(
                        "IBKR_BASE_URL", "https://localhost:5000/v1/api"
                    ),
                    "account": get_config("IBKR_ACCOUNT"),
                    "verify_tls": parse_bool("IBKR_VERIFY_TLS"),
                    "skip_auth_check": parse_bool("IBKR_SKIP_AUTH_CHECK"),
                    "require_brokerage_session": parse_bool(
                        "IBKR_REQUIRE_BROKERAGE_SESSION"
                    ),
                }

        elif connector.name == "trading212":
            t212_api_key = os.environ.get("T212_API_KEY")
            if not t212_api_key:
                print(f"Skipping {connector.display_name}: T212_API_KEY not set")
                continue
            t212_api_secret = os.environ.get("T212_API_SECRET", "")
            demo = parse_bool("T212_DEMO")
            default_base = (
                "https://demo.trading212.com/api/v0"
                if demo
                else "https://live.trading212.com/api/v0"
            )
            base_url = get_config("T212_BASE_URL") or default_base
            common = {
                "api_key": t212_api_key,
                "api_secret": t212_api_secret,
                "account_id": get_config("T212_ACCOUNT_ID") or "",
                "base_url": base_url,
                "user_agent": get_config("T212_USER_AGENT") or "Mozilla/5.0",
            }
            snapshot_kwargs = {
                **common,
                "include_metadata": not parse_bool("T212_SKIP_METADATA"),
            }
            cdc_kwargs = common

        elif connector.name == "xtb":
            if not args.xtb_file:
                print(f"Skipping {connector.display_name}: no --xtb-file provided")
                continue
            for xtb_file in args.xtb_file:
                xtb_kwargs = {
                    "file_path": xtb_file,
                    "account_id": get_config("XTB_ACCOUNT_ID"),
                }
                try:
                    raw = connector.fetch_snapshot(**xtb_kwargs)
                    table_path = get_raw_path(connector.name, "snapshot")
                    get_storage().backend.ensure_parent(table_path)
                    count = ingest_raw(raw, table_path, fernet_key)
                    print(f"  {connector.display_name} snapshot: {count} rows written")
                except Exception as exc:
                    print(
                        f"  Error fetching {connector.display_name} snapshot: {exc}",
                        file=sys.stderr,
                    )
            continue

        try:
            raw = connector.fetch_snapshot(**snapshot_kwargs)
            table_path = get_raw_path(connector.name, "snapshot")
            get_storage().backend.ensure_parent(table_path)
            count = ingest_raw(raw, table_path, fernet_key)
            print(f"  {connector.display_name} snapshot: {count} rows written")
        except NotImplementedError:
            print(f"  {connector.display_name} snapshot: not implemented")
        except Exception as exc:
            print(
                f"  Error fetching {connector.display_name} snapshot: {exc}",
                file=sys.stderr,
            )

        # Try CDC
        try:
            raw_cdc = connector.fetch_cdc(**cdc_kwargs)
            cdc_path = get_raw_path(connector.name, "cdc")
            get_storage().backend.ensure_parent(cdc_path)
            count = ingest_raw(raw_cdc, cdc_path, fernet_key)
            print(f"  {connector.display_name} CDC: {count} rows written")
        except NotImplementedError:
            print(f"  {connector.display_name} CDC: not implemented")
        except Exception as exc:
            print(
                f"  Error fetching {connector.display_name} CDC: {exc}",
                file=sys.stderr,
            )

    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    """Transform raw data into normalized Delta tables."""
    from deltalake import DeltaTable

    from pipeline.crypto import load_key

    fernet_key = load_key()
    connectors_list = all()
    storage_opts = get_storage().storage_options

    for connector in connectors_list:
        for layer in ("snapshot", "cdc"):
            raw_path = get_raw_path(connector.name, layer)
            try:
                dt = DeltaTable(raw_path, storage_options=storage_opts)
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

                config = get_storage()
                norm_path = config.normalized_path(f"{connector.name}_{layer}")
                config.backend.ensure_parent(norm_path)
                from deltalake import write_deltalake

                if normalized.num_rows == 0:
                    print(f"  {connector.display_name} {layer}: no data to transform")
                    continue
                write_deltalake(
                    norm_path,
                    normalized,
                    mode="overwrite",
                    storage_options=storage_opts,
                )
                print(
                    f"  {connector.display_name} {layer}: {normalized.num_rows} rows transformed"
                )
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
    storage_opts = get_storage().storage_options

    for connector in connectors_list:
        config = get_storage()
        snapshot_path = config.normalized_path(f"{connector.name}_snapshot")
        try:
            DeltaTable(str(snapshot_path), storage_options=storage_opts)
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
        allocate_percentages(fernet_key=fernet_key)
        return 0
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def get_raw_path(connector_name: str, layer: str) -> str:
    """Get the raw data path for a connector and layer.

    Returns a plain string — S3 URIs must not be wrapped in ``Path()``
    because ``pathlib.Path`` collapses ``s3://`` to ``s3:/``.
    """
    config = get_storage()
    return config.raw_path(f"{connector_name}_{layer}")


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
    # Load .env and validate available secrets.
    inject_secrets()

    # Shared arguments available to all subcommands via parents.
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--target-currency",
        default="EUR",
        help="Target currency for consolidation/allocation (default: EUR)",
    )

    parser = argparse.ArgumentParser(
        description="Investment portfolio data pipeline",
        epilog="Connectors are enabled by default. Set IBKR_ENABLED=0, "
        "T212_ENABLED=0, or XTB_ENABLED=0 to disable. "
        "Secrets come from environment variables "
        "(GitHub Secrets in CI, .env file locally).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # keygen
    subparsers.add_parser("keygen", help="Generate encryption key")

    # fetch
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from brokers")
    fetch_parser.add_argument(
        "--xtb-file",
        action="append",
        type=str,
        default=None,
        help="Path to XTB Excel report (can be specified multiple times)",
    )

    # transform
    subparsers.add_parser("transform", help="Transform raw data into normalized tables")

    # consolidate
    consolidate_parser = subparsers.add_parser(
        "consolidate",
        parents=[common_parser],
        help="Consolidate normalized snapshots into holdings table",
    )
    consolidate_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )
    consolidate_parser.add_argument(
        "--isin",
        action="append",
        type=parse_isin_override,
        default=[],
        help="ISIN override as TICKER=ISIN",
    )
    consolidate_parser.add_argument(
        "--isin-map-file",
        action="append",
        type=str,
        default=[],
        help="CSV file with ticker,isin columns",
    )

    # allocate
    allocate_parser = subparsers.add_parser(
        "allocate",
        parents=[common_parser],
        help="Calculate portfolio allocation",
    )
    allocate_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )
    allocate_parser.add_argument(
        "--isin",
        action="append",
        type=parse_isin_override,
        default=[],
        help="ISIN override as TICKER=ISIN",
    )
    allocate_parser.add_argument(
        "--isin-map-file",
        action="append",
        type=str,
        default=[],
        help="CSV file with ticker,isin columns",
    )

    # full
    full_parser = subparsers.add_parser(
        "full",
        parents=[common_parser],
        help="Run full pipeline",
    )
    full_parser.add_argument(
        "--xtb-file",
        action="append",
        type=str,
        default=None,
        help="Path to XTB Excel report (can be specified multiple times)",
    )
    full_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )
    full_parser.add_argument(
        "--isin",
        action="append",
        type=parse_isin_override,
        default=[],
        help="ISIN override as TICKER=ISIN",
    )
    full_parser.add_argument(
        "--isin-map-file",
        action="append",
        type=str,
        default=[],
        help="CSV file with ticker,isin columns",
    )

    args = parser.parse_args()

    # Resolve storage configuration before any path access.
    from pipeline.storage import resolve_storage

    resolve_storage()

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
