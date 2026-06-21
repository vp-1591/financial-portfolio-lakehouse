"""Unified CLI for the investment portfolio pipeline.

Usage::

    python -m pipeline.run fetch --ibkr [--xtb-file report.xlsx] [--t212-api-key KEY]
    python -m pipeline.run transform
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
from pipeline.paths import ENCRYPTION_KEY_FILE, SECRETS_DIR


def add_ibkr_args(parser: argparse._ArgumentGroup) -> None:
    parser.add_argument("--ibkr", action="store_true", help="Enable IBKR connector (connects to the default Client Portal Gateway URL)")
    parser.add_argument("--ibkr-base-url", default="https://localhost:5000/v1/api")
    parser.add_argument("--ibkr-account", default=None)
    parser.add_argument("--ibkr-base-currency", default=None)
    parser.add_argument("--ibkr-verify-tls", action="store_true")
    parser.add_argument("--ibkr-skip-auth-check", action="store_true")
    parser.add_argument("--ibkr-require-brokerage-session", action="store_true")


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
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    if ENCRYPTION_KEY_FILE.exists():
        print(f"Encryption key already exists at {ENCRYPTION_KEY_FILE}")
        return 0
    key = generate_key()
    ENCRYPTION_KEY_FILE.write_bytes(key)
    print(f"Encryption key written to {ENCRYPTION_KEY_FILE}")
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
            if not args.ibkr:
                print(f"Skipping {connector.display_name}: use --ibkr to enable")
                continue
            snapshot_kwargs = cdc_kwargs = {
                "base_url": args.ibkr_base_url,
                "account": args.ibkr_account,
                "verify_tls": args.ibkr_verify_tls,
                "skip_auth_check": args.ibkr_skip_auth_check,
                "require_brokerage_session": args.ibkr_require_brokerage_session,
            }
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

            raw = dt.to_pandas()
            if raw.empty:
                continue

            import pyarrow as pa
            raw_table = pa.Table.from_pandas(raw)

            try:
                if layer == "snapshot":
                    normalized = connector.transform_snapshot(raw_table, fernet_key)
                else:
                    normalized = connector.transform_cdc(raw_table, fernet_key)

                from pathlib import Path
                from pipeline.paths import NORMALIZED_DIR
                norm_path = str(NORMALIZED_DIR / f"{connector.name}_{layer}")
                Path(norm_path).parent.mkdir(parents=True, exist_ok=True)
                from deltalake import write_deltalake
                write_deltalake(norm_path, normalized, mode="append")
                print(f"  {connector.display_name} {layer}: {normalized.num_rows} rows transformed")
            except NotImplementedError:
                print(f"  {connector.display_name} {layer} transform: not implemented")

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
    from pipeline.paths import RAW_DIR
    return RAW_DIR / f"{connector_name}_{layer}"


def cmd_full(args: argparse.Namespace) -> int:
    """Run the full pipeline: fetch → transform → allocate."""
    result = cmd_fetch(args)
    if result != 0:
        return result
    result = cmd_transform(args)
    if result != 0:
        return result
    return cmd_allocate(args)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Investment portfolio data pipeline"
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

    commands = {
        "keygen": cmd_keygen,
        "fetch": cmd_fetch,
        "transform": cmd_transform,
        "allocate": cmd_allocate,
        "full": cmd_full,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())