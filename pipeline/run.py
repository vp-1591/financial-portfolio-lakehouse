"""Unified CLI for the investment portfolio pipeline.

Usage::

    python -m pipeline.run fetch
    python -m pipeline.run transform
    python -m pipeline.run consolidate --target-currency EUR
    python -m pipeline.run analytics --target-currency EUR
    python -m pipeline.run full
    python -m pipeline.run keygen
    python -m pipeline.run query "SELECT * FROM portfolio_allocation_analytics"
    python -m pipeline.run query "SELECT * FROM ibkr_snapshot_normalized" --decrypt
    python -m pipeline.run run-connector ibkr
    python -m pipeline.run run-connector xtb --xtb-file s3://bucket/staging/xtb/report.xlsx
    python -m pipeline.run run-consolidate-analytics --target-currency EUR

Connector enable/disable is controlled by environment variables:

- ``IBKR_ENABLED`` — set to ``0``, ``false``, or ``no`` to disable IBKR
- ``T212_ENABLED`` — set to ``0``, ``false``, or ``no`` to disable Trading 212
- ``XTB_ENABLED`` — set to ``0``, ``false``, or ``no`` to disable XTB

All connectors are **enabled by default**.  Secrets come from environment
variables (set by GitHub Actions via GitHub Secrets, or locally via ``.env``).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pipeline.connectors.registry import all, get
from pipeline.crypto import load_key
from pipeline.keygen import main as keygen_main
from pipeline.secrets import (
    inject_secrets,
    is_enabled,
)
from pipeline.storage import get_storage

logger = logging.getLogger(__name__)


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


def cmd_query(args: argparse.Namespace) -> int:
    """Execute a SQL query against Delta tables and print results."""
    from pipeline.query import decrypt_df, get_connection, refresh

    refresh()  # Re-discover tables in case new ones were written
    conn = get_connection()

    try:
        result = conn.sql(args.sql).pl()
    except Exception as exc:
        print(f"Query error: {exc}", file=sys.stderr)
        return 1

    if args.decrypt:
        result = decrypt_df(result)

    if args.format == "table":
        print(result)
    elif args.format == "csv":
        print(result.write_csv())
    elif args.format == "json":
        print(result.write_json())

    return 0


def fetch_connector(connector, args: argparse.Namespace, fernet_key: bytes) -> int:
    """Fetch data from a single connector and write to raw Delta tables.

    Returns 0 on success or when the connector is skipped (missing secrets,
    not implemented).  Returns 1 if any fetch operation failed, so that
    callers like :func:`cmd_run_connector` can skip the transform step.
    """
    from pipeline.raw.ingest import ingest_raw

    error_occurred = False

    # XTB handles multiple files — iterate over each one.
    if connector.name == "xtb":
        xtb_files = getattr(args, "xtb_file", None)
        if not xtb_files:
            return 0
        for xtb_file in xtb_files if isinstance(xtb_files, list) else [xtb_files]:
            try:
                raw = connector.fetch_snapshot(file_path=xtb_file)
                table_path = get_raw_path(connector.name, "snapshot")
                get_storage().backend.ensure_parent(table_path)
                count = ingest_raw(raw, table_path, fernet_key)
                logger.debug(
                    "%s snapshot: %d rows written", connector.display_name, count
                )
            except Exception as exc:
                error_occurred = True
                print(
                    f"  Error fetching {connector.display_name} snapshot: {exc}",
                    file=sys.stderr,
                )
        return 1 if error_occurred else 0

    # All other connectors use the fetch_kwargs protocol.
    snapshot_kwargs = connector.fetch_kwargs(args)
    if not snapshot_kwargs:
        logger.debug(
            "Skipping %s: required secrets not configured", connector.display_name
        )
        return 0

    cdc_kwargs = connector.fetch_cdc_kwargs()

    try:
        raw = connector.fetch_snapshot(**snapshot_kwargs)
        table_path = get_raw_path(connector.name, "snapshot")
        get_storage().backend.ensure_parent(table_path)
        count = ingest_raw(raw, table_path, fernet_key)
        logger.debug("%s snapshot: %d rows written", connector.display_name, count)
    except NotImplementedError:
        logger.debug("%s snapshot: not implemented", connector.display_name)
    except Exception as exc:
        error_occurred = True
        print(
            f"  Error fetching {connector.display_name} snapshot: {exc}",
            file=sys.stderr,
        )

    # Try CDC
    if not cdc_kwargs:
        logger.debug(
            "Skipping %s CDC: required secrets not configured", connector.display_name
        )
    else:
        try:
            raw_cdc = connector.fetch_cdc(**cdc_kwargs)
            cdc_path = get_raw_path(connector.name, "cdc")
            get_storage().backend.ensure_parent(cdc_path)
            count = ingest_raw(raw_cdc, cdc_path, fernet_key)
            logger.debug("%s CDC: %d rows written", connector.display_name, count)
        except NotImplementedError:
            logger.debug("%s CDC: not implemented", connector.display_name)
        except Exception as exc:
            error_occurred = True
            print(
                f"  Error fetching {connector.display_name} CDC: {exc}",
                file=sys.stderr,
            )

    return 1 if error_occurred else 0


def transform_connector(connector, fernet_key: bytes) -> int:
    """Transform raw data for a single connector into normalized Delta tables.

    Returns 0 on success or when the connector is skipped (no raw data,
    not implemented).
    """
    from deltalake import DeltaTable, write_deltalake

    storage_opts = get_storage().storage_options

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

            write_deltalake(
                norm_path,
                normalized,
                mode="overwrite",
                storage_options=storage_opts,
            )
            logger.debug(
                "%s %s: %d rows transformed",
                connector.display_name,
                layer,
                normalized.num_rows,
            )
        except NotImplementedError:
            logger.debug(
                "%s %s transform: not implemented", connector.display_name, layer
            )

    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    """Fetch data from brokers and write to raw Delta tables."""
    fernet_key = load_key()

    for connector in all():
        if not is_enabled(connector.enabled_env_var):
            logger.debug(
                "Skipping %s: %s is false",
                connector.display_name,
                connector.enabled_env_var,
            )
            continue
        fetch_connector(connector, args, fernet_key)

    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    """Transform raw data into normalized Delta tables."""
    fernet_key = load_key()

    for connector in all():
        transform_connector(connector, fernet_key)

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
            logger.debug(
                "Skipping %s: no normalized snapshot data", connector.display_name
            )
            continue

        holdings = extract_holdings(connector.name, str(snapshot_path), fernet_key)
        logger.debug("%s: %d holdings extracted", connector.display_name, len(holdings))
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
    logger.debug("Consolidated: %d rows written", table.num_rows)
    return 0


def cmd_analytics(args: argparse.Namespace) -> int:
    """Build all analytics tables: portfolio allocation and CDC analytics.

    Runs allocation first, then CDC analytics tables (dividend income,
    interest income, cash flow summary).  If CDC events are not
    available, logs a warning and continues — allocation still succeeds.
    """
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
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    # Build portfolio_holdings gold table (enriches consolidated holdings
    # with native currency, position type from broker snapshots).
    from pipeline.analytics.holdings import build_portfolio_holdings

    try:
        build_portfolio_holdings(fernet_key=fernet_key)
    except FileNotFoundError as exc:
        logger.warning("portfolio_holdings skipped: %s", exc)
    except Exception as exc:
        logger.warning("portfolio_holdings failed: %s", exc)

    # Build CDC analytics tables.  These are optional — if cdc_events
    # doesn't exist yet, log a warning and continue.
    from pipeline.analytics.cdc_tables import (
        build_cash_flow_summary,
        build_dividend_income,
        build_interest_income,
    )

    try:
        build_dividend_income(fernet_key=fernet_key)
        build_interest_income(fernet_key=fernet_key)
        build_cash_flow_summary(fernet_key=fernet_key)
    except FileNotFoundError as exc:
        logger.warning("CDC analytics tables skipped: %s", exc)
    except Exception as exc:
        logger.warning("CDC analytics tables failed: %s", exc)

    return 0


def get_raw_path(connector_name: str, layer: str) -> str:
    """Get the raw data path for a connector and layer.

    Returns a plain string — S3 URIs must not be wrapped in ``Path()``
    because ``pathlib.Path`` collapses ``s3://`` to ``s3:/``.
    """
    config = get_storage()
    return config.raw_path(f"{connector_name}_{layer}")


def cmd_validate(args: argparse.Namespace) -> int:
    """Run data quality checks against normalized and analytics tables."""
    from pipeline.analytics.quality import run_validation
    from pipeline.crypto import load_key

    return run_validation(
        fernet_key=load_key(),
        freshness_days=args.freshness_days,
        fail_on_warn=args.fail_on_warn,
    )


def cmd_report(args: argparse.Namespace) -> int:
    """Generate a self-contained HTML portfolio report from analytics tables."""
    from pipeline.report import generate_report

    try:
        return generate_report(
            output_path=args.output,
            base_currency=args.base_currency,
            open_browser=args.open,
        )
    except Exception as exc:
        print(f"Error generating report: {exc}", file=sys.stderr)
        return 1


def cmd_full(args: argparse.Namespace) -> int:
    """Run the full pipeline: fetch → transform → consolidate → analytics."""
    result = cmd_fetch(args)
    if result != 0:
        return result
    result = cmd_transform(args)
    if result != 0:
        return result
    result = cmd_consolidate(args)
    if result != 0:
        return result
    # Consolidate CDC events after snapshot consolidation
    _consolidate_cdc()
    return cmd_analytics(args)


def _consolidate_cdc() -> None:
    """Consolidate broker CDC events into a unified table."""
    from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

    consolidate_cdc_events()


def cmd_run_connector(args: argparse.Namespace) -> int:
    """Run fetch+transform for a single connector.

    Used by the Step Functions orchestrator: each Fargate task runs one
    connector through fetch and transform in a single process, cutting
    cold starts.
    """
    connector = get(args.connector)

    if not is_enabled(connector.enabled_env_var):
        logger.info(
            "Skipping %s: %s is false",
            connector.display_name,
            connector.enabled_env_var,
        )
        return 0

    # XTB requires --xtb-file in dedicated subcommand mode.
    if connector.name == "xtb" and not getattr(args, "xtb_file", None):
        print("Error: run-connector xtb requires --xtb-file", file=sys.stderr)
        return 1

    fernet_key = load_key()
    rc = fetch_connector(connector, args, fernet_key)
    if rc:
        return rc
    return transform_connector(connector, fernet_key)


def cmd_run_consolidate_analytics(args: argparse.Namespace) -> int:
    """Run consolidate then analytics — idempotent full-overwrite steps.

    Used by the Step Functions orchestrator after all connector tasks
    have completed.
    """
    rc = cmd_consolidate(args)
    if rc:
        return rc
    _consolidate_cdc()
    return cmd_analytics(args)


def cmd_upload_xtb(args: argparse.Namespace) -> int:
    """Upload an XTB .xlsx report to S3 staging.

    The pipeline's Step Function will detect the file via EventBridge
    and trigger the XTB fetch → transform → consolidate → analytics
    pipeline automatically.
    """
    from pipeline.s3 import upload_to_staging
    from pipeline.storage import S3Backend

    config = get_storage()

    if not isinstance(config.backend, S3Backend):
        print(
            "Error: upload-xtb requires S3 storage. "
            "Set S3_BUCKET to use cloud storage.",
            file=sys.stderr,
        )
        return 1

    file_path = Path(args.file).resolve()
    if not file_path.exists():
        print(f"Error: file not found: {file_path}", file=sys.stderr)
        return 1

    s3_uri = config.staging_path("xtb", file_path.name)
    result_uri = upload_to_staging(file_path, s3_uri)
    print(f"Uploaded {file_path.name} → {result_uri}")
    print("EventBridge will trigger the orchestrator on this file's arrival.")
    return 0


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

    # query
    query_parser = subparsers.add_parser(
        "query",
        help="Query Delta tables via SQL",
    )
    query_parser.add_argument("sql", help="SQL query to execute")
    query_parser.add_argument(
        "--decrypt",
        action="store_true",
        help="Decrypt Fernet-encrypted binary columns",
    )
    query_parser.add_argument(
        "--format",
        choices=["table", "csv", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # fetch
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from brokers")
    fetch_parser.add_argument(
        "--xtb-file",
        action="append",
        type=str,
        default=None,
        help="Path to XTB Excel report, or an s3:// URI (can be specified multiple times)",
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

    # analytics
    analytics_parser = subparsers.add_parser(
        "analytics",
        parents=[common_parser],
        help="Build all analytics tables (allocation, dividend income, interest income, cash flow summary)",
    )
    analytics_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )
    analytics_parser.add_argument(
        "--isin",
        action="append",
        type=parse_isin_override,
        default=[],
        help="ISIN override as TICKER=ISIN",
    )
    analytics_parser.add_argument(
        "--isin-map-file",
        action="append",
        type=str,
        default=[],
        help="CSV file with ticker,isin columns",
    )

    # validate
    validate_parser = subparsers.add_parser(
        "validate",
        parents=[common_parser],
        help="Run data quality checks against pipeline tables",
    )
    validate_parser.add_argument(
        "--freshness-days",
        type=int,
        default=7,
        help="Maximum age in days for data to be considered fresh (default: 7)",
    )
    validate_parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        default=False,
        help="Exit non-zero on WARN results (default: only FAIL exits non-zero)",
    )

    # report
    report_parser = subparsers.add_parser(
        "report",
        parents=[common_parser],
        help="Generate a self-contained HTML portfolio report",
    )
    report_parser.add_argument(
        "--output",
        type=str,
        default="data/report.html",
        help="Output HTML file path (default: data/report.html)",
    )
    report_parser.add_argument(
        "--base-currency",
        type=str,
        default=None,
        help="Base currency label for display (default: inferred from data)",
    )
    report_parser.add_argument(
        "--open",
        action="store_true",
        default=False,
        help="Open the generated report in the default browser",
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
        help="Path to XTB Excel report, or an s3:// URI (can be specified multiple times)",
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

    # upload-xtb
    upload_xtb_parser = subparsers.add_parser(
        "upload-xtb",
        help="Upload XTB .xlsx report to S3 staging",
    )
    upload_xtb_parser.add_argument(
        "file",
        type=str,
        help="Path to XTB .xlsx report to upload",
    )

    # run-connector
    run_connector_parser = subparsers.add_parser(
        "run-connector",
        parents=[common_parser],
        help="Run fetch+transform for a single connector (orchestrator task)",
    )
    run_connector_parser.add_argument(
        "connector",
        type=str,
        help="Connector name (e.g. ibkr, trading212, xtb)",
    )
    run_connector_parser.add_argument(
        "--xtb-file",
        action="append",
        type=str,
        default=None,
        help="Path to XTB Excel report, or an s3:// URI (can be specified multiple times)",
    )

    # run-consolidate-analytics
    run_consolidate_analytics_parser = subparsers.add_parser(
        "run-consolidate-analytics",
        parents=[common_parser],
        help="Run consolidate then analytics (orchestrator task)",
    )
    run_consolidate_analytics_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )
    run_consolidate_analytics_parser.add_argument(
        "--isin",
        action="append",
        type=parse_isin_override,
        default=[],
        help="ISIN override as TICKER=ISIN",
    )
    run_consolidate_analytics_parser.add_argument(
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
        "analytics": cmd_analytics,
        "validate": cmd_validate,
        "report": cmd_report,
        "full": cmd_full,
        "query": cmd_query,
        "upload-xtb": cmd_upload_xtb,
        "run-connector": cmd_run_connector,
        "run-consolidate-analytics": cmd_run_consolidate_analytics,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
