"""Unified CLI for the investment portfolio pipeline.

Usage::

    python -m pipeline.run fetch
    python -m pipeline.run transform
    python -m pipeline.run consolidate --target-currency EUR
    python -m pipeline.run analytics --target-currency EUR
    python -m pipeline.run full
    python -m pipeline.run keygen
    python -m pipeline.run query "SELECT * FROM portfolio_holdings_analytics"
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
from enum import IntEnum
from pathlib import Path

from pipeline.analytics.quality import run_validation
from pipeline.connectors.registry import all as all_connectors, get
from pipeline.crypto import load_key
from pipeline.keygen import main as keygen_main
from pipeline.secrets import (
    inject_secrets,
    is_enabled,
    load_env,
)
from pipeline.storage import get_storage

logger = logging.getLogger(__name__)


class FetchResult(IntEnum):
    """Exit status for :func:`fetch_connector`."""

    SUCCESS = 0  # Data was fetched successfully
    ERROR = 1  # Fetch attempted but failed
    SKIPPED = 2  # No credentials configured — connector was skipped


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


def fetch_connector(
    connector, args: argparse.Namespace, fernet_key: bytes
) -> FetchResult:
    """Fetch data from a single connector and write to raw Delta tables.

    Returns :class:`FetchResult`:
    - ``SUCCESS`` — data was fetched and written
    - ``SKIPPED`` — connector had no credentials or required input (e.g. XTB
      without ``--xtb-file``)
    - ``ERROR`` — fetch was attempted but failed
    """
    from pipeline.raw.ingest import ingest_raw

    error_occurred = False

    # XTB handles multiple files — iterate over each one.
    if connector.name == "xtb":
        xtb_files = getattr(args, "xtb_file", None)
        if not xtb_files:
            return FetchResult.SKIPPED
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
        return FetchResult.ERROR if error_occurred else FetchResult.SUCCESS

    # All other connectors use the fetch_kwargs protocol.
    snapshot_kwargs = connector.fetch_kwargs(args)
    if not snapshot_kwargs:
        logger.debug(
            "Skipping %s: required secrets not configured", connector.display_name
        )
        return FetchResult.SKIPPED

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

    return FetchResult.ERROR if error_occurred else FetchResult.SUCCESS


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
            logger.debug(
                "%s %s: raw table not present at %s, skipping",
                connector.display_name,
                layer,
                raw_path,
            )
            continue

        raw_table = dt.to_pyarrow_table()
        if raw_table.num_rows == 0:
            logger.warning(
                "%s %s: raw table is empty (0 rows); skipping transform",
                connector.display_name,
                layer,
            )
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
    inject_secrets()
    fernet_key = load_key()

    results: list[FetchResult] = []
    for connector in all_connectors():
        if not is_enabled(connector.enabled_env_var):
            logger.debug(
                "Skipping %s: %s is false",
                connector.display_name,
                connector.enabled_env_var,
            )
            continue
        results.append(fetch_connector(connector, args, fernet_key))

    if not results:
        # All connectors are disabled — not an error, but nothing to do.
        return 0

    if all(r == FetchResult.SKIPPED for r in results):
        print(
            "No broker credentials found. In Docker mode, set IBKR_FLEX_TOKEN "
            "or T212_API_KEY in .env. In staging/prod mode, use --mode staging "
            "or --mode prod to trigger the pipeline in AWS "
            "(no local broker credentials needed).",
            file=sys.stderr,
        )
        return 1

    if any(r == FetchResult.ERROR for r in results):
        succeeded = sum(1 for r in results if r == FetchResult.SUCCESS)
        failed = sum(1 for r in results if r == FetchResult.ERROR)
        print(
            f"{succeeded} of {len(results)} connector(s) succeeded, "
            f"{failed} failed. Check errors above.",
            file=sys.stderr,
        )
        return 1

    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    """Transform raw data into normalized Delta tables."""
    fernet_key = load_key()

    for connector in all_connectors():
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
    connectors_list = all_connectors()
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
    """Build all analytics tables: portfolio holdings (with percentages) and CDC analytics.

    Builds portfolio holdings first, then CDC analytics tables (dividend
    income, interest income, cash flow summary).  If CDC events are not
    available, logs a warning and continues — holdings still succeeds.
    """
    import csv
    from pathlib import Path

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

    # Build portfolio_holdings gold table (reads security_value,
    # position_type, security_ccy, and target_ccy from consolidated_holdings).
    from pipeline.analytics.holdings import build_portfolio_holdings

    holdings_ok = True
    try:
        build_portfolio_holdings(fernet_key=fernet_key)
    except FileNotFoundError as exc:
        logger.warning("portfolio_holdings skipped: %s", exc)
        holdings_ok = False
    except Exception as exc:
        logger.warning("portfolio_holdings failed: %s", exc)
        holdings_ok = False

    # Build CDC analytics tables.  CDC is mandatory — consolidation
    # guarantees cdc_events exists (or fails).  The guard below is
    # defense-in-depth and should rarely trigger.
    # Decision: docs/adr/0087-make-cdc-mandatory-and-fail-on-empty-silver-cdc.md
    from pipeline.analytics.cdc_tables import (
        build_cash_flow_summary,
        build_dividend_income,
        build_interest_income,
    )

    cdc_ok = True
    for builder in (
        build_dividend_income,
        build_interest_income,
        build_cash_flow_summary,
    ):
        try:
            builder(fernet_key=fernet_key)
        except FileNotFoundError as exc:
            logger.warning("%s skipped: %s", builder.__name__, exc)
            cdc_ok = False
        except Exception as exc:
            logger.warning("%s failed: %s", builder.__name__, exc)
            cdc_ok = False

    if not holdings_ok or not cdc_ok:
        return 1
    return 0


def cmd_run_migration(args: argparse.Namespace) -> int:
    """Run schema migrations for existing Delta tables."""
    from pipeline.migrations.migrate_001_encrypt_gold_values import run_migration

    return run_migration()


def get_raw_path(connector_name: str, layer: str) -> str:
    """Get the raw data path for a connector and layer.

    Returns a plain string — S3 URIs must not be wrapped in ``Path()``
    because ``pathlib.Path`` collapses ``s3://`` to ``s3:/``.
    """
    config = get_storage()
    return config.raw_path(f"{connector_name}_{layer}")


def cmd_validate(args: argparse.Namespace) -> int:
    """Run data quality checks against normalized and analytics tables."""
    from pipeline.crypto import load_key

    return run_validation(
        fernet_key=load_key(),
        freshness_days=args.freshness_days,
        fail_on_warn=args.fail_on_warn,
        tables=args.tables,
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
    inject_secrets()
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
    cdc_rc = _consolidate_cdc()
    if cdc_rc:
        return cdc_rc
    # Normalize target currency columns in CDC events
    norm_rc = _normalize_cdc(args)
    if norm_rc:
        return norm_rc
    return cmd_analytics(args)


def _consolidate_cdc() -> int:
    """Consolidate broker CDC events into a unified table.

    Returns 0 on success, 1 if consolidation raised (e.g. a required
    broker CDC table is missing or empty).
    """
    from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

    try:
        consolidate_cdc_events()
    except RuntimeError as exc:
        print(f"Error consolidating CDC events: {exc}", file=sys.stderr)
        return 1
    return 0


def _normalize_cdc(args: argparse.Namespace) -> int:
    """Normalize target currency columns in CDC events.

    Fills in ``target_fx_rate``, ``target_value``, and ``target_ccy``
    using the CurrencyConverter.  Runs after CDC events are consolidated
    and before CDC analytics tables are built.

    Returns 0 on success, 1 if the CDC events table is missing (which
    indicates a prior consolidation failure).
    """
    from pipeline.normalized.normalize import normalize_currency

    manual_rates: dict[str, float] = {}
    if args.fx_rate:
        manual_rates.update(dict(args.fx_rate))

    target_currency = getattr(args, "target_currency", "EUR").upper()
    try:
        normalize_currency(
            target_currency=target_currency,
            manual_rates=manual_rates,
        )
    except FileNotFoundError as exc:
        logger.error("CDC events table not found for currency normalization: %s", exc)
        return 1
    return 0


def cmd_run_connector(args: argparse.Namespace) -> int:
    """Run fetch+transform for a single connector.

    Used by the Step Functions orchestrator: each Fargate task runs one
    connector through fetch and transform in a single process, cutting
    cold starts.
    """
    inject_secrets()
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
    if rc == FetchResult.SKIPPED:
        # Connector has no credentials — skip transform and validation gracefully.
        return 0
    if rc == FetchResult.ERROR:
        return 1
    rc = transform_connector(connector, fernet_key)
    if rc:
        return rc
    # Validate connector's normalized tables after transform
    # Decision: docs/adr/0087-make-cdc-mandatory-and-fail-on-empty-silver-cdc.md
    # Only validate CDC table for connectors that support CDC (XTB does not).
    tables = [f"{connector.name}_snapshot"]
    if connector.cdc_supported:
        tables.append(f"{connector.name}_cdc")
    return run_validation(
        fernet_key=fernet_key,
        tables=tables,
    )


def cmd_run_consolidate_analytics(args: argparse.Namespace) -> int:
    """Run consolidate then analytics — idempotent full-overwrite steps.

    Used by the Step Functions orchestrator after all connector tasks
    have completed.  Validates silver tables after consolidate/CDC and gold
    tables after analytics — a FAIL-level check causes a non-zero exit.
    """
    fernet_key = load_key()
    rc = cmd_consolidate(args)
    if rc:
        return rc
    cdc_rc = _consolidate_cdc()
    if cdc_rc:
        return cdc_rc
    # Normalize target currency columns in CDC events
    norm_rc = _normalize_cdc(args)
    if norm_rc:
        return norm_rc
    # Validate silver tables after consolidate + CDC
    silver_rc = run_validation(
        fernet_key=fernet_key,
        tables=["consolidated_holdings", "cdc_events"],
    )
    if silver_rc:
        return silver_rc
    analytics_rc = cmd_analytics(args)
    if analytics_rc:
        return analytics_rc
    # Validate gold tables after analytics
    return run_validation(
        fernet_key=fernet_key,
        tables=[
            "portfolio_holdings",
            "dividend_income",
            "interest_income",
            "cash_flow_summary",
        ],
    )


def cmd_upload_xtb(args: argparse.Namespace) -> int:
    """Upload an XTB .xlsx report to S3 staging.

    The pipeline's Step Function will detect the file via EventBridge
    and trigger the XTB fetch → transform → consolidate → analytics
    pipeline automatically.
    """
    inject_secrets()
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
    # Load .env file silently (no secret warnings).
    # Commands that need broker/S3 credentials call inject_secrets()
    # themselves.
    load_env()

    # Shared arguments available to all subcommands via parents.
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--target-currency",
        default="EUR",
        help="Target currency for consolidation (default: EUR)",
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
        help="Build all analytics tables (portfolio holdings, dividend income, interest income, cash flow summary)",
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
    validate_parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Only validate specified tables (default: all registered tables)",
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

    subparsers.add_parser(
        "run-migration",
        parents=[common_parser],
        help="Run schema migrations for existing Delta tables",
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
        "run-migration": cmd_run_migration,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
