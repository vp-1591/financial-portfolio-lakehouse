"""Unified CLI for the investment portfolio pipeline.

Usage::

    python -m pipeline.run full --mode docker
    python -m pipeline.run full --mode staging
    python -m pipeline.run keygen
    python -m pipeline.run query "SELECT * FROM portfolio_holdings_analytics" --mode docker
    python -m pipeline.run run-connector ibkr --mode docker
    python -m pipeline.run run-connector xtb --xtb-file report.xlsx --mode docker
    python -m pipeline.run run-consolidate-analytics --mode docker --connectors ibkr trading212

The ``--mode`` flag (``docker|staging|prod``) is required for all commands
except ``keygen``.  It determines storage backend and credential resolution:

- **docker** — MinIO (local S3-compatible storage)
- **staging** — staging S3 bucket, secrets under base names from SSM
- **prod** — production S3 bucket, production secrets

Secrets come from environment variables (set by GitHub Actions via GitHub
Secrets, or locally via ``.env``).  Connectors are always enabled when
invoked explicitly (e.g. ``run-connector ibkr``) or as part of ``full``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import IntEnum
from pathlib import Path

from pipeline.analytics.quality import run_validation
from pipeline.connectors.registry import all as all_connectors
from pipeline.connectors.registry import get
from pipeline.crypto import load_key
from pipeline.keygen import main as keygen_main
from pipeline.secrets import (
    get_mode,
    inject_secrets,
    load_env,
    set_mode,
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
        # Show all columns — Polars hides middle columns when a row exceeds
        # its default 80-char print width.
        import polars as pl

        pl.Config.set_fmt_str_lengths(100)
        pl.Config.set_tbl_rows(100)
        pl.Config.set_tbl_cols(200)
        pl.Config.set_tbl_width_chars(None)
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

    events_kwargs = connector.fetch_events_kwargs()

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

    # Try events
    if not events_kwargs:
        logger.debug(
            "Skipping %s events: required secrets not configured",
            connector.display_name,
        )
    else:
        try:
            raw_events = connector.fetch_events(**events_kwargs)
            events_path = get_raw_path(connector.name, "events")
            get_storage().backend.ensure_parent(events_path)
            count = ingest_raw(raw_events, events_path, fernet_key)
            logger.debug("%s events: %d rows written", connector.display_name, count)
        except NotImplementedError:
            logger.debug("%s events: not implemented", connector.display_name)
        except Exception as exc:
            error_occurred = True
            print(
                f"  Error fetching {connector.display_name} events: {exc}",
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

    for layer in ("snapshot", "events"):
        # D17: the events transform reads from the connector's events_raw_layer
        # (default "events"; XTB overrides to "snapshot" for shared bronze). The
        # snapshot layer always reads the snapshot raw. The normalized output
        # is still written to ``normalized/{name}_{layer}`` below.
        raw_layer = layer if layer == "snapshot" else connector.events_raw_layer
        raw_path = get_raw_path(connector.name, raw_layer)
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
                normalized = connector.transform_events(raw_table, fernet_key)

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


def cmd_consolidate(args: argparse.Namespace) -> int:
    """Consolidate normalized broker snapshots into the holdings table."""
    from deltalake import DeltaTable

    from pipeline.crypto import load_key
    from pipeline.normalized.consolidate import (
        CurrencyConverter,
        Holding,
        consolidate_holdings,
    )
    from pipeline.normalized.extract import extract_holdings

    fernet_key = load_key()

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
    )
    logger.debug("Consolidated: %d rows written", table.num_rows)
    return 0


def cmd_analytics(args: argparse.Namespace) -> int:
    """Build all analytics tables: portfolio holdings (with percentages) and events analytics.

    Builds portfolio holdings first, then events analytics tables (dividend
    income, interest income, cash flow summary).  If events are not
    available, logs a warning and continues — holdings still succeeds.
    """
    from pipeline.crypto import load_key

    fernet_key = load_key()

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

    # Build events analytics tables. Missing or empty event inputs are
    # reported by the final quality validation without masking connector
    # task failures.
    # Decision: ADR 0087 - make the events layer mandatory and fail on empty
    # silver events.
    from pipeline.analytics.events_tables import (
        build_cash_flow_summary,
        build_dividend_income,
        build_interest_income,
    )

    events_ok = True
    for builder in (
        build_dividend_income,
        build_interest_income,
        build_cash_flow_summary,
    ):
        try:
            builder(fernet_key=fernet_key)
        except FileNotFoundError as exc:
            logger.warning("%s skipped: %s", builder.__name__, exc)
            events_ok = False
        except Exception as exc:
            logger.warning("%s failed: %s", builder.__name__, exc)
            events_ok = False

    if not holdings_ok or not events_ok:
        return 1
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
            mode=args.mode,
        )
    except Exception as exc:
        print(f"Error generating report: {exc}", file=sys.stderr)
        return 1


def cmd_full(args: argparse.Namespace) -> int:
    """Run the full pipeline.

    In **docker** mode, mirrors the Step Functions workflow locally: each
    connector runs fetch+transform+validate via :func:`cmd_run_connector`,
    then :func:`cmd_run_consolidate_analytics` runs
    consolidate+events+analytics+validate.

    In **staging** and **prod** modes, triggers a Step Functions execution
    instead of running locally.  The caller needs only AWS credentials with
    ``states:StartExecution`` permission; broker secrets are injected into
    ECS containers by SSM at runtime.  With ``--wait``, polls the execution
    and prints failure details (TaskFailed history + CloudWatch container
    logs) on a non-successful terminal status.

    Decision: docs/adr/0091-trigger-step-functions-in-cmd-full.md.
    """
    mode = get_mode()
    if mode == "docker":
        inject_secrets()
        rc = _run_connectors_parallel(args)
        if rc:
            return rc
        args.connectors = [connector.name for connector in all_connectors()]
        return cmd_run_consolidate_analytics(args)

    return _trigger_sfn_execution(args, mode)


def _trigger_sfn_execution(args: argparse.Namespace, mode: str) -> int:
    """Start (and optionally wait on) a Step Functions execution for staging/prod.

    Decision: docs/adr/0091-trigger-step-functions-in-cmd-full.md.
    """
    # The --with-xtb and --xtb-file flags are local-docker-only; they do not
    # apply to the SFN-triggered run. XTB is NOT part of the scheduled run at
    # all (DEFAULT_CONNECTORS in sfn.py excludes it): the EventBridge S3
    # file-arrival rule runs XTB fetch + transform automatically once a report
    # is uploaded via ``upload-xtb``, and ``run-consolidate-analytics`` picks
    # up XTB silver on every run. Reject the flags here so callers don't
    # expect a local file to reach the SFN execution.
    if getattr(args, "with_xtb", False) or getattr(args, "xtb_file", None):
        print(
            "--with-xtb/--xtb-file are not supported in staging/prod 'full'. "
            "Use 'upload-xtb' to push the file to S3; the EventBridge "
            "file-arrival trigger runs the XTB connector automatically.",
            file=sys.stderr,
        )
        return 1

    import boto3

    session = boto3.Session()
    if session.get_credentials() is None:
        print(
            "AWS credentials not found. Run `aws configure` or set "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.",
            file=sys.stderr,
        )
        return 1

    from pipeline.sfn import (
        DEFAULT_CONNECTORS,
        DEFAULT_POLL_INTERVAL_SECONDS,
        DEFAULT_TIMEOUT_SECONDS,
        build_clients,
        build_execution_input,
        console_url,
        execution_name,
        fetch_failure_details,
        resolve_all_arns,
        resolve_state_machine_arn,
        start_execution,
        wait_for_execution,
    )

    region = session.region_name or "eu-west-1"
    sfn_client, ecs_client, logs_client = build_clients(region)

    arn = resolve_state_machine_arn(sfn_client, mode)
    if not arn:
        return 1
    target_currency = getattr(args, "target_currency", "EUR")
    connector_arns, consolidate_arn = resolve_all_arns(
        ecs_client, mode, DEFAULT_CONNECTORS
    )
    exec_input = build_execution_input(
        DEFAULT_CONNECTORS, connector_arns, consolidate_arn, mode, target_currency
    )

    prefix = "staging" if mode == "staging" else "manual"
    name = execution_name(prefix)
    execution_arn = start_execution(sfn_client, arn, exec_input, name)
    print(f"Started execution: {name}")
    print(f"ARN: {execution_arn}")
    print(f"Monitor: {console_url(execution_arn, region)}")

    if not getattr(args, "wait", False):
        return 0

    try:
        status = wait_for_execution(
            sfn_client,
            execution_arn,
            DEFAULT_TIMEOUT_SECONDS,
            DEFAULT_POLL_INTERVAL_SECONDS,
        )
    except TimeoutError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1

    if status == "SUCCEEDED":
        print("Step Function execution succeeded")
        return 0

    print(
        f"::error::Step Function execution failed with status: {status}",
        file=sys.stderr,
    )
    print(
        fetch_failure_details(sfn_client, logs_client, execution_arn, mode),
        file=sys.stderr,
    )
    return 1


def _run_connectors_parallel(args: argparse.Namespace) -> int:
    """Run all connectors in parallel via :func:`cmd_run_connector`.

    Mirrors the Step Functions Map state: each connector runs
    fetch+transform+validate in its own thread, with fail-fast on first
    error.  Returns 0 if all connectors succeed.
    """
    connectors_list = all_connectors()
    if not connectors_list:
        logger.info("All connectors are disabled — nothing to fetch.")
        return 0

    base_ns = argparse.Namespace(
        target_currency=getattr(args, "target_currency", "EUR"),
        fx_rate=getattr(args, "fx_rate", []),
        xtb_file=getattr(args, "xtb_file", None),
        mode=get_mode(),
    )

    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=len(connectors_list)) as pool:
        future_to_name = {
            pool.submit(cmd_run_connector, _ns_for(base_ns, c.name)): c.name
            for c in connectors_list
        }
        for fut in as_completed(future_to_name):
            name = future_to_name[fut]
            try:
                rc = fut.result()
            except Exception as exc:
                errors.append(f"{name}: {exc}")
                # Fail-fast: cancel not-yet-started futures.
                for f in future_to_name:
                    f.cancel()
                break
            if rc != 0:
                errors.append(f"{name}: exit code {rc}")
                for f in future_to_name:
                    f.cancel()
                break

    if errors:
        print(
            "Connector stage failed (fail-fast):\n  " + "\n  ".join(errors),
            file=sys.stderr,
        )
        return 1
    return 0


def _ns_for(base: argparse.Namespace, connector: str) -> argparse.Namespace:
    """Create a per-connector args namespace from a shared base."""
    ns = argparse.Namespace(**vars(base))
    ns.connector = connector
    return ns


def _consolidate_events() -> int:
    """Consolidate broker events into a unified table.

    Missing or empty registered event tables are warnings and do not fail the run.
    """
    from pipeline.normalized.consolidate_events import consolidate_events

    try:
        consolidate_events()
    except RuntimeError as exc:
        print(f"Error consolidating events: {exc}", file=sys.stderr)
        return 1
    return 0


def _normalize_events(args: argparse.Namespace) -> int:
    """Normalize target currency columns in events.

    Fills in ``target_fx_rate``, ``target_value``, and ``target_ccy``
    using the CurrencyConverter.  Runs after events are consolidated
    and before events analytics tables are built.

    Returns 0 on success, 1 if the events table is missing (which
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
        logger.error("events table not found for currency normalization: %s", exc)
        return 1
    except RuntimeError as exc:
        logger.error("Currency normalization failed: %s", exc)
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

    fernet_key = load_key()
    rc = fetch_connector(connector, args, fernet_key)
    if rc == FetchResult.SKIPPED:
        # Connector has no credentials or required input (e.g. XTB without
        # --xtb-file) — skip transform and validation gracefully.
        return 0
    if rc == FetchResult.ERROR:
        return 1
    rc = transform_connector(connector, fernet_key)
    if rc:
        return rc
    # Validate connector's normalized tables after transform.
    # D14: validation is unconditional — events are mandatory for every
    # connector now that XTB produces events via the shared bronze (D17). The
    # ``events_supported`` flag is removed.
    # Decision: docs/adr/0108-xtb-new-format-connector-overhaul.md
    tables = [f"{connector.name}_snapshot", f"{connector.name}_events"]
    return run_validation(
        fernet_key=fernet_key,
        tables=tables,
        connectors=[connector.name],
    )


def cmd_run_consolidate_analytics(args: argparse.Namespace) -> int:
    """Run consolidate then analytics — idempotent full-overwrite steps.

    Used by the Step Functions orchestrator after all connector tasks
    have completed.  Validates silver tables after consolidate/events and
    gold tables after analytics — a FAIL-level check causes a non-zero exit.
    """
    fernet_key = load_key()
    connectors = list(getattr(args, "connectors", []))
    rc = cmd_consolidate(args)
    if rc:
        return rc
    events_rc = _consolidate_events()
    if events_rc:
        return events_rc
    # Normalize target currency columns in events
    norm_rc = _normalize_events(args)
    if norm_rc:
        return norm_rc
    # Validate silver tables after consolidate + events
    silver_rc = run_validation(
        fernet_key=fernet_key,
        tables=[
            "consolidated_holdings",
            "events",
            *[f"{connector}_events" for connector in connectors],
        ],
        connectors=connectors,
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
            "consolidated_holdings",
            "events",
            *[f"{connector}_events" for connector in connectors],
            "portfolio_holdings",
            "dividend_income",
            "interest_income",
            "cash_flow_summary",
        ],
        connectors=connectors,
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
            "Error: upload-xtb requires S3 storage. Use --mode staging or --mode prod.",
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

    # Mode flag — required for all commands that touch storage/credentials.
    # Appears after the subcommand (e.g. pipeline run full --mode docker).
    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_parser.add_argument(
        "--mode",
        choices=["docker", "staging", "prod"],
        required=True,
        help="Execution mode: docker=MinIO local, staging=staging S3, prod=prod S3",
    )

    parser = argparse.ArgumentParser(
        description="Investment portfolio data pipeline",
        epilog="Secrets come from environment variables "
        "(GitHub Secrets in CI, .env file locally). "
        "Connectors are always enabled when invoked explicitly.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # keygen (no --mode needed — no storage/credentials)
    subparsers.add_parser("keygen", help="Generate encryption key")

    # query
    query_parser = subparsers.add_parser(
        "query",
        parents=[mode_parser],
        help="Query Delta tables via SQL",
    )
    query_parser.add_argument("sql", help="SQL query to execute")
    query_parser.add_argument(
        "--decrypt",
        action="store_true",
        help="Decrypt Fernet-encrypted binary columns "
        "(requires the same ENCRYPTION_KEY used at ingest)",
    )
    query_parser.add_argument(
        "--format",
        choices=["table", "csv", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # validate
    validate_parser = subparsers.add_parser(
        "validate",
        parents=[common_parser, mode_parser],
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
        parents=[common_parser, mode_parser],
        help="Generate a self-contained HTML portfolio report",
    )
    report_parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Output HTML file path "
            "(default: data/reports/report-<timestamp>-<mode>.html)"
        ),
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
        parents=[common_parser, mode_parser],
        help="Run full pipeline (docker runs locally; staging/prod trigger Step Functions)",
    )
    full_parser.add_argument(
        "--xtb-file",
        action="append",
        type=str,
        default=None,
        help=(
            "Path to XTB Excel report, or an s3:// URI (can be specified "
            "multiple times). Local/docker only: rejected in staging/prod, "
            "where XTB runs via 'upload-xtb' + the EventBridge file-arrival "
            "trigger instead."
        ),
    )
    full_parser.add_argument(
        "--with-xtb",
        action="store_true",
        default=False,
        help=(
            "Vestigial flag kept to reject accidental use: XTB is not part of "
            "the scheduled run at all. In staging/prod use 'upload-xtb' + the "
            "EventBridge file-arrival trigger instead."
        ),
    )
    full_parser.add_argument(
        "--wait",
        action="store_true",
        default=False,
        help=(
            "Poll the Step Functions execution and print failure details "
            "(staging/prod only; default timeout 900s, poll interval 30s)"
        ),
    )
    full_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )

    # upload-xtb
    upload_xtb_parser = subparsers.add_parser(
        "upload-xtb",
        parents=[mode_parser],
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
        parents=[common_parser, mode_parser],
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
        parents=[common_parser, mode_parser],
        help="Run consolidate then analytics (orchestrator task)",
    )
    run_consolidate_analytics_parser.add_argument(
        "--connectors",
        nargs="+",
        required=True,
        help="Enabled connector names for this execution",
    )
    run_consolidate_analytics_parser.add_argument(
        "--fx-rate",
        action="append",
        type=parse_fx_rate,
        default=[],
        help="Manual FX rate override as CURRENCY=RATE",
    )

    args = parser.parse_args()

    # keygen is a standalone utility that generates an encryption key without
    # touching storage or secrets, so it does not require --mode and must not
    # trigger storage resolution (which would raise with no mode set).
    if args.command != "keygen":
        # Set execution mode from --mode flag before storage resolution.
        set_mode(args.mode)

        # Resolve storage configuration before any path access — except for
        # the SFN-trigger path (full --mode staging|prod), which needs no S3
        # data-plane config on the caller's machine (broker secrets are
        # injected into ECS containers at runtime).
        # Decision: docs/adr/0091-trigger-step-functions-in-cmd-full.md
        if not (args.command == "full" and args.mode in ("staging", "prod")):
            from pipeline.storage import resolve_storage

            resolve_storage()

    commands = {
        "keygen": cmd_keygen,
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
