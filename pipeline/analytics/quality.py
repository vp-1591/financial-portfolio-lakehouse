"""Data quality validation framework.

Runs a suite of diagnostic checks against Delta tables in the normalized and
analytics layers.  Each check produces a :class:`CheckResult` with a status of
PASS, FAIL, or WARN.  Results are persisted to the ``data_quality`` analytics
table (append mode) so that historical trends — particularly row-count
stability — can be tracked across runs.

FAIL-level issues (schema mismatch, nulls in required fields) cause
``pipeline validate`` to exit non-zero, surfacing via Step Function status
tracking (ADR 0062).  WARN-level issues (row-count drops, stale data,
structural reconciliation mismatches) are logged but allow the pipeline to
continue.

Checks are **diagnostic** — they report problems but never drop or filter data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.analytics.models import (
    cash_flow_summary_schema,
    data_quality_schema,
    dividend_income_schema,
    interest_income_schema,
    portfolio_allocation_schema,
    portfolio_holdings_schema,
)
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    consolidated_holdings_schema,
    ibkr_snapshot_normalized_schema,
    trading212_snapshot_normalized_schema,
    xtb_snapshot_normalized_schema,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"


@dataclass
class CheckResult:
    """Result of a single quality check."""

    status: str  # PASS | FAIL | WARN
    details: str
    threshold: str | None = None
    actual: str | None = None


# ---------------------------------------------------------------------------
# Table schema & freshness-column registries
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: dict[str, pa.Schema] = {
    # Silver tables
    "consolidated_holdings": consolidated_holdings_schema,
    "cdc_events": cdc_events_normalized_schema,
    "ibkr_snapshot": ibkr_snapshot_normalized_schema,
    "trading212_snapshot": trading212_snapshot_normalized_schema,
    "xtb_snapshot": xtb_snapshot_normalized_schema,
    "ibkr_cdc": cdc_events_normalized_schema,
    "trading212_cdc": cdc_events_normalized_schema,
    "xtb_cdc": cdc_events_normalized_schema,
    # Gold tables
    "portfolio_allocation": portfolio_allocation_schema,
    "portfolio_holdings": portfolio_holdings_schema,
    "dividend_income": dividend_income_schema,
    "interest_income": interest_income_schema,
    "cash_flow_summary": cash_flow_summary_schema,
}

FRESHNESS_COLUMNS: dict[str, str] = {
    # Silver tables
    "consolidated_holdings": "fetched_at",
    "cdc_events": "fetched_at",
    "ibkr_snapshot": "fetched_at",
    "trading212_snapshot": "fetched_at",
    "xtb_snapshot": "fetched_at",
    "ibkr_cdc": "fetched_at",
    "trading212_cdc": "fetched_at",
    "xtb_cdc": "fetched_at",
    # Gold tables
    "portfolio_allocation": "calculated_at",
    "portfolio_holdings": "calculated_at",
    "dividend_income": "calculated_at",
    "interest_income": "calculated_at",
    "cash_flow_summary": "calculated_at",
}

# Fields that must never be null in each table.  Delta Lake schemas mark all
# fields as nullable for compatibility, so we maintain an explicit registry of
# semantically required fields instead of relying on the PyArrow nullable flag.
REQUIRED_FIELDS: dict[str, list[str]] = {
    # Silver tables
    "consolidated_holdings": [
        "fetched_at",
        "broker",
        "ticker",
        "currency",
        "value",
    ],
    "cdc_events": [
        "fetched_at",
        "broker",
        "event_id",
        "event_type",
        "cash_amount",
    ],
    "ibkr_snapshot": [
        "fetched_at",
        "account_id",
        "currency",
        "value",
    ],
    "trading212_snapshot": [
        "fetched_at",
        "account_id",
        "currency",
        "value",
    ],
    "xtb_snapshot": [
        "fetched_at",
        "account_id",
        "currency",
        "value",
    ],
    "ibkr_cdc": [
        "fetched_at",
        "broker",
        "event_id",
        "event_type",
        "cash_amount",
    ],
    "trading212_cdc": [
        "fetched_at",
        "broker",
        "event_id",
        "event_type",
        "cash_amount",
    ],
    "xtb_cdc": [
        "fetched_at",
        "broker",
        "event_id",
        "event_type",
        "cash_amount",
    ],
    # Gold tables
    "portfolio_allocation": [
        "calculated_at",
        "ticker",
        "percentage",
        "broker",
    ],
    "portfolio_holdings": [
        "calculated_at",
        "broker",
        "ticker",
        "currency",
        "value",
        "base_currency",
        "position_type",
    ],
    "dividend_income": [
        "calculated_at",
        "period_month",
        "broker",
        "currency",
        "cash_amount",
        "event_count",
    ],
    "interest_income": [
        "calculated_at",
        "period_month",
        "broker",
        "currency",
        "cash_amount",
        "event_count",
    ],
    "cash_flow_summary": [
        "calculated_at",
        "period_month",
        "broker",
        "event_type",
        "currency",
        "cash_amount",
        "event_count",
    ],
}


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------


def check_schema(
    table_name: str,
    arrow_table: pa.Table,
    expected: pa.Schema,
) -> CheckResult:
    """Verify that *arrow_table* matches *expected* schema."""
    if arrow_table.schema.equals(expected, check_metadata=False):
        return CheckResult(status=PASS, details="Schema matches expected")

    actual_fields = {f.name: f.type for f in arrow_table.schema}
    expected_fields = {f.name: f.type for f in expected}

    issues: list[str] = []
    for name, typ in expected_fields.items():
        if name not in actual_fields:
            issues.append(f"missing field: {name}")
        elif actual_fields[name] != typ:
            issues.append(
                f"type mismatch for {name}: expected {typ}, got {actual_fields[name]}"
            )
    for name in actual_fields:
        if name not in expected_fields:
            issues.append(f"extra field: {name}")

    return CheckResult(
        status=FAIL,
        details="; ".join(issues) if issues else "Schema mismatch",
    )


def check_required_nulls(
    table_name: str,
    arrow_table: pa.Table,
    expected: pa.Schema,
) -> CheckResult:
    """Verify that semantically required fields have no null values.

    Delta Lake schemas mark all fields as nullable for compatibility, so this
    check uses the :data:`REQUIRED_FIELDS` registry instead of the PyArrow
    schema's ``nullable`` flag.
    """
    required = REQUIRED_FIELDS.get(table_name, [])
    if not required:
        return CheckResult(status=PASS, details="No required fields registered")

    null_issues: list[str] = []
    for field_name in required:
        if field_name not in arrow_table.column_names:
            # Already caught by schema check — skip here
            continue
        col = arrow_table.column(field_name)
        null_count = col.null_count
        if null_count > 0:
            null_issues.append(f"{field_name}: {null_count} nulls")

    if not null_issues:
        return CheckResult(status=PASS, details="No nulls in required fields")

    return CheckResult(
        status=FAIL,
        details="Nulls in required fields: " + "; ".join(null_issues),
    )


def check_row_count_stability(
    table_name: str,
    arrow_table: pa.Table,
    previous_count: int | None,
) -> CheckResult:
    """Verify that row count hasn't dropped >50% vs previous run."""
    current = arrow_table.num_rows
    if previous_count is None:
        return CheckResult(
            status=PASS,
            details="First run, no previous count to compare",
            actual=str(current),
        )

    if current < previous_count * 0.5:
        return CheckResult(
            status=WARN,
            details=f"Row count dropped from {previous_count} to {current} (>50% drop)",
            threshold=">50% drop",
            actual=str(current),
        )

    return CheckResult(
        status=PASS,
        details=f"Row count stable: {current} (previous: {previous_count})",
        actual=str(current),
    )


def check_freshness(
    table_name: str,
    arrow_table: pa.Table,
    freshness_column: str,
    freshness_days: int,
) -> CheckResult:
    """Verify that the latest timestamp in *freshness_column* is within threshold."""
    if freshness_column not in arrow_table.column_names:
        return CheckResult(
            status=WARN,
            details=f"Freshness column '{freshness_column}' not found in table",
        )

    col = arrow_table.column(freshness_column)
    if col.length == 0:
        return CheckResult(
            status=WARN,
            details=f"Table {table_name} is empty, cannot check freshness",
        )

    # Convert to polars for easy max
    df = pl.from_arrow(arrow_table)
    max_ts = df.select(pl.col(freshness_column).max()).item()
    if max_ts is None:
        return CheckResult(
            status=WARN,
            details=f"Freshness column '{freshness_column}' has all null values",
        )

    # Ensure timezone-aware comparison
    cutoff = datetime.now(timezone.utc) - timedelta(days=freshness_days)
    if isinstance(max_ts, datetime):
        if max_ts.tzinfo is None:
            # Assume UTC if no timezone info
            max_ts = max_ts.replace(tzinfo=timezone.utc)
    else:
        # String timestamp — parse it
        max_ts = datetime.fromisoformat(str(max_ts)).replace(tzinfo=timezone.utc)

    if max_ts < cutoff:
        age_days = (datetime.now(timezone.utc) - max_ts).days
        return CheckResult(
            status=WARN,
            details=f"Data is {age_days} days old (threshold: {freshness_days} days)",
            threshold=f"<{freshness_days} days old",
            actual=f"{age_days} days old",
        )

    age_days = (datetime.now(timezone.utc) - max_ts).days
    return CheckResult(
        status=PASS,
        details=f"Data is {age_days} days old (within {freshness_days}-day threshold)",
    )


def check_reconciliation(
    table_name: str,
    arrow_table: pa.Table,
    cdc_table: pa.Table | None,
) -> CheckResult:
    """Structural reconciliation: every broker in holdings appears in CDC events."""
    if table_name != "consolidated_holdings":
        # Only consolidated_holdings has structural reconciliation for now
        return CheckResult(
            status=PASS,
            details="Reconciliation not applicable for this table",
        )

    if cdc_table is None:
        return CheckResult(
            status=WARN,
            details="CDC events table not available for reconciliation",
        )

    holdings_df = pl.from_arrow(arrow_table)
    cdc_df = pl.from_arrow(cdc_table)

    holdings_brokers = set(holdings_df["broker"].unique().to_list())
    cdc_brokers = set(cdc_df["broker"].unique().to_list())

    missing = holdings_brokers - cdc_brokers
    if missing:
        return CheckResult(
            status=WARN,
            details=f"Brokers in holdings but not in CDC events: {sorted(missing)}",
            threshold="All holdings brokers present in CDC",
            actual=f"Missing: {sorted(missing)}",
        )

    # Currency coverage check
    holdings_currencies = set(holdings_df["currency"].unique().to_list())
    # For CDC, amount_base is encrypted — use currency column instead
    cdc_currencies = (
        set(cdc_df["currency"].unique().to_list())
        if "currency" in cdc_df.columns
        else set()
    )

    uncovered = holdings_currencies - cdc_currencies
    if uncovered:
        return CheckResult(
            status=WARN,
            details=f"Currencies in holdings but not in CDC events: {sorted(uncovered)}",
            threshold="Holdings currency set ⊆ CDC currency set",
            actual=f"Uncovered: {sorted(uncovered)}",
        )

    return CheckResult(
        status=PASS,
        details="All holdings brokers and currencies present in CDC events",
    )


# ---------------------------------------------------------------------------
# Previous row-count lookup
# ---------------------------------------------------------------------------


def _get_previous_row_count(
    table_name: str,
    storage_options: dict[str, str] | None,
) -> int | None:
    """Look up the most recent row-count stability check result for *table_name*.

    Returns the previous ``actual`` count as an integer, or ``None`` if no
    previous result exists (first run).
    """
    try:
        dq_path_func = (
            __import__("pipeline.storage", fromlist=["get_storage"])
            .get_storage()
            .analytics_path("data_quality")
        )
        dt = DeltaTable(dq_path_func, storage_options=storage_options)
        dq_df = pl.from_arrow(dt.to_pyarrow_table())
    except Exception:
        return None

    stability_rows = dq_df.filter(
        (pl.col("table_name") == table_name)
        & (pl.col("check_name") == "row_count_stability")
        & (pl.col("status") != FAIL)
    ).sort("checked_at", descending=True)

    if stability_rows.is_empty():
        return None

    actual_str = stability_rows.row(0, named=True)["actual"]
    try:
        return int(actual_str)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_validation(
    fernet_key: bytes | None = None,
    freshness_days: int = 7,
    fail_on_warn: bool = False,
    tables: list[str] | None = None,
) -> int:
    """Run quality checks and persist results.

    Parameters
    ----------
    fernet_key:
        Fernet key for decrypting value columns.  When *None*, loaded from
        the default location.
    freshness_days:
        Maximum age in days for data to be considered fresh.
    fail_on_warn:
        If ``True``, WARN results cause a non-zero exit code.
    tables:
        If provided, validate only the named tables.  Unknown names produce a
        WARN.  When ``None`` (default), validate all registered tables.

    Returns
    -------
    int
        Exit code: 0 if all PASS (or WARN without ``fail_on_warn``),
        1 otherwise.
    """
    from pipeline.storage import get_storage

    storage = get_storage()
    storage_options = storage.storage_options

    if fernet_key is None:
        from pipeline.crypto import load_key

        fernet_key = load_key()

    # Master dict of all validatable tables: name -> Delta table path.
    # data_quality is excluded — it is the output table, not an input.
    all_tables: dict[str, str] = {
        # Silver tables
        "consolidated_holdings": storage.normalized_path("consolidated_holdings"),
        "cdc_events": storage.normalized_path("cdc_events"),
        "ibkr_snapshot": storage.normalized_path("ibkr_snapshot"),
        "trading212_snapshot": storage.normalized_path("trading212_snapshot"),
        "xtb_snapshot": storage.normalized_path("xtb_snapshot"),
        "ibkr_cdc": storage.normalized_path("ibkr_cdc"),
        "trading212_cdc": storage.normalized_path("trading212_cdc"),
        "xtb_cdc": storage.normalized_path("xtb_cdc"),
        # Gold tables
        "portfolio_allocation": storage.analytics_path("portfolio_allocation"),
        "portfolio_holdings": storage.analytics_path("portfolio_holdings"),
        "dividend_income": storage.analytics_path("dividend_income"),
        "interest_income": storage.analytics_path("interest_income"),
        "cash_flow_summary": storage.analytics_path("cash_flow_summary"),
    }

    # Filter to requested tables (or validate all)
    if tables is not None:
        validated_tables: dict[str, str] = {}
        for name in tables:
            if name in all_tables:
                validated_tables[name] = all_tables[name]
            else:
                logger.warning("Unknown table name: %s", name)
        # Include unknown names with a placeholder so they produce a WARN later
        for name in tables:
            if name not in all_tables and name not in validated_tables:
                validated_tables[name] = ""
    else:
        validated_tables = all_tables

    # Load CDC events for reconciliation (shared across checks)
    cdc_table: pa.Table | None = None
    try:
        cdc_dt = DeltaTable(
            storage.normalized_path("cdc_events"),
            storage_options=storage_options,
        )
        cdc_table = cdc_dt.to_pyarrow_table()
    except Exception:
        logger.warning("CDC events table not found, skipping reconciliation")

    all_results: list[CheckResult] = []
    result_metadata: list[tuple[str, str]] = []  # (table_name, check_name)

    for table_name, table_path in validated_tables.items():
        # Handle unknown table names (not in registry)
        if not table_path:
            result = CheckResult(
                status=WARN,
                details=f"Unknown table name: {table_name}",
            )
            all_results.append(result)
            result_metadata.append((table_name, "table_present"))
            continue

        try:
            dt = DeltaTable(table_path, storage_options=storage_options)
            arrow_table = dt.to_pyarrow_table()
        except Exception:
            logger.warning("Table %s not found, skipping", table_name)
            # Record a WARN for missing table
            result = CheckResult(
                status=WARN,
                details=f"Table {table_name} not found",
            )
            all_results.append(result)
            result_metadata.append((table_name, "table_present"))
            continue

        expected_schema = TABLE_SCHEMAS.get(table_name)
        if expected_schema is None:
            logger.warning("No schema registered for table %s", table_name)
            continue

        # 1. Schema check
        result = check_schema(table_name, arrow_table, expected_schema)
        all_results.append(result)
        result_metadata.append((table_name, "schema"))

        # 2. Required nulls check
        result = check_required_nulls(table_name, arrow_table, expected_schema)
        all_results.append(result)
        result_metadata.append((table_name, "required_nulls"))

        # 3. Row count stability
        previous_count = _get_previous_row_count(table_name, storage_options)
        result = check_row_count_stability(table_name, arrow_table, previous_count)
        all_results.append(result)
        result_metadata.append((table_name, "row_count_stability"))

        # 4. Freshness check
        freshness_col = FRESHNESS_COLUMNS.get(table_name)
        if freshness_col:
            result = check_freshness(
                table_name, arrow_table, freshness_col, freshness_days
            )
            all_results.append(result)
            result_metadata.append((table_name, "freshness"))

        # 5. Reconciliation (consolidated_holdings only)
        if table_name == "consolidated_holdings":
            result = check_reconciliation(table_name, arrow_table, cdc_table)
            all_results.append(result)
            result_metadata.append((table_name, "reconciliation"))

    # Persist results to data_quality table
    now = datetime.now(timezone.utc)
    records = {
        "checked_at": [now] * len(all_results),
        "table_name": [m[0] for m in result_metadata],
        "check_name": [m[1] for m in result_metadata],
        "status": [r.status for r in all_results],
        "details": [r.details for r in all_results],
        "threshold": [r.threshold for r in all_results],
        "actual": [r.actual for r in all_results],
    }
    result_table = pa.table(records, schema=data_quality_schema)

    dq_path = storage.analytics_path("data_quality")
    storage.backend.ensure_parent(dq_path)
    write_deltalake(
        dq_path,
        result_table,
        mode="append",
        storage_options=storage_options,
    )

    # Print summary
    pass_count = sum(1 for r in all_results if r.status == PASS)
    warn_count = sum(1 for r in all_results if r.status == WARN)
    fail_count = sum(1 for r in all_results if r.status == FAIL)

    print("\n=== Data Quality Summary ===")
    for (table_name, check_name), result in zip(result_metadata, all_results):
        print(f"  [{result.status}] {table_name}/{check_name}: {result.details}")
    print(f"\n  PASS: {pass_count}  WARN: {warn_count}  FAIL: {fail_count}")

    # Determine exit code
    if fail_count > 0:
        return 1
    if fail_on_warn and warn_count > 0:
        return 1
    return 0
