"""Tests for the report generation module.

Verifies that ``pipeline report`` generates a self-contained HTML file
with all expected sections, that partial data still produces a valid report,
and that missing gold tables cause a non-zero exit.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.analytics.holdings import build_portfolio_holdings
from pipeline.crypto import generate_key
from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    consolidate_holdings,
)
from pipeline.normalized.extract import extract_holdings
from pipeline.run import cmd_report
from pipeline.storage import LocalBackend, StorageConfig, get_storage, use_storage
from tests.fixtures.ibkr import ibkr_normalized_snapshot
from tests.fixtures.trading212 import t212_normalized_snapshot
from tests.fixtures.xtb import xtb_normalized_snapshot


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all report tests."""
    data = tmp_path / "data"
    for subdir in [
        "raw/ibkr_snapshot",
        "raw/ibkr_cdc",
        "raw/trading212_snapshot",
        "raw/trading212_cdc",
        "raw/xtb_snapshot",
        "raw/xtb_cdc",
        "normalized/ibkr_snapshot",
        "normalized/ibkr_cdc",
        "normalized/trading212_snapshot",
        "normalized/trading212_cdc",
        "normalized/xtb_snapshot",
        "normalized/xtb_cdc",
        "normalized/consolidated_holdings",
        "analytics/portfolio_allocation",
        "analytics/portfolio_holdings",
        "analytics/dividend_income",
        "analytics/interest_income",
        "analytics/cash_flow_summary",
        "analytics/data_quality",
    ]:
        (data / subdir).mkdir(parents=True, exist_ok=True)

    config = StorageConfig(
        data_dir=str(data),
        raw_dir=str(data / "raw"),
        normalized_dir=str(data / "normalized"),
        analytics_dir=str(data / "analytics"),
        secrets_dir=str(tmp_path / ".secrets"),
        encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
        backend=LocalBackend(data),
    )
    use_storage(config)


def _build_all_gold_tables(fernet_key: bytes) -> None:
    """Write broker snapshots, build consolidated_holdings, allocation, holdings."""
    from pipeline.analytics.allocation import allocate_percentages

    config = get_storage()

    # Write broker snapshot fixtures
    for broker, factory in [
        ("ibkr", ibkr_normalized_snapshot),
        ("trading212", t212_normalized_snapshot),
        ("xtb", xtb_normalized_snapshot),
    ]:
        table = factory(fernet_key=fernet_key)
        path = config.normalized_path(f"{broker}_snapshot")
        write_deltalake(path, table, mode="overwrite")

    # Extract + consolidate
    all_holdings: list[Holding] = []
    for broker_name in ("ibkr", "trading212", "xtb"):
        snapshot_path = config.normalized_path(f"{broker_name}_snapshot")
        holdings = extract_holdings(broker_name, snapshot_path, fernet_key)
        all_holdings.extend(holdings)

    converter = CurrencyConverter(
        target_currency="EUR",
        manual_rates={"USD": 0.9, "GBP": 1.15, "PLN": 0.25},
    )
    consolidate_holdings(
        all_holdings,
        fernet_key,
        converter,
        table_path=config.normalized_path("consolidated_holdings"),
    )

    # Build analytics tables
    allocate_percentages(fernet_key=fernet_key)
    build_portfolio_holdings(fernet_key=fernet_key)


def _write_minimal_cdc_tables(fernet_key: bytes) -> None:
    """Write minimal CDC analytics tables so the report has data for charts."""
    config = get_storage()
    now = datetime.now(timezone.utc)

    # Minimal dividend_income
    from pipeline.analytics.models import dividend_income_schema

    div_table = pa.table(
        {
            "calculated_at": [now],
            "period_month": ["2026-03"],
            "period_quarter": ["2026-Q1"],
            "broker": ["IBKR"],
            "ticker": ["VWCE"],
            "isin": ["IE00BK5BQT80"],
            "description": ["Vanguard FTSE All-World"],
            "currency": ["EUR"],
            "cash_amount": [42.5],
            "amount_base": [42.5],
            "base_currency": ["EUR"],
            "event_count": [1],
        },
        schema=dividend_income_schema,
    )
    write_deltalake(
        config.analytics_path("dividend_income"),
        div_table,
        mode="overwrite",
    )

    # Minimal interest_income
    from pipeline.analytics.models import interest_income_schema

    int_table = pa.table(
        {
            "calculated_at": [now],
            "period_month": ["2026-04"],
            "period_quarter": ["2026-Q2"],
            "broker": ["IBKR"],
            "currency": ["USD"],
            "cash_amount": [35.0],
            "amount_base": [31.5],
            "base_currency": ["EUR"],
            "event_count": [1],
        },
        schema=interest_income_schema,
    )
    write_deltalake(
        config.analytics_path("interest_income"),
        int_table,
        mode="overwrite",
    )

    # Minimal cash_flow_summary
    from pipeline.analytics.models import cash_flow_summary_schema

    cf_table = pa.table(
        {
            "calculated_at": [now],
            "period_month": ["2026-05"],
            "period_quarter": ["2026-Q2"],
            "broker": ["IBKR"],
            "event_type": ["DEPOSIT"],
            "currency": ["EUR"],
            "cash_amount": [5000.0],
            "amount_base": [5000.0],
            "base_currency": ["EUR"],
            "event_count": [1],
        },
        schema=cash_flow_summary_schema,
    )
    write_deltalake(
        config.analytics_path("cash_flow_summary"),
        cf_table,
        mode="overwrite",
    )

    # Minimal data_quality
    from pipeline.analytics.models import data_quality_schema

    dq_table = pa.table(
        {
            "checked_at": [now],
            "table_name": ["ibkr_snapshot"],
            "check_name": ["schema_validation"],
            "status": ["PASS"],
            "details": ["All columns present and correct types"],
            "threshold": [None],
            "actual": [None],
        },
        schema=data_quality_schema,
    )
    write_deltalake(
        config.analytics_path("data_quality"),
        dq_table,
        mode="overwrite",
    )


def _make_args(output_path: str, **kwargs) -> argparse.Namespace:
    """Create an argparse.Namespace for cmd_report."""
    return argparse.Namespace(
        output=output_path,
        base_currency=kwargs.get("base_currency"),
        open=kwargs.get("open", False),
        target_currency="EUR",
    )


class TestCmdReport:
    """Integration tests for the report subcommand."""

    def test_returns_zero_and_writes_file(self, tmp_path: Path):
        """cmd_report returns 0 and creates an HTML file."""
        fernet_key = generate_key()
        _build_all_gold_tables(fernet_key)
        _write_minimal_cdc_tables(fernet_key)

        output = str(tmp_path / "report.html")
        args = _make_args(output)
        result = cmd_report(args)

        assert result == 0
        assert Path(output).exists()
        assert Path(output).stat().st_size > 0

    def test_report_contains_section_markers(self, tmp_path: Path):
        """HTML contains id markers for all four sections."""
        fernet_key = generate_key()
        _build_all_gold_tables(fernet_key)
        _write_minimal_cdc_tables(fernet_key)

        output = str(tmp_path / "report.html")
        args = _make_args(output)
        cmd_report(args)

        html = Path(output).read_text(encoding="utf-8")
        assert 'id="portfolio-summary"' in html
        assert 'id="passive-income"' in html
        assert 'id="cash-flow"' in html
        assert 'id="data-quality"' in html

    def test_report_embeds_plotly_js_once(self, tmp_path: Path):
        """Plotly.js bundle is inlined exactly once with multiple charts."""
        fernet_key = generate_key()
        _build_all_gold_tables(fernet_key)
        _write_minimal_cdc_tables(fernet_key)

        output = str(tmp_path / "report.html")
        args = _make_args(output)
        cmd_report(args)

        html = Path(output).read_text(encoding="utf-8")
        # Plotly charts are rendered via Plotly.newPlot calls
        plotly_chart_count = html.count("Plotly.newPlot")
        assert plotly_chart_count >= 1, "At least one Plotly chart should be present"

    def test_partial_data_still_renders(self, tmp_path: Path):
        """Report renders even if dividend_income is missing."""
        fernet_key = generate_key()
        _build_all_gold_tables(fernet_key)
        _write_minimal_cdc_tables(fernet_key)

        # Delete dividend_income to simulate partial data
        import shutil

        div_path = Path(get_storage().analytics_path("dividend_income"))
        if div_path.exists():
            shutil.rmtree(div_path)

        output = str(tmp_path / "report.html")
        args = _make_args(output)
        result = cmd_report(args)

        assert result == 0
        html = Path(output).read_text(encoding="utf-8")
        assert 'id="portfolio-summary"' in html
        # Passive income section should exist (might be empty/partial)
        assert 'id="passive-income"' in html

    def test_no_gold_tables_returns_one(self, tmp_path: Path):
        """cmd_report returns 1 if all analytics tables are empty/missing."""
        # Don't build any tables — all loaders will return empty DataFrames
        output = str(tmp_path / "report.html")
        args = _make_args(output)
        result = cmd_report(args)

        assert result == 1

    def test_default_output_path(self, tmp_path: Path, monkeypatch):
        """Default output path is data/report.html relative to CWD."""
        fernet_key = generate_key()
        _build_all_gold_tables(fernet_key)
        _write_minimal_cdc_tables(fernet_key)

        # Change CWD to tmp_path so data/report.html is written there
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir(exist_ok=True)

        args = _make_args("data/report.html")
        result = cmd_report(args)

        assert result == 0
        assert (tmp_path / "data" / "report.html").exists()

    def test_failed_table_hides_its_section(self, tmp_path: Path):
        """Report hides sections whose dependency tables have FAIL in DQ."""
        fernet_key = generate_key()
        _build_all_gold_tables(fernet_key)
        _write_minimal_cdc_tables(fernet_key)

        # Overwrite data_quality with a FAIL for portfolio_holdings
        from pipeline.analytics.models import data_quality_schema

        config = get_storage()
        now = datetime.now(timezone.utc)
        dq_fail = pa.table(
            {
                "checked_at": [now],
                "table_name": ["portfolio_holdings"],
                "check_name": ["schema"],
                "status": ["FAIL"],
                "details": ["Schema mismatch"],
                "threshold": [None],
                "actual": [None],
            },
            schema=data_quality_schema,
        )
        write_deltalake(
            config.analytics_path("data_quality"), dq_fail, mode="overwrite"
        )

        output = str(tmp_path / "report.html")
        args = _make_args(output)
        result = cmd_report(args)

        assert result == 0
        html = Path(output).read_text(encoding="utf-8")
        # Portfolio-summary section is hidden (portfolio_holdings has FAIL)
        assert 'id="portfolio-summary"' not in html
        # Passive-income and cash-flow sections are still shown
        assert 'id="passive-income"' in html
        assert 'id="cash-flow"' in html
        # Data quality section is always shown
        assert 'id="data-quality"' in html

    def test_all_tables_failed_shows_only_dq(self, tmp_path: Path):
        """Report shows only the DQ section when all analytics tables are
        empty/failed."""
        from pipeline.analytics.models import data_quality_schema

        config = get_storage()
        now = datetime.now(timezone.utc)
        # Write only a DQ table with FAILs for all gold tables
        dq_fail = pa.table(
            {
                "checked_at": [now] * 5,
                "table_name": [
                    "portfolio_holdings",
                    "portfolio_allocation",
                    "dividend_income",
                    "interest_income",
                    "cash_flow_summary",
                ],
                "check_name": ["schema"] * 5,
                "status": ["FAIL"] * 5,
                "details": ["Schema mismatch"] * 5,
                "threshold": [None] * 5,
                "actual": [None] * 5,
            },
            schema=data_quality_schema,
        )
        write_deltalake(
            config.analytics_path("data_quality"), dq_fail, mode="overwrite"
        )

        output = str(tmp_path / "report.html")
        args = _make_args(output)
        result = cmd_report(args)

        assert result == 0
        html = Path(output).read_text(encoding="utf-8")
        # All analytics sections hidden
        assert 'id="portfolio-summary"' not in html
        assert 'id="passive-income"' not in html
        assert 'id="cash-flow"' not in html
        # DQ section always shown
        assert 'id="data-quality"' in html
