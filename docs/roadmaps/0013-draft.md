TODO: Big Refactor

 - Polars joins for snapshot instrument lookups —
 pipeline/connectors/ibkr/transform.py transform_snapshot() (lines 52-220),
 pipeline/connectors/xtb/transform.py, pipeline/connectors/trading212/transform.py;
 separate from this CDC bug fix
 - Replace iter_raw_payloads() — pipeline/connectors/transform_utils.py (lines 105-170);
 IBKR and XTB still use it; refactor in a separate step. Also applies to
 decrypt_cdc_payloads() in the same file (lines 242-296). Both extract columns via
 .to_pylist() instead of df.iter_rows(named=True)
 - Drop first_value() / nested_dict() from client.py —
 pipeline/connectors/ibkr/client.py first_value(), pipeline/connectors/trading212/client.py;
 still used by snapshot transform; refactor when snapshot moves to Polars
 - Deduplicate raw tables with Polars instead of manual Python sets —
 pipeline/raw/ingest.py dedup_raw() (lines 86-126) extracts 3 columns via .to_pylist(),
 builds a set of tuples, creates a boolean mask, then filters. Replace with Polars
 anti-join or unique()
 - Build raw tables with Polars instead of manual list accumulation —
 pipeline/raw/ingest.py build_raw_table() (lines 46-71) initializes 6 empty lists and
 appends in a loop. Replace with pl.DataFrame() from dict. Similarly,
 pipeline/normalized/consolidate.py consolidate_holdings() (lines 284-317) builds 10
 empty lists
 - Encrypt raw payloads with Polars instead of column extraction —
 pipeline/raw/ingest.py encrypt_raw_payloads() (lines 74-83) extracts column to pylist,
 encrypts, set_column(). Replace with with_columns(map_elements())
 - Replace PyArrow compute with Polars for filter_latest_snapshot() —
 pipeline/connectors/transform_utils.py (lines 56-87) uses pc.max() + pc.equal() + filter().
 One-liner: df.filter(pl.col("fetched_at") == pl.col("fetched_at").max())
 - Stop rebuilding pa.table after Polars aggregation —
 pipeline/analytics/cdc_tables.py build_dividend_income() (lines 372-389),
 build_interest_income() (lines 476-489), build_cash_flow_summary() (lines 575-589) all
 extract columns via .to_list() and manually rebuild pa.table(). Replace with
 agg.to_arrow().cast(schema). Same pattern in
 pipeline/analytics/holdings.py build_portfolio_holdings() (lines 151-160) with
 manual column-by-column casting loop
 - Accept pl.DataFrame in _write_analytics_table() —
 pipeline/analytics/cdc_tables.py (lines 247-256) does manual column casting on pa.Table.
 Since write_deltalake accepts pl.DataFrame, the PyArrow round-trip is unnecessary
 - Move test inline imports to file level — ~250 `from ... import` statements live inside
 test function bodies across ~25 test files (pre-existing style, not a DRY violation but
 non-idiomatic; PEP 8 wants top-level imports). Tests have no circular-import reason to
 defer. Verify the fixture-level `import pipeline.storage` / `import pipeline.secrets` lines
 aren't relying on import timing before moving them. Done opportunistically for the files
 touched by the LocalBackend move (ADR 0090); the rest is a separate sweep.