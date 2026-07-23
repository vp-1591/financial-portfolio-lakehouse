TODO: Big Refactor

 - Polars joins for snapshot instrument lookups — affects transform_snapshot(), separate from
 this CDC bug fix
 - Replace iter_raw_payloads() entirely — IBKR and XTB still use it; refactor in a separate step
 - Drop first_value() / nested_dict() from client.py — still used by snapshot transform;
 refactor when snapshot moves to Polars
 - Move test inline imports to file level — ~250 `from ... import` statements live inside
 test function bodies across ~25 test files (pre-existing style, not a DRY violation but
 non-idiomatic; PEP 8 wants top-level imports). Tests have no circular-import reason to
 defer. Verify the fixture-level `import pipeline.storage` / `import pipeline.secrets` lines
 aren't relying on import timing before moving them. Done opportunistically for the files
 touched by the LocalBackend move (ADR 0090); the rest is a separate sweep.