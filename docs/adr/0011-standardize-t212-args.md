# 11 — Standardize Trading 212 CLI args to t212 prefix

## Context

The argparse definitions in `portfolio_percentages.py` used `--t212-*` flags (e.g. `--t212-api-key`), which Python converts to `t212_api_key` attributes on the `Namespace` object. However, the `main()` function referenced these as `args.trading212_*` (e.g. `args.trading212_api_key`), causing an `AttributeError` at runtime. The README also documented the old `--trading212-*` flag names.

The pipeline connector args already use the `t212` prefix, so standardizing the script args to match eliminates the naming inconsistency.

## Decision

- Renamed all `args.trading212_*` attribute accesses in `main()` to `args.t212_*` to match the argparse flag definitions.
- Updated README CLI examples from `--trading212-api-key`/`--trading212-api-secret` to `--t212-api-key`/`--t212-api-secret`.
- Updated README prose reference from `--trading212-account-id` to `--t212-account-id`.

## Consequences

- The script now works correctly — argparse `--t212-*` flags map to `args.t212_*` attributes that are actually used.
- All Trading 212 CLI arguments now consistently use the `t212` prefix across both the pipeline and the scripts.
- This is a breaking change for anyone using the old `--trading212-*` flags, but those flags were already broken (they never matched the argparse definitions).

## Validation

- All 49 trading212 and pipeline integration tests pass.
- The pre-existing IBKR test failure (`test_load_assets_from_flex_response`) is unrelated.