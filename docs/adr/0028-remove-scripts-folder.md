# 0028 — Remove the `scripts/` folder

## Context

The `scripts/` folder contained 5 standalone CLI scripts that predated the `pipeline/` package:

- `scripts/ibkr_net_worth.py`
- `scripts/trading212_net_worth.py`
- `scripts/xtb_net_worth.py`
- `scripts/portfolio_connectors.py`
- `scripts/portfolio_percentages.py`

These scripts were the original entry points for broker data fetching and portfolio allocation. Over time, all their logic was migrated into the `pipeline/` package (connectors, transforms, run steps). This left the scripts as a redundant copy of functionality that already lived in `pipeline/`. Tests for the scripts also remained (`test_ibkr_net_worth.py`, `test_trading212_net_worth.py`, `test_xtb_net_worth.py`, `test_portfolio_percentages.py`, `test_live_fx.py`) even though the pipeline tests already covered the same behavior.

The duplication caused several problems:

- Tests used `sys.path.insert` hacks to import from `scripts/`.
- Some modules used lazy imports from `scripts/` as a bridge during migration.
- Having two code locations for the same broker logic made it unclear which was the canonical source.

## Decision

1. **Remove the entire `scripts/` folder.** All broker logic now lives exclusively in `pipeline/`. The scripts are no longer needed.

2. **Move `IbkrFlexClient` and Flex XML parsing helpers** from `scripts/ibkr_net_worth.py` into `pipeline/connectors/ibkr/client.py`. These were the only pieces that still had no pipeline counterpart.

3. **Remove legacy test files** (`test_ibkr_net_worth.py`, `test_trading212_net_worth.py`, `test_xtb_net_worth.py`, `test_portfolio_percentages.py`, `test_live_fx.py`). Their functionality is fully covered by pipeline tests.

## Consequences

- **Positive**: No more `sys.path.insert` hacks in tests. Import paths are clean and standard.
- **Positive**: No more lazy imports from `scripts/`. There is a single code location for all broker logic.
- **Positive**: Reduced maintenance burden — no need to keep scripts and pipeline in sync.
- **Negative**: Any user who was invoking `scripts/*.py` directly must switch to `python -m pipeline.run` commands.

## Validation

- All pipeline tests pass after the removal.
- `IbkrFlexClient` and Flex XML parsing helpers are accessible from `pipeline/connectors/ibkr/client.py`.
- Ruff linting and formatting pass cleanly.