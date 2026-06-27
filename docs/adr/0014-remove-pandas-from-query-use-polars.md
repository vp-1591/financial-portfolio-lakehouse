# 0014: Remove pandas from query module, use Polars throughout

## Context

After replacing pandas with Polars in the transform pipeline (ADR 0013), `pipeline/query.py` was the only remaining module that imported and used pandas. The `decrypted_df()` function returned a pandas DataFrame and used `pd.set_option("display.float_format", ...)` to control float formatting.

Since the rest of the pipeline uses Polars, keeping pandas in the query module was inconsistent. Users asked whether they could do SQL-style transformations on the returned DataFrame — Polars supports this natively (`.filter()`, `.group_by()`, `.select()`, `.join()`, `.sort()`) without DuckDB.

## Decision

Replace all pandas usage in `pipeline/query.py` with Polars:

1. **`decrypted_df()`** now returns a `polars.DataFrame` instead of `pandas.DataFrame`
2. **`result.pl()`** replaces `result.df()` — DuckDB natively supports converting to Polars
3. **`pl.col().map_elements()`** replaces `df[col].apply()` for decryption
4. **`pl.col().round(2)`** replaces `df[col].round(2)` for float rounding
5. Removed `import pandas as pd` and `pd.set_option()` — Polars displays floats in fixed-point by default

Added a helper `_decrypt_value()` to keep the `map_elements` lambda clean.

## Consequences

- **No pandas anywhere in the project** — `pandas` is no longer imported by any pipeline module
- **`decrypted_df()` returns Polars** — users can chain Polars operations directly: `df.filter(pl.col("value") > 100).sort("value")`
- **SQL still available via `query()`** — returns a `DuckDBPyRelation` for raw SQL access
- **Notebook updated** — removed `import pandas as pd` from exploration notebook
- **CLAUDE.md created** — documents venv usage, Polars-only policy, and project conventions

## Validation

- All 176 tests pass
- `decrypted_df()` returns a Polars DataFrame with decrypted float values displayed in fixed-point format