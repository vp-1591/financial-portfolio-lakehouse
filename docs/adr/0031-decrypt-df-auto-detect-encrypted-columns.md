# 0031 — Make decrypt_df auto-detect encrypted columns

## Context

`decrypt_df()` only decrypted the hardcoded columns `["value", "quantity", "amount"]` (normalized-table float columns). When querying a raw table like `ibkr_snapshot_raw`, the encrypted column is `payload` — a Fernet-encrypted string (XML/JSON body), not a float. Users had to manually call `decrypt_string` with `load_key()`, which was clunky and error-prone.

## Decision

Changed `decrypt_df` to auto-detect all `Binary` columns in the DataFrame when `columns=None` (the default). The function now:

- Scans for `pl.Binary` columns automatically instead of relying on a hardcoded list
- Infers the return type per column by sampling the first non-null value — `Float64` for float data, `String` for string data (like `payload`)
- Rounds only float columns to 2 decimal places, leaving string columns untouched
- Accepts an explicit `columns` list as an override for edge cases

Removed the `_ENCRYPTED_COLUMNS` constant since it's no longer needed.

Added `_first_non_null` helper to sample the dtype without decrypting the entire column.

## Consequences

- **Breaking change (internal):** `_ENCRYPTED_COLUMNS` is removed. Any code importing it will break.
- **User-facing:** `decrypt_df(df)` now works universally on any table with encrypted binary columns — no need to specify columns or know which are float vs string.
- The explicit `columns` parameter still works for selective decryption.

## Validation

All 69 tests pass, including new tests:
- `test_auto_detect_binary_columns` — mixed float/string binary columns
- `test_auto_detect_no_binary_columns` — DataFrame with no binary columns returns unchanged
- `test_decrypt_string_column` — payload-like string decryption
- `test_no_rounding_on_string_columns` — string columns are not rounded