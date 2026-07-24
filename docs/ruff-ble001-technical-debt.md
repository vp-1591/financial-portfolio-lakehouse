# Ruff BLE001 Technical Debt

BLE001 ("Do not catch blind exception: `Exception`") is currently suppressed project-wide via `pyproject.toml` to unblock CI with ruff 0.16.0. This is temporary — the rule should be re-enabled and individual sites addressed.

## Why It Was Suppressed

ruff 0.16.0 introduced new rules that flagged 35 `except Exception` sites. Of these:

- **4 sites** can be narrowed to `except (DeltaTableError, OSError)` — these open DeltaTable objects that may not exist yet.
- **31 sites** are deliberate broad catches for resilience patterns: graceful degradation (missing tables, decryption failures), best-effort cleanup (S3 staging deletes), per-row/per-endpoint error isolation (API calls, currency conversion), CLI catch-alls, and thread pool fail-fast.

## Sites That Can Be Narrowed

| File | Line | Current | Should narrow to |
|---|---|---|---|
| `pipeline/analytics/holdings.py` | 71 | `except Exception` | `except (DeltaTableError, OSError)` |
| `pipeline/analytics/cdc_tables.py` | 197 | `except Exception` | `except (DeltaTableError, OSError)` |
| `pipeline/normalized/consolidate_cdc.py` | 55 | `except Exception` | `except (DeltaTableError, OSError)` |
| `pipeline/normalized/normalize.py` | 86 | `except Exception` | `except (DeltaTableError, OSError)` |

## Recommended Approach

1. Narrow the 4 sites above to specific exception types.
2. For the remaining 31 sites, add `# noqa: BLE001` with a brief comment explaining why the broad catch is intentional (e.g., `# noqa: BLE001  # graceful degradation for missing table`).
3. Remove `BLE001` from the `pyproject.toml` ignore list.