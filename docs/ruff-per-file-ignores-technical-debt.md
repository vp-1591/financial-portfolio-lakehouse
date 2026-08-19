# Ruff Per-File Ignores Technical Debt

Three ruff rules are suppressed for the `tests/**` glob via `pyproject.toml` per-file-ignores to unblock CI with ruff 0.16.0. This is temporary — each rule should be evaluated and the ignores replaced with targeted `# noqa` comments or code fixes.

## Rules

| Rule | Message | Sites |
|---|---|---|
| TRY002 | Create your own exception | 7 |
| B017 | `pytest.raises(Exception)` or `assertRaises(Exception)` is too broad | 2 |
| S110 | `try...except Exception` + `pass` — use logging instead | 1 |

## TRY002 — Create your own exception (7 sites)

All 7 sites are `raise Exception(...)` inside mock functions in test fixtures, not production code. They simulate failure paths of patched dependencies (e.g., `DeltaTable` raising on missing data, unknown paths in mock routing).

| File | Line | Pattern | Context |
|---|---|---|---|
| `tests/test_consolidate_events.py` | 109 | `raise Exception("no data")` | Mock DeltaTable failure for XTB |
| `tests/test_consolidate_events.py` | 114 | `raise Exception("unknown path")` | Mock DeltaTable routing fallback |
| `tests/test_consolidate_events.py` | 165 | `raise Exception("unknown path")` | Mock DeltaTable routing fallback |
| `tests/test_consolidate_events.py` | 211 | `raise Exception("unknown path")` | Mock DeltaTable routing fallback |
| `tests/test_consolidate_events.py` | 266 | `raise Exception("no data")` | Mock DeltaTable failure for XTB |
| `tests/test_consolidate_events.py` | 271 | `raise Exception("unknown path")` | Mock DeltaTable routing fallback |
| `tests/test_consolidate_events.py` | 333 | `raise Exception("unknown path")` | Mock DeltaTable routing fallback |

**Recommended fix:** Define a local test helper exception (e.g., `_MockDeltaError`) and use it instead of `Exception`. This makes the mock intent explicit while keeping tests readable.

## B017 — pytest.raises(Exception) is too broad (2 sites)

Both sites test that Fernet decryption fails with the wrong key or a tampered ciphertext. The underlying exception type is an implementation detail of the `cryptography` library.

| File | Line | Context |
|---|---|---|
| `tests/test_crypto.py` | 56 | `pytest.raises(Exception)` for wrong-key decryption |
| `tests/test_crypto.py` | 63 | `pytest.raises(Exception)` for tampered ciphertext decryption |

**Recommended fix:** Narrow to `cryptography.fernet.InvalidToken` (or catch the base `Exception` but add `# noqa: B017` with a comment that the exception type is a library implementation detail).

## S110 — try/except Exception with pass (1 site)

| File | Line | Context |
|---|---|---|
| `tests/test_query_list_tables.py` | 320 | `except Exception: pass` to tolerate missing storage in a cache-warmup step |

**Recommended fix:** Replace with `except Exception:  # noqa: S110 — optional cache warmup, failure is acceptable` or narrow to the expected exception type if one is known.

## Recommended Approach

1. **TRY002:** Create a `_MockDeltaError(Exception)` helper in `test_consolidate_events.py` and replace all 7 `raise Exception(...)` calls. Remove TRY002 from per-file-ignores.
2. **B017:** Narrow the 2 `pytest.raises(Exception)` sites to `pytest.raises(InvalidToken)` (imported from `cryptography.fernet`). Remove B017 from per-file-ignores.
3. **S110:** Add a `# noqa: S110` comment with justification at the single site. Remove S110 from per-file-ignores.
4. Remove the entire `[tool.ruff.lint.per-file-ignores]` section from `pyproject.toml` once all three rules are addressed.