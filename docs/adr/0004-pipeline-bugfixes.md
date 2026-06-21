# 0004: Pipeline End-to-End Bugfixes

## Context

First end-to-end run of the medallion pipeline exposed three bugs:

1. **T212 CDC kwargs leak**: `cmd_fetch` passed the same `connector_kwargs` dict to
   both `fetch_snapshot()` and `fetch_cdc()`. The `include_metadata` parameter is
   valid for `fetch_snapshot` but not for `fetch_cdc`, causing `TypeError`.

2. **PyArrow `set_column()` API change**: PyArrow >= 24 removed the 4-argument
   overload `set_column(i, name, type, data)`. Only the 3-argument form
   `set_column(i, field_, column)` remains. The `encrypt_raw_payloads()` function
   in `raw/ingest.py` used the old 4-arg form and crashed.

3. **Missing parent directories**: On first run, `data/` subdirectories don't
   exist. Delta table writes fail with `Os { code: 3, kind: NotFound }` when
   the target directory hasn't been created yet.

Additionally, the `@register` decorator on connector classes was calling the
class as a constructor, but `__init__.py` files also instantiated manually,
causing a `TypeError: 'X' object is not callable` double-instantiation bug.
The registry was updated to auto-instantiate classes passed to `register()`.

## Decision

- **Separate snapshot/CDC kwargs**: `cmd_fetch` now builds `snapshot_kwargs`
  and `cdc_kwargs` separately. For Trading 212, `include_metadata` is only in
  `snapshot_kwargs`.

- **Fix `set_column()` call**: Changed `encrypt_raw_payloads()` to use the
  3-argument form: `table.set_column(idx, "payload", pa.array(encrypted,
  type=pa.binary()))`.

- **Create parent directories**: Added `Path(table_path).parent.mkdir(parents=True,
  exist_ok=True)` before all Delta table writes in `ingest_raw()`,
  `consolidate_holdings()`, `allocate_percentages()`, `cmd_fetch()`, and
  `cmd_transform()`.

- **Auto-instantiate in registry**: `register()` now detects if a class (not
  instance) is passed and instantiates it automatically. Removed manual
  `connector = XxxConnector()` from each `__init__.py`.

- **Added `--ibkr` flag**: IBKR connector is opt-in via `--ibkr`, matching the
  pattern of T212 (`--t212-api-key`) and XTB (`--xtb-file`).

- **Added `--t212-base-url`**: Missing CLI argument that was referenced in
  `cmd_fetch` but not defined in `add_trading212_args()`.

- **Added `pandas>=2.0`** to `pyproject.toml` pipeline dependencies, since
  transform modules import it.

## Consequences

- Pipeline can now run end-to-end without crashes on first-run scenarios
- Connector registry correctly handles both class and instance registration
- All 123 tests pass (117 existing + 6 new integration tests)

## Validation

- `test_encrypt_payloads_roundtrip`: verifies `set_column()` works with PyArrow 24
- `test_cdc_does_not_receive_include_metadata`: verifies T212 CDC gets correct kwargs
- `test_ingest_raw_creates_parent_dirs`: verifies Delta writes create missing directories
- `test_consolidate_creates_parent_dirs`: verifies consolidate path creation
- `test_allocate_creates_parent_dirs`: verifies analytics path creation