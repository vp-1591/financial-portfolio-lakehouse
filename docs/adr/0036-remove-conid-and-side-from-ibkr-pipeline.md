# 0036 — Remove `conid` and `side` from IBKR pipeline

## Context

The IBKR Flex connector stored two fields that are not consumed by any
downstream logic:

- **`conid`** (IBKR contract ID) was included in the normalized schema
  (`ibkr_snapshot_normalized_schema`) and passed through
  `pipeline/connectors/ibkr/transform.py` → `pipeline/normalized/extract.py`.
  In `extract.py`, `conid` was used as a fallback identifier (`CONID:<conid>`)
  when ISIN was missing. However, for equities and ETFs (the pipeline's current
  asset-class scope), ISIN is always present in Flex responses, so the fallback
  path was never reached in production. ADR 0002 originally specified
  `IBKR:<conid>` as the identifier strategy for IBKR rows, but the actual code
  had already shifted to `ISIN:<isin>` as primary with `CONID:<conid>` as
  fallback.

- **`side`** (`Long`/`Short` attribute on `<OpenPosition>`) was parsed by
  `parse_positions()` (which captures all XML attributes) but never read by any
  transformation, extraction, or reporting logic.

Both fields increased the Flex Query surface area (users had to tick them in the
IBKR editor) and the schema width, without providing runtime value.

Additionally, the Flex Query required-fields doc had an ambiguous
"Required (recommended)" label on the Currency Conversion Rate section, and was
missing `accountId` from the Open Positions and Cash Report tables despite the
pipeline relying on it for multi-account support.

## Decision

Remove `conid` and `side` from the IBKR pipeline entirely:

- **`pipeline/normalized/models.py`**: remove `pa.field("conid", pa.string())`
  from `ibkr_snapshot_normalized_schema`.
- **`pipeline/connectors/ibkr/transform.py`**: remove the `conids` list,
  `conid` read, `conid` output column, and `conid` from the
  `_flex_position_label` fallback chain. The label fallback is now `symbol →
  description → "UNKNOWN"` (was `symbol → description → conid → "UNKNOWN"`).
- **`pipeline/normalized/extract.py`**: remove the `CONID:<conid>` fallback
  identifier. IBKR positions now use `ISIN:<isin>` exclusively; positions
  without ISIN have an empty identifier.
- **Tests and fixtures**: remove `conid` attributes from all test XML strings,
  expected-output dicts, and assertions. Remove `side` attributes from test XML.

Update `docs/ibkr/flex-query-required-fields.md`:

- Drop `conid` and `side` from the Open Positions table.
- Add `accountId` as Required to both Open Positions and Cash Report tables.
- Change Currency Conversion Rate from "Required (recommended)" to "Required"
  with a note that without it, non-base-currency cash converts at 1:1.
- Add cross-reference to `docs/_vendor/ibkr/flex-query-fields.md`.

Reorganise vendor reference material:

- Move `docs/docs.ibkr/` → `docs/_vendor/ibkr/`.
- Move `docs/docs.trading212.com/` → `docs/_vendor/trading212/`.

The `_vendor` prefix makes it clear these are external reference documents,
not project-authored content.

## Consequences

- The normalized IBKR snapshot schema has one fewer column (`conid`). Existing
  Delta tables that still have the column will simply have it ignored on the
  next overwrite — no migration needed.
- Positions without ISIN will have an empty identifier instead of a
  `CONID:<conid>` fallback. For the current scope (equities and ETFs), this is
  acceptable because IBKR Flex always provides ISIN for these asset classes.
  If the pipeline is extended to options, futures, or bonds in the future, an
  alternative identifier strategy will be needed (separate ADR).
- The Flex Query configuration is simpler: users no longer need to tick
  "Conid" or "Side" in the Open Positions section.
- ADR 0002's IBKR-specific guidance (`IBKR:<conid>` identifier, contract
  enrichment via Client Portal) is superseded by this change. The ADR's
  Trading 212 and XTB sections remain unaffected.
- The `side` attribute is no longer parsed from XML in a way that any logic
  depends on (it still appears in the raw `dict(pos.attrib)` result from
  `parse_positions`, but is never consumed).

## Validation

- All 239 tests pass after the edits.
- `ruff check --fix .` and `ruff format .` produce no changes.
- The full test suite passes after re-running post-lint.