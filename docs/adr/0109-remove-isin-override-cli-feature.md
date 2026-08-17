# 0109 — Remove ISIN Override CLI Feature

## Context

Since ADR 0002, the consolidation pipeline accepted optional ISIN overrides via
two CLI arguments — `--isin TICKER=ISIN` (repeatable) and `--isin-map-file PATH`
(a CSV of the same) — so a user with authoritative ISIN mappings could fill the
`Identifier` column for rows where a broker omitted ISIN. The override flowed
through `aggregate_percentages` / `consolidate_holdings` as an `isin_overrides`
dict, looked up per holding and formatted with `format_identifier("ISIN", …)`.

The XTB new-format connector overhaul (ADR 0108) changed XTB to emit
`identifier = f"TICKER:{ticker}"` — a truthy broker-native identifier for every
row. `consolidate_holdings` resolved the identifier with
`holding.identifier or format_identifier("ISIN", override_isin) if override_isin
else holding.identifier`. Because the new `TICKER:` identifier is truthy, the
`or` short-circuits and the override is silently ignored for every XTB row: the
feature was broken for the one broker it was most likely to be used on (XTB
exports often omit ISIN).

A max-effort code review of PR #125 surfaced this regression alongside the
broader override surface (`parse_isin_override`, the loading blocks in
`cmd_consolidate` / `cmd_analytics`, the `--isin` / `--isin-map-file`
registrations, and their tests).

## Decision

Remove the ISIN override CLI feature entirely. Do not fix the short-circuit
precedence; delete the feature.

Identifiers now come solely from the connector-provided `Holding.identifier`
(IBKR `IBKR:<conid>`, Trading 212 / XTB `ISIN:<value>` or `TICKER:<ticker>`),
with the existing `or "-"` fallback for rows where a broker provides none. No
CLI path exists to inject external ISIN mappings.

Reasoning:

- The override feature was never used in production. It existed as a
  convenience path, and the one broker where broker data most often omits ISIN
  (XTB) is precisely where it was silently broken by ADR 0108's broker-native
  identifier — so it was not even delivering its intended value.
- Any future equivalent (supplying authoritative identifiers for rows a broker
  omits) will not be delivered through CLI args. Building it back as a CLI flag
  would re-introduce the same short-circuit hazard against truthy broker-native
  identifiers and the same untested precedence. A future need, if it arises,
  deserves a design that integrates with the broker-native identifier model
  rather than a fallback `or` branch.
- Keeping a broken, unused feature is worse than removing it: the override code
  path carried real maintenance (CSV parsing, key normalization, two functions,
  tests) and a latent correctness trap for any future connector that sets a
  truthy identifier.

Also addressed in the same change (not the subject of this ADR's decision, but
shipped together): the connector registry gained an `unregister` path so tests
that register a `FakeConnector` can clean it up, closing a test-isolation leak
where the leaked `"fake"` broker added a skipped iteration to later tests that
derive candidates from `all_connectors()`.

## Constraints

- The broker-native identifier design from ADR 0002 — a generic `Identifier`
  column, IBKR rows as `IBKR:<conid>`, Trading 212 / XTB rows as `ISIN:<value>`
  when broker data provides it — remains unchanged (originally decided in ADR
  0002, §Decision). Only the override-input clause is reversed.
- The `identifier or "-"` fallback in `consolidate_holdings` and
  `aggregate_percentages` stays: rows where a broker provides no identifier
  still render `"-"`, not an empty string or an error.
- No connector changes: connectors continue to construct identifiers with
  f-strings; they never used `format_identifier` (that helper was override-only).
- The `full` and `run-consolidate-analytics` subparsers lose `--isin` and
  `--isin-map-file`; the plain `consolidate` and `analytics` subparsers never
  had them and are unaffected.

## Consequences

- **Real downside accepted:** there is no longer a CLI path to supply an
  authoritative ISIN for a row where a broker omits one. Such rows render
  `TICKER:<ticker>` (XTB) or `"-"` rather than an overridden `ISIN:<value>`. If
  this becomes a real production need, it must be re-designed against the
  broker-native identifier model, not restored as a CLI fallback.
- **Correctness trap removed:** no `or`-chain can silently swallow an override
  against a truthy broker-native identifier, because there is no override to
  swallow.
- **Smaller surface:** `parse_isin_override`, `format_identifier`,
  `normalize_isin_lookup_key`, two override-loading blocks, four
  `add_argument` calls, the `isin_overrides` parameters on
  `aggregate_percentages` / `consolidate_holdings`, and their tests are gone.
  `aggregate_percentages` is retained as a function (legacy / test-only) with
  its override parameter and logic stripped.
- **ADR 0065 constraint obsoleted:** ADR 0065 §Constraints required the
  `analytics` command to accept `--isin` and `--isin-map-file` "for allocation."
  That constraint was already stale (the `analytics` subparser never registered
  those flags) and is now fully obsolete — there are no such flags to accept.

## Validation

- `grep -rn "parse_isin_override\|format_identifier\|normalize_isin_lookup_key\|isin_overrides\|isin_map_file\|--isin\b" pipeline/ tests/` returns no hits (override surface fully removed; only this ADR's prose mentions the terms).
- `tests/test_consolidate.py`: `TestFormatIdentifier` (2 tests) and
  `test_fills_missing_isin_from_override_map` deleted; the non-override test
  `test_converts_and_groups_by_ticker_and_broker` retained and still passes
  against the stripped `aggregate_percentages` signature.
- `tests/test_run_subcommands.py`: the `isin=[]` / `isin_map_file=[]` keys
  removed from all argparse `Namespace` fixtures; no assertion referenced them.
- `tests/test_connector_registry.py::test_register_and_get_connector` now
  wraps `register(FakeConnector())` in `try/finally: unregister("fake")`, so
  `"fake"` no longer leaks into `all()`.
- Full checks green: `ruff check --fix . && ruff format .` clean, `pyright
  pipeline/ tests/` reports 0 errors / 0 warnings / 0 informations, `pytest
  tests/ -q -rf` reports 782 passed (down 3 from 785 — the two
  `TestFormatIdentifier` tests plus `test_fills_missing_isin_from_override_map`).