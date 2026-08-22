# Retention and Events Contract

## Broker policy

| Broker | Logical retention identity | Required retained state | Main risk |
| --- | --- | --- | --- |
| IBKR | `source` | Latest report per source; normalized events retain the durable event history | Flex query window moves |
| Trading 212 | endpoint `source` | Latest complete response per endpoint | Assumes endpoint completeness |
| XTB | nullable raw `account_id` | Latest report per account | Filename may not provide an account ID |

`source` remains a routing value. Raw `account_id` is nullable: XTB derives it from the report filename during fetch, while IBKR and Trading 212 leave it null. XTB transform parses a raw payload when the field is null and uses `fetched_at` plus `payload_hash` if a deterministic tie-break is needed.

## Delta retention facts

Delta Lake and delta-rs use a default one-week deleted-file retention threshold and a default 30-day transaction-log retention. The deleted-file threshold is relevant only when files become unreferenced through delete, overwrite, compaction, or similar operations and VACUUM is run. Append-only bronze writes do not delete old rows, and VACUUM is not automatically triggered by Delta.

Therefore, “seven-day Delta retention” does not by itself implement seven-day bronze retention. Before every fetch, retention maintenance must remove or compact out-of-policy rows/files and then run VACUUM. This matters both for bounded storage and for keeping subsequent raw reads and deduplication fast.

## Event preservation

IBKR normalization currently deduplicates repeated payload output by `event_id` and keeps the latest `fetched_at`, but the normalized table is written in overwrite mode. The target behavior for every broker event table is to merge new normalized event rows with the existing normalized event table, deduplicate by `event_id` using latest-fetch precedence, and preserve existing IDs absent from the current response. Because normalized events are incremental, IBKR raw does not need multiple historical reports; its raw retention can keep only the latest report required per source.

The merge must be idempotent and must not turn a missing event from the latest response into a deletion.

## Handoff assessment

The current handoff was introduced to avoid Trading 212 rereading accumulated raw history. The target contract generalizes that optimization: each broker run reads its bronze table once and reuses the shared decoded result for both snapshot and event normalization. Incremental event writes make historical raw event reports unnecessary for IBKR and other broker event tables.

The T212 handoff is removed for a measurement experiment. Existing handoff memory and runtime measurements are the baseline; restore the handoff if removal causes a material regression. The alternatives and trade-offs are captured in `handoff-decision-matrix.md`.

## Issue mapping

- Issue #157: addressed when freshness uses the current successful fetch rather than the timestamp of the first stored byte-identical payload.
- Issue #155: addressed by the single-bronze-read capability; snapshot and event outputs share one raw read and must not independently re-read or reparse the same payload.
