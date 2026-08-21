# Retention and Events Contract

## Broker policy

| Broker | Logical retention identity | Required retained state | Main risk |
| --- | --- | --- | --- |
| IBKR | `source` plus event history | Do not discard event payload coverage needed to rebuild retained normalized events | Flex query window moves |
| Trading 212 | endpoint `source` | Latest complete response per endpoint | Assumes endpoint completeness |
| XTB | derived account identity | Latest report per account | `account_id` currently exists only in silver derivation |

`source` remains a routing value. `source_file` remains XTB provenance and currently supplies the account ID during transformation; it is not a raw schema account column.

## Delta retention facts

Delta Lake and delta-rs use a default one-week deleted-file retention threshold and a default 30-day transaction-log retention. The deleted-file threshold is relevant only when files become unreferenced through delete, overwrite, compaction, or similar operations and VACUUM is run. Append-only bronze writes do not delete old rows, and VACUUM is not automatically triggered by Delta.

Therefore, “seven-day Delta retention” does not by itself implement seven-day bronze retention. The implementation must specify the operation that removes old rows/files and the maintenance schedule that runs VACUUM.

## Event preservation

IBKR normalization currently deduplicates repeated payload output by `event_id` and keeps the latest `fetched_at`, but the normalized table is written in overwrite mode. The target behavior is to merge the new normalized event rows with the existing normalized event table, deduplicate by `event_id` using latest-fetch precedence, and preserve existing IDs absent from the current Flex response.

The merge must be idempotent and must not turn a missing event from the latest response into a deletion.

## Handoff assessment

The current handoff was introduced to avoid Trading 212 rereading accumulated raw history. Incremental event writes do not automatically remove that memory problem. The handoff remains useful for Trading 212 if its current-fetch-only transform is retained. IBKR and XTB still need accumulated or filtered raw reads unless the new retention contract proves that current fetches contain all required data. Issue #155 can be included only after its exact acceptance criteria are confirmed and the implementation does not conflict with the IBKR preservation rule.

## Issue mapping

- Issue #157: addressed when freshness uses the current successful fetch rather than the timestamp of the first stored byte-identical payload.
- Issue #155: related to the handoff/read strategy, but inclusion in the same PR remains an open scope decision until its acceptance criteria are verified.
