# Deferred Work

- source_spec: `_bmad-output/implementation-artifacts/spec-gh-154-reduce-trading212-peak-memory.md`
  summary: `dedup_raw`'s projected key scan still materializes the whole accumulated `(broker, source, payload_hash)` set in Python memory (O(history) per run, per connector), even though the payload column is never loaded.
  evidence: reviewed finding — projection to 3 columns avoids payload bytes but the `to_pylist()` + `set(zip(...))` key set still scales with the table; the spec's "Never" list bans `DeltaTable.merge`/bounded scans without offering an alternative. Already scoped as issue #155 item 3 (bounded/merge dedup).
