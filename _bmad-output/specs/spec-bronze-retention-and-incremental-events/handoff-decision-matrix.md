# T212 Fetch-to-Transform Handoff Decision Matrix

## Decision

Decision: remove the T212 handoff for a measurement experiment. Bronze retention and the single shared bronze read remove the original unbounded-table-read pressure; existing handoff measurements provide the baseline for deciding whether the optimization is still worth its complexity.

| Criterion | Remove the handoff | Keep the current handoff | Winner |
| --- | --- | --- | --- |
| Code complexity | ✅ Removes the opt-in connector capability, in-memory handoff threading, fallback branches, and related protocol/tests. | Preserves connector capability flags, handoff construction, threading, and fallback behavior. | ✅ Remove |
| Bronze read cost | One bounded `raw/{broker}` read per broker run, shared by snapshot and event normalization. A remote Delta table normally implies storage metadata/data requests, so this is also a network roundtrip cost. | ✅ Avoids the transform’s bronze read for T212; the encrypted current fetch already exists in memory. | ✅ Keep |
| Memory cost | Reads the bounded retained table once; cost is controlled by retention and projected reads. | Holds the encrypted current fetch until both normalized outputs finish; avoids loading retained bronze for T212. | ⚖️ Measure |
| Correctness under current retention | Equivalent if T212 retention keeps the latest complete response per endpoint and normalization shares one read. | Uses the current fetch directly and preserves the existing current-fetch-only behavior. | ⚖️ Equivalent |
| Partial-fetch behavior | Can fall back to retained bronze, but the current fail-loud T212 fetch contract must remain if partial responses are unsafe. | Requires the current fail-loud behavior because a missing endpoint would otherwise disappear from current-only normalization. | ⚖️ Contract-dependent |
| Operational fallback | ✅ Simpler: the normal table-read path is always available. | More paths to test; missing handoff layers fall back to bronze reads. | ✅ Remove |
| Performance uncertainty | Read latency should be measured after retention/VACUUM; the table is expected to be bounded but not free. | Avoids one remote read, but the benefit may be small relative to fetch and transform work. | ⚖️ Measure |

Legend: ✅ stronger option for the dimension; ⚖️ no unconditional winner.

## Decision rule

Measure a representative T212 run without the handoff against the existing handoff baseline. Keep the removal if memory peak and runtime remain within the agreed budget; restore the handoff if either regresses materially.

Either choice must preserve the separate capability that one shared bronze result feeds both normalized snapshot and event outputs; removing the handoff must not reintroduce independent reads for those outputs.
