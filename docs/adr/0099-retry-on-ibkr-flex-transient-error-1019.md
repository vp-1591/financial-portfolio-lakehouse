# Retry on IBKR Flex Transient Error 1019

## Context

Prod `ibkr_snapshot_raw` (bronze) has been accumulating garbage rows since 2026-07-22: instead of a `<FlexQueryResponse>` with real data, `GetStatement` sometimes returns a `<FlexStatementResponse>` with `Status=Warn`, `ErrorCode=1019`, `ErrorMessage="Statement generation in progress. Please try again shortly."`. IBKR documents 1019 as transient — the report is still being generated and the request should be retried.

`IbkrFlexClient.fetch_report` (`pipeline/connectors/ibkr/client.py`) treated `Status=Warn` as a successful report and returned the response immediately. The existing transient retry logic only recognized error code 1018 (the legacy not-ready code), and it was unreachable for 1019 because the `WARN` branch returned before the error-code check was reached.

This is a timing race between report generation and the first `GetStatement` poll, not an environment-specific bug: the bronze code paths for staging and prod are identical (only secret values and the S3 bucket differ), and prod has stored both response types on the same day (2026-07-22 08:01 `FlexQueryResponse`, 15:57 `FlexStatementResponse`). Staging still returns `FlexQueryResponse` because its demo query generates faster, so it never hits the window.

Each 1019 payload carries a fresh server timestamp, so every stored copy has a unique payload hash — the bronze dedup in `ingest_raw` (on `(broker, source, payload_hash)`) cannot prevent accumulation.

## Decision

Change `fetch_report` so a `Status=Warn` response is treated as "not ready" and retried, never stored as data:

1. **WARN falls through to retry.** Restructure the status handling so only `Status=Success` (or a response carrying `FlexStatement` children) returns a report; `Status=FAIL` still raises `IbkrError` immediately; `Status=Warn` records the error code/message as `last_error` and falls through to the retry loop (a bare `Warn` with no ErrorCode is treated the same).
2. **Retry the full set of IBKR transient error codes.** The error-code block retries every IBKR-documented "try again shortly" code — `1001`, `1004`–`1009`, `1018` (rate limit), `1019` (generation in progress), and `1021` — up to `retries` attempts; any other error code (e.g. `1003`, `1010`–`1017`, `1020`) remains fatal.
3. **Add `initial_delay: float = 3.0`.** IBKR commonly returns 1019 on an immediate first `GetStatement` call, so `fetch_report` sleeps once before the first poll to avoid a near-certain wasted round trip. Existing callers (`fetch.py`, `connector.py`) don't pass it and get the default — no call-site changes.

Alternatives considered and rejected:

- **Return the Warn response to the caller and let it retry** — rejected: retry belongs in the client's existing polling loop, which already owns `retries`/`delay`; the callers have no retry structure.
- **Store Warn payloads in a separate dead-letter table** — rejected: 1019 is not real data but a transient API signal; retrying is cheaper and keeps bronze clean.
- **Extend the retry count without `initial_delay`** — rejected: the first poll is likely to hit 1019, so sleeping before the first attempt is strictly better than burning a retry.

## Constraints

- No new dependencies — the hand-rolled polling loop stays; `tenacity` (used in ADR 0098 for the currency converter) is deliberately not introduced here.
- Default parameters maintain backward compatibility — callers that don't pass `initial_delay` get the 3.0s default.
- A `Warn` response must never be stored in bronze as a report.
- Purging the existing 1019 rows from prod `ibkr_snapshot_raw` is out of scope: their unique payload hashes mean dedup won't help, so they need a separate one-off data cleanup.

## Consequences

- **Positive**: transient 1019 responses are retried up to `retries` times (default 6) and stored only once a real `FlexQueryResponse` arrives; no new garbage rows accumulate after deploy.
- **Positive**: the 3.0s initial delay avoids the near-certain failed first poll.
- **Positive**: legacy 1018 behavior is unchanged, and `FAIL` still raises immediately so real report-generation errors are not masked.
- **Negative**: every `fetch_report` now waits at least `initial_delay` (3.0s default) before the first poll, adding ~3s latency per report fetch. Acceptable for a pipeline that runs a handful of polls per run.
- **Follow-up**: existing garbage rows in prod `ibkr_snapshot_raw` remain until a separate cleanup removes them.

## Validation

- New `TestFetchReport` class in `tests/test_ibkr_connector.py` covers: success returns immediately; success with no ErrorCode; `FlexStatement` data with no Status returns; 1019 Warn retries then succeeds; 1018 Warn retries then succeeds; every other transient code (1001, 1004–1009, 1021) retries then succeeds (parametrized with verbatim IBKR messages); Warn without ErrorCode retries then succeeds; `FAIL` raises immediately (1017 "Reference code is invalid."); permanent error codes raise (1003, 1014, 1020, parametrized); retry exhaustion raises after max retries; `initial_delay` sleeps before the first poll (asserted via a `time.sleep` recorder). All error codes and messages are taken verbatim from IBKR's official Flex Web Service Version 3 error table.
- Full suite: `pytest tests/ -q -rf` → 671 passed.
- `pyright pipeline/ tests/` → 0 errors; ruff → clean.
