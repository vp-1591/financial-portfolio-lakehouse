# Add Retry Logic to CurrencyConverter.request_json

## Context

The CI test `test_gbx_live_conversion_via_gbp` failed with `TimeoutError: The read operation timed out` when calling the Frankfurter API from GitHub Actions. The `request_json` method had no retry mechanism — any HTTP or network failure raised `PortfolioConnectorError` immediately. Additionally, `TimeoutError` was not caught at all, so it propagated as a raw Python exception instead of being wrapped in `PortfolioConnectorError`.

The IBKR Flex client has a polling retry pattern (6 attempts, 3s delay) for report generation, but no transient-failure retry pattern existed in the codebase.

Current error handling in `request_json`:
- `urllib.error.HTTPError` (4xx/5xx) — raised `PortfolioConnectorError` immediately
- `urllib.error.URLError` (network errors) — raised `PortfolioConnectorError` immediately
- `TimeoutError` — **not caught at all**, propagated as raw exception
- `json.JSONDecodeError` and non-dict responses — raised `PortfolioConnectorError` immediately

## Decision

Add configurable retry logic to `request_json` using the `tenacity` library for transient errors:

1. **Retried errors** (transient, may succeed on retry) — raised as `TransientHttpError`, a new subclass of `PortfolioConnectorError`:
   - `TimeoutError` (read/connect timeout)
   - `urllib.error.URLError` (network errors: connection refused, DNS failure)
   - `urllib.error.HTTPError` with status >= 500 (server errors)

2. **Non-retried errors** (permanent, will not succeed on retry) — raised as `PortfolioConnectorError`:
   - `urllib.error.HTTPError` with status < 500 (client errors: 400, 404, etc.)
   - `json.JSONDecodeError` (server returned non-JSON content)
   - Non-dict JSON responses (unexpected API response format)

3. **Configuration** via `CurrencyConverter.__init__`:
   - `retries: int = 2` (3 total attempts)
   - `retry_delay: float = 1.0` (1 second fixed delay)

4. **Implementation**: `tenacity.retry` decorator on the inner `_do_request` function with `retry_if_exception_type(TransientHttpError)`, `stop_after_attempt(1 + retries)`, and `wait_fixed(retry_delay)`.

5. **`TransientHttpError`** is a subclass of `PortfolioConnectorError`. This means the `fetch_rate` provider loop's `except PortfolioConnectorError` clause catches both permanent and transient errors, which is correct — after retries are exhausted, the transient error propagates as `TransientHttpError(PortfolioConnectorError)` and is reported to the user with the `--fx-rate` suggestion.

6. **`tenacity`** is added as a pipeline dependency (`tenacity==9.0.0`) in `pyproject.toml`.

## Constraints

- `tenacity` is the only new dependency. No other third-party retry libraries.
- Retry is at the `request_json` level, not the provider level. The `fetch_rate` provider loop pattern remains unchanged.
- Default parameters maintain backward compatibility — existing call sites that don't pass `retries` or `retry_delay` get sensible defaults.
- `TransientHttpError` is a subclass of `PortfolioConnectorError`, so existing `except PortfolioConnectorError` clauses continue to work.

## Consequences

- **Positive**: Transient network errors (timeouts, connection failures, 5xx server errors) are automatically retried up to 2 times with 1s delay. This resolves the CI failure on `test_gbx_live_conversion_via_gbp`.
- **Positive**: `TimeoutError` is now properly caught and wrapped in `TransientHttpError`, consistent with how other errors are handled.
- **Positive**: The retry is configurable — callers can disable it (`retries=0`) or increase it for production use.
- **Positive**: Using `tenacity` provides a well-tested retry mechanism with clear semantics (`retry_if_exception_type`, `stop_after_attempt`, `wait_fixed`).
- **Negative**: Retries add up to 2s latency on transient failures (2 retries × 1s delay). This is acceptable for a pipeline that runs a small number of API calls.
- **Negative**: New `tenacity` dependency added to the pipeline.

## Validation

- New unit tests in `TestRequestJsonRetry` cover: success on first attempt, retry on `TimeoutError`, `URLError`, and 5xx, no retry on 4xx, no retry on `JSONDecodeError`, no retry on non-dict response, raise after all retries exhausted, `retries=0` disables retry, and default parameter values.
- All existing unit and integration tests continue to pass.
- The failing CI test `test_gbx_live_conversion_via_gbp` will now retry on timeout instead of failing immediately.