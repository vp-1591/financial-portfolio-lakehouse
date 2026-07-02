# 0010-fix-trading212-authentication-method

## Context

The Trading 212 API v0 requires HTTP Basic Authentication (`Authorization: Basic <base64(API_KEY:API_SECRET)>`) as documented in the official API spec (`docs/_vendor/trading212/api/section/general-information/api.json`) under the `authWithSecretKey` security scheme.

### Timeline of the regression

1. **Commit `b59a691` (Jun 13)** — Script created with correct **Basic auth**. The commit message says "documented Basic authentication" and the local API reference (`api.json`) confirmed `scheme: basic`. Tests verified `basic_auth_header` format.

2. **Commit `f7c3674` (Jun 21)** — Auth changed to **Bearer token**. The commit message claimed "Trading 212 API v0 uses Authorization: Bearer <API_KEY>, not Basic base64(key:secret)." This was a misdiagnosis: the script was likely getting 401 for unrelated reasons (IP restriction, wrong key, etc.) and Bearer was assumed to work because it appeared to fix the issue. The change:
   - Replaced `basic_auth_header(api_key, api_secret)` with `bearer_auth_header(api_key)` 
   - Made `api_secret` optional/unused
   - Updated tests to assert Bearer format
   - ADR 0005 documented this as intentional

3. **Current (Jun 25)** — Bearer auth stopped returning 200. Diagnostic testing confirmed all three auth methods (Basic, Bearer, raw key) returned 401, revealing the real cause was IP-restricted API keys, not the auth format. The T212 API docs continue to specify `scheme: basic`.

### Root cause

The regression was caused by **testing auth changes against an IP-restricted key**. When the 401 was caused by an IP restriction, switching from Basic to Bearer appeared to "fix" the issue — but only because the key happened to work from the correct IP at that moment. The auth format change was a false fix. The local API reference (`api.json`) that correctly specified Basic auth was never consulted before making the change.

### How to prevent future regressions

- The auth format is now **pinned by a regression test** (`test_auth_method_is_basic_with_key_and_secret`) that asserts the exact header format and rejects any change to Bearer or raw-key auth.
- The local API spec (`docs/_vendor/trading212/api/section/general-information/api.json`) contains the authoritative security scheme definition (`authWithSecretKey: { scheme: basic }`).
- 401 errors should be investigated for credential issues (IP restrictions, expired keys, wrong environment) before changing the auth method.

## Decision

Reverted to HTTP Basic Authentication (`Authorization: Basic <base64(API_KEY:API_SECRET)>`):

1. `basic_auth_header(api_key, api_secret)` replaces `bearer_auth_header(api_key)` in both `scripts/trading212_net_worth.py` and `pipeline/connectors/trading212/client.py`
2. `--api-secret` is required again (essential for Basic auth)
3. Added a regression test that locks in the Basic auth header format

## Consequences

- Both `--api-key` and `--api-secret` are required command-line arguments
- The pipeline client also uses Basic auth and requires the API secret
- A regression test prevents silent downgrade to Bearer or other auth methods

## Validation

- All 34 T212-related unit tests pass
- `test_auth_method_is_basic_with_key_and_secret` regression test added
- Successfully tested with real Trading 212 credentials (unrestricted key)