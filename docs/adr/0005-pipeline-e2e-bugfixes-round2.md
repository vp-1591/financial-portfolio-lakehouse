# 0005: Pipeline End-to-End Bugfixes (Round 2)

## Context

Second end-to-end run of the medallion pipeline exposed four bugs:

1. **Transform functions don't decrypt payloads**: All five transform functions
   (IBKR, T212 snapshot/CDC, XTB snapshot/CDC) read encrypted payloads from raw
   Delta tables and tried `json.loads()` on the ciphertext. Since the payloads
   are Fernet-encrypted in `ingest_raw()`, every `json.loads()` call failed
   silently (caught by `try/except`) and all rows were skipped, producing 0-row
   normalized tables. The `fernet_key` was already passed to all transforms but
   never used for decryption.

2. **T212 auth was incorrectly changed from Basic to Bearer token**: This change
   was based on a misdiagnosed 401 error — the real cause was an IP-restricted API
   key, not the auth method. The Trading 212 API v0 requires HTTP Basic Authentication
   (`Authorization: Basic <base64(API_KEY:API_SECRET)>`) as documented in the local
   API spec. This was reverted in ADR 0010.

3. **`allocate_percentages` crashes on missing Delta table**: When no data was
   successfully fetched/transformed, `DeltaTable(table_path)` throws a low-level
   error with no context. The user saw `Invalid table location` and `Os { code: 2 }`
   instead of a clear message.

4. **`cmd_transform` writes empty normalized tables**: When transform produced 0
   rows (due to bug #1), it still wrote an empty Delta table. Now it prints
   "no data to transform" and skips the write.

## Decision

- **Decrypt payloads in all transforms**: Every transform function now calls
  `decrypt(payload_bytes, fernet_key)` before `json.loads()`. Failed decryptions
  are silently skipped (same pattern as failed JSON parsing).

- **Incorrectly switched T212 auth to Bearer token**: Replaced `basic_auth_header(key, secret)`
  with `bearer_auth_header(key)` returning `Bearer <key>`. The `api_secret`
  parameter was made optional and unused. This was a misdiagnosis — the 401 was
  caused by IP restrictions on the API key, not the auth format. Reverted in
  ADR 0010 back to HTTP Basic Authentication.

- **Graceful error for missing Delta table**: `allocate_percentages()` now wraps
  `DeltaTable(table_path)` in a try/except and raises `FileNotFoundError` with a
  clear message telling the user to run fetch and transform first.

- **Skip empty transform results**: `cmd_transform` now checks if
  `normalized.num_rows == 0` before writing, printing "no data to transform"
   and continuing.

- **Updated all transform tests**: Test helpers that build raw tables now
  encrypt payloads with `encrypt()` to match the real pipeline flow.

## Consequences

- Transforms now correctly decrypt and parse raw Delta table payloads
- T212 API calls use HTTP Basic Authentication (reverted from Bearer — see ADR 0010)
- Pipeline exits with a clear error message when no data is available
- All 129 tests pass (6 new integration tests added)

## Validation

- `test_xtb_transform_decrypts_encrypted_payload`: verifies XTB transform works
  with encrypted payloads
- `test_t212_transform_decrypts_encrypted_payload`: verifies T212 transform
  works with encrypted payloads
- `test_basic_auth_header_format`: verifies Basic auth header format
- `test_auth_method_is_basic_with_key_and_secret`: regression test preventing downgrade from Basic auth
- `test_client_sends_basic_auth`: verifies Trading212Client sends Basic auth
- `test_allocate_raises_filenotfound_for_missing_table`: verifies graceful error
  for missing Delta table
- All existing connector transform tests updated to encrypt payloads