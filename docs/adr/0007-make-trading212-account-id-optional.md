# 0007: Make Trading 212 Account IDs Optional and Update Documentation

## Context

The Trading 212 scripts (`trading212_net_worth.py` and `portfolio_percentages.py`) required `--account-id` / `--trading212-account-id` arguments even though the underlying implementation already had a default empty string value. This created an unnecessary requirement for users who might not need to specify an account ID. Additionally, the README was out of date — it only mentioned `--api-key` but the script also requires `--api-secret` (now required for Basic auth), and it showed `--account-id` as required in examples.

## Decision

1. Made `--account-id` optional (default `""`) in `trading212_net_worth.py`
2. Made `--trading212-account-id` optional (default `""`) in `portfolio_percentages.py`
3. Updated README.md to:
   - Show both `--api-key` and `--api-secret` arguments
   - Remove `--account-id` from example commands to avoid confusion
   - Add notes that account ID parameters are optional

## Consequences

- Users are no longer required to provide a Trading 212 account ID
- The scripts work with a default empty string when no account ID is provided
- Backward compatible — users can still provide an account ID if needed
- README accurately reflects current script argument requirements including `--api-secret`

## Validation

- Verified that both scripts import and run successfully after the change
- Confirmed that `--account-id` / `--trading212-account-id` are no longer required arguments
- Confirmed default value is an empty string as expected

## Merged from

This ADR supersedes:
- `0008-update-readme-trading212-requirements.md` — same change (README update for optional account ID)
- `0009-make-trading212-net-worth-account-id-optional.md` — same change (optional account ID in net worth script)