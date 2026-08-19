# B2 Runbook — SSM parameter swap: /portfolio/demo/ to /portfolio/staging/

Operator runbook for the demo-to-staging SSM secret migration (rename-plan
Migration B2). The **value copy and the retire are user-executed manual
steps** — the implementation agent has no secret access. This runbook gives
the exact commands and the AD-4 sequencing.

Secrets moved (same names, new path prefix):

| Secret | Source path | Target path |
|---|---|---|
| IBKR Flex token | `/portfolio/demo/IBKR_FLEX_TOKEN` | `/portfolio/staging/IBKR_FLEX_TOKEN` |
| IBKR Flex query ID | `/portfolio/demo/IBKR_FLEX_QUERY_ID` | `/portfolio/staging/IBKR_FLEX_QUERY_ID` |
| Trading 212 API key | `/portfolio/demo/T212_API_KEY` | `/portfolio/staging/T212_API_KEY` |
| Trading 212 API secret | `/portfolio/demo/T212_API_SECRET` | `/portfolio/staging/T212_API_SECRET` |
| Fernet encryption key | `/portfolio/demo/ENCRYPTION_KEY` | `/portfolio/staging/ENCRYPTION_KEY` |

Values are **copied, never regenerated, never printed**. Prod
(`/portfolio/prod/*`) is never touched.

## Sequencing (AD-4 — migration-first, deploy-last)

Strict order, per environment. The encrypted data is never in only one place:

1. **Copy first.** Set `/portfolio/staging/*` to the same secret values
   BEFORE the terraform apply. The apply references the new paths
   (`/portfolio/staging/IBKR_FLEX_TOKEN`, etc.), so the values must already
   exist or the re-pointed ECS tasks launch without their secrets.
2. **Apply second.** Terraform repoints ECS to the new paths (B3 moved
   blocks / `terraform state mv`).
3. **Retire third, immediately.** Delete `/portfolio/demo/*` IMMEDIATELY
   after the swap is live — **no grace period**. Do not leave both path sets
   in place; a stale `/portfolio/demo/*` is a shadow credential the next
   person may mistake for live config.

## Prerequisites

- AWS credentials for the staging account via boto3's default chain
  (env vars, `~/.aws/credentials`, or AWS SSO profile). The operator's IAM
  principal needs:
  - `ssm:GetParameter` + `ssm:DescribeParameters` + `kms:Decrypt` (read
    source values and resolve the source's KMS key — `get_parameter` does
    not return `KeyId`, so the script resolves it via `describe_parameters`
    so the destination reuses the same key)
  - `ssm:PutParameter` + `kms:Encrypt` on the SSM KMS key (create target)
  - `ssm:GetParametersByPath` + `ssm:DeleteParameter` (retire step)
- Region: `eu-west-1` (override with `--region` or `AWS_REGION`).
- The project venv on Windows: `.venv/Scripts/python` (never system python).

## Step 1 — Preflight / dry run

```bash
.venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging --dry-run
```

Confirms each `/portfolio/demo/<SECRET>` exists, shows the exact
`/portfolio/staging/<SECRET>` parameters it would create (names and KMS key
only — never values), and skips anything already migrated.

## Step 2 — Copy the values (user-executed)

```bash
.venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging
```

Creates each `/portfolio/staging/<SECRET>` as a `SecureString` with the
**same KMS key** as its source, `Overwrite=false`. Idempotent:

- Source absent → skipped (exit 0).
- Destination present and matching → skipped (already migrated).
- Destination present but **different** → the script raises and refuses to
  overwrite. Reconcile manually — `ENCRYPTION_KEY` must exactly match the
  key used to write the existing staging Delta tables; a regenerated key
  would make every stored value undecryptable.

## Step 3 — Verify the new parameters

Names, types, and KMS key only — never print the values:

```bash
aws ssm describe-parameters --region eu-west-1 \
  --parameter-filters "Key=Name,Option=BeginsWith,Values=/portfolio/staging/"

aws ssm describe-parameters --region eu-west-1 \
  --parameter-filters "Key=Type,Option=Equals,Values=SecureString"
```

Confirm all five staging parameters exist, are `SecureString`, and their
`KeyId` matches the source parameters' `KeyId` (same KMS key).

## Step 4 — Terraform apply (B3, repoints ECS to the new paths)

```bash
cd terraform/staging
terraform apply
```

The apply references `/portfolio/staging/*`; Step 2 must have run first.
Never apply prod terraform.

## Step 5 — Retire /portfolio/demo/* immediately (no grace period)

Run the retire step immediately after the swap is live:

```bash
# Preview first (recommended)
.venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging --retire --dry-run

# Interactive: lists the exact parameters, then requires typing RETIRE
.venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging --retire

# Non-interactive (CI / scripted): explicit --yes
.venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging --retire --yes
```

The retire step only deletes parameters under `/portfolio/demo/`, never
`/portfolio/staging/*` or `/portfolio/prod/*`.

## Step 6 — Verify retirement

```bash
aws ssm describe-parameters --region eu-west-1 \
  --parameter-filters "Key=Name,Option=BeginsWith,Values=/portfolio/demo/"
```

Must return an empty parameter list.

## Safety notes

- **Never print secret values.** The script never does; do not echo
  parameter values in the verification commands either.
- **No grace period.** Retire `/portfolio/demo/*` immediately after the
  swap is live (Q2 decision). Do not carry a stale copy of production-facing
  secrets under the old path.
- **`ENCRYPTION_KEY` is the dangerous one.** It must be copied byte-for-byte
  from the source; the script's mismatch guard exists exactly so a stale or
  regenerated key cannot silently break existing encrypted data.
- **Prod is out of scope.** This runbook is for the staging environment
  only; `/portfolio/prod/*` values and ECS references are untouched.
