# Migration B1 — demo bucket -> staging bucket (S3 data copy)

One-time data migration for the demo -> staging rename (Track B, AD-4).  The
S3 bucket name is globally unique, so this is a **new** bucket
(`investment-portfolio-pipeline-staging`) plus a full copy of the encrypted
data — never an in-place rename.

The script copies every object under `pipeline_demo/*` in the historical
bucket `investment-portfolio-pipeline-demo` to the **bucket root** of
`investment-portfolio-pipeline-staging`.  The staging data prefix is removed
entirely (ADR 0038/0039), NOT renamed to `pipeline_staging`.  Objects are
copied as-is (server-side copy), so the Fernet ciphertext is preserved
byte-for-byte — never decrypted/re-encrypted.  The old bucket is left
untouched, and the script never sets `force_destroy` on the staging bucket
(it does not create, delete, or configure the bucket at all).

## Operator commands

Full runbook, safety guarantees, and exit codes are in the script's docstring
(`pipeline/migrations/migrate_demo_bucket_to_staging.py`).

```bash
# 1. Plan the copy (no changes)
.venv/Scripts/python -m pipeline.migrations.migrate_demo_bucket_to_staging --mode staging --dry-run

# 2. Run the copy + post-copy verification
.venv/Scripts/python -m pipeline.migrations.migrate_demo_bucket_to_staging --mode staging
```

## AD-4 sequencing

The destination bucket must already exist (create it through the terraform
staging config if it does not), and **B1 runs BEFORE the terraform apply that
repoints the bucket** — the apply that flips `S3_BUCKET` / SSM paths /
orchestrator to `investment-portfolio-pipeline-staging`.  Only after B1
passes (final "Verification passed" line) do you apply that terraform, then
retire the `/portfolio/demo/*` SSM parameters immediately.
