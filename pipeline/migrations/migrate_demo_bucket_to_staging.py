"""Migration B1: copy the encrypted demo bucket to the new staging bucket.

Part of the ``demo`` -> ``staging`` rename (Track B, AD-4).  The S3 bucket
name is globally unique, so this is a *new* bucket
(``investment-portfolio-pipeline-staging``) plus a full copy of the encrypted
data -- never an in-place rename.

What this script does
---------------------
1. Lists every object under ``pipeline_demo/*`` in the historical source
   bucket ``investment-portfolio-pipeline-demo``.
2. Server-side copies each object (``s3.copy_object``) to the destination
   bucket ``investment-portfolio-pipeline-staging`` at the **bucket root** --
   the ``pipeline_demo`` prefix is removed entirely (ADR 0038/0039: buckets
   already isolate environments), NOT renamed to ``pipeline_staging``.
3. Leaves the old bucket untouched (read-only access: list/head/get only).
4. Verifies post-copy: every expected destination object exists with a
   matching size, and a bounded sample is compared byte-for-byte against the
   source.

Encryption
----------
Objects are copied as-is.  ``s3.copy_object`` is a server-side copy that
preserves the object bytes exactly, so the Fernet ciphertext inside the
Delta/Parquet payloads is never decrypted, re-encrypted, or otherwise touched
by this script.  The post-copy byte comparison of the sampled objects is the
proof that encryption is intact.  (SSE at the S3 layer is a separate storage
concern and follows the destination bucket's own default; the application-
level Fernet encryption is what matters and is preserved.)

Safety
------
- Idempotent: re-running skips objects already present in the destination
  with a matching size, and exits 0 when there is nothing to copy.
- Never overwrites: if a destination object exists with a *different* size,
  the script raises and aborts instead of clobbering data.
- Never sets ``force_destroy`` on the staging bucket -- the script does not
  create, delete, or configure the bucket at all; it only requires the
  destination bucket to already exist.
- Dry-run: ``--dry-run`` prints the full plan without writing anything.

Runbook (operator steps, in order)
----------------------------------
1. Confirm the destination bucket ``investment-portfolio-pipeline-staging``
   already exists (B1 never creates it).  Create it through the terraform
   staging config if it does not.
2. Export the staging AWS credentials (env vars or ``~/.aws`` profile).
3. Plan the copy::

       .venv/Scripts/python -m pipeline.migrations.migrate_demo_bucket_to_staging --mode staging --dry-run

4. Run the copy::

       .venv/Scripts/python -m pipeline.migrations.migrate_demo_bucket_to_staging --mode staging

5. Confirm the final "Verification passed" line (count + sampled bytes).
6. AD-4 sequencing -- this script runs BEFORE the terraform apply that
   repoints the bucket.  Only after B1 has passed do you apply the terraform
   that flips ``S3_BUCKET`` / SSM / orchestrator to
   ``investment-portfolio-pipeline-staging`` (Migration B3), then retire the
   ``/portfolio/demo/*`` SSM parameters immediately.

Exit codes
----------
0 -- copy complete and verified, or a dry-run, or an idempotent no-op.
1 -- AWS request failure, destination conflict, or verification failure.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError

# Historical (pre-rename) names are intentional inputs to this one-time
# migration and are exempt from the rename grep bars.
DEMO_BUCKET = "investment-portfolio-pipeline-demo"
DEMO_PREFIX = "pipeline_demo"

# New staging bucket.  The staging data prefix is removed entirely (AD-4):
# objects land at the bucket root, NOT under a ``pipeline_staging`` prefix.
STAGING_BUCKET = "investment-portfolio-pipeline-staging"
STAGING_PREFIX = ""

# Post-copy byte-for-byte sample comparison bounds: at most
# SAMPLE_OBJECT_LIMIT objects, each at most _MAX_SAMPLE_BYTES.
SAMPLE_OBJECT_LIMIT = 5
_MAX_SAMPLE_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True)
class ObjectInfo:
    """A single S3 object key plus its size in bytes."""

    key: str
    size: int


@dataclass
class CopyReport:
    """Outcome of a B1 run."""

    total: int
    copied: int = 0
    skipped: int = 0
    total_bytes: int = 0
    sample_verified: int = 0
    dry_run: bool = False


@dataclass
class VerificationResult:
    """Outcome of the post-copy verification."""

    errors: list[str]
    sample_verified: int
    extra_objects: list[str]


def list_objects(client: Any, bucket: str, prefix: str) -> list[ObjectInfo]:
    """List all objects under *prefix* in *bucket* (paginated)."""
    objects: list[ObjectInfo] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            objects.append(ObjectInfo(key=item["Key"], size=int(item["Size"])))
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
        if not token:
            raise RuntimeError(
                f"list_objects_v2 for {bucket} truncated without NextContinuationToken"
            )
    return objects


def head_object_size(client: Any, bucket: str, key: str) -> int | None:
    """Return the size of *key* in *bucket*, or ``None`` when it is absent."""
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NotFound", "NoSuchKey"):
            return None
        raise
    return int(response["ContentLength"])


def strip_demo_prefix(key: str) -> str:
    """Strip the ``pipeline_demo/`` prefix, yielding the destination key."""
    prefix = f"{DEMO_PREFIX.rstrip('/')}/"
    if not key.startswith(prefix):
        raise ValueError(f"Key {key!r} is outside the source prefix {prefix!r}")
    return key[len(prefix) :]


def verify_copy(
    client: Any,
    source_objects: list[ObjectInfo],
    *,
    sample_limit: int = SAMPLE_OBJECT_LIMIT,
) -> VerificationResult:
    """Verify the copy: counts, per-object sizes, and a byte-for-byte sample.

    The destination listing is read from the bucket root (``STAGING_PREFIX``
    is empty).  Every expected destination key (source key with the
    ``pipeline_demo/`` prefix stripped) must exist with the same size.  A
    bounded sample of the smaller objects is then downloaded from both
    buckets and compared byte-for-byte -- the proof that the Fernet
    ciphertext was preserved exactly.
    """
    errors: list[str] = []
    dest_by_key = {
        obj.key: obj.size
        for obj in list_objects(client, STAGING_BUCKET, STAGING_PREFIX)
    }
    expected_by_key: dict[str, int] = {}
    for obj in source_objects:
        expected_by_key[strip_demo_prefix(obj.key)] = obj.size

    for dest_key, expected_size in sorted(expected_by_key.items()):
        actual_size = dest_by_key.get(dest_key)
        if actual_size is None:
            errors.append(f"missing in destination: {dest_key}")
        elif actual_size != expected_size:
            errors.append(
                f"size mismatch for {dest_key}: expected {expected_size}, got {actual_size}"
            )

    sample_verified = 0
    for obj in [o for o in source_objects if o.size <= _MAX_SAMPLE_BYTES][
        :sample_limit
    ]:
        dest_key = strip_demo_prefix(obj.key)
        if dest_key not in dest_by_key:
            continue  # already reported as missing above
        source_body = client.get_object(Bucket=DEMO_BUCKET, Key=obj.key)["Body"].read()
        dest_body = client.get_object(Bucket=STAGING_BUCKET, Key=dest_key)[
            "Body"
        ].read()
        if source_body != dest_body:
            errors.append(
                f"byte mismatch for {dest_key}: destination content differs from "
                "source (encryption/ciphertext not preserved)"
            )
        else:
            sample_verified += 1

    extra_objects = sorted(set(dest_by_key) - set(expected_by_key))
    return VerificationResult(
        errors=errors,
        sample_verified=sample_verified,
        extra_objects=extra_objects,
    )


def run_migration(
    client: Any,
    *,
    dry_run: bool = False,
    sample_limit: int = SAMPLE_OBJECT_LIMIT,
) -> CopyReport:
    """Execute (or plan) the B1 bucket copy.

    Idempotent: destination objects that already exist with the same size are
    skipped.  A destination object with a *different* size is a conflict and
    raises.  After copying, :func:`verify_copy` runs and any error raises
    ``RuntimeError`` so the caller exits non-zero.
    """
    source_objects = list_objects(client, DEMO_BUCKET, DEMO_PREFIX)
    report = CopyReport(
        total=len(source_objects),
        total_bytes=sum(obj.size for obj in source_objects),
        dry_run=dry_run,
    )

    print("Migration B1: demo bucket -> staging bucket (AD-4)")
    print(f"  Source:      s3://{DEMO_BUCKET}/{DEMO_PREFIX.rstrip('/')}/*")
    print(f"  Destination: s3://{STAGING_BUCKET}/   (bucket root, prefix removed)")
    print(f"  Objects:     {report.total}  ({report.total_bytes} bytes)")

    if not source_objects:
        print("  No objects under the source prefix; idempotent no-op (exit 0).")
        return report

    if dry_run:
        for obj in source_objects[:10]:
            print(
                f"  would copy: {obj.key} -> {strip_demo_prefix(obj.key)} "
                f"({obj.size} bytes)"
            )
        if report.total > 10:
            print(f"  ... and {report.total - 10} more")
        print("[DRY RUN] no changes made")
        return report

    for obj in source_objects:
        dest_key = strip_demo_prefix(obj.key)
        existing_size = head_object_size(client, STAGING_BUCKET, dest_key)
        if existing_size is not None:
            if existing_size == obj.size:
                report.skipped += 1
                print(f"  Already copied (size {existing_size}): {dest_key}")
                continue
            raise RuntimeError(
                f"Conflict: s3://{STAGING_BUCKET}/{dest_key} already exists with "
                f"size {existing_size} but the source has size {obj.size}. "
                "Refusing to overwrite; investigate before re-running."
            )
        print(f"  Copying: {obj.key} -> {dest_key} ({obj.size} bytes)")
        client.copy_object(
            Bucket=STAGING_BUCKET,
            Key=dest_key,
            CopySource={"Bucket": DEMO_BUCKET, "Key": obj.key},
        )
        report.copied += 1

    verification = verify_copy(client, source_objects, sample_limit=sample_limit)
    report.sample_verified = verification.sample_verified
    if verification.errors:
        raise RuntimeError(
            "Post-copy verification FAILED:\n- " + "\n- ".join(verification.errors)
        )

    print(
        f"\nVerification passed: {report.total} object(s) present at the bucket "
        f"root with matching sizes; {verification.sample_verified} sampled "
        "byte-for-byte identical (Fernet ciphertext preserved)."
    )
    if verification.extra_objects:
        print(
            "  Note: destination also contains "
            f"{len(verification.extra_objects)} object(s) not part of this "
            f"migration: {', '.join(verification.extra_objects[:5])}"
            + ("..." if len(verification.extra_objects) > 5 else "")
        )
    return report


def _build_s3_client() -> Any:
    """Build a boto3 S3 client using the project's consolidated credentials."""
    import boto3

    from pipeline.secrets import resolve_aws_credentials

    creds = resolve_aws_credentials()
    kwargs: dict[str, Any] = {"region_name": creds.region}
    if creds.key_id is not None or creds.secret_key is not None:
        kwargs["aws_access_key_id"] = creds.key_id or ""
        kwargs["aws_secret_access_key"] = creds.secret_key or ""
    if creds.session_token:
        kwargs["aws_session_token"] = creds.session_token
    if creds.endpoint_url:
        kwargs["endpoint_url"] = creds.endpoint_url
    return boto3.client("s3", **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Migration B1: copy the encrypted data from the demo bucket "
            "(investment-portfolio-pipeline-demo, prefix pipeline_demo/*) to the "
            "new staging bucket investment-portfolio-pipeline-staging at the "
            "bucket root (prefix removed, AD-4). Staging only."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("staging",),
        help="Execution mode. B1 is a staging-only migration (demo -> staging).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the copy plan without making any changes",
    )
    args = parser.parse_args()

    from pipeline.secrets import load_env, set_mode

    load_env()
    set_mode(args.mode)

    client = _build_s3_client()
    try:
        run_migration(client, dry_run=args.dry_run)
    except ClientError as exc:
        print(f"FATAL: AWS request failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except RuntimeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
