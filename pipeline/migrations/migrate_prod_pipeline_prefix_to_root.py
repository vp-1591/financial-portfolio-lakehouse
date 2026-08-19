"""Migration: strip the prod ``pipeline/`` storage prefix to the bucket root.

Follow-up to the ADR 0113 rename (applied 2026-08-19): staging stores at the
bucket root while prod still stores under ``s3://investment-portfolio-pipeline/
pipeline/...``. The legacy storage-prefix concept is deleted from the whole
codebase (ADR 0113 records the follow-up), so prod data must move to the bucket
root before the prefix-removed code deploys.

What this script does
---------------------
1. Lists every object under ``pipeline/*`` in the prod bucket
   ``investment-portfolio-pipeline``.
2. Server-side copies each object (``s3.copy_object``) to the **bucket root** —
   the ``pipeline`` prefix is removed entirely (buckets already isolate
   environments, so no prefix is inserted).
3. Verifies post-copy: every expected root object exists with a matching size,
   and a bounded sample is compared byte-for-byte against the source.
4. Only then deletes the ``pipeline/*`` source objects (chunked
   ``delete_objects``).

Encryption
----------
Objects are copied as-is.  ``s3.copy_object`` is a server-side copy that
preserves the object bytes exactly, so the Fernet ciphertext inside the
Delta/Parquet payloads is never decrypted, re-encrypted, or otherwise touched
by this script.  The post-copy byte comparison of the sampled objects is the
proof that encryption is intact.

Safety
------
- Idempotent: re-running skips objects already present at the root with a
  matching size (a run interrupted between copy and delete is completed, not
  duplicated), and exits 0 when there is nothing to migrate.
- Never overwrites: a root object with a *different* size is a conflict and
  raises instead of clobbering data.
- Source objects are deleted only after post-copy verification passes; a
  verification failure raises and leaves the sources in place.
- Dry-run: ``--dry-run`` prints the full plan without writing anything.

Runbook (operator steps, in order)
----------------------------------
1. Export the prod AWS credentials (env vars or ``~/.aws`` profile).
2. Plan the copy::

       S3_BUCKET=investment-portfolio-pipeline .venv/Scripts/python -m pipeline.migrations.migrate_prod_pipeline_prefix_to_root --mode prod --dry-run

3. Run the copy::

       S3_BUCKET=investment-portfolio-pipeline .venv/Scripts/python -m pipeline.migrations.migrate_prod_pipeline_prefix_to_root --mode prod

4. Confirm the final "Verification passed" line and the source deletion.
5. Sequencing: run this AFTER migration A1 (``migrate_cdc_to_events``, already
   applied to prod) and BEFORE the prefix-removed code deploys.  Between this
   migration and the deploy, do not upload xtb files: the old image reads
   ``pipeline/...``, the new one reads the bucket root, and the EventBridge
   rule prefix is updated by the terraform apply.

Exit codes
----------
0 -- migration complete and verified, or a dry-run, or an idempotent no-op.
1 -- AWS request failure, destination conflict, or verification failure.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError

# The physical prod bucket and its historical (pre-removal) storage prefix.
# These are the migration's inputs and are exempt from any prefix grep bars.
PROD_BUCKET = "investment-portfolio-pipeline"
SOURCE_PREFIX = "pipeline"

# Post-copy byte-for-byte sample comparison bounds: at most
# SAMPLE_OBJECT_LIMIT objects, each at most _MAX_SAMPLE_BYTES.
SAMPLE_OBJECT_LIMIT = 5
_MAX_SAMPLE_BYTES = 256 * 1024 * 1024

# boto3 delete_objects accepts at most this many keys per call.
_DELETE_CHUNK = 1000


@dataclass(frozen=True)
class ObjectInfo:
    """A single S3 object key plus its size in bytes."""

    key: str
    size: int


@dataclass
class MigrationReport:
    """Outcome of a prefix-strip run."""

    total: int
    copied: int = 0
    skipped: int = 0
    deleted: int = 0
    total_bytes: int = 0
    sample_verified: int = 0
    dry_run: bool = False


@dataclass
class VerificationResult:
    """Outcome of the post-copy verification."""

    errors: list[str]
    sample_verified: int


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


def strip_source_prefix(key: str) -> str:
    """Strip the ``pipeline/`` prefix, yielding the destination root key."""
    prefix = f"{SOURCE_PREFIX.rstrip('/')}/"
    if not key.startswith(prefix):
        raise ValueError(f"Key {key!r} is outside the source prefix {prefix!r}")
    return key[len(prefix) :]


def _delete_objects(client: Any, bucket: str, keys: list[str]) -> None:
    """Delete *keys* from *bucket* in bounded batches."""
    for i in range(0, len(keys), _DELETE_CHUNK):
        chunk = keys[i : i + _DELETE_CHUNK]
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk]},
        )


def verify_copy(
    client: Any,
    source_objects: list[ObjectInfo],
    *,
    sample_limit: int = SAMPLE_OBJECT_LIMIT,
) -> VerificationResult:
    """Verify the copy: counts, per-object sizes, and a byte-for-byte sample.

    The destination listing is read from the bucket root (empty prefix).  Every
    expected root key (source key with the ``pipeline/`` prefix stripped) must
    exist with the same size.  A bounded sample of the smaller objects is then
    downloaded from both locations and compared byte-for-byte -- the proof that
    the Fernet ciphertext was preserved exactly.
    """
    errors: list[str] = []
    root_by_key = {obj.key: obj.size for obj in list_objects(client, PROD_BUCKET, "")}

    for obj in source_objects:
        dest_key = strip_source_prefix(obj.key)
        actual_size = root_by_key.get(dest_key)
        if actual_size is None:
            errors.append(f"missing in destination: {dest_key}")
        elif actual_size != obj.size:
            errors.append(
                f"size mismatch for {dest_key}: expected {obj.size}, got {actual_size}"
            )

    sample_verified = 0
    for obj in [o for o in source_objects if o.size <= _MAX_SAMPLE_BYTES][
        :sample_limit
    ]:
        dest_key = strip_source_prefix(obj.key)
        if dest_key not in root_by_key:
            continue  # already reported as missing above
        source_body = client.get_object(Bucket=PROD_BUCKET, Key=obj.key)["Body"].read()
        dest_body = client.get_object(Bucket=PROD_BUCKET, Key=dest_key)["Body"].read()
        if source_body != dest_body:
            errors.append(
                f"byte mismatch for {dest_key}: destination content differs from "
                "source (encryption/ciphertext not preserved)"
            )
        else:
            sample_verified += 1

    return VerificationResult(errors=errors, sample_verified=sample_verified)


def run_migration(
    client: Any,
    *,
    dry_run: bool = False,
    sample_limit: int = SAMPLE_OBJECT_LIMIT,
) -> MigrationReport:
    """Execute (or plan) the prod prefix strip.

    Idempotent: root objects that already exist with the same size are skipped
    (completing a run interrupted between copy and delete).  A root object with
    a *different* size is a conflict and raises.  After copying,
    :func:`verify_copy` runs; only after it passes are the ``pipeline/*``
    sources deleted.  Any verification error raises ``RuntimeError`` so the
    caller exits non-zero with the sources untouched.
    """
    source_objects = list_objects(client, PROD_BUCKET, SOURCE_PREFIX)
    report = MigrationReport(
        total=len(source_objects),
        total_bytes=sum(obj.size for obj in source_objects),
        dry_run=dry_run,
    )

    print(
        "Migration: strip prod 'pipeline/' prefix to the bucket root (ADR 0113 follow-up)"
    )
    print(f"  Bucket:      s3://{PROD_BUCKET}")
    print(f"  Source:      s3://{PROD_BUCKET}/{SOURCE_PREFIX.rstrip('/')}/*")
    print(f"  Destination: s3://{PROD_BUCKET}/   (bucket root, prefix removed)")
    print(f"  Objects:     {report.total}  ({report.total_bytes} bytes)")

    if not source_objects:
        print("  No objects under the source prefix; idempotent no-op (exit 0).")
        return report

    if dry_run:
        for obj in source_objects[:10]:
            print(
                f"  would copy: {obj.key} -> {strip_source_prefix(obj.key)} "
                f"({obj.size} bytes)"
            )
        if report.total > 10:
            print(f"  ... and {report.total - 10} more")
        print("[DRY RUN] no changes made")
        return report

    to_delete: list[str] = []
    for obj in source_objects:
        dest_key = strip_source_prefix(obj.key)
        existing_size = head_object_size(client, PROD_BUCKET, dest_key)
        if existing_size is not None:
            if existing_size == obj.size:
                report.skipped += 1
                print(f"  Already at root (size {existing_size}): {dest_key}")
            else:
                raise RuntimeError(
                    f"Conflict: s3://{PROD_BUCKET}/{dest_key} already exists with "
                    f"size {existing_size} but the source has size {obj.size}. "
                    "Refusing to overwrite; investigate before re-running."
                )
        else:
            print(f"  Copying: {obj.key} -> {dest_key} ({obj.size} bytes)")
            client.copy_object(
                Bucket=PROD_BUCKET,
                Key=dest_key,
                CopySource={"Bucket": PROD_BUCKET, "Key": obj.key},
            )
            report.copied += 1
        to_delete.append(obj.key)

    verification = verify_copy(client, source_objects, sample_limit=sample_limit)
    report.sample_verified = verification.sample_verified
    if verification.errors:
        raise RuntimeError(
            "Post-copy verification FAILED; source objects NOT deleted:\n- "
            + "\n- ".join(verification.errors)
        )

    _delete_objects(client, PROD_BUCKET, to_delete)
    report.deleted = len(to_delete)

    print(
        f"\nVerification passed: {report.total} object(s) present at the bucket "
        f"root with matching sizes; {verification.sample_verified} sampled "
        "byte-for-byte identical (Fernet ciphertext preserved)."
    )
    print(f"Deleted {report.deleted} source object(s) under {SOURCE_PREFIX}/.")
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
            "Strip the prod 'pipeline/' storage prefix to the bucket root "
            "(ADR 0113 follow-up): copy s3://investment-portfolio-pipeline/"
            "pipeline/* to the bucket root, verify byte-for-byte, then delete "
            "the sources. Prod only."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("prod",),
        help="Execution mode. This migration targets prod only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the copy/delete plan without making any changes",
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
