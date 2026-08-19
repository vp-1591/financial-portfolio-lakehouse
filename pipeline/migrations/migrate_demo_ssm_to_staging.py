"""Migration B2: copy SSM parameter values from /portfolio/demo/ to /portfolio/staging/.

The staging AWS environment is being renamed from ``demo`` to ``staging``.
ECS task definitions currently inject broker secrets from SSM parameters
under ``/portfolio/demo/<SECRET>``; after the rename they reference
``/portfolio/staging/<SECRET>`` instead.  The secret **values** are unchanged
— they must be copied, never regenerated.

This script performs the value copy for the five secrets the pipeline uses
(``pipeline.secrets.REQUIRED_SECRETS``):

- ``IBKR_FLEX_TOKEN``
- ``IBKR_FLEX_QUERY_ID``
- ``T212_API_KEY``
- ``T212_API_SECRET``
- ``ENCRYPTION_KEY``

For each secret it reads the decrypted value of ``/portfolio/demo/<SECRET>``
and writes it to ``/portfolio/staging/<SECRET>`` as a ``SecureString`` using
the **same KMS key** (the ``KeyId`` reported by the source parameter).  It
never prints secret values, never regenerates them, and never touches
``/portfolio/prod/*``.

**Sequencing (AD-4 — migration-first, deploy-last).**  The value copy runs
BEFORE the terraform apply that repoints ECS to the new paths (the apply
references ``/portfolio/staging/*``, so the values must already exist).
The retire step runs IMMEDIATELY after the swap is live — there is no grace
period during which both path sets are valid.

Idempotent: a missing source is skipped (exit 0), a destination that already
matches its source is skipped (already migrated), and a destination that
exists with a *different* value raises (never silently overwrite — the
``ENCRYPTION_KEY`` must keep matching the existing staging Delta tables).
Genuine AWS failures (auth, permission, throttling) propagate and exit
non-zero, so a pre-deploy gate cannot mistake a real failure for
"nothing to migrate".

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging \
        [--region eu-west-1] [--dry-run]
    .venv/Scripts/python -m pipeline.migrations.migrate_demo_ssm_to_staging \
        --retire [--dry-run] [--yes]

The default (no ``--retire``) runs the copy step.  ``--retire`` runs the
retire step instead: it deletes every parameter under ``/portfolio/demo/``.
The retire step lists the exact parameters it will delete and requires a
typed ``RETIRE`` confirmation (or ``--yes`` for non-interactive use).
Credentials come from boto3's default credential chain (env vars,
``~/.aws/credentials``, AWS SSO, IMDS); the caller's IAM principal needs
``ssm:GetParameter`` + ``ssm:DescribeParameters`` + ``kms:Decrypt`` for the
copy (``DescribeParameters`` resolves the source's KMS key — see
:func:`_resolve_kms_key_id`), ``ssm:PutParameter`` + ``kms:Encrypt`` for the
new parameters, and ``ssm:GetParametersByPath`` + ``ssm:DeleteParameter``
for the retire step.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import boto3

# Secrets moved by this migration — the pipeline's REQUIRED_SECRETS
# (pipeline/secrets.py), renamed only by their SSM path prefix.
_SECRETS: tuple[str, ...] = (
    "IBKR_FLEX_TOKEN",
    "IBKR_FLEX_QUERY_ID",
    "T212_API_KEY",
    "T212_API_SECRET",
    "ENCRYPTION_KEY",
)

# Historical source path (pre-rename ``demo`` environment) and target path
# (post-rename ``staging`` environment).  Prod (/portfolio/prod/) is never
# touched.
_SOURCE_PREFIX = "/portfolio/demo/"
_DEST_PREFIX = "/portfolio/staging/"


def _get_parameter(ssm: Any, name: str) -> dict[str, Any] | None:
    """Fetch a decrypted SSM parameter, or ``None`` if it does not exist.

    Returns the raw ``Parameter`` dict from boto3 (including ``Value`` and,
    for ``SecureString``, ``KeyId``).
    """
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=True)
    except ssm.exceptions.ParameterNotFound:
        return None
    return response["Parameter"]


def _resolve_kms_key_id(
    ssm: Any,
    source: str,
    source_param: dict[str, Any],
) -> str | None:
    """Resolve the KMS key that encrypts a SecureString source parameter.

    ``get_parameter`` does not reliably return ``KeyId`` — verified against
    live AWS: the field is absent from the response even for SecureString
    parameters (``describe_parameters`` does return it).  ``aws/ssm`` is the
    default SSM key; omitting ``KeyId`` on ``put_parameter`` has the same
    effect, so it is normalized to ``None`` so the destination reuses the
    source key whenever it is not the default.
    """
    key_id = source_param.get("KeyId")
    if not key_id:
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": [source]}],
            MaxResults=10,
        )
        parameters = response.get("Parameters", [])
        if parameters:
            key_id = parameters[0].get("KeyId")
    if key_id in ("aws/ssm", ""):
        return None
    return key_id


def copy_parameters(ssm: Any, dry_run: bool = False) -> int:
    """Copy each /portfolio/demo/<SECRET> value to /portfolio/staging/<SECRET>.

    Never prints a secret value.  Returns the number of parameters created.

    Raises :exc:`RuntimeError` if a destination exists with a value that
    differs from its source — an overwrite could silently break the
    ``ENCRYPTION_KEY`` contract with existing staging data, so mismatches are
    surfaced for manual reconciliation instead.
    """
    created = 0
    for secret in _SECRETS:
        source = f"{_SOURCE_PREFIX}{secret}"
        dest = f"{_DEST_PREFIX}{secret}"

        source_param = _get_parameter(ssm, source)
        if source_param is None:
            print(f"  Source not present, skipping: {source}")
            continue

        dest_param = _get_parameter(ssm, dest)
        if dest_param is not None:
            if dest_param["Value"] == source_param["Value"]:
                print(f"  Already migrated (destination matches source): {secret}")
            else:
                raise RuntimeError(
                    f"{dest} already exists with a different value than {source}; "
                    "refusing to overwrite. Reconcile the mismatch manually — the "
                    "ENCRYPTION_KEY must match the existing staging Delta tables."
                )
            continue

        key_id = _resolve_kms_key_id(ssm, source, source_param)
        print(f"  Copying: {source} -> {dest}", end="")
        print(
            f" (SecureString, KMS {key_id})"
            if key_id
            else " (SecureString, SSM default KMS key)"
        )
        if dry_run:
            print(f"  [DRY RUN] Would create {dest}")
            continue

        put_kwargs: dict[str, Any] = {
            "Name": dest,
            "Value": source_param["Value"],
            "Type": "SecureString",
            "Overwrite": False,
        }
        if key_id:
            put_kwargs["KeyId"] = key_id
        ssm.put_parameter(**put_kwargs)
        print(f"  Done: {dest}")
        created += 1

    return created


def _list_source_parameters(ssm: Any) -> list[str]:
    """Return the sorted names of every parameter under /portfolio/demo/."""
    names: list[str] = []
    paginator = ssm.get_paginator("get_parameters_by_path")
    for page in paginator.paginate(Path=_SOURCE_PREFIX, Recursive=True):
        for param in page.get("Parameters", []):
            names.append(param["Name"])
    return sorted(names)


def _confirm_retire() -> bool:
    """Require a typed RETIRE confirmation for the retire step.

    Raises :exc:`RuntimeError` when stdin is not interactive and ``--yes``
    was not passed, rather than hanging or auto-proceeding.
    """
    if not sys.stdin.isatty():
        raise RuntimeError(
            "Non-interactive stdin and --yes not given; refusing to delete "
            "/portfolio/demo/* parameters without explicit confirmation."
        )
    answer = input(
        "Type RETIRE to confirm deleting all /portfolio/demo/* parameters: "
    ).strip()
    return answer == "RETIRE"


def retire_demo_parameters(
    ssm: Any,
    dry_run: bool = False,
    assume_yes: bool = False,
) -> int:
    """Delete every parameter under /portfolio/demo/ (the retire step).

    Guards: only parameters under :data:`_SOURCE_PREFIX` are considered,
    the exact names are printed first, and deletion requires a typed
    ``RETIRE`` confirmation (or ``assume_yes``).  Returns the number of
    parameters deleted.
    """
    names = _list_source_parameters(ssm)
    if not names:
        print(f"  No parameters under {_SOURCE_PREFIX} to retire.")
        return 0

    print(f"  {len(names)} parameter(s) under {_SOURCE_PREFIX}:")
    for name in names:
        print(f"    {name}")

    if dry_run:
        print("  [DRY RUN] Would delete-parameter each of the above.")
        return 0
    if not assume_yes and not _confirm_retire():
        print("  Aborted: retirement not confirmed. No parameters were deleted.")
        return 0

    for name in names:
        ssm.delete_parameter(Name=name)
        print(f"  Deleted: {name}")
    print(f"  Retired {len(names)} parameter(s).")
    return len(names)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migration B2: copy /portfolio/demo/* SSM parameter values "
        "to /portfolio/staging/* (run before the terraform apply that repoints "
        "ECS to the new paths), or --retire the /portfolio/demo/* parameters "
        "immediately after the swap is live.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION") or "eu-west-1",
        help="AWS region (default: AWS_REGION env var, else eu-west-1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--retire",
        action="store_true",
        help="Run the retire step instead of the copy: delete all parameters "
        "under /portfolio/demo/ (requires a typed RETIRE confirmation unless "
        "--yes is given)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive RETIRE confirmation in the retire step",
    )
    args = parser.parse_args()

    ssm = boto3.client("ssm", region_name=args.region)

    if args.retire:
        print(f"Retiring {_SOURCE_PREFIX} SSM parameters...")
        if args.dry_run:
            print("[DRY RUN MODE - no changes will be made]")
        print()
        retired = retire_demo_parameters(ssm, dry_run=args.dry_run, assume_yes=args.yes)
        print(f"\nRetire complete. {retired} parameter(s) retired.")
        if args.dry_run:
            print("[DRY RUN - no changes were made]")
    else:
        print(f"Copying {_SOURCE_PREFIX} values to {_DEST_PREFIX}...")
        if args.dry_run:
            print("[DRY RUN MODE - no changes will be made]")
        print()
        created = copy_parameters(ssm, dry_run=args.dry_run)
        print(f"\nCopy complete. {created} parameter(s) created.")
        if args.dry_run:
            print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
