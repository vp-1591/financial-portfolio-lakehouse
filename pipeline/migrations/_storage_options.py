"""Shared helpers for standalone migration scripts."""

from __future__ import annotations

from pipeline.secrets import _boto3_default_chain_credentials
from pipeline.storage import get_storage


def get_storage_options_with_credentials() -> dict[str, str]:
    """Resolve storage options, injecting AWS credentials via boto3.

    The deltalake Rust backend (object_store) cannot read AWS credential
    files on all platforms.  Use
    :func:`pipeline.secrets._boto3_default_chain_credentials` (boto3's
    default chain) to discover credentials and pass them explicitly
    when not already present in the storage options.
    """
    storage = get_storage()
    opts = dict(storage.storage_options or {})

    if "aws_access_key_id" not in opts:
        boto = _boto3_default_chain_credentials(opts.get("aws_region", "eu-west-1"))
        if boto is not None:
            key_id, secret_key, token = boto
            opts["aws_access_key_id"] = key_id
            opts["aws_secret_access_key"] = secret_key
            if token:
                opts["aws_session_token"] = token

    return opts
