"""Shared utilities for bronze → silver (raw → normalized) transforms.

Provides helpers to decrypt, parse, and iterate raw Delta table rows,
build normalized PyArrow tables from row dicts using Polars for column
encryption and schema casting, and finalize Polars DataFrames into
typed PyArrow tables with encryption.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import polars as pl
import pyarrow as pa

from pipeline.crypto import decrypt, encrypt_float

logger = logging.getLogger(__name__)


@dataclass
class DecodedRow:
    """A decoded raw-layer row with decrypted payload and parsed content."""

    fetched_at: datetime
    source: str
    source_file: str
    payload_parsed: Any  # Parsed JSON (dict or list), or None
    payload_raw: bytes  # Decrypted bytes (for XML/XLSX payloads)


def decode_payload(payload: bytes | memoryview, fernet_key: bytes) -> bytes | None:
    """Decrypt a raw payload. Returns None on decryption failure."""
    if isinstance(payload, memoryview):
        payload = bytes(payload)
    try:
        return decrypt(payload, fernet_key)
    except Exception:
        return None


def parse_json(data: bytes) -> Any | None:
    """Parse JSON bytes. Returns None on parse failure."""
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return None


def filter_latest_snapshot(raw: pa.Table) -> pa.Table:
    """Filter a raw table to keep only the latest fetch row per source.

    All rows from a single fetch share the same ``fetched_at`` timestamp.
    This function keeps the latest row(s) for each distinct ``source`` value,
    effectively discarding stale snapshot batches that accumulated via
    append-mode writes.

    Grouping per ``source`` is necessary for brokers like Trading 212 where
    a snapshot is fetched from multiple API endpoints. If one endpoint's
    payload is deduped on a subsequent fetch because its content didn't change,
    its timestamp remains at the previous fetch time while other endpoints get
    newer timestamps. Filtering per source preserves the latest payload for
    every endpoint, preventing missing-endpoint errors in the snapshot transform.

    For CDC (change data capture) data this filter should **not** be used
    -- CDC rows are chronological events, not replaceable snapshots.

    Parameters
    ----------
    raw:
        PyArrow table matching :data:`RAW_SCHEMA`.

    Returns
    -------
    pa.Table
        The same table filtered to the latest ``fetched_at`` value per source.
        Returns the input unchanged if it has 0 or 1 rows.
    """
    if raw.num_rows <= 1:
        return raw

    # Decision: docs/adr/0100-fix-snapshot-dedup-per-source-and-t212-encryption.md
    # (reverses the global-max filter decided in ADR 0057). The max MUST be
    # computed per ``source`` via ``.over("source")`` -- a bare global
    # ``.max()`` would drop stale-but-valid per-endpoint rows and regress
    # ADR 0100 (issue #109 proposed exactly that regression).
    df = pl.DataFrame(raw)
    df = df.filter(pl.col("fetched_at") == pl.col("fetched_at").max().over("source"))
    # Cast back to the input schema: polars to_arrow() emits large_string /
    # large_binary, so the result would not .equals(RAW_SCHEMA). Mirrors the
    # build_normalized_table cast convention (ADR 0045).
    return df.to_arrow().cast(raw.schema)


def coerce_fetched_at(value: Any) -> datetime:
    """Convert a fetched_at value to a timezone-aware datetime.

    Handles: datetime objects, ISO-format strings, and Arrow/Pandas Timestamp objects.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    # pandas Timestamp or other datetime-like
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    return value


def empty_arrow_table(schema: pa.Schema) -> pa.Table:
    """Build an empty PyArrow table matching *schema* (columns, dtypes, order).

    Shared by the normalized builder (``build_normalized_table``) and the
    analytics empty-frame helper (``_empty_analytics_frame``) so the
    empty-table recipe is defined once.
    """
    return pa.table(
        {field.name: pa.array([], type=field.type) for field in schema},
        schema=schema,
    )


def iter_raw_payloads(
    raw: pa.Table,
    fernet_key: bytes,
    *,
    require_json: bool = True,
) -> Iterator[DecodedRow]:
    """Iterate over rows of a raw table, decrypting and parsing payloads.

    Yields :class:`DecodedRow` for rows where decryption (and optionally
    JSON parsing) succeeds.  Skips rows that fail.

    When *require_json* is ``False``, rows with non-JSON payloads (e.g.\
    XML) are still yielded with ``payload_parsed=None`` and
    ``payload_raw`` set to the decrypted bytes.

    Parameters
    ----------
    raw:
        PyArrow table matching :data:`RAW_SCHEMA`.
    fernet_key:
        Fernet key for decrypting the ``payload`` column.
    require_json:
        If True (default), skip rows whose payloads cannot be parsed as
        JSON.  Set to False for sources that produce XML or other formats.
    """

    fetched_ats = raw.column("fetched_at").to_pylist()
    sources = raw.column("source").to_pylist()
    payloads = raw.column("payload").to_pylist()
    source_files = raw.column("source_file").to_pylist()

    decode_failures = 0
    parse_failures = 0

    for i in range(len(fetched_ats)):
        fetched_at = coerce_fetched_at(fetched_ats[i])
        source = str(sources[i] or "")
        source_file = str(source_files[i] or "")
        payload_bytes = payloads[i]

        decrypted = decode_payload(payload_bytes, fernet_key)
        if decrypted is None:
            decode_failures += 1
            continue

        parsed = parse_json(decrypted)
        if require_json and parsed is None:
            parse_failures += 1
            continue

        yield DecodedRow(
            fetched_at=fetched_at,
            source=source,
            source_file=source_file,
            payload_parsed=parsed,
            payload_raw=decrypted,
        )

    if decode_failures or parse_failures:
        logger.warning(
            "iter_raw_payloads: %d decode failures, %d parse failures "
            "(rows dropped out of %d total)",
            decode_failures,
            parse_failures,
            len(fetched_ats),
        )


def build_normalized_table(
    records: list[dict[str, Any]],
    schema: pa.Schema,
    fernet_key: bytes,
    encrypt_columns: list[str] | None = None,
) -> pa.Table:
    """Build a normalized PyArrow table from row dicts, encrypting specified columns.

    Replaces the manual list-append pattern (initialize N empty lists, loop
    and append to each, encrypt inline, assemble ``pa.table()``) with a single
    Polars DataFrame construction followed by batch column encryption.

    Parameters
    ----------
    records:
        List of dicts, one per output row.  Keys must match schema field names.
        Values for columns listed in *encrypt_columns* must be plain floats
        (not yet encrypted) — the function applies Fernet encryption via
        ``encrypt_float``.
    schema:
        Target PyArrow schema.  Encrypted columns must be ``pa.binary()``
        in the schema but ``float`` in the input dicts.
    fernet_key:
        Fernet key for encrypting float columns.
    encrypt_columns:
        Column names whose float values should be Fernet-encrypted to binary.
        Defaults to an empty list (no encryption).
    """
    if encrypt_columns is None:
        encrypt_columns = []

    # Empty result set: return a correctly-typed empty table.
    if not records:
        return empty_arrow_table(schema)

    df = pl.DataFrame(records)

    # Encrypt specified float columns to binary Fernet tokens.
    for col_name in encrypt_columns:
        if col_name in df.columns:
            df = df.with_columns(
                pl.col(col_name)
                .map_elements(
                    lambda v, _key=fernet_key: (
                        encrypt_float(v, _key) if v is not None else None
                    ),
                    return_dtype=pl.Binary,
                )
                .alias(col_name),
            )

    # Ensure all schema columns are present; fill missing with null.
    for field in schema:
        if field.name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field.name))

    # Reorder columns to match schema order.
    df = df.select([field.name for field in schema])

    # Convert to PyArrow and cast to target schema.
    arrow_table = df.to_arrow()
    return arrow_table.cast(schema)


def decrypt_cdc_payloads(
    raw: pa.Table, fernet_key: bytes
) -> list[tuple[datetime, str, list[dict]]]:
    """Decrypt and parse CDC payloads, returning event lists ready for transform.

    Replaces :func:`iter_raw_payloads` for CDC transforms.  Instead of
    yielding one :class:`DecodedRow` at a time, returns a list of
    ``(fetched_at, source, events)`` tuples where *events* is the unwrapped
    list of event dicts from each payload.  This allows callers to construct
    Polars DataFrames directly from the event dicts.

    Paginated responses (``{"items": [...], "nextPagePath": ...}``) are
    automatically unwrapped.
    """
    fetched_ats = raw.column("fetched_at").to_pylist()
    sources = raw.column("source").to_pylist()
    payloads = raw.column("payload").to_pylist()

    results: list[tuple[datetime, str, list[dict]]] = []
    decode_failures = 0
    parse_failures = 0
    empty_events = 0

    for i in range(len(fetched_ats)):
        fetched_at = coerce_fetched_at(fetched_ats[i])
        source = str(sources[i] or "")
        payload_bytes = payloads[i]

        decrypted = decode_payload(payload_bytes, fernet_key)
        if decrypted is None:
            decode_failures += 1
            continue

        parsed = parse_json(decrypted)
        if parsed is None:
            parse_failures += 1
            continue

        events = _unwrap_events(parsed)
        if events:
            results.append((fetched_at, source, events))
        else:
            empty_events += 1

    if decode_failures or parse_failures or empty_events:
        logger.warning(
            "decrypt_cdc_payloads: %d decode failures, %d parse failures, "
            "%d empty-event payloads (out of %d total rows)",
            decode_failures,
            parse_failures,
            empty_events,
            len(fetched_ats),
        )

    return results


def _unwrap_events(payload: object) -> list[dict]:
    """Unwrap a CDC API response into a list of event dicts.

    Handles both bare JSON lists and paginated dicts with
    ``{"items": [...], "nextPagePath": ...}``.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "items" in payload:
        items = payload["items"]
        if isinstance(items, list):
            return items
    return []


def finalize_table(
    df: pl.DataFrame,
    schema: pa.Schema,
    fernet_key: bytes,
    encrypt_columns: list[str] | None = None,
) -> pa.Table:
    """Finalize a Polars DataFrame into a typed PyArrow table with encryption.

    Like :func:`build_normalized_table` but starts from a Polars DataFrame
    instead of a list of dicts.  Encrypts specified float columns via
    ``encrypt_float``, fills missing columns with null, and casts to the
    target schema.

    Parameters
    ----------
    df:
        Polars DataFrame with column names matching *schema* field names.
        Values for columns listed in *encrypt_columns* must be plain floats
        (not yet encrypted).
    schema:
        Target PyArrow schema.  Encrypted columns must be ``pa.binary()``
        in the schema but ``float`` in the DataFrame.
    fernet_key:
        Fernet key for encrypting float columns.
    encrypt_columns:
        Column names whose float values should be Fernet-encrypted to binary.
        Defaults to an empty list (no encryption).
    """
    if encrypt_columns is None:
        encrypt_columns = []

    # Encrypt specified float columns to binary Fernet tokens.
    for col_name in encrypt_columns:
        if col_name in df.columns:
            df = df.with_columns(
                pl.col(col_name)
                .map_elements(
                    lambda v, _key=fernet_key: (
                        encrypt_float(v, _key) if v is not None else None
                    ),
                    return_dtype=pl.Binary,
                )
                .alias(col_name),
            )

    # Ensure all schema columns are present; fill missing with null.
    for field in schema:
        if field.name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field.name))

    # Reorder columns to match schema order.
    df = df.select([field.name for field in schema])

    # Convert to PyArrow and cast to target schema.
    arrow_table = df.to_arrow()
    return arrow_table.cast(schema)


def dedup_cdc_events(
    df: pl.DataFrame,
    subset: list[str],
    *,
    sort_after: list[str] | None = None,
    label: str = "CDC",
) -> pl.DataFrame:
    """Deduplicate CDC events, keeping the latest-``fetched_at`` version.

    Sorts by ``fetched_at`` descending so the newest fetch is first, then
    keeps the first row per *subset* group.  ``keep="first"`` is required:
    ``unique()``'s default ``keep="any"`` is non-deterministic and may drop
    the newest version, violating the "latest fetched_at wins" contract
    (Decision: docs/adr/0105-fix-t212-cdc-dedup-and-concat-type-mismatch.md).
    When *sort_after* is given, re-sorts for deterministic row order across
    runs.  Logs how many duplicates were removed under *label*.

    A no-op (returns *df* unchanged) when *df* is empty.

    Parameters
    ----------
    df:
        Polars DataFrame with ``fetched_at`` and every column in *subset*.
    subset:
        Columns defining event identity (e.g. ``["event_type", "event_id"]``).
    sort_after:
        Optional columns to sort by after dedup for stable output order.
    label:
        Prefix for the dedup log line.
    """
    if df.height == 0:
        return df
    before = df.height
    df = df.sort("fetched_at", descending=True).unique(subset=subset, keep="first")
    if sort_after is not None:
        df = df.sort(sort_after)
    after = df.height
    if before > after:
        logger.info(
            "%s dedup: removed %d duplicate events (%d → %d)",
            label,
            before - after,
            before,
            after,
        )
    return df
