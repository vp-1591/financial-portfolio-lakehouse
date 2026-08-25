"""Per-broker bronze retention policy: merge keys and the per-run VACUUM.

The single source of truth for which raw column each broker's table merges on
(AD-1) and for the per-run VACUUM invocation (AD-3). XTB keys on
``account_id``; Trading 212 and IBKR key on ``source``. Trading 212's key is
the declared endpoint base — the pagination cursor suffix (``?cursor=...``) is
stripped before keying so cursor pages replace their endpoint's row instead of
fragmenting the key (AC-4, reconcile H1).
"""

from __future__ import annotations

import logging

from deltalake import DeltaTable

logger = logging.getLogger(__name__)

# Per-broker retention-key column (AD-1): XTB keys on the account id, every
# other broker on the ``source`` endpoint/query discriminator.
_RETENTION_KEYS: dict[str, str] = {"xtb": "account_id"}

# Trading 212 declares pagination via ``?cursor=...`` suffixes on the source.
_PAGINATED_CONNECTOR = "trading212"


def retention_key(connector_name: str) -> str:
    """Return the raw column the broker's raw table merges on (AD-1)."""
    return _RETENTION_KEYS.get(connector_name, "source")


def strip_pagination_suffix(source: str) -> str:
    """Strip a Trading 212 pagination cursor suffix from an endpoint path.

    Paginated T212 responses capture one raw row per page, with page-2+
    ``source`` values carrying the per-run ``?cursor=...`` token. The stored
    ``source`` column is stripped at fetch time (``trading212/fetch.py``) so
    ``SELECT DISTINCT source`` stays stable across runs (AC-7); the merge key
    strips again defensively so a page still lands on its endpoint's row
    (AC-4), including rows written before the fetch-time strip existed.
    """
    return source.split("?", 1)[0]


def retention_value(connector_name: str, key_value: str) -> str:
    """Return the effective merge-key value for a raw row's key column.

    Trading 212 keys on the endpoint base (pagination suffix stripped, AC-4);
    all other brokers key on the column value as-is.
    """
    if connector_name == _PAGINATED_CONNECTOR:
        return strip_pagination_suffix(key_value)
    return key_value


def merge_predicate(
    connector_name: str, source_alias: str = "s", target_alias: str = "t"
) -> str:
    """Return the Delta MERGE predicate on the broker's retention key (AC-1).

    Trading 212's predicate compares the pagination-stripped endpoint base on
    both sides so cursor pages replace their endpoint's row (AC-4).
    ``split_part`` is a datafusion scalar available in delta-rs 1.6.0 merge
    expressions (verified against a real local Delta table).
    """
    key = retention_key(connector_name)
    if connector_name == _PAGINATED_CONNECTOR:
        return (
            f"split_part({source_alias}.{key}, '?', 1) = "
            f"split_part({target_alias}.{key}, '?', 1)"
        )
    return f"{source_alias}.{key} = {target_alias}.{key}"


def vacuum_raw(raw_path: str, storage_options: dict[str, str] | None = None) -> None:
    """VACUUM a broker's raw table with the Delta 7-day default (AD-3, AC-5).

    Invoked at the end of each broker run by ``fetch_connector``. deltalake
    1.6.0 vacuums in ``dry_run=True`` by default — a no-op that only lists
    files — so ``dry_run=False`` is mandatory. ``retention_hours`` is omitted
    and ``enforce_retention_duration`` stays ``True``. Only the caller's own
    ``raw/{broker}`` path is ever passed here; silver event tables are never
    vacuumed (their MERGE target is off-limits). A missing table is not an
    error (first run, or a run whose writes produced no table).
    """
    try:
        target = DeltaTable(raw_path, storage_options=storage_options)
    except Exception:
        logger.debug("vacuum: no raw table at %s, skipping", raw_path)
        return
    target.vacuum(dry_run=False)
