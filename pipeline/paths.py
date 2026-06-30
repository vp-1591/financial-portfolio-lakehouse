"""Path constants for data/ directories.

DEPRECATED: These constants are now resolved dynamically via
``pipeline.storage.get_storage()``.  They remain here for backward
compatibility during the transition period.  For new code, prefer
using ``pipeline.storage`` directly.
"""

from pipeline.storage import get_storage


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    """Lazy attribute access that delegates to :class:`StorageConfig`.

    This allows existing imports like ``from pipeline.paths import RAW_DIR``
    to continue working while the storage system is adopted.
    """
    config = get_storage()
    mapping = {
        "DATA_DIR": config.data_dir,
        "RAW_DIR": config.raw_dir,
        "NORMALIZED_DIR": config.normalized_dir,
        "ANALYTICS_DIR": config.analytics_dir,
        "SECRETS_DIR": config.secrets_dir,
        "ENCRYPTION_KEY_FILE": config.encryption_key_file,
        # Raw table paths
        "RAW_IBKR_SNAPSHOT": config.raw_path("ibkr_snapshot"),
        "RAW_IBKR_CDC": config.raw_path("ibkr_cdc"),
        "RAW_TRADING212_SNAPSHOT": config.raw_path("trading212_snapshot"),
        "RAW_TRADING212_CDC": config.raw_path("trading212_cdc"),
        "RAW_XTB_SNAPSHOT": config.raw_path("xtb_snapshot"),
        "RAW_XTB_CDC": config.raw_path("xtb_cdc"),
        # Normalized table paths
        "NORMALIZED_IBKR_SNAPSHOT": config.normalized_path("ibkr_snapshot"),
        "NORMALIZED_IBKR_CDC": config.normalized_path("ibkr_cdc"),
        "NORMALIZED_TRADING212_SNAPSHOT": config.normalized_path("trading212_snapshot"),
        "NORMALIZED_TRADING212_CDC": config.normalized_path("trading212_cdc"),
        "NORMALIZED_XTB_SNAPSHOT": config.normalized_path("xtb_snapshot"),
        "NORMALIZED_XTB_CDC": config.normalized_path("xtb_cdc"),
        "NORMALIZED_CONSOLIDATED_HOLDINGS": config.normalized_path("consolidated_holdings"),
        # Analytics table paths
        "ANALYTICS_PORTFOLIO_ALLOCATION": config.analytics_path("portfolio_allocation"),
    }
    if name in mapping:
        return mapping[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")