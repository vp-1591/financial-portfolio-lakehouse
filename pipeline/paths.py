"""Path constants for data/ directories."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
NORMALIZED_DIR = DATA_DIR / "normalized"
ANALYTICS_DIR = DATA_DIR / "analytics"

SECRETS_DIR = PROJECT_ROOT / ".secrets"
ENCRYPTION_KEY_FILE = SECRETS_DIR / "encryption.key"

# Raw table paths
RAW_IBKR_SNAPSHOT = RAW_DIR / "ibkr_snapshot"
RAW_IBKR_CDC = RAW_DIR / "ibkr_cdc"
RAW_TRADING212_SNAPSHOT = RAW_DIR / "trading212_snapshot"
RAW_TRADING212_CDC = RAW_DIR / "trading212_cdc"
RAW_XTB_SNAPSHOT = RAW_DIR / "xtb_snapshot"
RAW_XTB_CDC = RAW_DIR / "xtb_cdc"

# Normalized table paths
NORMALIZED_IBKR_SNAPSHOT = NORMALIZED_DIR / "ibkr_snapshot"
NORMALIZED_IBKR_CDC = NORMALIZED_DIR / "ibkr_cdc"
NORMALIZED_TRADING212_SNAPSHOT = NORMALIZED_DIR / "trading212_snapshot"
NORMALIZED_TRADING212_CDC = NORMALIZED_DIR / "trading212_cdc"
NORMALIZED_XTB_SNAPSHOT = NORMALIZED_DIR / "xtb_snapshot"
NORMALIZED_XTB_CDC = NORMALIZED_DIR / "xtb_cdc"
NORMALIZED_CONSOLIDATED_HOLDINGS = NORMALIZED_DIR / "consolidated_holdings"

# Analytics table paths
ANALYTICS_PORTFOLIO_ALLOCATION = ANALYTICS_DIR / "portfolio_allocation"