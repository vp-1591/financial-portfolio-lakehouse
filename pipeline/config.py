"""Load pipeline configuration from YAML files.

Configuration precedence (highest wins):

1. CLI flags
2. ``pipeline.yaml`` (gitignored local overrides)
3. ``pipeline.defaults.yaml`` (version-controlled defaults)

Secrets are **never** stored in these files — they come from Bitwarden
or environment variables via :mod:`pipeline.secrets`.

Usage::

    from pipeline.config import load_config

    config = load_config()
    ibkr_enabled = config.get("connectors", {}).get("ibkr", {}).get("enabled", False)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULTS_FILE = PROJECT_ROOT / "pipeline.defaults.yaml"
OVERRIDES_FILE = PROJECT_ROOT / "pipeline.yaml"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge *override* into *base*.

    Lists and scalars in *override* replace values in *base*.
    Dicts are merged recursively.
    """
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config() -> dict[str, Any]:
    """Load pipeline configuration from YAML files.

    Merges ``pipeline.defaults.yaml`` with ``pipeline.yaml`` (if it
    exists).  Returns an empty dict if neither file exists.
    """
    config: dict[str, Any] = {}

    # 1. Load version-controlled defaults.
    if DEFAULTS_FILE.exists():
        with DEFAULTS_FILE.open() as f:
            defaults = yaml.safe_load(f)
            if defaults:
                config = defaults

    # 2. Layer gitignored local overrides on top.
    if OVERRIDES_FILE.exists():
        with OVERRIDES_FILE.open() as f:
            overrides = yaml.safe_load(f)
            if overrides:
                config = _deep_merge(config, overrides)

    return config


def get_connector_config(config: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the config dict for a specific connector.

    Returns an empty dict if the connector is not configured.
    """
    return config.get("connectors", {}).get(name, {})