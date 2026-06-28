"""Tests for pipeline.config module."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from pipeline.config import _deep_merge, get_connector_config, load_config


class TestDeepMerge:
    """Test the _deep_merge helper."""

    def test_simple_merge(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        base = {"connectors": {"ibkr": {"enabled": False, "base_url": "http://default"}}}
        override = {"connectors": {"ibkr": {"enabled": True}}}
        result = _deep_merge(base, override)
        assert result == {"connectors": {"ibkr": {"enabled": True, "base_url": "http://default"}}}

    def test_override_replaces_list(self):
        base = {"items": [1, 2, 3]}
        override = {"items": [4, 5]}
        result = _deep_merge(base, override)
        assert result == {"items": [4, 5]}

    def test_override_replaces_scalar(self):
        base = {"target_currency": "USD"}
        override = {"target_currency": "EUR"}
        result = _deep_merge(base, override)
        assert result == {"target_currency": "EUR"}

    def test_add_new_key(self):
        base = {"a": 1}
        override = {"b": 2}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": 2}


class TestLoadConfig:
    """Test load_config() with defaults and overrides files."""

    def test_defaults_only(self, tmp_path: Path, monkeypatch):
        """When only defaults file exists, its values are returned."""
        defaults = tmp_path / "pipeline.defaults.yaml"
        defaults.write_text(dedent("""\
            target_currency: EUR
            connectors:
              ibkr:
                enabled: false
        """))

        from pipeline import config as config_module
        monkeypatch.setattr(config_module, "DEFAULTS_FILE", defaults)
        monkeypatch.setattr(config_module, "OVERRIDES_FILE", tmp_path / "pipeline.yaml")

        result = load_config()
        assert result["target_currency"] == "EUR"
        assert result["connectors"]["ibkr"]["enabled"] is False

    def test_overrides_layer_on_defaults(self, tmp_path: Path, monkeypatch):
        """Overrides are deep-merged onto defaults."""
        defaults = tmp_path / "pipeline.defaults.yaml"
        defaults.write_text(dedent("""\
            target_currency: USD
            connectors:
              ibkr:
                enabled: false
                base_url: http://default
              trading212:
                enabled: false
        """))

        overrides = tmp_path / "pipeline.yaml"
        overrides.write_text(dedent("""\
            target_currency: EUR
            connectors:
              ibkr:
                enabled: true
        """))

        from pipeline import config as config_module
        monkeypatch.setattr(config_module, "DEFAULTS_FILE", defaults)
        monkeypatch.setattr(config_module, "OVERRIDES_FILE", overrides)

        result = load_config()
        assert result["target_currency"] == "EUR"
        assert result["connectors"]["ibkr"]["enabled"] is True
        # Default is preserved when not overridden
        assert result["connectors"]["ibkr"]["base_url"] == "http://default"
        assert result["connectors"]["trading212"]["enabled"] is False

    def test_no_files_returns_empty(self, tmp_path: Path, monkeypatch):
        """When neither file exists, return empty dict."""
        from pipeline import config as config_module
        monkeypatch.setattr(config_module, "DEFAULTS_FILE", tmp_path / "nonexistent.yaml")
        monkeypatch.setattr(config_module, "OVERRIDES_FILE", tmp_path / "also-nonexistent.yaml")

        result = load_config()
        assert result == {}

    def test_overrides_only(self, tmp_path: Path, monkeypatch):
        """When only overrides file exists (no defaults), return overrides."""
        overrides = tmp_path / "pipeline.yaml"
        overrides.write_text(dedent("""\
            target_currency: GBP
        """))

        from pipeline import config as config_module
        monkeypatch.setattr(config_module, "DEFAULTS_FILE", tmp_path / "nonexistent.yaml")
        monkeypatch.setattr(config_module, "OVERRIDES_FILE", overrides)

        result = load_config()
        assert result["target_currency"] == "GBP"


class TestGetConnectorConfig:
    """Test get_connector_config() helper."""

    def test_get_existing_connector(self):
        config = {
            "connectors": {
                "ibkr": {"enabled": True, "base_url": "http://test"}
            }
        }
        result = get_connector_config(config, "ibkr")
        assert result == {"enabled": True, "base_url": "http://test"}

    def test_get_missing_connector(self):
        config = {"connectors": {"ibkr": {"enabled": False}}}
        result = get_connector_config(config, "xtb")
        assert result == {}

    def test_get_from_empty_config(self):
        result = get_connector_config({}, "ibkr")
        assert result == {}