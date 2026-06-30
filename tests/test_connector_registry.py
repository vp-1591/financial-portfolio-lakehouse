"""Tests for the connector registry."""

from __future__ import annotations

import pytest

from pipeline.connectors.registry import all, get, register


class TestRegistry:
    def test_register_and_get_connector(self) -> None:
        class FakeConnector:
            name = "fake"
            display_name = "Fake"

            def fetch_snapshot(self, **kwargs):
                raise NotImplementedError

            def fetch_cdc(self, **kwargs):
                raise NotImplementedError

            def transform_snapshot(self, raw, fernet_key):
                raise NotImplementedError

            def transform_cdc(self, raw, fernet_key):
                raise NotImplementedError

        connector = register(FakeConnector())
        assert connector.name == "fake"
        assert get("fake") is connector

    def test_get_unknown_connector_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown connector"):
            get("nonexistent")

    def test_all_returns_registered_connectors(self) -> None:
        connectors = all()
        names = {c.name for c in connectors}
        # The three built-in connectors should be registered
        assert "ibkr" in names
        assert "trading212" in names
        assert "xtb" in names

    def test_ibkr_connector_is_registered(self) -> None:
        connector = get("ibkr")
        assert connector.name == "ibkr"
        assert connector.display_name == "IBKR"

    def test_trading212_connector_is_registered(self) -> None:
        connector = get("trading212")
        assert connector.name == "trading212"
        assert connector.display_name == "Trading 212"

    def test_xtb_connector_is_registered(self) -> None:
        connector = get("xtb")
        assert connector.name == "xtb"
        assert connector.display_name == "XTB"
