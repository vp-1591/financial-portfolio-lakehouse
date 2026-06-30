from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import portfolio_connectors as connectors  # noqa: E402


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_LIVE_FX_TESTS") != "1",
    reason="live FX tests require RUN_LIVE_FX_TESTS=1",
)


def test_live_fx_converter_converts_pln_to_eur() -> None:
    converter = connectors.CurrencyConverter("EUR", timeout=20.0)

    converted = converter.convert(100.0, "PLN")

    assert 15.0 < converted < 35.0
