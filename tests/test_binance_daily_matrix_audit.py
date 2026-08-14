from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from collectors.binance_daily_matrix import FEATURES, _build_matrix_audit_report


def _universe(symbols: list[str]) -> dict:
    return {
        "updated_at": "2026-08-14T00:00:00+00:00",
        "symbols": symbols,
        "active_symbols": symbols,
        "universe_policy": {
            "top_n": 400,
            "contractType": "PERPETUAL",
            "underlyingType": "COIN",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "min_history_days": 365,
        },
    }


class TestBinanceDailyMatrixAudit(unittest.TestCase):
    def write_matrix(self, root: Path, *, missing_tail: bool = False) -> None:
        index = pd.date_range("2020-01-01", periods=2 if missing_tail else 3, freq="D", name="time")
        values = {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [10, 11, 12],
        }
        for feature in FEATURES:
            data = values[feature][: len(index)]
            frame = pd.DataFrame({"BTCUSDT": data, "ETHUSDT": data}, index=index)
            frame.to_parquet(root / f"{feature}.parquet", engine="pyarrow")

    def test_passes_complete_aligned_matrix_and_universe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_matrix(root)
            report = _build_matrix_audit_report(
                root,
                universe_state=_universe(["BTCUSDT", "ETHUSDT"]),
                backfill_start=pd.Timestamp("2020-01-01"),
                closed_end=pd.Timestamp("2020-01-03"),
                top_n=400,
                min_history_days=365,
            )

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["quality"]["continuity_gap_count"], 0)
        self.assertEqual(report["universe"]["matrix_column_count"], 2)

    def test_rejects_missing_closed_day_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_matrix(root, missing_tail=True)
            report = _build_matrix_audit_report(
                root,
                universe_state=_universe(["BTCUSDT", "ETHUSDT"]),
                backfill_start=pd.Timestamp("2020-01-01"),
                closed_end=pd.Timestamp("2020-01-03"),
                top_n=400,
                min_history_days=365,
            )

        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["quality"]["missing_tail_symbol_count"], 2)


if __name__ == "__main__":
    unittest.main()
