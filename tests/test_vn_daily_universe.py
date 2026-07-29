import os
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from collectors.vn_daily_universe import build_universe_report, configured_equity_symbols


class EnvCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        self.old_config_root = os.environ.get("CONFIG_ROOT")
        root = Path(self.tmp.name)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        os.environ["CONFIG_ROOT"] = str(root / "configs")
        Path(os.environ["CONFIG_ROOT"]).mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        _restore_env("DATA_ROOT", self.old_data_root)
        _restore_env("STATE_ROOT", self.old_state_root)
        _restore_env("CONFIG_ROOT", self.old_config_root)
        self.tmp.cleanup()

    def _write_symbol(self, symbol: str, dates: list[str], *, close: float = 10.0, volume: float = 1000.0) -> None:
        frame = pd.DataFrame({
            "time": pd.to_datetime(dates),
            "symbol": symbol,
            "open": close,
            "high": close * 1.02,
            "low": close * 0.98,
            "close": close,
            "volume": volume,
        })
        root = Path(os.environ["DATA_ROOT"]) / "vn" / "equity" / "1d" / f"symbol={symbol}" / f"year={pd.Timestamp(dates[0]).year:04d}"
        root.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(root / "part.parquet", index=False)

    def _write_futures_1m(self, symbol: str = "VN30F1M") -> None:
        frame = pd.DataFrame({
            "time": pd.to_datetime(["2024-01-02 09:00", "2024-01-02 09:01", "2024-01-03 09:00", "2024-01-03 09:01"]),
            "symbol": symbol,
            "open": [1000.0, 1001.0, 1010.0, 1012.0],
            "high": [1002.0, 1005.0, 1013.0, 1016.0],
            "low": [999.0, 1000.0, 1008.0, 1011.0],
            "close": [1001.0, 1004.0, 1012.0, 1015.0],
            "volume": [10.0, 15.0, 20.0, 25.0],
        })
        root = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "1m" / f"symbol={symbol}" / "year=2024" / "month=01"
        root.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(root / "part.parquet", index=False)


class TestVNDailyUniverse(EnvCase):
    def test_configured_equity_symbols_merges_candidates_once(self):
        config = {
            "symbols": ["FPT", "VCB", "FPT"],
            "candidate_symbols": ["MSB", "VCB", "qns"],
        }
        self.assertEqual(configured_equity_symbols(config), ["FPT", "VCB", "MSB", "QNS"])

    def test_report_starts_coverage_at_first_valid_date(self):
        dates = pd.bdate_range("2024-01-05", periods=10).strftime("%Y-%m-%d").tolist()
        self._write_symbol("NEW", dates, close=20.0, volume=2_000_000.0)

        report = build_universe_report(equity_symbols=["NEW"], as_of_date="2024-01-25", write=True)
        row = report.iloc[0].to_dict()

        self.assertEqual(row["symbol"], "NEW")
        self.assertEqual(row["first_valid_date"], "2024-01-05")
        self.assertGreater(row["coverage_ratio"], 0.9)
        self.assertNotIn("low_coverage", str(row["reasons"]))
        self.assertTrue((Path(os.environ["STATE_ROOT"]) / "vn_daily_universe_report.csv.gz").exists())

    def test_high_liquidity_new_listing_is_not_review(self):
        new_dates = pd.bdate_range("2024-06-03", periods=20).strftime("%Y-%m-%d").tolist()
        old_dates = pd.bdate_range("2020-01-01", periods=260).strftime("%Y-%m-%d").tolist()
        self._write_symbol("NEW", new_dates, close=50.0, volume=10_000_000.0)
        self._write_symbol("OLD", old_dates, close=5.0, volume=10_000.0)

        report = build_universe_report(equity_symbols=["NEW", "OLD"], as_of_date="2024-07-01", write=False)
        tier = report.loc[report["symbol"] == "NEW", "tier"].iloc[0]
        self.assertIn(tier, {"core", "extended"})

    def test_external_symbol_is_auxiliary_even_without_data(self):
        report = build_universe_report(equity_symbols=[], external_symbols=["VN30F1M"], as_of_date="2024-07-01", write=False)
        row = report.iloc[0].to_dict()
        self.assertEqual(row["symbol"], "VN30F1M")
        self.assertEqual(row["asset_type"], "future")
        self.assertEqual(row["tier"], "auxiliary")

    def test_vn_daily_main_calls_report_generation(self):
        config_path = Path(os.environ["CONFIG_ROOT"]) / "symbols.vn_daily.yml"
        config_path.write_text("backfill_start: '2016-01-01'\nsymbols: [FPT]\ncandidate_symbols: [MSB]\nexternal_symbols: [VN30F1M]\n")
        with patch.object(sys, "argv", ["vn_daily.py", "--mode", "once", "--max-symbols", "1"]):
            with patch("collectors.vn_daily.run_symbol") as run_symbol:
                with patch("collectors.vn_daily.build_matrix") as build_matrix:
                    with patch("collectors.vn_daily.build_universe_report") as build_report:
                        build_report.return_value = pd.DataFrame({"symbol": ["FPT"]})
                        from collectors.vn_daily import main

                        main()
        run_symbol.assert_called_once()
        build_matrix.assert_called_once()
        build_report.assert_called_once()

    def test_matrix_builder_includes_vn30f1m_auxiliary_from_1m(self):
        from collectors.vn_daily_matrix import build_matrix

        config_path = Path(os.environ["CONFIG_ROOT"]) / "symbols.vn_daily.yml"
        config_path.write_text("symbols: [FPT]\ncandidate_symbols: []\nexternal_symbols: [VN30F1M]\n")
        self._write_symbol("FPT", ["2024-01-02", "2024-01-03"], close=100.0, volume=1000.0)
        self._write_futures_1m("VN30F1M")

        result = build_matrix(start_date="2024-01-01", end_date="2024-01-05")

        self.assertEqual(result["equity_symbols"], ["FPT"])
        self.assertEqual(result["auxiliary_symbols"], ["VN30F1M"])
        close = pd.read_parquet(Path(os.environ["DATA_ROOT"]) / "vn" / "equity" / "daily_matrix" / "close.parquet")
        self.assertIn("FPT", close.columns)
        self.assertIn("VN30F1M", close.columns)
        self.assertEqual(float(close.loc[pd.Timestamp("2024-01-02"), "VN30F1M"]), 1004.0)
        self.assertEqual(float(close.loc[pd.Timestamp("2024-01-03"), "VN30F1M"]), 1015.0)

        futures_daily = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "1d" / "symbol=VN30F1M" / "year=2024" / "part.parquet"
        self.assertTrue(futures_daily.exists())
        daily = pd.read_parquet(futures_daily)
        self.assertEqual(float(daily.loc[daily["time"] == pd.Timestamp("2024-01-02"), "volume"].iloc[0]), 25.0)

        state = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_daily_matrix_symbols.json").read_text())
        self.assertEqual(state["equity_symbols"], ["FPT"])
        self.assertEqual(state["auxiliary_symbols"], ["VN30F1M"])
        self.assertIn("VN30F1M", state["symbols"])


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
