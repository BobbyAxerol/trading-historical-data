import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import pandas as pd

from collectors.binance_orderbook_snapshot_1h import normalize_rest_depth, normalize_vision_book_depth


class TestBinanceOrderBookSnapshot(unittest.TestCase):
    def test_normalize_vision_book_depth_maps_percent_bands(self):
        raw = pd.DataFrame(
            {
                "timestamp": [
                    "2026-07-11 00:00:03",
                    "2026-07-11 00:00:03",
                    "2026-07-11 00:00:03",
                    "2026-07-11 00:00:03",
                    "2026-07-11 00:59:33",
                    "2026-07-11 00:59:33",
                    "2026-07-11 00:59:33",
                    "2026-07-11 00:59:33",
                ],
                "percentage": [-1.0, 1.0, -0.2, 0.2, -1.0, 1.0, -0.2, 0.2],
                "depth": [10, 12, 3, 4, 20, 30, 7, 8],
                "notional": [1000, 1200, 300, 400, 2000, 3000, 700, 800],
            }
        )
        df = normalize_vision_book_depth(
            raw,
            symbol="BTCUSDT_260925",
            contract_type="CURRENT_QUARTER",
            percent_bands=[0.002, 0.01],
            primary_band=0.01,
            source="test",
        )
        self.assertEqual(len(df), 1)
        self.assertEqual(str(df.loc[0, "time"]), "2026-07-11 00:00:00")
        self.assertEqual(str(df.loc[0, "sample_time"]), "2026-07-11 00:59:33")
        self.assertAlmostEqual(df.loc[0, "bid_depth_1pct"], 20)
        self.assertAlmostEqual(df.loc[0, "ask_depth_1pct"], 30)
        self.assertAlmostEqual(df.loc[0, "q_bid_depth_1pct"], 2000)
        self.assertAlmostEqual(df.loc[0, "q_ask_depth_1pct"], 3000)
        self.assertAlmostEqual(df.loc[0, "primary_imbalance"], -0.2)

    def test_normalize_rest_depth_computes_depth_bands(self):
        payload = {
            "lastUpdateId": 1,
            "bids": [["99", "2"], ["98", "3"], ["90", "10"]],
            "asks": [["101", "4"], ["102", "5"], ["110", "10"]],
        }
        df = normalize_rest_depth(
            payload,
            symbol="BTCUSDT",
            contract_type="PERPETUAL",
            depth_limit=20,
            percent_bands=[0.01, 0.05],
            primary_band=0.01,
            source="test",
        )
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.loc[0, "mid_price"], 100)
        self.assertAlmostEqual(df.loc[0, "bid_depth_1pct"], 2)
        self.assertAlmostEqual(df.loc[0, "ask_depth_1pct"], 4)
        self.assertAlmostEqual(df.loc[0, "q_bid_depth_1pct"], 198)
        self.assertAlmostEqual(df.loc[0, "q_ask_depth_1pct"], 404)
        self.assertAlmostEqual(df.loc[0, "bid_depth_5pct"], 5)
        self.assertAlmostEqual(df.loc[0, "ask_depth_5pct"], 9)


if __name__ == "__main__":
    unittest.main()
