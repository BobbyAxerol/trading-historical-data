import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import pandas as pd

from collectors.binance_futures_metrics_5m import normalize_metrics_frame


class TestBinanceFuturesMetrics(unittest.TestCase):
    def test_normalize_metrics_frame(self):
        raw = pd.DataFrame(
            {
                "create_time": ["2026-07-11 00:05:01"],
                "symbol": ["BTCUSDT"],
                "sum_open_interest": ["103887.929"],
                "sum_open_interest_value": ["6655883739.441257"],
                "count_toptrader_long_short_ratio": ["1.39895175"],
                "sum_toptrader_long_short_ratio": ["1.34758900"],
                "count_long_short_ratio": ["1.26964914"],
                "sum_taker_long_short_vol_ratio": ["0.40791800"],
            }
        )
        df = normalize_metrics_frame(raw, symbol="BTCUSDT", contract_type="PERPETUAL", source="test")
        self.assertEqual(len(df), 1)
        self.assertEqual(str(df.loc[0, "time"]), "2026-07-11 00:05:00")
        self.assertEqual(df.loc[0, "symbol"], "BTCUSDT")
        self.assertEqual(df.loc[0, "contract_type"], "PERPETUAL")
        self.assertAlmostEqual(df.loc[0, "sum_open_interest"], 103887.929)
        self.assertAlmostEqual(df.loc[0, "count_long_short_ratio"], 1.26964914)


if __name__ == "__main__":
    unittest.main()
