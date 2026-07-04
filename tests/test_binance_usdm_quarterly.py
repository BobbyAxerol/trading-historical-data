import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import unittest

import pandas as pd

from collectors.binance_usdm_quarterly_1m import _delivery_from_symbol, normalize_kline_frame


class TestBinanceUsdmQuarterly(unittest.TestCase):
    def test_delivery_from_symbol(self):
        self.assertEqual(_delivery_from_symbol("BTCUSDT_240329"), "2024-03-29")
        self.assertIsNone(_delivery_from_symbol("BTCUSDT"))

    def test_normalize_vision_header_schema(self):
        raw = pd.DataFrame(
            {
                "open_time": [1709251200000],
                "open": ["62110.8"],
                "high": ["62163.7"],
                "low": ["62088.5"],
                "close": ["62162.9"],
                "volume": ["1.805"],
                "close_time": [1709251259999],
                "quote_volume": ["112127.0301"],
                "count": [84],
                "taker_buy_volume": ["1.210"],
                "taker_buy_quote_volume": ["75170.5038"],
                "ignore": [0],
            }
        )
        df = normalize_kline_frame(raw, symbol="BTCUSDT_240329", source="test")
        self.assertEqual(df.loc[0, "symbol"], "BTCUSDT_240329")
        self.assertEqual(df.loc[0, "number_of_trades"], 84)
        self.assertAlmostEqual(df.loc[0, "taker_buy_base_volume"], 1.210)
        self.assertEqual(str(df.loc[0, "time"]), "2024-03-01 00:00:00")


if __name__ == "__main__":
    unittest.main()
