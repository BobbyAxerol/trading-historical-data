import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import unittest

import pandas as pd

from collectors.binance_spot_1m import normalize_kline_frame, read_vision_zip
from data_loader import CryptoBinanceSpot1m


def _zip_csv(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as archive:
        archive.writestr("BTCUSDT-1m-test.csv", text)
    return buf.getvalue()


class TestBinanceSpot1m(unittest.TestCase):
    def test_normalize_spot_schema(self):
        raw = pd.DataFrame(
            {
                "open_time": [1514764800000],
                "open": ["13715.65000000"],
                "high": ["13715.65000000"],
                "low": ["13400.01000000"],
                "close": ["13556.15000000"],
                "volume": ["45.98000000"],
                "close_time": [1514764859999],
                "quote_volume": ["624000.5"],
                "number_of_trades": [559],
                "taker_buy_base_volume": ["20.10000000"],
                "taker_buy_quote_volume": ["273000.0"],
                "ignore": [0],
            }
        )
        df = normalize_kline_frame(raw, symbol="BTCUSDT", source="test")
        self.assertEqual(list(df.columns)[0], "time")
        self.assertEqual(df.loc[0, "symbol"], "BTCUSDT")
        self.assertEqual(str(df.loc[0, "time"]), "2018-01-01 00:00:00")
        self.assertAlmostEqual(df.loc[0, "volume"], 45.98)

    def test_read_vision_zip_without_header_keeps_first_row(self):
        content = _zip_csv(
            "1514764800000,13715.65,13715.65,13400.01,13556.15,45.98,1514764859999,624000.5,559,20.1,273000.0,0\n"
            "1514764860000,13556.15,13600.00,13500.00,13590.00,12.34,1514764919999,167000.0,140,6.0,81500.0,0\n"
        )
        df = read_vision_zip(content, symbol="BTCUSDT", source="test")
        self.assertEqual(len(df), 2)
        self.assertEqual(str(df.loc[0, "time"]), "2018-01-01 00:00:00")

    def test_read_vision_zip_with_header_drops_header_row(self):
        header = ",".join(
            [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "number_of_trades",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
                "ignore",
            ]
        )
        content = _zip_csv(
            header
            + "\n1514764800000,13715.65,13715.65,13400.01,13556.15,45.98,1514764859999,624000.5,559,20.1,273000.0,0\n"
        )
        df = read_vision_zip(content, symbol="BTCUSDT", source="test")
        self.assertEqual(len(df), 1)
        self.assertEqual(str(df.loc[0, "time"]), "2018-01-01 00:00:00")

    def test_loader_normalize_keeps_fractional_volume(self):
        raw = pd.DataFrame(
            {
                "time": ["2018-01-01 00:00:00"],
                "symbol": ["BTCUSDT"],
                "open": [1],
                "high": [2],
                "low": [1],
                "close": [1.5],
                "volume": [2.345678],
            }
        )
        df = CryptoBinanceSpot1m()._normalize(raw)
        self.assertAlmostEqual(df.loc[0, "volume"], 2.345678)


if __name__ == "__main__":
    unittest.main()
