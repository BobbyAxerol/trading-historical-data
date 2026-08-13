import io
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import unittest

import pandas as pd

from collectors.binance_spot_1m import audit_symbol, normalize_kline_frame, proxy_fill_gaps_from_futures, read_vision_zip
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

    def test_audit_checks_partition_boundaries_without_concatenating_history(self):
        class Store:
            def files(self, attrs):
                if attrs != {"symbol": "BTCUSDT"}:
                    raise AssertionError(attrs)
                return [Path("2020-01.parquet"), Path("2020-02.parquet")]

        def frame(times):
            return pd.DataFrame(
                {
                    "time": times,
                    "symbol": ["BTCUSDT"] * len(times),
                    "open": [1.0] * len(times),
                    "high": [2.0] * len(times),
                    "low": [1.0] * len(times),
                    "close": [1.5] * len(times),
                    "volume": [1.0] * len(times),
                    "quote_volume": [1.0] * len(times),
                }
            )

        with patch(
            "collectors.binance_spot_1m.read_partition_file",
            side_effect=[
                frame(["2020-01-31 23:58:00", "2020-01-31 23:59:00"]),
                frame(["2020-02-01 00:00:00", "2020-02-01 00:01:00"]),
            ],
        ) as reader:
            audit = audit_symbol(Store(), "BTCUSDT", expected_start="2020-01-31 23:58:00")

        self.assertEqual(reader.call_count, 2)
        self.assertEqual(audit["rows"], 4)
        self.assertEqual(audit["gaps"], [])
        self.assertEqual(audit["duplicate_rows"], 0)

    def test_proxy_gap_check_does_not_load_complete_spot_or_futures_history(self):
        gap = {"start": "2018-01-04 03:01:00", "end": "2018-01-04 05:05:00", "minutes": "125"}
        with patch("collectors.binance_spot_1m.load_symbol_range", return_value=pd.DataFrame()) as range_loader, patch(
            "collectors.binance_spot_1m.load_symbol_frame", side_effect=AssertionError("must not load all partitions")
        ):
            rows = proxy_fill_gaps_from_futures(
                symbol="BTCUSDT",
                spot_store=object(),
                futures_store=object(),
                manifest=object(),
                gaps=[gap],
                max_gap_minutes=10080,
                context_hours=6,
                logger=__import__("logging").getLogger("test"),
            )
        self.assertEqual(rows, 0)
        self.assertEqual(range_loader.call_count, 1)


if __name__ == "__main__":
    unittest.main()
