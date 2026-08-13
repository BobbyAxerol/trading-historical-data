import io
import logging
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import unittest

import pandas as pd

from collectors.binance_usdm_quarterly_1m import (
    NUMERIC_COLUMNS,
    _archive_symbol_in_scope,
    _delivery_from_symbol,
    audit_symbol,
    normalize_kline_frame,
    read_vision_zip,
    sync_rest_tail,
)


def _zip_csv(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("BTCUSDT_240329-1m-test.csv", text)
    return buffer.getvalue()


class TestBinanceUsdmQuarterly(unittest.TestCase):
    def test_delivery_from_symbol(self):
        self.assertEqual(_delivery_from_symbol("BTCUSDT_240329"), "2024-03-29")
        self.assertIsNone(_delivery_from_symbol("BTCUSDT"))

    def test_archive_scope_excludes_contracts_before_configured_start(self):
        self.assertFalse(_archive_symbol_in_scope("BTCUSDT_201225", "2021-02"))
        self.assertTrue(_archive_symbol_in_scope("BTCUSDT_210326", "2021-02"))

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

    def test_read_vision_zip_without_header_keeps_first_candle(self):
        content = _zip_csv(
            "1709251200000,62110.8,62163.7,62088.5,62162.9,1.805,1709251259999,112127.0301,84,1.210,75170.5038,0\n"
            "1709251260000,62162.9,62170.0,62150.0,62160.0,2.000,1709251319999,124000.0000,85,1.000,62160.0000,0\n"
        )

        frame = read_vision_zip(content, symbol="BTCUSDT_240329", source="test")

        self.assertEqual(len(frame), 2)
        self.assertEqual(str(frame.loc[0, "time"]), "2024-03-01 00:00:00")

    def test_streaming_audit_passes_a_complete_contract_partition(self):
        class Store:
            def files(self, attrs):
                if attrs != {"symbol": "BTCUSDT_240329"}:
                    raise AssertionError(attrs)
                return [Path("2024-03.parquet")]

        frame = pd.DataFrame(
            {
                "time": ["2024-03-01 00:00:00", "2024-03-01 00:01:00"],
                "symbol": ["BTCUSDT_240329", "BTCUSDT_240329"],
                "source": ["binance_vision_futures_um_monthly", "binance_vision_futures_um_monthly"],
            }
        )
        for column in NUMERIC_COLUMNS:
            frame[column] = 1.0
        frame["high"] = 2.0

        with patch("collectors.binance_usdm_quarterly_1m.read_partition_file", return_value=frame):
            audit = audit_symbol(
                Store(),
                "BTCUSDT_240329",
                is_active=False,
                expected_first_archive_month="2024-03",
                expected_last_archive_month="2024-03",
            )

        self.assertEqual(audit["status"], "pass")
        self.assertEqual(audit["rows"], 2)
        self.assertEqual(audit["gap_count"], 0)
        self.assertEqual(audit["source_mismatch_rows"], 0)

    def test_streaming_audit_records_but_does_not_reject_source_after_symbol_date(self):
        class Store:
            def files(self, attrs):
                if attrs != {"symbol": "BTCUSDT_230929"}:
                    raise AssertionError(attrs)
                return [Path("2023-11.parquet")]

        frame = pd.DataFrame(
            {
                "time": ["2023-11-01 00:00:00", "2023-11-01 00:01:00"],
                "symbol": ["BTCUSDT_230929", "BTCUSDT_230929"],
                "source": ["binance_vision_futures_um_monthly", "binance_vision_futures_um_monthly"],
            }
        )
        for column in NUMERIC_COLUMNS:
            frame[column] = 1.0
        frame["high"] = 2.0

        with patch("collectors.binance_usdm_quarterly_1m.read_partition_file", return_value=frame):
            audit = audit_symbol(
                Store(),
                "BTCUSDT_230929",
                is_active=False,
                expected_first_archive_month="2023-11",
                expected_last_archive_month="2023-11",
            )

        self.assertEqual(audit["status"], "pass")
        self.assertEqual(audit["after_symbol_date_rows"], 2)

    def test_explicit_rest_bridge_is_not_shortened_by_newer_local_tail(self):
        class Store:
            def latest_time(self, **kwargs):
                if kwargs != {"attrs": {"symbol": "BTCUSDT_260925"}, "time_col": "time"}:
                    raise AssertionError(kwargs)
                return pd.Timestamp("2026-08-13 14:13:00")

        class Manifest:
            def update_symbol(self, *args, **kwargs):
                return None

        with patch(
            "collectors.binance_usdm_quarterly_1m.fetch_1m",
            return_value=pd.DataFrame({"time": [pd.Timestamp("2026-08-12 00:00:00")]}),
        ) as fetch, patch(
            "collectors.binance_usdm_quarterly_1m._append",
            return_value={"rows_written": 1, "latest_time": "2026-08-13T17:00:00"},
        ):
            sync_rest_tail(
                symbol="BTCUSDT_260925",
                meta=None,
                store=Store(),
                manifest=Manifest(),
                overlap_minutes=10,
                rest_start="2026-08-12T00:00:00Z",
                logger=logging.getLogger("test"),
            )

        self.assertEqual(fetch.call_args.args[1], pd.Timestamp("2026-08-12T00:00:00Z"))


if __name__ == "__main__":
    unittest.main()
