import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import unittest
import pandas as pd
from data_loader import (
    load_data,
    validate_data,
    VnStock1m,
    VnStockDaily,
    VNDailyMatrix,
    VnFutures1m,
    CryptoBinance1m,
    CryptoBinanceQuarterly1m,
    BinanceOrderBookSnapshot1h,
    BinanceFuturesMetrics5m,
    CryptoDailyMatrix,
)


class TestDataLoaderClasses(unittest.TestCase):
    def test_vn_stock_daily_class(self):
        # AAA exists in partitioned storage
        df = VnStockDaily().load(symbols="AAA")
        self.assertFalse(df.empty, "Stock 1D should not be empty")
        self.assertEqual(df["symbol"].iloc[0], "AAA")
        self.assertIn("time", df.columns)
        
        # Verify timezone naive output (Asia/Ho_Chi_Minh)
        self.assertIsNone(df["time"].dt.tz)
        self.assertTrue(pd.api.types.is_numeric_dtype(df["close"]))

    def test_vn_stock_1m_class(self):
        # ACB exists in partitioned storage
        df = VnStock1m().load(symbols="ACB")
        self.assertFalse(df.empty, "Stock 1M should not be empty")
        self.assertEqual(df["symbol"].iloc[0], "ACB")
        self.assertIsNone(df["time"].dt.tz)

    def test_vn_futures_1m_class(self):
        # VN30F1M exists in partitioned storage
        df = VnFutures1m().load(symbols="VN30F1M")
        if not df.empty:
            self.assertEqual(df["symbol"].iloc[0], "VN30F1M")
            self.assertIsNone(df["time"].dt.tz)

    def test_crypto_binance_1m_class(self):
        # BTCUSDT exists in partitioned storage
        df = CryptoBinance1m().load(symbols="BTCUSDT")
        if not df.empty:
            self.assertEqual(df["symbol"].iloc[0], "BTCUSDT")
            self.assertIsNone(df["time"].dt.tz)

    def test_crypto_binance_quarterly_1m_alias(self):
        df = CryptoBinanceQuarterly1m().load(symbols="BTCUSDT_240329", limit=5, check_val=False)
        if not df.empty:
            self.assertEqual(df["symbol"].iloc[0], "BTCUSDT_240329")
            self.assertIsNone(df["time"].dt.tz)

    def test_binance_orderbook_snapshot_class_exists(self):
        df = BinanceOrderBookSnapshot1h().load(symbols="BTCUSDT", limit=5, check_val=False)
        if not df.empty:
            self.assertEqual(df["symbol"].iloc[0], "BTCUSDT")
            self.assertIn("bid_depth_1pct", df.columns)
            self.assertIn("q_bid_depth_1pct", df.columns)

    def test_binance_futures_metrics_class_exists(self):
        df = BinanceFuturesMetrics5m().load(symbols="BTCUSDT", limit=5, check_val=False)
        if not df.empty:
            self.assertEqual(df["symbol"].iloc[0], "BTCUSDT")
            self.assertIn("sum_open_interest", df.columns)
            self.assertIn("count_long_short_ratio", df.columns)

    def test_crypto_daily_matrix_class(self):
        # Close feature daily matrix
        df = CryptoDailyMatrix().load(feature="close")
        self.assertFalse(df.empty, "Close matrix should not be empty")
        self.assertIn("BTCUSDT", df.columns)
        self.assertIsNone(df.index.tz)

        # Columns symbol filtering
        df_filtered = CryptoDailyMatrix().load(feature="close", symbols=["BTCUSDT", "ETHUSDT"])
        self.assertEqual(list(df_filtered.columns), ["BTCUSDT", "ETHUSDT"])

        # Strategy-friendly OHLCV dict format
        data_dict = CryptoDailyMatrix().load_ohlcv(symbols=["BTCUSDT", "ETHUSDT"], limit=5)
        self.assertIn("BTCUSDT", data_dict)
        self.assertEqual(list(data_dict["BTCUSDT"].columns), ["open", "high", "low", "close", "volume"])
        self.assertIsNone(data_dict["BTCUSDT"].index.tz)
        self.assertTrue(data_dict["BTCUSDT"].index.is_monotonic_increasing)

        long_df = CryptoDailyMatrix().load_ohlcv_frame(symbols="BTCUSDT", limit=3)
        self.assertEqual(list(long_df.columns), ["time", "symbol", "open", "high", "low", "close", "volume"])
        self.assertEqual(len(long_df), 3)

    def test_vn_daily_matrix_class(self):
        df = VNDailyMatrix().load(feature="close", symbols=["FPT", "VCB"])
        self.assertFalse(df.empty, "VN close matrix should not be empty")
        self.assertEqual(list(df.columns), ["FPT", "VCB"])
        self.assertIsNone(df.index.tz)

        data_dict = VNDailyMatrix().load_ohlcv(symbols=["FPT", "VCB"], start_date="2016-01-04", limit=5)
        self.assertIn("FPT", data_dict)
        self.assertEqual(list(data_dict["FPT"].columns), ["open", "high", "low", "close", "volume"])
        self.assertTrue(data_dict["FPT"].index.is_monotonic_increasing)

        for df_symbol in data_dict.values():
            self.assertTrue((df_symbol["high"] >= df_symbol[["open", "close", "low"]].max(axis=1)).all())
            self.assertTrue((df_symbol["low"] <= df_symbol[["open", "close", "high"]].min(axis=1)).all())

        malformed = pd.DataFrame({
            "open": [10.0], "high": [9.0], "low": [11.0], "close": [12.0], "volume": [100],
        })
        repaired = VNDailyMatrix()._normalize_ohlcv(malformed)
        self.assertEqual(repaired.loc[0, "high"], 12.0)
        self.assertEqual(repaired.loc[0, "low"], 9.0)

    def test_date_range_and_limit(self):
        # Check limit logic on daily matrix
        df = CryptoDailyMatrix().load(feature="close", limit=3)
        self.assertEqual(len(df), 3)

        # Date slicing
        df_dates = CryptoDailyMatrix().load(
            feature="close",
            start_date="2026-06-02",
            end_date="2026-06-05",
        )
        self.assertTrue((df_dates.index >= pd.to_datetime("2026-06-02")).all())
        self.assertTrue((df_dates.index <= pd.to_datetime("2026-06-05")).all())

    def test_master_router(self):
        # Routing to stock daily
        df1 = load_data("vn_stock_daily", symbols="AAA")
        self.assertFalse(df1.empty)

        # Routing to daily matrix
        df2 = load_data("binance_daily_matrix", feature="close", limit=2)
        self.assertEqual(len(df2), 2)

        df3 = load_data("vn_daily_matrix", feature="close", symbols="FPT", limit=2)
        self.assertEqual(len(df3), 2)


if __name__ == "__main__":
    unittest.main()
