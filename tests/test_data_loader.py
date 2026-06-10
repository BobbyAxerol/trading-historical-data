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
    VnFutures1m,
    CryptoBinance1m,
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

    def test_crypto_daily_matrix_class(self):
        # Close feature daily matrix
        df = CryptoDailyMatrix().load(feature="close")
        self.assertFalse(df.empty, "Close matrix should not be empty")
        self.assertIn("BTCUSDT", df.columns)
        self.assertIsNone(df.index.tz)

        # Columns symbol filtering
        df_filtered = CryptoDailyMatrix().load(feature="close", symbols=["BTCUSDT", "ETHUSDT"])
        self.assertEqual(list(df_filtered.columns), ["BTCUSDT", "ETHUSDT"])

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


if __name__ == "__main__":
    unittest.main()
