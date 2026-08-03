import unittest

import numpy as np
import pandas as pd

from collectors.binance_daily_matrix import _merge_feature_matrix, _symbol_fetch_start


class TestBinanceDailyMatrixCollector(unittest.TestCase):
    def test_volume_fetch_start_detects_corrupted_zero_gap(self):
        index = pd.date_range("2020-01-01", periods=10, freq="D")
        open_df = pd.DataFrame({"BTCUSDT": range(10)}, index=index)
        volume_df = pd.DataFrame(
            {"BTCUSDT": [100, 100, 0, 0, 0, 0, 0, 100, 100, 100]},
            index=index,
        )

        start = _symbol_fetch_start(
            volume_df,
            "BTCUSDT",
            backfill_start=pd.Timestamp("2020-01-01"),
            end=pd.Timestamp("2020-01-10"),
            overlap_days=2,
            feature="volume",
            reference_df=open_df,
        )

        self.assertEqual(start, pd.Timestamp("2020-01-01"))

    def test_volume_fetch_start_ignores_pre_listing_matrix_rows(self):
        index = pd.date_range("2020-01-01", periods=10, freq="D")
        reference_index = index[5:]
        open_df = pd.DataFrame({"BTCUSDT": range(5)}, index=reference_index)
        volume_df = pd.DataFrame({"BTCUSDT": [0] * 5 + [100] * 5}, index=index)

        start = _symbol_fetch_start(
            volume_df,
            "BTCUSDT",
            backfill_start=pd.Timestamp("2020-01-01"),
            end=pd.Timestamp("2020-01-10"),
            overlap_days=2,
            feature="volume",
            reference_df=open_df,
        )

        self.assertEqual(start, pd.Timestamp("2020-01-08"))

    def test_missing_pivot_volume_does_not_overwrite_existing_value(self):
        index = pd.date_range("2020-01-01", periods=2, freq="D")
        fetched = pd.DataFrame(
            {
                "BTCUSDT": [10, np.nan],
                "ETHUSDT": [np.nan, 20],
            },
            index=index,
        )
        existing = pd.DataFrame(
            {
                "BTCUSDT": [100, 200],
                "ETHUSDT": [300, 400],
            },
            index=index,
        )

        merged = _merge_feature_matrix(
            fetched,
            existing,
            feature="volume",
            symbols=["BTCUSDT", "ETHUSDT"],
            end=pd.Timestamp("2020-01-02"),
        )

        self.assertEqual(merged.loc[index[0], "BTCUSDT"], 10)
        self.assertEqual(merged.loc[index[1], "BTCUSDT"], 200)
        self.assertEqual(merged.loc[index[0], "ETHUSDT"], 300)
        self.assertEqual(merged.loc[index[1], "ETHUSDT"], 20)
        self.assertTrue(pd.api.types.is_integer_dtype(merged["BTCUSDT"]))


if __name__ == "__main__":
    unittest.main()
