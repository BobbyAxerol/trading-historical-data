import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import pandas as pd

from collectors.binance_futures_metrics_5m import (
    _date_from_key,
    _fetch_rest_metric,
    effective_start_day,
    missing_coverage_key_days,
    normalize_metrics_frame,
)


class TestBinanceFuturesMetrics(unittest.TestCase):
    def test_usdm_rest_metric_endpoints_use_symbol_not_pair(self):
        start = pd.Timestamp("2026-08-13T00:00:00Z")
        end = pd.Timestamp("2026-08-13T00:05:00Z")
        endpoints = (
            "openInterestHist",
            "topLongShortAccountRatio",
            "topLongShortPositionRatio",
            "globalLongShortAccountRatio",
            "takerlongshortRatio",
        )

        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                with patch("collectors.binance_futures_metrics_5m._request_futures_data", return_value=[]) as request:
                    _fetch_rest_metric(endpoint, symbol="BTCUSDT", start=start, end=end)

                params = request.call_args.kwargs["params"]
                self.assertEqual(params["symbol"], "BTCUSDT")
                self.assertNotIn("pair", params)

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

    def test_date_from_key_supports_quarterly_symbols(self):
        key = "data/futures/um/daily/metrics/BTCUSDT_260925/BTCUSDT_260925-metrics-2026-07-11.zip"
        self.assertEqual(_date_from_key(key), "2026-07-11")

    def test_effective_start_auto_uses_earliest_vision_key(self):
        keys = [
            "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2020-01-02.zip",
            "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2020-01-01.zip",
        ]
        self.assertEqual(str(effective_start_day(keys, None).date()), "2020-01-01")
        self.assertEqual(str(effective_start_day(keys, "2020-01-02").date()), "2020-01-02")

    def test_missing_coverage_schedules_neighbor_file_for_midnight_bucket(self):
        available_days = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]).tolist()
        key_days, missing_days = missing_coverage_key_days(
            available_days=available_days,
            local_day_counts={"2020-01-01": 287, "2020-01-02": 287, "2020-01-03": 288},
            effective_start=pd.Timestamp("2020-01-01"),
            min_rows_per_full_day=288,
        )
        self.assertEqual([str(day.date()) for day in key_days], ["2020-01-01", "2020-01-02"])
        self.assertEqual(missing_days, [{"date": "2020-01-02", "rows": 287, "expected_rows": 288}])


if __name__ == "__main__":
    unittest.main()
