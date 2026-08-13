import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

import pandas as pd

from collectors.binance_futures_metrics_5m import (
    _date_from_key,
    _fetch_rest_metric,
    METRIC_COLUMNS,
    STORE_PARTS,
    PartitionedCsvGzStore,
    append_metrics,
    audit_symbol,
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

    def test_normalize_metrics_coalesces_duplicate_bucket_without_inventing_values(self):
        raw = pd.DataFrame(
            {
                "create_time": ["2026-07-11 00:05:01", "2026-07-11 00:09:59"],
                "sum_open_interest": [100.0, 101.0],
                "sum_open_interest_value": [200.0, 201.0],
                "count_toptrader_long_short_ratio": [1.2, pd.NA],
                "sum_toptrader_long_short_ratio": [1.3, pd.NA],
                "count_long_short_ratio": [1.4, 1.5],
                "sum_taker_long_short_vol_ratio": [1.6, 1.7],
            }
        )

        df = normalize_metrics_frame(raw, symbol="BTCUSDT", contract_type="PERPETUAL", source="test")

        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.loc[0, "count_toptrader_long_short_ratio"], 1.2)
        self.assertAlmostEqual(df.loc[0, "sum_toptrader_long_short_ratio"], 1.3)
        self.assertAlmostEqual(df.loc[0, "sum_open_interest"], 101.0)

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

    def test_missing_coverage_reschedules_nullable_metric_day(self):
        available_days = pd.to_datetime(["2020-01-01", "2020-01-02"]).tolist()
        key_days, missing_days = missing_coverage_key_days(
            available_days=available_days,
            local_day_counts={"2020-01-01": 287, "2020-01-02": 288},
            nullable_metric_rows={"2020-01-02": 2},
            effective_start=pd.Timestamp("2020-01-01"),
            min_rows_per_full_day=288,
        )

        self.assertEqual([str(day.date()) for day in key_days], ["2020-01-01", "2020-01-02"])
        self.assertEqual(
            missing_days,
            [{"date": "2020-01-02", "rows": 288, "expected_rows": 288, "nullable_metric_rows": 2}],
        )


class TestBinanceFuturesMetricsAudit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        root = Path(self.tmp.name)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        self.store = PartitionedCsvGzStore(STORE_PARTS, partition="month")

    def tearDown(self):
        if self.old_data_root is None:
            os.environ.pop("DATA_ROOT", None)
        else:
            os.environ["DATA_ROOT"] = self.old_data_root
        if self.old_state_root is None:
            os.environ.pop("STATE_ROOT", None)
        else:
            os.environ["STATE_ROOT"] = self.old_state_root
        self.tmp.cleanup()

    @staticmethod
    def _metrics_frame(start: str, periods: int = 288) -> pd.DataFrame:
        times = pd.date_range(start, periods=periods, freq="5min")
        frame = pd.DataFrame({"time": times})
        frame["market"] = "usdm_futures"
        frame["symbol"] = "BTCUSDT"
        frame["contract_type"] = "PERPETUAL"
        for column in METRIC_COLUMNS:
            if column.startswith(("sum_", "count_")):
                frame[column] = 1.0
        frame["source"] = "binance_vision_usdm_metrics"
        frame["ingested_at"] = "2026-08-13T00:00:00+00:00"
        return frame[METRIC_COLUMNS]

    def test_streaming_audit_passes_complete_metrics_partitions(self):
        frame = pd.concat([
            self._metrics_frame("2020-01-01 00:00:00"),
            self._metrics_frame("2020-01-02 00:00:00"),
        ], ignore_index=True)
        append_metrics(self.store, frame, "BTCUSDT")

        audit = audit_symbol(
            self.store,
            "BTCUSDT",
            effective_start=pd.Timestamp("2020-01-01"),
            expected_end=pd.Timestamp("2020-01-02"),
        )

        self.assertEqual(audit["status"], "pass")
        self.assertEqual(audit["rows"], 576)
        self.assertEqual(audit["duplicate_rows"], 0)
        self.assertEqual(audit["gap_count"], 0)
        self.assertEqual(audit["partial_day_count"], 0)

    def test_streaming_audit_reports_nullable_upstream_metric_values(self):
        frame = self._metrics_frame("2020-01-01 00:00:00")
        frame.loc[0, "sum_open_interest"] = pd.NA
        append_metrics(self.store, frame, "BTCUSDT")

        audit = audit_symbol(
            self.store,
            "BTCUSDT",
            effective_start=pd.Timestamp("2020-01-01"),
            expected_end=pd.Timestamp("2020-01-01"),
        )

        self.assertEqual(audit["status"], "pass_with_documented_source_gaps")
        self.assertEqual(audit["invalid_numeric_rows"], 0)
        self.assertEqual(audit["nullable_metric_rows"], 1)
        self.assertEqual(audit["nullable_metric_values"]["sum_open_interest"], 1)

    def test_streaming_audit_rejects_non_numeric_metric_values(self):
        frame = self._metrics_frame("2020-01-01 00:00:00")
        frame.loc[0, "sum_open_interest"] = "not-a-number"
        append_metrics(self.store, frame, "BTCUSDT")

        audit = audit_symbol(
            self.store,
            "BTCUSDT",
            effective_start=pd.Timestamp("2020-01-01"),
            expected_end=pd.Timestamp("2020-01-01"),
        )

        self.assertEqual(audit["status"], "fail")
        self.assertEqual(audit["invalid_numeric_rows"], 1)


if __name__ == "__main__":
    unittest.main()
