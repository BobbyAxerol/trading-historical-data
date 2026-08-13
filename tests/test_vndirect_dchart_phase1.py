import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pandas as pd

import data_loader
from collectors.common.calendar_vn import is_trading_day
from collectors.providers.vndirect_dchart_derivatives import DChartFetchResult, VndirectDChartProvider
from collectors.vn_daily_matrix import build_matrix
from collectors.vn_derivatives.vndirect import (
    VndirectDailyOptions,
    VndirectProbeOptions,
    audit_vndirect_daily,
    last_closed_vn_daily,
    run_vndirect_probe,
    sync_vndirect_daily,
)
from data_loader import VnDerivativesContinuousDaily


class FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class EnvCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.old_state_root = os.environ.get("STATE_ROOT")
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_config_root = os.environ.get("CONFIG_ROOT")
        self.old_loader_storage = data_loader.STORAGE_DIR
        os.environ["STATE_ROOT"] = str(root / "state")
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["CONFIG_ROOT"] = str(root / "configs")
        data_loader.STORAGE_DIR = Path(os.environ["DATA_ROOT"])
        Path(os.environ["CONFIG_ROOT"]).mkdir(parents=True, exist_ok=True)
        (Path(os.environ["CONFIG_ROOT"]) / "symbols.vn_daily.yml").write_text("symbols: []\ncandidate_symbols: []\nexternal_symbols: [VN30F1M]\n")

    def tearDown(self):
        _restore_env("STATE_ROOT", self.old_state_root)
        _restore_env("DATA_ROOT", self.old_data_root)
        _restore_env("CONFIG_ROOT", self.old_config_root)
        data_loader.STORAGE_DIR = self.old_loader_storage
        self.tmp.cleanup()


class TestVndirectDChartProvider(unittest.TestCase):
    def _provider_with_response(self, response: FakeResponse) -> VndirectDChartProvider:
        provider = VndirectDChartProvider()
        provider.session.get = Mock(return_value=response)
        return provider

    def test_success_normalizes_udf_payload(self):
        payload = {
            "s": "ok",
            "t": [1534125660, 1534125720],
            "o": [950.0, 951.0],
            "h": [951.0, 952.0],
            "l": [949.5, 950.5],
            "c": [950.8, 951.8],
            "v": [1234, 100],
        }
        result = self._provider_with_response(FakeResponse(200, payload)).fetch(
            start=pd.Timestamp("2018-08-13"),
            end=pd.Timestamp("2018-08-14"),
            resolution="1m",
        )

        self.assertEqual(result.status, "success")
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.data["source"].iloc[0], "vndirect_dchart")
        self.assertEqual(result.data["quality_flags"].iloc[0], "CONTINUOUS_ALIAS")

    def test_no_data_is_explicit_only(self):
        result = self._provider_with_response(FakeResponse(200, {"s": "no_data"})).fetch(
            start=pd.Timestamp("2018-08-01"),
            end=pd.Timestamp("2018-09-01"),
            resolution="1m",
        )

        self.assertEqual(result.status, "no_data")
        self.assertEqual(result.row_count, 0)

    def test_empty_ok_arrays_are_no_data(self):
        payload = {"s": "ok", "t": [], "o": [], "h": [], "l": [], "c": [], "v": []}
        result = self._provider_with_response(FakeResponse(200, payload)).fetch(
            start=pd.Timestamp("2018-08-01"),
            end=pd.Timestamp("2018-09-01"),
            resolution="1m",
        )

        self.assertEqual(result.status, "no_data")
        self.assertEqual(result.row_count, 0)

    def test_http_and_schema_errors_are_not_no_data(self):
        http = self._provider_with_response(FakeResponse(500, text="server error")).fetch(
            start=pd.Timestamp("2024-01-01"),
            end=pd.Timestamp("2024-01-02"),
            resolution="1d",
        )
        invalid_json = self._provider_with_response(FakeResponse(200, ValueError("bad json"))).fetch(
            start=pd.Timestamp("2024-01-01"),
            end=pd.Timestamp("2024-01-02"),
            resolution="1d",
        )
        missing = self._provider_with_response(FakeResponse(200, {"s": "ok", "t": [1]})).fetch(
            start=pd.Timestamp("2024-01-01"),
            end=pd.Timestamp("2024-01-02"),
            resolution="1d",
        )

        self.assertEqual(http.status, "http_error")
        self.assertEqual(invalid_json.status, "schema_error")
        self.assertEqual(missing.status, "schema_error")

    def test_invalid_rows_fail_schema(self):
        payload = {
            "s": "ok",
            "t": [1534125660],
            "o": [950.0],
            "h": [949.0],
            "l": [949.5],
            "c": [950.8],
            "v": [1234],
        }
        result = self._provider_with_response(FakeResponse(200, payload)).fetch(
            start=pd.Timestamp("2018-08-13"),
            end=pd.Timestamp("2018-08-14"),
            resolution="1m",
        )

        self.assertEqual(result.status, "schema_error")
        self.assertIn("invalid OHLC", result.error)


class TestVndirectProbeGate(EnvCase):
    def _result(self, status: str, rows: int) -> DChartFetchResult:
        frame = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01", periods=rows, freq="min", tz="Asia/Ho_Chi_Minh"),
                "open": [1.0] * rows,
                "high": [1.0] * rows,
                "low": [1.0] * rows,
                "close": [1.0] * rows,
                "volume": [1.0] * rows,
            }
        )
        return DChartFetchResult(
            status=status,
            data=frame,
            requested_start=pd.Timestamp("2024-01-01"),
            requested_end=pd.Timestamp("2024-01-02"),
            first_bar=frame["time"].min() if rows else None,
            last_bar=frame["time"].max() if rows else None,
            http_status=200,
            error=None,
        )

    def test_probe_passes_only_with_recent_1m_and_daily_positive(self):
        provider = Mock()
        provider.fetch.side_effect = [self._result("success", 101), self._result("no_data", 0), self._result("success", 501)]
        with patch("collectors.vn_derivatives.vndirect.VndirectDChartProvider", return_value=provider):
            payload = run_vndirect_probe(VndirectProbeOptions(fail_on_gate=True))

        self.assertEqual(payload["production_gate"], "PASS")
        self.assertTrue((Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "vndirect_dchart_probe.json").exists())

    def test_probe_fails_and_writes_report_without_positive_daily(self):
        provider = Mock()
        provider.fetch.side_effect = [self._result("success", 101), self._result("no_data", 0), self._result("success", 10)]
        with patch("collectors.vn_derivatives.vndirect.VndirectDChartProvider", return_value=provider):
            with self.assertRaises(RuntimeError):
                run_vndirect_probe(VndirectProbeOptions(fail_on_gate=True))

        report = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "vndirect_dchart_probe.json").read_text())
        self.assertEqual(report["production_gate"], "FAIL")
        self.assertIn("daily row_count <= 500", report["gate_errors"][0])


class TestVndirectDailySync(EnvCase):
    def _daily_result(self) -> DChartFetchResult:
        frame = pd.DataFrame(
            {
                "time": pd.to_datetime(["2024-01-02 07:00", "2024-01-03 07:00"]).tz_localize("Asia/Ho_Chi_Minh"),
                "open": [1000.0, 1002.0],
                "high": [1005.0, 1004.0],
                "low": [999.0, 1001.0],
                "close": [1003.0, 1002.5],
                "volume": [1200.0, 1300.0],
                "source": ["vndirect_dchart", "vndirect_dchart"],
                "source_symbol": ["VN30F1M", "VN30F1M"],
                "quality_flags": ["CONTINUOUS_ALIAS", "CONTINUOUS_ALIAS"],
                "ingested_at": ["2026-07-30T00:00:00+00:00", "2026-07-30T00:00:00+00:00"],
            }
        )
        return DChartFetchResult(
            status="success",
            data=frame,
            requested_start=pd.Timestamp("2024-01-01"),
            requested_end=pd.Timestamp("2024-01-03"),
            first_bar=frame["time"].min(),
            last_bar=frame["time"].max(),
            http_status=200,
            error=None,
        )

    def test_daily_sync_writes_source_partition_and_loader_reads_it(self):
        provider = Mock()
        provider.fetch.return_value = self._daily_result()
        with patch("collectors.vn_derivatives.vndirect.VndirectDChartProvider", return_value=provider):
            payload = sync_vndirect_daily(VndirectDailyOptions(start="2024-01-01", end="2024-01-03", update_matrix=False))

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["rows_written"], 2)
        part = (
            Path(os.environ["DATA_ROOT"])
            / "vn"
            / "futures"
            / "continuous"
            / "1d"
            / "symbol=VN30F1M"
            / "source=vndirect_dchart"
            / "version=v1"
            / "year=2024"
            / "part.parquet"
        )
        self.assertTrue(part.exists())

        loaded = VnDerivativesContinuousDaily().load(symbols="VN30F1M", start_date="2024-01-01", check_val=True, columns="full")
        self.assertEqual(len(loaded), 2)
        self.assertEqual(list(pd.to_datetime(loaded["time"]).dt.strftime("%Y-%m-%d")), ["2024-01-02", "2024-01-03"])
        self.assertEqual(set(loaded["source"]), {"vndirect_dchart"})

        manifest = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "vndirect_dchart_1d.json").read_text())
        self.assertEqual(manifest["provider"], "vndirect_dchart")
        self.assertEqual(manifest["latest_time"], "2024-01-03T00:00:00")

    def test_daily_matrix_reads_vndirect_source_partition(self):
        provider = Mock()
        provider.fetch.return_value = self._daily_result()
        with patch("collectors.vn_derivatives.vndirect.VndirectDChartProvider", return_value=provider):
            sync_vndirect_daily(VndirectDailyOptions(start="2024-01-01", end="2024-01-03", update_matrix=False))

        result = build_matrix(start_date="2024-01-01", end_date="2024-01-03")

        self.assertEqual(result["auxiliary_symbols"], ["VN30F1M"])
        close = pd.read_parquet(Path(os.environ["DATA_ROOT"]) / "vn" / "equity" / "daily_matrix" / "close.parquet")
        self.assertIn("VN30F1M", close.columns)

    def test_phase_d_audit_streams_daily_partition_and_enforces_calendar_tail(self):
        provider = Mock()
        provider.fetch.return_value = self._daily_result()
        with patch("collectors.vn_derivatives.vndirect.VndirectDChartProvider", return_value=provider):
            payload = sync_vndirect_daily(
                VndirectDailyOptions(
                    start="2024-01-01",
                    end="2024-01-03",
                    update_matrix=False,
                    audit_phase_d=True,
                )
            )

        audit = payload["audit"]
        self.assertEqual(audit["status"], "pass")
        self.assertEqual(audit["rows"], 2)
        self.assertEqual(audit["duplicate_rows"], 0)
        self.assertEqual(audit["ohlc_bad_rows"], 0)
        self.assertEqual(audit["calendar_missing_trading_day_count"], 0)
        path = Path(os.environ["STATE_ROOT"]) / "audits" / "vn30f1m_vndirect_dchart_1d_phase_d.json"
        self.assertTrue(path.exists())

    def test_phase_d_audit_rejects_a_missing_supported_calendar_day(self):
        provider = Mock()
        result = self._daily_result()
        result = DChartFetchResult(
            status=result.status,
            data=result.data.iloc[:1].copy(),
            requested_start=result.requested_start,
            requested_end=result.requested_end,
            first_bar=result.first_bar,
            last_bar=result.first_bar,
            http_status=result.http_status,
            error=result.error,
        )
        provider.fetch.return_value = result
        with patch("collectors.vn_derivatives.vndirect.VndirectDChartProvider", return_value=provider):
            sync_vndirect_daily(VndirectDailyOptions(start="2024-01-01", end="2024-01-02", update_matrix=False))

        audit = audit_vndirect_daily(expected_latest=pd.Timestamp("2024-01-03"))
        self.assertEqual(audit["status"], "fail")
        self.assertEqual(audit["calendar_missing_trading_days"], ["2024-01-03"])


class TestVndirectClosedDailyBoundary(unittest.TestCase):
    def test_current_trading_day_is_not_canonical_until_after_close_buffer(self):
        zone = ZoneInfo("Asia/Ho_Chi_Minh")
        before_close = datetime(2026, 8, 13, 14, 59, tzinfo=zone)
        after_close = datetime(2026, 8, 13, 15, 0, tzinfo=zone)

        self.assertEqual(last_closed_vn_daily(before_close), pd.Timestamp("2026-08-12"))
        self.assertEqual(last_closed_vn_daily(after_close), pd.Timestamp("2026-08-13"))

    def test_verified_exchange_bridge_holidays_are_not_expected_trading_days(self):
        for date in ("2024-04-29", "2024-09-03", "2025-05-02", "2026-01-02"):
            with self.subTest(date=date):
                self.assertFalse(is_trading_day(datetime.fromisoformat(date)))


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
