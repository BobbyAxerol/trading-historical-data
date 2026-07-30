import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from collectors.providers.vndirect_dchart_derivatives import DChartFetchResult, VndirectDChartProvider
from collectors.vn_derivatives.vndirect import VndirectProbeOptions, run_vndirect_probe


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
        os.environ["STATE_ROOT"] = str(root / "state")
        os.environ["DATA_ROOT"] = str(root / "storage")

    def tearDown(self):
        _restore_env("STATE_ROOT", self.old_state_root)
        _restore_env("DATA_ROOT", self.old_data_root)
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


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
