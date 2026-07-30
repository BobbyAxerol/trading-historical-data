import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from collectors.vn_derivatives.provider_registry import SourceProbeOptions, run_source_probe
from collectors.vn_derivatives.source_gates import ProviderFetchResult, classify_http_status, empty_ohlcv_frame, normalize_ohlcv_frame
from collectors.providers import vietstock_derivatives


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


class TestVNDerivativesV2Phase1(EnvCase):
    def test_http_400_is_invalid_request_not_empty(self):
        self.assertEqual(classify_http_status(400), "invalid_request")
        self.assertEqual(classify_http_status(429), "rate_limited")
        self.assertEqual(classify_http_status(403), "blocked")

    def test_public_web_style_frame_normalizes_date_and_time_columns(self):
        raw = pd.DataFrame(
            {
                "Date": ["2018-08-13", "2018-08-13"],
                "time": ["09:01:00", "09:02:00"],
                "Open": [943.0, 943.1],
                "High": [943.2, 943.3],
                "Low": [942.9, 943.0],
                "Close": [943.1, 943.2],
                "volume": [220.0, 121.0],
            }
        )
        frame, error = normalize_ohlcv_frame(raw, resolution="1m", source="vietstock", source_symbol="VN30F1M")

        self.assertIsNone(error)
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame["time"].iloc[0], pd.Timestamp("2018-08-13 09:01:00"))
        self.assertEqual(float(frame["close"].iloc[-1]), 943.2)
        self.assertEqual(frame["source"].iloc[0], "vietstock")

    def test_vietstock_public_search_resolution_is_not_promoted_without_ohlcv(self):
        search_payload = '{"code":0,"data":"VN30F2508|HDTL VN30|https://finance.vietstock.vn/chung-khoan-phai-sinh/VN30F2508/hop-dong-tuong-lai.htm|VN30F2508|HNX|3"}'
        page_payload = "<html><body><div>VN30F2508</div><form id='login-form'>recaptcha login modal</form></body></html>"

        with patch(
            "collectors.providers.vietstock_derivatives.get_public",
            side_effect=[
                (200, search_payload, "/tmp/search.json", None),
                (200, page_payload, "/tmp/page.html", None),
            ],
        ):
            result = vietstock_derivatives.fetch_daily("VN30F2508")

        self.assertEqual(result.status, "empty_confirmed")
        self.assertEqual(result.row_count, 0)
        self.assertIn("no parseable OHLCV", result.error)

    def test_source_probe_writes_status_and_blocks_without_positive(self):
        def fake_empty(symbol):
            return ProviderFetchResult("vietstock", symbol, symbol, symbol, "1D", "empty_confirmed", empty_ohlcv_frame())

        with patch("collectors.providers.vietstock_derivatives.fetch_daily", side_effect=fake_empty):
            with self.assertRaises(RuntimeError):
                run_source_probe(SourceProbeOptions(providers=("vietstock",), contracts=("VN30F2508",), fail_on_no_positive=True))

        summary_path = Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "source_probe_v2.json"
        status_path = Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "source_status.json"
        self.assertTrue(summary_path.exists())
        self.assertTrue(status_path.exists())
        summary = json.loads(summary_path.read_text())
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(summary["positive_request_count"], 0)

    def test_source_probe_positive_gate_with_mocked_vietstock(self):
        def fake_fetch(symbol):
            rows = pd.DataFrame(
                {
                    "time": pd.to_datetime(["2018-08-13 09:01", "2018-08-13 09:02"]),
                    "open": [1.0, 2.0],
                    "high": [1.2, 2.2],
                    "low": [0.9, 1.9],
                    "close": [1.1, 2.1],
                    "volume": [10.0, 11.0],
                }
            )
            return ProviderFetchResult("vietstock", symbol, symbol, symbol, "1D", "success", rows)

        with patch("collectors.providers.vietstock_derivatives.fetch_daily", side_effect=fake_fetch):
            summary = run_source_probe(SourceProbeOptions(providers=("vietstock",), contracts=("VN30F2508",), fail_on_no_positive=True))

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["expected_request_count"], 1)
        self.assertEqual(summary["actual_request_count"], 1)
        self.assertEqual(summary["positive_request_count"], 1)
        status = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "source_status.json").read_text())
        self.assertIn("vietstock", status["providers"])


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
