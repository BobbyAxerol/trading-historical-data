import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from collectors.vn_derivatives.instruments import build_initial_instrument_dimension, instrument_dimension_path
from collectors.vn_derivatives.probe import ProbeRequest, run_provider_probe
from collectors.vn_derivatives.symbols import (
    contract_for_month,
    generate_contracts,
    is_vn30_future_symbol,
    legacy_to_krx,
    parse_canonical_symbol,
)


class EnvCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")

    def tearDown(self):
        _restore_env("DATA_ROOT", self.old_data_root)
        _restore_env("STATE_ROOT", self.old_state_root)
        self.tmp.cleanup()


class TestVNDerivativeSymbols(unittest.TestCase):
    def test_legacy_to_krx_examples(self):
        self.assertEqual(legacy_to_krx(2025, 8), "41I1F8000")
        self.assertEqual(legacy_to_krx(2017, 9), "41I179000")
        self.assertEqual(legacy_to_krx(2029, 12), "41I1KC000")
        self.assertEqual(legacy_to_krx(2039, 12), "41I1VC000")
        self.assertEqual(legacy_to_krx(2040, 1), "41I101000")

    def test_parse_and_symbol_detection(self):
        from collectors.vn_intraday_dnse import is_derivative_symbol

        self.assertEqual(parse_canonical_symbol("VN30F2508"), (2025, 8))
        self.assertTrue(is_vn30_future_symbol("VN30F2503"))
        self.assertTrue(is_vn30_future_symbol("41I1F8000"))
        self.assertTrue(is_vn30_future_symbol("VN30F1M"))
        self.assertFalse(is_vn30_future_symbol("FPT"))
        self.assertTrue(is_derivative_symbol("VN30F2503"))
        self.assertTrue(is_derivative_symbol("41I1F8000"))
        self.assertFalse(is_derivative_symbol("FPT"))

    def test_contract_generation_includes_opening_contracts(self):
        symbols = [contract.canonical_symbol for contract in generate_contracts(start="2017-08-10", end="2018-03-01", horizon_months=0)]
        for expected in ["VN30F1708", "VN30F1709", "VN30F1712", "VN30F1803"]:
            self.assertIn(expected, symbols)
        contract = contract_for_month(2025, 8)
        self.assertEqual(contract.krx_symbol, "41I1F8000")
        self.assertEqual(str(contract.expiry_date), "2025-08-21")

    def test_kbs_empty_provider_error_is_classified_as_empty(self):
        from collectors.providers.kbs_derivatives import _is_empty_provider_error

        self.assertTrue(_is_empty_provider_error(ValueError("Dữ liệu trống cho mã 41I1F8000 với interval 1m.")))
        try:
            raise RuntimeError("wrapper") from ValueError("Dữ liệu trống cho mã 41I1F8000 với interval 1D.")
        except RuntimeError as exc:
            self.assertTrue(_is_empty_provider_error(exc))
        self.assertFalse(_is_empty_provider_error(RuntimeError("connection refused")))


class TestVNDerivativePhase1(EnvCase):
    def test_instrument_dimension_written(self):
        df = build_initial_instrument_dimension(start="2017-08-10", end="2017-10-01", horizon_months=0)
        path = instrument_dimension_path()
        self.assertTrue(path.exists())
        loaded = pd.read_parquet(path)
        self.assertEqual(list(loaded["canonical_symbol"]), ["VN30F1708", "VN30F1709", "VN30F1710"])
        self.assertEqual(set(df.columns), set(loaded.columns))

    def test_probe_writes_parquet_and_json_with_fake_fetchers(self):
        def fake_kbs(request: ProbeRequest) -> pd.DataFrame:
            if request.provider_symbol == "VN30F2508" and request.resolution == "1m":
                return pd.DataFrame(
                    {
                        "time": pd.to_datetime(["2025-08-01 09:00", "2025-08-01 09:01"]),
                        "open": [1000.0, 1000.1],
                        "high": [1000.2, 1000.3],
                        "low": [999.9, 1000.0],
                        "close": [1000.1, 1000.2],
                        "volume": [10, 11],
                    }
                )
            return pd.DataFrame()

        def fake_dnse(request: ProbeRequest) -> pd.DataFrame:
            if request.provider_symbol == "41I1F8000" and request.resolution == "1d":
                return pd.DataFrame(
                    {
                        "time": pd.to_datetime(["2025-08-01"]),
                        "open": [1000.0],
                        "high": [1001.0],
                        "low": [999.0],
                        "close": [1000.5],
                        "volume": [100],
                    }
                )
            return pd.DataFrame()

        summary = run_provider_probe(contracts=["VN30F2508"], fetchers={"kbs": fake_kbs, "dnse": fake_dnse}, window_days=10)

        parquet_path = Path(summary["parquet_path"])
        json_path = Path(summary["json_path"])
        self.assertTrue(parquet_path.exists())
        self.assertTrue(json_path.exists())
        probe = pd.read_parquet(parquet_path)
        self.assertEqual(len(probe), 8)
        self.assertGreater(probe["request_success"].sum(), 0)
        self.assertIn("2025-08-01", str(summary["earliest_kbs_1m"]))
        self.assertIn("41I1F8000:krx", summary["dnse_1d_symbols_with_data"])
        loaded_summary = json.loads(json_path.read_text())
        self.assertEqual(loaded_summary["status"], "ok")


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
