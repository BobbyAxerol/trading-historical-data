import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from collectors.vn_derivatives.contracts import (
    BackfillOptions,
    ProviderResult,
    backfill_contracts,
    merge_provider_rows,
)
from collectors.vn_derivatives.symbols import contract_for_month
from collectors.vn_derivatives.validate import validate_contract_frame, validate_storage


class EnvCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        self.old_config_root = os.environ.get("CONFIG_ROOT")
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        os.environ["CONFIG_ROOT"] = str(root / "configs")
        Path(os.environ["CONFIG_ROOT"]).mkdir(parents=True, exist_ok=True)
        (Path(os.environ["CONFIG_ROOT"]) / "vn_derivatives.yml").write_text(
            "\n".join(
                [
                    "dataset_version: v1",
                    "backfill_start: '2017-08-10'",
                    "resolutions: [1m, 1d]",
                    "requests:",
                    "  kbs_1m_window_days: 7",
                    "  dnse_1m_window_days: 5",
                    "  daily_window_days: 365",
                    "validation:",
                    "  min_1m_bars_for_daily: 2",
                ]
            )
        )

    def tearDown(self):
        _restore_env("DATA_ROOT", self.old_data_root)
        _restore_env("STATE_ROOT", self.old_state_root)
        _restore_env("CONFIG_ROOT", self.old_config_root)
        self.tmp.cleanup()


def _bars(times: list[str], *, offset: float = 0.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "time": pd.to_datetime(times),
            "open": [1000.0 + offset + i * 0.1 for i in range(len(times))],
            "high": [1000.2 + offset + i * 0.1 for i in range(len(times))],
            "low": [999.9 + offset + i * 0.1 for i in range(len(times))],
            "close": [1000.1 + offset + i * 0.1 for i in range(len(times))],
            "volume": [10.0 + i for i in range(len(times))],
        }
    )


class TestVNDerivativePhase2(EnvCase):
    def test_merge_keeps_kbs_primary_and_uses_dnse_only_for_missing_times(self):
        contract = contract_for_month(2025, 8)
        kbs = ProviderResult("kbs", "VN30F2508", _bars(["2025-08-01 09:00", "2025-08-01 09:01"], offset=0), True, False)
        dnse = ProviderResult("dnse", "41I1F8000", _bars(["2025-08-01 09:01", "2025-08-01 09:02"], offset=5), True, False)

        merged, stats = merge_provider_rows(contract, kbs, dnse)

        self.assertEqual(len(merged), 3)
        row_0901 = merged.loc[merged["time"] == pd.Timestamp("2025-08-01 09:01")].iloc[0]
        self.assertEqual(row_0901["source"], "kbs")
        row_0902 = merged.loc[merged["time"] == pd.Timestamp("2025-08-01 09:02")].iloc[0]
        self.assertEqual(row_0902["source"], "dnse")
        self.assertEqual(stats["dnse_fallback_rows"], 1)

    def test_validation_catches_bad_ohlc(self):
        contract = contract_for_month(2025, 8)
        df = pd.DataFrame(
            {
                "time": [pd.Timestamp("2025-08-01 09:00")],
                "instrument_id": [contract.instrument_id],
                "open": [1000.0],
                "high": [999.0],
                "low": [1001.0],
                "close": [1000.0],
                "volume": [1.0],
                "source": ["kbs"],
                "quality_flags": ["KBS_PRIMARY"],
                "ingested_at": ["2026-01-01T00:00:00+00:00"],
            }
        )
        codes = {issue.code for issue in validate_contract_frame(df, expiry_date=pd.Timestamp(contract.expiry_date))}
        self.assertIn("high_lt_open", codes)
        self.assertIn("low_gt_close", codes)

    def test_backfill_writes_contract_storage_and_resumes(self):
        calls: list[tuple[str, str, str]] = []

        def fetcher(contract, provider, resolution, start, end):
            calls.append((contract.canonical_symbol, provider, resolution))
            if provider == "kbs":
                if resolution == "1m":
                    return ProviderResult(provider, contract.legacy_symbol, _bars(["2025-08-01 09:00", "2025-08-01 09:01"]), True, False)
                return ProviderResult(provider, contract.legacy_symbol, _bars(["2025-08-01"], offset=0), True, False)
            if resolution == "1m":
                return ProviderResult(provider, contract.krx_symbol, _bars(["2025-08-01 09:01", "2025-08-01 09:02"], offset=3), True, False)
            return ProviderResult(provider, contract.krx_symbol, pd.DataFrame(), True, True)

        options = BackfillOptions(
            symbols=("VN30F2508",),
            resolutions=("1m", "1d"),
            start="2025-08-01",
            end="2025-08-01",
            max_windows=2,
            kbs_1m_window_days=1,
            daily_window_days=1,
            min_1m_bars_for_daily=2,
        )
        result = backfill_contracts(options, fetcher=fetcher)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["windows_done"], 2)

        one_minute = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "contracts" / "1m" / "symbol=VN30F2508" / "year=2025" / "month=08" / "part.parquet"
        daily = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "contracts" / "1d" / "symbol=VN30F2508" / "year=2025" / "part.parquet"
        self.assertTrue(one_minute.exists())
        self.assertTrue(daily.exists())
        one_minute_df = pd.read_parquet(one_minute)
        self.assertEqual(len(one_minute_df), 3)
        self.assertEqual(one_minute_df.loc[one_minute_df["time"] == pd.Timestamp("2025-08-01 09:01"), "source"].iloc[0], "kbs")

        validation = validate_storage(symbols=["VN30F2508"])
        self.assertEqual(validation["status"], "ok")

        calls_before = len(calls)
        resumed = backfill_contracts(options, fetcher=fetcher)
        self.assertEqual(resumed["windows_done"], 0)
        self.assertEqual(len(calls), calls_before)
        manifest = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "contracts_1m.json").read_text())
        self.assertIn("VN30F2508", manifest["symbols"])

    def test_provider_errors_without_rows_do_not_advance_manifest(self):
        def failing_fetcher(contract, provider, resolution, start, end):
            return ProviderResult(provider, None, pd.DataFrame(), False, False, error=f"{provider} failed")

        options = BackfillOptions(
            symbols=("VN30F2508",),
            resolutions=("1d",),
            start="2025-08-01",
            end="2025-08-01",
            max_windows=1,
            daily_window_days=1,
        )
        with self.assertRaises(RuntimeError):
            backfill_contracts(options, fetcher=failing_fetcher)

        manifest_path = Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "contracts_1d.json"
        manifest = json.loads(manifest_path.read_text())
        state = manifest["symbols"]["VN30F2508"]
        self.assertIn("last_error", state)
        self.assertNotIn("completed_windows", state)

    def test_daily_empty_confirmed_can_complete_without_rows(self):
        def empty_fetcher(contract, provider, resolution, start, end):
            return ProviderResult(provider, contract.legacy_symbol, pd.DataFrame(), True, True)

        options = BackfillOptions(
            symbols=("VN30F2508",),
            resolutions=("1d",),
            start="2025-08-01",
            end="2025-08-01",
            max_windows=1,
            daily_window_days=1,
        )
        result = backfill_contracts(options, fetcher=empty_fetcher)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["windows_done"], 1)
        manifest = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_derivatives" / "contracts_1d.json").read_text())
        self.assertIn("completed_windows", manifest["symbols"]["VN30F2508"])


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
