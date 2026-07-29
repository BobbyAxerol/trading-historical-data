import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import data_loader
from collectors.vn_daily_matrix import build_matrix
from collectors.vn_derivatives.continuous import (
    ContinuousOptions,
    active_map_from_rolls,
    build_continuous,
    build_roll_table,
    read_roll_table,
    validate_continuous_storage,
)
from collectors.vn_derivatives.symbols import contract_for_month
from data_loader import VnDerivativesContinuousDaily, load_data


class EnvCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        self.old_config_root = os.environ.get("CONFIG_ROOT")
        self.old_loader_storage = data_loader.STORAGE_DIR
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        os.environ["CONFIG_ROOT"] = str(root / "configs")
        data_loader.STORAGE_DIR = Path(os.environ["DATA_ROOT"])
        Path(os.environ["CONFIG_ROOT"]).mkdir(parents=True, exist_ok=True)
        (Path(os.environ["CONFIG_ROOT"]) / "symbols.vn_daily.yml").write_text("symbols: [FPT]\ncandidate_symbols: []\nexternal_symbols: [VN30F1M]\n")
        (Path(os.environ["CONFIG_ROOT"]) / "vn_derivatives.yml").write_text(
            "\n".join(
                [
                    "dataset_version: v1",
                    "backfill_start: '2017-08-10'",
                    "resolutions: [1m, 1d]",
                    "requests:",
                    "  daily_sync_lookback_days: 45",
                    "continuous:",
                    "  calendar_series: VN30F1M",
                    "  tradable_series: VN30F1M_TRADE",
                    "  volume_confirmation_days: 2",
                    "  hard_roll_sessions_before_expiry: 1",
                ]
            )
        )

    def tearDown(self):
        _restore_env("DATA_ROOT", self.old_data_root)
        _restore_env("STATE_ROOT", self.old_state_root)
        _restore_env("CONFIG_ROOT", self.old_config_root)
        data_loader.STORAGE_DIR = self.old_loader_storage
        self.tmp.cleanup()

    def _write_contract_daily(self, contract, dates: list[str], *, close: float, volume: list[float] | float = 100.0) -> None:
        volumes = volume if isinstance(volume, list) else [volume] * len(dates)
        frame = pd.DataFrame(
            {
                "time": pd.to_datetime(dates),
                "instrument_id": contract.instrument_id,
                "open": [close] * len(dates),
                "high": [close + 2.0] * len(dates),
                "low": [close - 2.0] * len(dates),
                "close": [close] * len(dates),
                "volume": volumes,
                "source": "kbs",
                "quality_flags": "KBS_PRIMARY",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            }
        )
        root = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "contracts" / "1d" / f"symbol={contract.canonical_symbol}" / f"year={pd.Timestamp(dates[0]).year:04d}"
        root.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(root / "part.parquet", index=False)

    def _write_contract_1m(self, contract, rows: list[tuple[str, float]]) -> None:
        frame = pd.DataFrame(
            {
                "time": pd.to_datetime([row[0] for row in rows]),
                "instrument_id": contract.instrument_id,
                "open": [row[1] for row in rows],
                "high": [row[1] + 1.0 for row in rows],
                "low": [row[1] - 1.0 for row in rows],
                "close": [row[1] + 0.5 for row in rows],
                "volume": [10.0] * len(rows),
                "source": "kbs",
                "quality_flags": "KBS_PRIMARY",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            }
        )
        first = pd.Timestamp(rows[0][0])
        root = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "contracts" / "1m" / f"symbol={contract.canonical_symbol}" / f"year={first.year:04d}" / f"month={first.month:02d}"
        root.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(root / "part.parquet", index=False)


class TestVNDerivativesPhase3(EnvCase):
    def test_calendar_rolls_after_expiry_and_continuous_uses_same_rolls(self):
        jan = contract_for_month(2024, 1)
        feb = contract_for_month(2024, 2)
        self._write_contract_daily(jan, ["2024-01-17", "2024-01-18"], close=1100.0)
        self._write_contract_daily(feb, ["2024-01-19"], close=1110.0)
        self._write_contract_1m(jan, [("2024-01-18 09:00", 1100.0), ("2024-01-18 09:01", 1101.0)])
        self._write_contract_1m(feb, [("2024-01-19 09:00", 1110.0), ("2024-01-19 09:01", 1111.0)])

        options = ContinuousOptions(start="2024-01-17", end="2024-01-19", series=("VN30F1M",), resolutions=("1m", "1d"))
        build_roll_table(options)
        rolls = read_roll_table("v1")
        active = active_map_from_rolls(rolls, series="VN30F1M", start=pd.Timestamp("2024-01-17"), end=pd.Timestamp("2024-01-19"))

        self.assertEqual(active[pd.Timestamp("2024-01-18")]["instrument_id"], jan.instrument_id)
        self.assertEqual(active[pd.Timestamp("2024-01-19")]["instrument_id"], feb.instrument_id)

        result = build_continuous(options)
        self.assertEqual(result["status"], "ok")
        validation = validate_continuous_storage(version="v1", series=["VN30F1M"])
        self.assertEqual(validation["status"], "ok")

        minute = pd.read_parquet(
            Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "continuous" / "1m" / "symbol=VN30F1M" / "version=v1" / "year=2024" / "month=01" / "part.parquet"
        )
        ids_per_day = minute.assign(day=pd.to_datetime(minute["time"]).dt.normalize()).groupby("day")["active_instrument_id"].nunique()
        self.assertTrue((ids_per_day == 1).all())
        daily = VnDerivativesContinuousDaily().load(symbols="VN30F1M", columns="full", check_val=True)
        self.assertIn("roll_flag", daily.columns)
        self.assertEqual(daily.loc[daily["time"] == pd.Timestamp("2024-01-19"), "active_instrument_id"].iloc[0], feb.instrument_id)

    def test_tradable_roll_uses_closed_prior_day_volume(self):
        mar = contract_for_month(2024, 3)
        apr = contract_for_month(2024, 4)
        dates = ["2024-03-11", "2024-03-12", "2024-03-13"]
        self._write_contract_daily(mar, dates, close=1200.0, volume=[100.0, 100.0, 100.0])
        self._write_contract_daily(apr, dates, close=1202.0, volume=[150.0, 160.0, 170.0])

        options = ContinuousOptions(start="2024-03-11", end="2024-03-13", series=("VN30F1M_TRADE",), resolutions=("1d",))
        build_roll_table(options)
        rolls = read_roll_table("v1")
        trade_rolls = rolls[(rolls["series"] == "VN30F1M_TRADE") & (rolls["roll_reason"] == "volume_confirmation")]

        self.assertEqual(trade_rolls["trading_date"].iloc[0], pd.Timestamp("2024-03-13"))
        self.assertEqual(trade_rolls["decision_date"].iloc[0], pd.Timestamp("2024-03-12"))

    def test_roll_table_incremental_build_preserves_existing_history(self):
        build_roll_table(ContinuousOptions(start="2024-01-17", end="2024-01-19", series=("VN30F1M",), resolutions=("1d",)))
        build_roll_table(ContinuousOptions(start="2024-03-18", end="2024-03-22", series=("VN30F1M",), resolutions=("1d",)))
        rolls = read_roll_table("v1")
        dates = set(pd.to_datetime(rolls.loc[rolls["series"] == "VN30F1M", "trading_date"]).dt.strftime("%Y-%m-%d"))

        self.assertIn("2024-01-19", dates)
        self.assertIn("2024-03-22", dates)

    def test_daily_matrix_prefers_rebuilt_continuous_over_legacy_alias(self):
        equity = pd.DataFrame(
            {
                "time": pd.to_datetime(["2024-01-19"]),
                "symbol": "FPT",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0,
            }
        )
        equity_root = Path(os.environ["DATA_ROOT"]) / "vn" / "equity" / "1d" / "symbol=FPT" / "year=2024"
        equity_root.mkdir(parents=True, exist_ok=True)
        equity.to_parquet(equity_root / "part.parquet", index=False)

        legacy = equity.copy()
        legacy["symbol"] = "VN30F1M"
        legacy[["open", "high", "low", "close"]] = 999.0
        legacy_root = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "1d" / "symbol=VN30F1M" / "year=2024"
        legacy_root.mkdir(parents=True, exist_ok=True)
        legacy.to_parquet(legacy_root / "part.parquet", index=False)

        continuous = legacy.copy()
        continuous[["open", "high", "low", "close"]] = 1234.0
        continuous["active_instrument_id"] = contract_for_month(2024, 2).instrument_id
        continuous["roll_flag"] = True
        continuous["roll_gap"] = 10.0
        continuous["roll_ratio"] = 1.01
        continuous["source"] = "continuous_rebuilt"
        continuous["quality_flags"] = "CALENDAR_FRONT_MONTH"
        continuous["ingested_at"] = "2026-01-01T00:00:00+00:00"
        continuous_root = Path(os.environ["DATA_ROOT"]) / "vn" / "futures" / "continuous" / "1d" / "symbol=VN30F1M" / "version=v1" / "year=2024"
        continuous_root.mkdir(parents=True, exist_ok=True)
        continuous.to_parquet(continuous_root / "part.parquet", index=False)

        result = build_matrix(start_date="2024-01-01", end_date="2024-01-31")
        close = pd.read_parquet(Path(os.environ["DATA_ROOT"]) / "vn" / "equity" / "daily_matrix" / "close.parquet")
        self.assertEqual(float(close.loc[pd.Timestamp("2024-01-19"), "VN30F1M"]), 1234.0)
        self.assertEqual(result["auxiliary_sources"]["VN30F1M"], "storage/vn/futures/continuous/1d")
        state = json.loads((Path(os.environ["STATE_ROOT"]) / "vn_daily_matrix_symbols.json").read_text())
        self.assertEqual(state["auxiliary_sources"]["VN30F1M"], "storage/vn/futures/continuous/1d")

        loaded = load_data("vn_derivatives_continuous_1d", symbols="VN30F1M", check_val=True)
        self.assertEqual(list(loaded.columns), ["time", "symbol", "open", "high", "low", "close", "volume"])
        self.assertEqual(float(loaded["close"].iloc[0]), 1234.0)


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
