import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from collectors.deribit.config import load_deribit_config
from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.instruments import write_instrument_dimension
from collectors.deribit.parquet_parts import write_parquet_atomic
from collectors.deribit.pilot import DEFAULT_WINDOWS, DeribitPilotRunner
from collectors.deribit.schema import canonical_trade_schema
from collectors.deribit_option_trades import main as deribit_cli_main


class EnvCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        os.environ["DATA_ROOT"] = str(Path(self.tmp.name) / "storage")
        os.environ["STATE_ROOT"] = str(Path(self.tmp.name) / "state")

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


def instrument_row():
    return {
        "instrument_id": 1001,
        "instrument_name": "BTC-25JUN27-100000-C",
        "currency": "BTC",
        "expiry_timestamp_ms": 1813900800000,
        "strike_usd": 100000.0,
        "option_type": 1,
        "creation_timestamp_ms": 1690000000000,
        "contract_size": 1.0,
        "tick_size": 0.0001,
        "min_trade_amount": 0.1,
        "settlement_currency": "BTC",
        "is_expired": False,
        "activated_at_ms": None,
        "activation_seq": None,
        "metadata_source": 1,
        "parse_status": 1,
        "quality_flags": 0,
        "dataset_version_id": 1,
    }


def canonical_row(seq: int, day: date):
    ts = int(datetime(day.year, day.month, day.day, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000) + seq
    return {
        "timestamp_ms": ts,
        "instrument_id": 1001,
        "trade_seq": seq,
        "trade_id_hash": seq,
        "price_btc": 0.01,
        "mark_price_btc": 0.01,
        "iv_pct": 55.0,
        "index_price_usd": 100000.0,
        "amount_base": 1.0,
        "contracts": None,
        "direction": 1,
        "tick_direction": 0,
        "flags": 1,
        "dataset_version_id": 1,
    }


class TestDeribitPilotPhase5(EnvCase):
    def _write_dimension(self, config):
        path = Path(os.environ["DATA_ROOT"]) / "options" / "deribit" / "instruments" / "version=v1" / "instruments.parquet"
        write_instrument_dimension([instrument_row()], path)

    def _write_canonical_day(self, config, day: date, seq: int):
        path = config.canonical_trades_root / "currency=BTC" / f"year={day.year:04d}" / f"month={day.month:02d}" / f"day={day.day:02d}" / "part-00000.parquet"
        table = pa.Table.from_pylist([canonical_row(seq, day)], schema=canonical_trade_schema())
        write_parquet_atomic(table, path)

    def test_pilot_reports_three_deterministic_windows_and_passes_when_sampled(self):
        config = load_deribit_config()
        DeribitCheckpointStore(config).initialize()
        self._write_dimension(config)
        for idx, (_, start) in enumerate(DEFAULT_WINDOWS, start=1):
            self._write_canonical_day(config, start, idx)
        with patch("collectors.deribit.pilot._peak_rss_mb", return_value=100.0):
            summary = DeribitPilotRunner(config, window_days=1, min_rows_per_window=1).run()
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(set(summary["reports"]), {"low", "normal", "high"})
        self.assertTrue(summary["acceptance"]["all_windows_have_samples"])
        self.assertEqual(summary["acceptance"]["strategy_package_coverage_pct"], 100.0)
        for path in summary["reports"].values():
            self.assertTrue(Path(path).exists())
        self.assertTrue(Path(summary["pilot_summary_path"]).exists())

    def test_pilot_blocks_without_representative_samples(self):
        config = load_deribit_config()
        DeribitCheckpointStore(config).initialize()
        self._write_dimension(config)
        with patch("collectors.deribit.pilot._peak_rss_mb", return_value=100.0):
            summary = DeribitPilotRunner(config, window_days=1, min_rows_per_window=1).run()
        self.assertEqual(summary["status"], "blocked")
        self.assertFalse(summary["acceptance"]["all_windows_have_samples"])
        self.assertTrue(Path(summary["pilot_summary_path"]).exists())

    def test_cli_pilot_invokes_runner(self):
        payload = {"status": "ok", "pilot_summary_path": "state/unit.json"}
        with patch("collectors.deribit_option_trades.DeribitPilotRunner") as pilot_cls:
            pilot_cls.return_value.run.return_value = payload
            with redirect_stdout(StringIO()):
                code = deribit_cli_main(["pilot", "--json", "--window-days", "1"])
        self.assertEqual(code, 0)
        pilot_cls.return_value.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
