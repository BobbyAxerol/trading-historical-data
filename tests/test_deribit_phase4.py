import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.cleanup import DeribitCleanup
from collectors.deribit.compact import DeribitCompactor
from collectors.deribit.config import load_deribit_config
from collectors.deribit.instruments import write_instrument_dimension
from collectors.deribit.parquet_parts import write_parquet_atomic
from collectors.deribit.repair import DeribitRepairPlanner
from collectors.deribit.schema import CANONICAL_TRADE_COLUMNS, staging_trade_schema
from collectors.deribit.validate import DeribitValidator
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


def staging_row(seq: int, *, price=0.01, source_priority=1, ingested_at=datetime(2026, 7, 24, 17, 0, 0)):
    return {
        "timestamp_ms": 1700000000000 + seq,
        "instrument_id": 1001,
        "instrument_name": "BTC-25JUN27-100000-C",
        "trade_seq": seq,
        "trade_id": f"trade-{seq}",
        "trade_id_hash": seq,
        "price_btc": price,
        "mark_price_btc": price,
        "iv_pct": 55.0,
        "index_price_usd": 100000.0,
        "amount_base": 1.0,
        "contracts": None,
        "direction": 1,
        "tick_direction": 0,
        "flags": 1,
        "source_priority": source_priority,
        "ingested_at": ingested_at,
        "dataset_version_id": 1,
    }


class TestDeribitPhase4(EnvCase):
    def _prepare(self, rows):
        config = load_deribit_config()
        store = DeribitCheckpointStore(config)
        store.initialize()
        inst_path = Path(os.environ["DATA_ROOT"]) / "options" / "deribit" / "instruments" / "version=v1" / "instruments.parquet"
        write_instrument_dimension([instrument_row()], inst_path)
        table = pa.Table.from_pylist(rows, schema=staging_trade_schema())
        staging_path = config.staging_root / "currency=BTC" / "shard=41" / "run_id=unit" / "instrument=1001" / "seq_000000000001_000000000010.parquet"
        write_parquet_atomic(table, staging_path, metadata={"unit": "true"})
        return config, store, staging_path

    def test_compact_dedupes_and_records_conflict(self):
        config, _, _ = self._prepare(
            [
                staging_row(1, price=0.01, source_priority=1),
                staging_row(1, price=0.02, source_priority=2),
                staging_row(2, price=0.03, source_priority=1),
            ]
        )
        result = DeribitCompactor(config).run()
        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["days_compacted"], 1)
        self.assertEqual(result["output_rows"], 2)
        self.assertEqual(result["conflict_groups"], 1)
        self.assertTrue(Path(result["outputs"][0]["path"]).exists())
        self.assertTrue(Path(result["conflict_reports"][0]).exists())
        table = pq.ParquetFile(result["outputs"][0]["path"]).read()
        self.assertEqual(table.schema.names, CANONICAL_TRADE_COLUMNS)
        self.assertEqual(table.column("trade_seq").to_pylist(), [1, 2])
        self.assertEqual(table.column("price_btc").to_pylist()[0], 0.02)

    def test_validate_and_cleanup_guard(self):
        config, _, staging_path = self._prepare([staging_row(1)])
        compact = DeribitCompactor(config).run()
        self.assertEqual(compact["status"], "ok")
        validation = DeribitValidator(config).run()
        self.assertEqual(validation["status"], "ok")
        self.assertEqual(validation["duplicate_keys"], 0)
        dry = DeribitCleanup(config).run(confirm=False)
        self.assertEqual(dry["status"], "ok")
        self.assertTrue(dry["dry_run"])
        self.assertTrue(staging_path.exists())
        confirmed = DeribitCleanup(config).run(confirm=True)
        self.assertEqual(confirmed["files_deleted"], 1)
        self.assertFalse(staging_path.exists())

    def test_repair_planner_reports_retryable_state(self):
        config, store, _ = self._prepare([staging_row(1)])
        store.upsert_instruments([instrument_row()])
        with sqlite3.connect(config.checkpoint_path) as con:
            con.execute("UPDATE instrument_state SET status='RETRYABLE_ERROR', failure_count=2, last_error_code='http_error'")
            con.commit()
        plan = DeribitRepairPlanner(config).run(only_unresolved=True)
        self.assertEqual(plan["status"], "needs_repair")
        self.assertEqual(plan["retryable_instruments"], 1)
        self.assertEqual(plan["tasks"][0]["type"], "retry_instrument")

    def test_cli_compact_invokes_runner(self):
        payload = {"status": "ok", "phase": "Phase 4", "days_compacted": 0}
        with patch("collectors.deribit_option_trades.DeribitCompactor") as compactor_cls:
            compactor_cls.return_value.run.return_value = payload
            with redirect_stdout(StringIO()):
                code = deribit_cli_main(["compact", "--json", "--max-days", "1"])
        self.assertEqual(code, 0)
        compactor_cls.return_value.run.assert_called_once_with(max_days=1)


if __name__ == "__main__":
    unittest.main()
