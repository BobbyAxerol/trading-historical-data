import os
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.client import DeribitApiResult
from collectors.deribit.config import load_deribit_config
from collectors.deribit.instruments import (
    DeribitInstrumentDiscovery,
    InstrumentQualityFlag,
    MetadataSource,
    OptionType,
    ParseStatus,
    instrument_dimension_path,
    normalize_instrument,
    parse_instrument_name,
    stable_instrument_id,
)
from collectors.deribit.schema import INSTRUMENT_COLUMNS
from collectors.deribit.tasks import plan_sequence_tasks


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


class FakeInstrumentClient:
    def __init__(self):
        self.active = [
            {
                "instrument_name": "BTC-27MAR26-100000-C",
                "expiration_timestamp": 1774598400000,
                "creation_timestamp": 1700000000000,
                "strike": 100000.0,
                "option_type": "call",
                "contract_size": 1.0,
                "tick_size": 0.0001,
                "min_trade_amount": 0.1,
                "settlement_currency": "BTC",
            }
        ]
        self.expired = [
            {
                "instrument_name": "BTC-29DEC23-40000-P",
                "creation_timestamp": 1680000000000,
                "contract_size": 1.0,
                "tick_size": 0.0001,
            },
            {
                "instrument_name": "BTC-BAD-NAME",
            },
        ]

    def get_instruments(self, *, expired: bool):
        rows = self.expired if expired else self.active
        return DeribitApiResult("get_instruments", {"expired": expired}, True, 200, result=rows, latency_ms=1.0, response_bytes=100)


class TestDeribitInstrumentParsingPhase2(EnvCase):
    def test_parse_deribit_option_name(self):
        parsed = parse_instrument_name("BTC-27MAR26-100000-C")
        self.assertTrue(parsed.ok)
        self.assertEqual(parsed.currency, "BTC")
        self.assertEqual(parsed.expiry_timestamp_ms, 1774598400000)
        self.assertEqual(parsed.strike_usd, 100000.0)
        self.assertEqual(parsed.option_type, OptionType.CALL)

    def test_normalize_uses_name_fallback_without_silent_zero(self):
        config = load_deribit_config()
        row = normalize_instrument(
            {"instrument_name": "BTC-29DEC23-40000-P", "creation_timestamp": 1680000000000},
            is_expired=True,
            config=config,
        )
        self.assertEqual(row["expiry_timestamp_ms"], 1703836800000)
        self.assertEqual(row["strike_usd"], 40000.0)
        self.assertEqual(row["option_type"], int(OptionType.PUT))
        self.assertEqual(row["metadata_source"], int(MetadataSource.MIXED))
        self.assertEqual(row["parse_status"], int(ParseStatus.OK))
        self.assertTrue(row["quality_flags"] & int(InstrumentQualityFlag.USED_NAME_EXPIRY))

    def test_invalid_metadata_is_flagged(self):
        config = load_deribit_config()
        row = normalize_instrument({"instrument_name": "BTC-BAD-NAME"}, is_expired=True, config=config)
        self.assertEqual(row["parse_status"], int(ParseStatus.INVALID))
        self.assertEqual(row["expiry_timestamp_ms"], -1)
        self.assertTrue(row["quality_flags"] & int(InstrumentQualityFlag.INVALID_NAME))
        self.assertTrue(row["quality_flags"] & int(InstrumentQualityFlag.MISSING_STRIKE))

    def test_future_contract_from_expired_endpoint_stays_resumable_active(self):
        config = load_deribit_config()
        row = normalize_instrument({"instrument_name": "BTC-25JUN27-140000-C"}, is_expired=True, config=config)
        self.assertFalse(row["is_expired"])

    def test_stable_id_does_not_depend_on_sort_order(self):
        self.assertEqual(stable_instrument_id("BTC-27MAR26-100000-C"), stable_instrument_id("BTC-27MAR26-100000-C"))
        self.assertNotEqual(stable_instrument_id("BTC-27MAR26-100000-C"), stable_instrument_id("BTC-27MAR26-100000-P"))


class TestDeribitDiscoveryPhase2(EnvCase):
    def test_discovery_writes_dimension_and_checkpoint_state(self):
        config = load_deribit_config()
        result = DeribitInstrumentDiscovery(config, client=FakeInstrumentClient()).run()
        self.assertEqual(result.total_rows, 3)
        self.assertEqual(result.active_rows, 1)
        self.assertEqual(result.expired_rows, 2)
        self.assertEqual(result.invalid_rows, 1)
        self.assertEqual(result.instrument_path, instrument_dimension_path(config))
        table = pq.ParquetFile(result.instrument_path).read()
        self.assertEqual(table.schema.names, INSTRUMENT_COLUMNS)
        names = table.column("instrument_name").to_pylist()
        self.assertEqual(names, sorted(names))

        rows = table.to_pylist()
        store = DeribitCheckpointStore(config)
        self.assertEqual(store.upsert_instruments(rows), 3)
        states = store.instrument_states()
        self.assertEqual(len(states), 3)
        active_state = next(row for row in states if row["instrument_name"] == "BTC-27MAR26-100000-C")
        self.assertEqual(active_state["status"], "NEW")
        self.assertEqual(active_state["is_expired"], 0)

    def test_checkpoint_rerun_preserves_cursor_and_plans_tasks(self):
        config = load_deribit_config()
        result = DeribitInstrumentDiscovery(config, client=FakeInstrumentClient()).run()
        rows = pq.ParquetFile(result.instrument_path).read().to_pylist()
        store = DeribitCheckpointStore(config)
        store.upsert_instruments(rows)
        with store.connect() as con:
            con.execute(
                "UPDATE instrument_state SET last_processed_seq=500, status='CAUGHT_UP_ACTIVE' WHERE instrument_name='BTC-27MAR26-100000-C'"
            )
            con.commit()

        store.upsert_instruments(rows)
        states = store.instrument_states()
        active_state = next(row for row in states if row["instrument_name"] == "BTC-27MAR26-100000-C")
        self.assertEqual(active_state["last_processed_seq"], 500)
        self.assertEqual(active_state["status"], "CAUGHT_UP_ACTIVE")

        tasks = plan_sequence_tasks(config, store)
        task = next(item for item in tasks if item.instrument_name == "BTC-27MAR26-100000-C")
        self.assertEqual(task.start_seq, 501)
        self.assertEqual(task.end_seq, 5500)


if __name__ == "__main__":
    unittest.main()
