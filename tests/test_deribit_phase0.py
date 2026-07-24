import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pandas as pd

import data_loader
from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.config import DeribitConfigError, load_deribit_config, validate_deribit_config
from collectors.deribit.schema import (
    CANONICAL_TRADE_COLUMNS,
    INSTRUMENT_COLUMNS,
    SNAPSHOT_5M_COLUMNS,
    canonical_trade_schema,
    instrument_schema,
    snapshot_5m_schema,
)
from collectors.deribit_option_trades import build_parser, main as deribit_cli_main
from data_loader import DeribitOptionSnapshots5m, DeribitOptionTrades, load_data


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


class TestDeribitConfigPhase0(EnvCase):
    def test_default_config_loads_and_resolves_roots(self):
        config = load_deribit_config()
        self.assertEqual(config.version, "v1")
        self.assertEqual(config.currency, "BTC")
        self.assertEqual(len(config.config_hash), 64)
        self.assertTrue(str(config.checkpoint_path).endswith("state/deribit_options/version=v1/BTC.sqlite"))
        self.assertIn("storage/_staging/options/deribit/version=v1", str(config.staging_root))
        self.assertIn("storage/options/deribit/trades/version=v1", str(config.canonical_trades_root))

    def test_config_validation_rejects_forbidden_1m_full_history(self):
        config = load_deribit_config()
        payload = dict(config.raw)
        payload["snapshot_1m"] = dict(payload["snapshot_1m"])
        payload["snapshot_1m"]["persistent_full_history"] = True
        with self.assertRaises(DeribitConfigError):
            validate_deribit_config(payload)


class TestDeribitSchemaPhase0(unittest.TestCase):
    def test_arrow_schemas_match_phase0_contracts(self):
        self.assertEqual(instrument_schema().names, INSTRUMENT_COLUMNS)
        self.assertEqual(canonical_trade_schema().names, CANONICAL_TRADE_COLUMNS)
        self.assertEqual(snapshot_5m_schema().names, SNAPSHOT_5M_COLUMNS)
        self.assertEqual(canonical_trade_schema().field("instrument_id").type.bit_width, 32)
        self.assertFalse(snapshot_5m_schema().field("entry_eligible").nullable)


class TestDeribitCheckpointPhase0(EnvCase):
    def test_checkpoint_schema_initializes_idempotently(self):
        config = load_deribit_config()
        store = DeribitCheckpointStore(config)
        first = store.initialize()
        second = store.initialize()
        self.assertEqual(first.path, second.path)
        self.assertTrue(first.path.exists())
        self.assertEqual(second.instrument_states, 0)
        self.assertEqual(second.download_ranges, 0)
        metadata = store.metadata()
        self.assertEqual(metadata["dataset_version"], "v1")
        self.assertEqual(metadata["currency"], "BTC")
        self.assertEqual(metadata["config_hash"], config.config_hash)


class TestDeribitCliPhase0(EnvCase):
    def test_cli_parser_has_phase0_subcommands(self):
        parser = build_parser()
        args = parser.parse_args(["init", "--version", "v1"])
        self.assertEqual(args.command, "init")
        args = parser.parse_args(["build-snapshot-1m", "--start", "2026-01-01", "--end", "2026-01-02"])
        self.assertEqual(args.command, "build-snapshot-1m")

    def test_cli_init_creates_checkpoint(self):
        with redirect_stdout(StringIO()):
            code = deribit_cli_main(["init", "--json"])
        self.assertEqual(code, 0)
        checkpoint = Path(os.environ["STATE_ROOT"]) / "deribit_options" / "version=v1" / "BTC.sqlite"
        self.assertTrue(checkpoint.exists())

    def test_reserved_command_blocks_in_phase0(self):
        with redirect_stdout(StringIO()):
            self.assertEqual(deribit_cli_main(["build-snapshot-5m", "--json"]), 2)


class TestDeribitLoaderPhase0(EnvCase):
    def test_loader_empty_schema_without_storage(self):
        trades = DeribitOptionTrades().load(start_date="2026-01-01", check_val=False)
        snapshots = DeribitOptionSnapshots5m().load(start_date="2026-01-01", check_val=False)
        self.assertTrue(trades.empty)
        self.assertEqual(list(trades.columns), CANONICAL_TRADE_COLUMNS)
        self.assertTrue(snapshots.empty)
        self.assertEqual(list(snapshots.columns), SNAPSHOT_5M_COLUMNS)

    def test_router_deribit_endpoints_do_not_break_existing_signature(self):
        trades = load_data("deribit_option_trades", start_date="2026-01-01", currency="BTC", check_val=False)
        snapshots = load_data("deribit_option_snapshots_5m", start_date="2026-01-01", currency="BTC", check_val=False)
        self.assertIsInstance(trades, pd.DataFrame)
        self.assertIsInstance(snapshots, pd.DataFrame)
        self.assertEqual(list(trades.columns), CANONICAL_TRADE_COLUMNS)
        self.assertEqual(list(snapshots.columns), SNAPSHOT_5M_COLUMNS)


if __name__ == "__main__":
    unittest.main()
