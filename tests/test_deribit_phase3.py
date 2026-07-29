import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.client import DeribitApiResult
from collectors.deribit.config import load_deribit_config
from collectors.deribit.engine import DeribitTradeDownloader, DownloaderOptions, date_boundary_ms
from collectors.deribit.instruments import write_instrument_dimension
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


def instrument(name="BTC-25JUN27-100000-C", *, is_expired=False):
    return {
        "instrument_id": 1001,
        "instrument_name": name,
        "currency": "BTC",
        "expiry_timestamp_ms": 1705000000000,
        "strike_usd": 100000.0,
        "option_type": 1,
        "creation_timestamp_ms": 1690000000000,
        "contract_size": 1.0,
        "tick_size": 0.0001,
        "min_trade_amount": 0.1,
        "settlement_currency": "BTC",
        "is_expired": is_expired,
        "activated_at_ms": None,
        "activation_seq": None,
        "metadata_source": 1,
        "parse_status": 1,
        "quality_flags": 0,
        "dataset_version_id": 1,
    }


def trade(seq: int, *, timestamp_ms=1700000000000, index_price=100000.0, iv=55.0):
    return {
        "trade_id": f"trade-{seq}",
        "trade_seq": seq,
        "instrument_name": "BTC-25JUN27-100000-C",
        "timestamp": timestamp_ms,
        "direction": "buy",
        "tick_direction": 0,
        "index_price": index_price,
        "price": 0.01,
        "amount": 1.0,
        "mark_price": 0.01,
        "iv": iv,
    }


class FakeTradeClient:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def get_last_trades_by_instrument(self, instrument_name: str, **params):
        self.calls.append((instrument_name, params))
        return self.result


class TestDeribitDownloaderPhase3(EnvCase):
    def _prepare(self, row):
        config = load_deribit_config()
        store = DeribitCheckpointStore(config)
        store.initialize()
        path = Path(os.environ["DATA_ROOT"]) / "options" / "deribit" / "instruments" / "version=v1" / "instruments.parquet"
        write_instrument_dimension([row], path)
        store.upsert_instruments([row])
        return config, store

    def _download_ranges(self, config):
        with sqlite3.connect(config.checkpoint_path) as con:
            con.row_factory = sqlite3.Row
            return [dict(row) for row in con.execute("SELECT * FROM download_ranges ORDER BY id").fetchall()]

    def test_success_writes_staging_before_checkpoint_advance(self):
        config, store = self._prepare(instrument())
        api_result = DeribitApiResult(
            "get_last_trades_by_instrument",
            {},
            True,
            200,
            result={"trades": [trade(2), trade(1)], "has_more": False},
        )
        summary = DeribitTradeDownloader(
            config,
            client=FakeTradeClient(api_result),
            store=store,
            options=DownloaderOptions(max_tasks=1, run_id="unit", allow_unprobed=True),
        ).run()
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["files_written"], 1)
        self.assertEqual(summary["retained_rows"], 2)

        ranges = self._download_ranges(config)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0]["response_trade_count"], 2)
        self.assertEqual(ranges[0]["retained_trade_count"], 2)
        output_path = config.resolve_storage_reference(ranges[0]["output_file"])
        self.assertTrue(output_path.exists())
        table = pq.ParquetFile(output_path).read()
        self.assertEqual(table.column("trade_seq").to_pylist(), [1, 2])
        self.assertEqual(table.column("contracts").to_pylist(), [1.0, 1.0])
        self.assertEqual(table.column("flags").to_pylist(), [129, 129])

        state = store.instrument_states()[0]
        self.assertEqual(state["last_processed_seq"], 2)
        self.assertEqual(state["status"], "CAUGHT_UP_ACTIVE")
        self.assertEqual(state["activation_seq"], 1)

    def test_unknown_response_does_not_advance_checkpoint_or_ledger(self):
        config, store = self._prepare(instrument())
        api_result = DeribitApiResult(
            "get_last_trades_by_instrument",
            {},
            False,
            503,
            error="temporary",
            error_type="http_error",
        )
        summary = DeribitTradeDownloader(
            config,
            client=FakeTradeClient(api_result),
            store=store,
            options=DownloaderOptions(max_tasks=1, run_id="unit", allow_unprobed=True),
        ).run()
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(self._download_ranges(config), [])
        state = store.instrument_states()[0]
        self.assertEqual(state["last_processed_seq"], 0)
        self.assertEqual(state["status"], "RETRYABLE_ERROR")

    def test_discarded_response_commits_coverage_without_empty_file(self):
        row = instrument(is_expired=True)
        config, store = self._prepare(row)
        api_result = DeribitApiResult(
            "get_last_trades_by_instrument",
            {},
            True,
            200,
            result={"trades": [trade(1, timestamp_ms=1600000000000)], "has_more": False},
        )
        summary = DeribitTradeDownloader(
            config,
            client=FakeTradeClient(api_result),
            store=store,
            options=DownloaderOptions(max_tasks=1, run_id="unit", allow_unprobed=True),
        ).run()
        self.assertEqual(summary["files_written"], 0)
        self.assertEqual(summary["response_rows"], 1)
        self.assertEqual(summary["discarded_rows"], 1)
        ranges = self._download_ranges(config)
        self.assertIsNone(ranges[0]["output_file"])
        self.assertEqual(ranges[0]["status"], "SUCCESS_DISCARDED")
        self.assertEqual(store.instrument_states()[0]["last_processed_seq"], 1)

    def test_empty_expired_instrument_becomes_empty_confirmed(self):
        row = instrument(is_expired=True)
        config, store = self._prepare(row)
        api_result = DeribitApiResult(
            "get_last_trades_by_instrument",
            {},
            True,
            200,
            result={"trades": [], "has_more": False},
        )
        summary = DeribitTradeDownloader(
            config,
            client=FakeTradeClient(api_result),
            store=store,
            options=DownloaderOptions(max_tasks=1, run_id="unit", allow_unprobed=True),
        ).run()
        self.assertEqual(summary["files_written"], 0)
        ranges = self._download_ranges(config)
        self.assertEqual(ranges[0]["status"], "EMPTY_CONFIRMED")
        state = store.instrument_states()[0]
        self.assertEqual(state["status"], "EMPTY_CONFIRMED")
        self.assertGreater(state["last_processed_seq"], 0)

    def test_cli_backfill_invokes_downloader(self):
        payload = {"status": "ok", "run_id": "unit", "tasks_attempted": 1, "files_written": 0, "retained_rows": 0}
        with patch("collectors.deribit_option_trades.DeribitTradeDownloader") as downloader_cls:
            downloader_cls.return_value.run.return_value = payload
            with redirect_stdout(StringIO()):
                code = deribit_cli_main(["backfill", "--json", "--max-tasks", "1", "--expiry-end", "2022-12-31", "--progress-every", "10"])
        self.assertEqual(code, 0)
        downloader_cls.return_value.run.assert_called_once()
        options = downloader_cls.call_args.kwargs["options"]
        self.assertEqual(options.expiry_end_ms, date_boundary_ms("2022-12-31", end=True))
        self.assertEqual(options.progress_every, 10)

    def test_expiry_filter_limits_planned_tasks(self):
        old = instrument("BTC-30DEC22-20000-C", is_expired=True)
        old["instrument_id"] = 1002
        old["expiry_timestamp_ms"] = date_boundary_ms("2022-12-30", end=False)
        new = instrument("BTC-29DEC23-30000-C", is_expired=True)
        new["instrument_id"] = 1003
        new["expiry_timestamp_ms"] = date_boundary_ms("2023-12-29", end=False)
        config = load_deribit_config()
        store = DeribitCheckpointStore(config)
        store.initialize()
        path = Path(os.environ["DATA_ROOT"]) / "options" / "deribit" / "instruments" / "version=v1" / "instruments.parquet"
        write_instrument_dimension([old, new], path)
        store.upsert_instruments([old, new])
        api_result = DeribitApiResult(
            "get_last_trades_by_instrument",
            {},
            True,
            200,
            result={"trades": [], "has_more": False},
        )
        client = FakeTradeClient(api_result)
        summary = DeribitTradeDownloader(
            config,
            client=client,
            store=store,
            options=DownloaderOptions(max_tasks=10, allow_unprobed=True, expiry_end_ms=date_boundary_ms("2022-12-31", end=True)),
        ).run()
        self.assertEqual(summary["tasks_planned"], 1)
        self.assertEqual(client.calls[0][0], "BTC-30DEC22-20000-C")


if __name__ == "__main__":
    unittest.main()
