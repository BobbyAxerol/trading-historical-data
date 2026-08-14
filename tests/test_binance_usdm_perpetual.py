from __future__ import annotations

import io
import argparse
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from collectors import binance_usdm_perpetual_1m as perpetual
from collectors.common.manifest import Manifest
from collectors.common.storage import PartitionedParquetStore


def _zip_csv(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("BTCUSDT-1m-test.csv", text)
    return buffer.getvalue()


def _frame(times: list[str], symbol: str = "BTCUSDT") -> pd.DataFrame:
    values = pd.to_datetime(times)
    return pd.DataFrame(
        {
            "time": values,
            "symbol": [symbol] * len(values),
            "open": [100.0] * len(values),
            "high": [101.0] * len(values),
            "low": [99.0] * len(values),
            "close": [100.5] * len(values),
            "volume": [1.0] * len(values),
            "close_time": values + pd.Timedelta(seconds=59),
            "quote_volume": [100.0] * len(values),
            "number_of_trades": [1] * len(values),
            "taker_buy_base_volume": [0.5] * len(values),
            "taker_buy_quote_volume": [50.0] * len(values),
            "source": ["test"] * len(values),
            "ingested_at": ["2026-08-13T00:00:00+00:00"] * len(values),
        }
    )


class TestBinanceUsdmPerpetual(unittest.TestCase):
    def test_read_vision_zip_keeps_headerless_first_row(self) -> None:
        content = _zip_csv(
            "1577836800000,7200,7210,7190,7205,1,1577836859999,7205,5,0.5,3602.5,0\n"
            "1577836860000,7205,7215,7200,7210,2,1577836919999,14420,6,1,7210,0\n"
        )
        frame = perpetual.read_vision_zip(content, symbol="BTCUSDT", source="test")
        self.assertEqual(len(frame), 2)
        self.assertEqual(str(frame.loc[0, "time"]), "2020-01-01 00:00:00")
        self.assertEqual(frame.loc[0, "number_of_trades"], 5)

    def test_read_vision_zip_drops_header_before_positional_normalization(self) -> None:
        content = _zip_csv(
            "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n"
            "1577836800000,7200,7210,7190,7205,1,1577836859999,7205,5,0.5,3602.5,0\n"
        )
        frame = perpetual.read_vision_zip(content, symbol="BTCUSDT", source="test")
        self.assertEqual(len(frame), 1)
        self.assertEqual(str(frame.loc[0, "time"]), "2020-01-01 00:00:00")
        self.assertEqual(frame.loc[0, "number_of_trades"], 5)

    def test_discover_active_perpetuals_filters_to_requested_usdm_contract(self) -> None:
        payload = {
            "symbols": [
                {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "marginAsset": "USDT", "status": "TRADING", "pair": "BTCUSDT", "onboardDate": 1569398400000},
                {"symbol": "BTCUSD_PERP", "contractType": "PERPETUAL", "quoteAsset": "USD", "marginAsset": "BTC", "status": "TRADING", "pair": "BTCUSD"},
                {"symbol": "ETHUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "marginAsset": "USDT", "status": "BREAK", "pair": "ETHUSDT"},
            ]
        }
        with patch.object(perpetual, "_request_json", return_value=payload):
            active = perpetual.discover_active_perpetuals(["BTCUSDT"])
        self.assertEqual(set(active), {"BTCUSDT"})
        self.assertEqual(active["BTCUSDT"]["contract_type"], "PERPETUAL")

    def test_validation_detects_a_gap_without_loading_multiple_partitions_together(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict(os.environ, {"DATA_ROOT": str(root / "storage"), "STATE_ROOT": str(root / "state")}, clear=False):
                store = PartitionedParquetStore(perpetual.STORE_PARTS, partition="month")
                perpetual._append(store, _frame(["2020-01-01 00:00:00", "2020-01-01 00:02:00"]), "BTCUSDT")
                with patch.object(perpetual, "_closed_until", return_value=pd.Timestamp("2020-01-01 00:02:00", tz="UTC")):
                    audit = perpetual.validate_symbol(store=store, symbol="BTCUSDT", expected_start="2020-01-01")
        self.assertEqual(audit["status"], "requires_repair")
        self.assertEqual(audit["gap_count"], 1)
        self.assertEqual(audit["max_gap_minutes"], 1)

    def test_rest_bridge_bounds_each_fetch_window_and_appends_immediately(self) -> None:
        calls: list[tuple[pd.Timestamp, pd.Timestamp]] = []

        def fake_fetch(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
            calls.append((start, end))
            return _frame([start.tz_convert(None).strftime("%Y-%m-%d %H:%M:%S")], symbol)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict(os.environ, {"DATA_ROOT": str(root / "storage"), "STATE_ROOT": str(root / "state")}, clear=False):
                store = PartitionedParquetStore(perpetual.STORE_PARTS, partition="month")
                manifest = Manifest(perpetual.DATASET)
                with patch.object(perpetual, "_closed_until", return_value=pd.Timestamp("2020-01-02 00:00:00", tz="UTC")), patch.object(perpetual, "fetch_1m", side_effect=fake_fetch):
                    result = perpetual.sync_rest_bridge(
                        symbol="BTCUSDT",
                        days=1,
                        window_minutes=1000,
                        store=store,
                        manifest=manifest,
                        logger=__import__("logging").getLogger("test"),
                    )
        self.assertEqual(result["windows"], 2)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all((end - start) <= pd.Timedelta(minutes=999) for start, end in calls))

    def test_audited_gap_repair_uses_only_exact_daily_vision_files(self) -> None:
        audit = {
            "gap_examples": [
                {"start": "2022-02-26T00:00:00+00:00", "end": "2022-02-28T23:59:00+00:00"},
                {"start": "2022-04-01T00:00:00+00:00", "end": "2022-04-02T23:59:00+00:00"},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict(os.environ, {"DATA_ROOT": str(root / "storage"), "STATE_ROOT": str(root / "state")}, clear=False):
                store = PartitionedParquetStore(perpetual.STORE_PARTS, partition="month")
                manifest = Manifest(perpetual.DATASET)
                with patch.object(perpetual, "_day_complete", side_effect=[False, True] * 5), patch.object(
                    perpetual, "sync_vision_file", return_value={"rows_written": 1440, "missing": False}
                ) as sync_file:
                    result = perpetual.repair_audited_vision_gaps(
                        symbol="SOLUSDT",
                        interval="1m",
                        audit=audit,
                        store=store,
                        manifest=manifest,
                        vision_base_url="https://vision.example",
                        logger=__import__("logging").getLogger("test"),
                    )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["rows_written"], 7200)
        self.assertEqual(
            [Path(call.kwargs["key"]).stem.rsplit("-1m-", 1)[-1] for call in sync_file.call_args_list],
            ["2022-02-26", "2022-02-27", "2022-02-28", "2022-04-01", "2022-04-02"],
        )
        self.assertTrue(all(call.kwargs["source"] == "binance_vision_futures_um_daily_repair" for call in sync_file.call_args_list))

    def test_run_writes_a_durable_per_symbol_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                symbols="BTCUSDT",
                start_month="2020-01",
                daily_bridge_days=1,
                rest_bridge_days=1,
                rest_window_minutes=60,
                no_validate=False,
            )
            audit = {"status": "pass", "rows": 3, "gap_count": 0}
            with patch.dict(
                os.environ,
                {
                    "DATA_ROOT": str(root / "storage"),
                    "STATE_ROOT": str(root / "state"),
                    "LOG_ROOT": str(root / "logs"),
                },
                clear=False,
            ), patch.object(perpetual, "load_yaml", return_value={"symbols": ["BTCUSDT"], "interval": "1m"}), patch.object(
                perpetual, "discover_active_perpetuals", return_value={"BTCUSDT": {"symbol": "BTCUSDT"}}
            ), patch.object(perpetual, "sync_symbol", return_value={"monthly": {}, "daily": {}, "rest": {}}), patch.object(perpetual, "validate_symbol", return_value=audit):
                result = perpetual.run(args)
                audit_path = root / "state" / "audits" / "crypto_binance_futures_1m_BTCUSDT_phase_d.json"
                stored = json.loads(audit_path.read_text(encoding="utf-8"))
        self.assertEqual(result["status"], "pass")
        self.assertEqual(stored["status"], "pass")
        self.assertEqual(stored["service"], perpetual.SERVICE)

    def test_run_repairs_audited_gaps_then_revalidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                symbols="BTCUSDT",
                start_month="2020-01",
                daily_bridge_days=1,
                rest_bridge_days=1,
                rest_window_minutes=60,
                no_validate=False,
                phase_label="e",
                allow_later_start=True,
            )
            requires_repair = {"status": "requires_repair", "gap_count": 1, "gap_examples": [{"start": "2020-01-02T00:00:00+00:00", "end": "2020-01-02T23:59:00+00:00"}]}
            passed = {"status": "pass", "gap_count": 0, "gap_examples": []}
            repair = {"status": "pass", "requested_days": ["2020-01-02"], "rows_written": 1440, "missing_days": [], "unrepaired_days": []}
            with patch.dict(
                os.environ,
                {"DATA_ROOT": str(root / "storage"), "STATE_ROOT": str(root / "state"), "LOG_ROOT": str(root / "logs")},
                clear=False,
            ), patch.object(perpetual, "load_yaml", return_value={"symbols": ["BTCUSDT"], "interval": "1m"}), patch.object(
                perpetual, "discover_active_perpetuals", return_value={"BTCUSDT": {"symbol": "BTCUSDT"}}
            ), patch.object(perpetual, "sync_symbol", return_value={"monthly": {}, "daily": {}, "rest": {}}), patch.object(
                perpetual, "validate_symbol", side_effect=[requires_repair, passed]
            ), patch.object(perpetual, "repair_audited_vision_gaps", return_value=repair) as repair_gaps:
                result = perpetual.run(args)
                stored = json.loads((root / "state" / "audits" / "crypto_binance_futures_1m_BTCUSDT_phase_e.json").read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["results"]["BTCUSDT"]["repair"], repair)
        self.assertEqual(stored["repair"], repair)
        repair_gaps.assert_called_once()


if __name__ == "__main__":
    unittest.main()
