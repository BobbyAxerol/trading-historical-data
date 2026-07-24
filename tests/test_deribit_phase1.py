import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from collectors.deribit.client import DeribitApiResult, DeribitHistoryClient
from collectors.deribit.config import load_deribit_config
from collectors.deribit.probe import DeribitApiProbeRunner, ProbeOptions
from collectors.deribit.rate_limit import parse_retry_after
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


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}
        self.content = (text or str(payload)).encode("utf-8")

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def get(self, *args, **kwargs):
        del args, kwargs
        if not self.responses:
            raise AssertionError("No fake response left")
        return self.responses.pop(0)


def trade(seq: int, instrument_name: str = "BTC-27MAR26-100000-C"):
    return {
        "trade_id": f"{instrument_name}-{seq}",
        "trade_seq": seq,
        "instrument_name": instrument_name,
        "timestamp": 1700000000000 + seq,
        "direction": "buy" if seq % 2 == 0 else "sell",
        "tick_direction": 0,
        "index_price": 100000.0,
        "price": 0.1,
        "amount": 1.0,
        "mark_price": 0.1,
        "iv": 55.0,
    }


class FakeProbeClient:
    def __init__(self):
        self.instrument_name = "BTC-27MAR26-100000-C"
        self.trades = [trade(seq, self.instrument_name) for seq in range(100, 140)]

    def get_instruments(self, *, expired: bool):
        rows = [
            {
                "instrument_name": self.instrument_name,
                "expiration_timestamp": 1774598400000,
                "creation_timestamp": 1700000000000,
            }
        ]
        return DeribitApiResult("get_instruments", {"expired": expired}, True, 200, result=rows, latency_ms=1.0, response_bytes=100)

    def get_last_trades_by_instrument(self, instrument_name: str, retry=True, **params):
        del retry
        rows = [dict(row) for row in self.trades if row["instrument_name"] == instrument_name]
        start_seq = params.get("start_seq")
        end_seq = params.get("end_seq")
        if start_seq is not None:
            rows = [row for row in rows if row["trade_seq"] >= int(start_seq)]
        if end_seq is not None:
            rows = [row for row in rows if row["trade_seq"] <= int(end_seq)]
        if params.get("sorting") == "desc":
            rows = sorted(rows, key=lambda item: item["trade_seq"], reverse=True)
        else:
            rows = sorted(rows, key=lambda item: item["trade_seq"])
        count = int(params.get("count", len(rows)))
        rows = rows[:count]
        result = {"trades": rows, "has_more": len(rows) >= count and bool(rows)}
        return DeribitApiResult("get_last_trades_by_instrument", params, True, 200, result=result, latency_ms=2.0, response_bytes=200)


class TestDeribitClientPhase1(EnvCase):
    def test_success_jsonrpc_trade_result_classifies_data(self):
        config = load_deribit_config()
        session = FakeSession(
            [
                FakeResponse(
                    payload={
                        "jsonrpc": "2.0",
                        "result": {"trades": [trade(1)], "has_more": False},
                    }
                )
            ]
        )
        client = DeribitHistoryClient(config, requests_per_second=1000, session=session)
        result = client.get_last_trades_by_instrument("BTC-27MAR26-100000-C", count=1)
        self.assertTrue(result.ok)
        self.assertEqual(result.classification(), "SUCCESS_WITH_DATA")
        self.assertEqual(len(result.trades), 1)

    def test_http_429_keeps_retry_after_and_unknown(self):
        config = load_deribit_config()
        session = FakeSession([FakeResponse(status_code=429, text="rate limited", headers={"Retry-After": "3"})])
        client = DeribitHistoryClient(config, requests_per_second=1000, session=session)
        result = client.get_last_trades_by_instrument("BTC-27MAR26-100000-C", count=1, retry=False)
        self.assertFalse(result.ok)
        self.assertEqual(result.classification(), "UNKNOWN")
        self.assertEqual(result.retry_after_seconds, 3.0)

    def test_malformed_and_missing_result_are_unknown(self):
        config = load_deribit_config()
        malformed = DeribitHistoryClient(
            config,
            requests_per_second=1000,
            session=FakeSession([FakeResponse(payload=ValueError("bad json"))]),
        ).get_last_trades_by_instrument("BTC-27MAR26-100000-C", count=1, retry=False)
        missing = DeribitHistoryClient(
            config,
            requests_per_second=1000,
            session=FakeSession([FakeResponse(payload={"jsonrpc": "2.0"})]),
        ).get_last_trades_by_instrument("BTC-27MAR26-100000-C", count=1, retry=False)
        self.assertFalse(malformed.ok)
        self.assertEqual(malformed.error_type, "malformed_json")
        self.assertEqual(malformed.classification(), "UNKNOWN")
        self.assertFalse(missing.ok)
        self.assertEqual(missing.error_type, "missing_result")
        self.assertEqual(missing.classification(), "UNKNOWN")

    def test_parse_retry_after(self):
        self.assertEqual(parse_retry_after("2"), 2.0)
        self.assertIsNone(parse_retry_after("not-a-date"))


class TestDeribitProbePhase1(EnvCase):
    def test_probe_runner_writes_report_with_mandatory_fields(self):
        config = load_deribit_config()
        runner = DeribitApiProbeRunner(
            config,
            client=FakeProbeClient(),
            options=ProbeOptions(rate_ramp=True, max_rps=1.0, requests_per_rps=1),
        )
        with patch("collectors.deribit.probe.time.sleep"):
            report = runner.run()
        self.assertEqual(report["status"], "ok")
        self.assertTrue(report["production_backfill_allowed"])
        self.assertEqual(report["selected_page_size"], 10000)
        self.assertEqual(report["safe_trade_rps"], 1.0)
        self.assertEqual(report["get_instruments_rps"], 1.0)
        self.assertEqual(report["instrument_discovery"]["probe_instrument"], "BTC-27MAR26-100000-C")
        self.assertTrue(report["sequence_probe"]["verified_sorting"])
        self.assertTrue(report["verified_sorting"])
        self.assertIn("sequence_boundary_semantics", report)
        self.assertIn("has_more_semantics", report)
        self.assertIn("expired_instrument_coverage", report)
        self.assertIn("field_presence_statistics", report)
        self.assertEqual(report["schema_probe"]["field_presence_statistics"]["trade_seq"]["presence_rate"], 1.0)
        self.assertTrue(runner.report_path.exists())
        self.assertTrue(str(runner.report_path).endswith("state/deribit_options/version=v1/api_probe_report.json"))

    def test_probe_without_rate_ramp_keeps_production_blocked(self):
        config = load_deribit_config()
        runner = DeribitApiProbeRunner(config, client=FakeProbeClient(), options=ProbeOptions(rate_ramp=False))
        report = runner.run()
        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["production_backfill_allowed"])
        self.assertEqual(report["rate_probe"]["status"], "conservative_default")
        self.assertIn("Rate ramp was not verified", " ".join(report["assumptions"]))

    def test_cli_probe_invokes_runner(self):
        payload = {
            "status": "ok",
            "selected_page_size": 1000,
            "safe_trade_rps": 1.0,
        }
        with patch("collectors.deribit_option_trades.DeribitApiProbeRunner") as runner_cls:
            runner = runner_cls.return_value
            runner.run.return_value = payload
            runner.report_path = Path(os.environ["STATE_ROOT"]) / "deribit_options" / "version=v1" / "api_probe_report.json"
            with redirect_stdout(StringIO()):
                code = deribit_cli_main(["probe", "--json"])
        self.assertEqual(code, 0)
        runner.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
