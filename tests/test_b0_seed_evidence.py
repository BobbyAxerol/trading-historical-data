from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import yaml

from collectors import b0_seed_evidence
from collectors import production_preflight


class TestB0SeedEvidence(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "runtime"
        self.root.mkdir(parents=True)
        self.policy = yaml.safe_load(production_preflight.POLICY_PATH.read_text(encoding="utf-8"))
        self.policy["runtime"]["root"] = str(self.root)
        for name in ("storage", "state", "logs", "releases"):
            (self.root / name).mkdir()
        (self.root / "state" / "bootstrap").mkdir()
        (self.root / "state" / "bootstrap" / "capacity_report.json").write_text(json.dumps({"status": "draft", "approval": {"status": "pending"}}))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_futures_success(self) -> None:
        manifest = self.root / "state" / "manifests" / "crypto_binance_futures_1m.json"
        manifest.parent.mkdir(parents=True)
        manifest.write_text(
            json.dumps({"symbols": {"BTCUSDT": {"last_success_at": "2026-08-13T00:00:00+00:00", "last_error": None}}})
        )
        heartbeat = self.root / "state" / "heartbeats" / "crypto_binance_futures_1m.json"
        heartbeat.parent.mkdir(parents=True)
        heartbeat.write_text(json.dumps({"status": "ok", "updated_at": "2026-08-13T00:00:00+00:00", "peak_rss_mb": 12.5}))
        parquet = self.root / "storage" / "crypto" / "binance_futures" / "1m" / "symbol=BTCUSDT" / "year=2026" / "month=08" / "part.parquet"
        parquet.parent.mkdir(parents=True)
        parquet.write_bytes(b"unit")

    def test_evaluate_seed_requires_manifest_heartbeat_and_parquet(self) -> None:
        runtime = production_preflight._runtime_paths(self.policy)
        blocked = b0_seed_evidence.evaluate_seed(runtime, "binance_futures_1m", process_exit_code=0)
        self.assertEqual(blocked["status"], "blocked")
        self._write_futures_success()
        passed = b0_seed_evidence.evaluate_seed(runtime, "binance_futures_1m", process_exit_code=0)
        self.assertEqual(passed["status"], "pass")
        self.assertEqual(passed["heartbeat"]["peak_rss_mb"], 12.5)

    def test_finalize_only_accepts_all_fixed_steps(self) -> None:
        b0_seed_evidence.start_bounded_seed(self.policy, plan={"unit": True})
        path = self.root / b0_seed_evidence.EVIDENCE_RELATIVE_PATH
        evidence = json.loads(path.read_text())
        evidence["steps"] = {seed_id: {"seed_id": seed_id, "status": "pass", "runtime_delta_bytes": {}, "heartbeat": {}} for seed_id in b0_seed_evidence.SEED_IDS}
        path.write_text(json.dumps(evidence))
        result = b0_seed_evidence.finalize_bounded_seed(self.policy)
        self.assertEqual(result["status"], "pass")
        capacity = json.loads((self.root / "state" / "bootstrap" / "capacity_report.json").read_text())
        self.assertEqual(capacity["status"], "pass")
        self.assertEqual(capacity["approval"]["scope"], "B0 bounded seed and staged single-writer operation only")


if __name__ == "__main__":
    unittest.main()
