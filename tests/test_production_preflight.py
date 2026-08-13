from __future__ import annotations

import json
import grp
import hashlib
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import yaml

from collectors import production_preflight


class TestProductionPreflight(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.policy_path = self.root / "policy.yml"
        policy = yaml.safe_load(production_preflight.POLICY_PATH.read_text())
        policy["runtime"]["root"] = str(self.root / "runtime")
        # The test can run as Bobby on the host or as root in the immutable
        # build image.  Use the current identity instead of assuming the host
        # UID/GID is present in every base image.
        policy["runtime"]["collector_uid"] = os.getuid()
        policy["runtime"]["collector_gid"] = os.getgid()
        policy["runtime"]["reader_group"] = grp.getgrgid(os.getgid()).gr_name
        policy["runtime"]["reader_group_gid"] = os.getgid()
        policy["environment"]["id"] = "test-new-vps"
        policy["environment"]["rollback"] = {
            "previous_approved_release": "primus-historical-market-data-v0.0.9",
            "previous_approved_data_root": "/srv/primus/old-approved-root",
        }
        policy["backup"]["destination"] = "s3://test-bucket/primus"
        policy["backup"]["retention"] = "30 daily copies"
        self.policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _policy(self) -> dict:
        return production_preflight._load_policy(self.policy_path)

    def test_init_runtime_creates_metadata_only(self) -> None:
        policy = self._policy()
        result = production_preflight.initialize_runtime(policy)
        runtime = Path(policy["runtime"]["root"])
        self.assertEqual(result["status"], "ok")
        for name in ("storage", "state", "logs", "releases"):
            self.assertTrue((runtime / name).is_dir())
        self.assertTrue((runtime / "state/bootstrap/source_inventory.json").exists())
        self.assertTrue((runtime / "storage/_primus_metadata/release_manifest.json").exists())
        monitoring = json.loads((runtime / "state/bootstrap/monitoring.json").read_text())
        self.assertIn("rss_alert", monitoring)
        self.assertEqual(monitoring["expected_heartbeat_datasets"], policy["monitoring"]["expected_heartbeat_datasets"])
        self.assertFalse(any((runtime / "storage").rglob("*.parquet")))
        self.assertFalse(any((runtime / "state").rglob("*.sqlite")))

    def test_status_is_fail_closed_for_draft_evidence(self) -> None:
        policy = self._policy()
        production_preflight.initialize_runtime(policy)
        payload = production_preflight._status(policy)
        self.assertEqual(payload["status"], "blocked")
        self.assertIn("capacity_and_concurrency", {check["name"] for check in payload["checks"] if check["status"] == "blocked"})

    def test_cli_accepts_documented_status_arguments(self) -> None:
        with redirect_stdout(StringIO()):
            self.assertEqual(
                production_preflight.main(["--policy", str(self.policy_path), "status", "--strict", "--json"]),
                2,
            )

    def test_status_passes_after_complete_evidence(self) -> None:
        policy = self._policy()
        production_preflight.initialize_runtime(policy)
        paths = production_preflight._runtime_paths(policy)
        capacity_path = paths["bootstrap"] / "capacity_report.json"
        capacity = json.loads(capacity_path.read_text())
        capacity["status"] = "pass"
        capacity["approval"] = {"status": "approved", "approved_by": "test", "approved_at": "2026-08-13T00:00:00+00:00"}
        capacity_path.write_text(json.dumps(capacity))
        inventory_path = paths["bootstrap"] / "source_inventory.json"
        inventory = json.loads(inventory_path.read_text())
        for dataset in inventory["datasets"]:
            dataset["source_probe"] = {"status": "pass", "observed_at": "2026-08-13T00:00:00+00:00", "evidence": "bounded"}
        inventory_path.write_text(json.dumps(inventory))
        release_path = paths["metadata"] / "release_manifest.json"
        release = json.loads(release_path.read_text())
        wheel_filename = "test-reader.whl"
        wheel_payload = b"test-wheel"
        (paths["releases"] / wheel_filename).write_bytes(wheel_payload)
        release.update(
            {
                "status": "draft",
                "source_inventory_reference": "state/bootstrap/source_inventory.json",
                "git": {"commit": "test-commit", "tag": "test-tag"},
                "build": {
                    "collector_image": "test-image@sha256:test",
                    "python_base_image": "python@sha256:test",
                    "docker_engine": "test-engine",
                    "docker_compose": "test-compose",
                    "python": "3.12.13",
                    "duckdb": "1.5.5",
                    "pyarrow": "20.0.0",
                },
                "artifacts": {
                    "collector_lock_sha256": "collector-lock",
                    "reader_lock_sha256": "reader-lock",
                    "wheel_filename": wheel_filename,
                    "wheel_sha256": hashlib.sha256(wheel_payload).hexdigest(),
                    "clean_rebuild_verified_at": "2026-08-13T00:00:00+00:00",
                },
                "configuration_sha256": {"test_policy": "test-config-hash"},
                "datasets": [
                    {
                        "dataset_id": "test-dataset",
                        "canonical_schema_version": "test-schema-v1",
                        "partition_layout_version": "test-layout-v1",
                        "supported_loader_contract_versions": ["hmd-loader-v1"],
                        "source_report_reference": "state/test-source-probe.json",
                    }
                ],
            }
        )
        release_path.write_text(json.dumps(release))
        for filename in ("restore_drill.json", "monitoring.json", "clock.json", "access_control.json", "compose_contract.json"):
            path = paths["bootstrap"] / filename
            payload = json.loads(path.read_text())
            payload["status"] = "pass"
            if filename == "clock.json":
                payload["ntp_synchronized"] = True
            path.write_text(json.dumps(payload))
        monitor_state = paths["state"] / "monitoring" / "discord_monitor.json"
        monitor_state.parent.mkdir(parents=True)
        monitor_state.write_text(
            json.dumps(
                {
                    "status": "pass",
                    "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "alert_categories": {
                        name: {"status": "pass"}
                        for name in (
                            "collector_exit_alert",
                            "retry_rate_limit_alert",
                            "validation_repair_alert",
                            "rss_alert",
                            "backup_failure_alert",
                        )
                    },
                }
            )
        )
        payload = production_preflight._status(policy)
        names = {check["name"]: check["status"] for check in payload["checks"]}
        self.assertEqual(names["capacity_and_concurrency"], "pass")
        self.assertEqual(names["source_inventory"], "pass")
        self.assertEqual(names["reproducible_release_and_storage_manifest"], "pass")
        self.assertEqual(payload["status"], "pass")

    def test_status_rejects_acl_evidence_for_a_different_reader_group_gid(self) -> None:
        policy = self._policy()
        production_preflight.initialize_runtime(policy)
        paths = production_preflight._runtime_paths(policy)
        access_path = paths["bootstrap"] / "access_control.json"
        access = json.loads(access_path.read_text())
        access.update({"status": "pass", "reader_group_gid": os.getgid() + 1})
        access_path.write_text(json.dumps(access))

        payload = production_preflight._status(policy)
        acl_check = next(check for check in payload["checks"] if check["name"] == "reader_group_and_acl")
        self.assertEqual(acl_check["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
