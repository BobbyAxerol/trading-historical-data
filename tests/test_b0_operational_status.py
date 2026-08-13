from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from collectors.b0_operational_status import operational_status


class TestB0OperationalStatus(unittest.TestCase):
    def _policy(self, root: Path) -> dict:
        return {
            "runtime": {"root": str(root)},
            "monitoring": {
                "free_disk_low_water_gib": 0,
                "inode_low_water_pct": 0,
                "heartbeat_max_age_minutes": 20,
                "expected_heartbeat_datasets": ["dataset_a"],
                "discord_monitor_state_relative_path": "state/monitoring/discord_monitor.json",
                "discord_monitor_max_age_minutes": 3,
            },
        }

    def _write_evidence(self, root: Path, *, now: datetime, alert_status: str = "pass") -> None:
        path = root / "state" / "monitoring" / "discord_monitor.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "status": "pass",
                    "updated_at": now.isoformat(),
                    "alert_categories": {
                        key: {"status": alert_status, "evidence": "unit"}
                        for key in (
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

    def test_blocks_when_heartbeat_and_alert_evidence_are_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            root.mkdir(exist_ok=True)
            payload = operational_status(self._policy(root), now=datetime(2026, 8, 13, tzinfo=timezone.utc))
        names = {check["name"]: check["status"] for check in payload["checks"]}
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(names["expected_heartbeats"], "blocked")
        self.assertEqual(names["backup_failure_alert"], "blocked")

    def test_passes_with_fresh_heartbeat_and_all_alert_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            now = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
            heartbeat = root / "state" / "heartbeats" / "dataset_a.json"
            heartbeat.parent.mkdir(parents=True)
            heartbeat.write_text(json.dumps({"status": "ok", "updated_at": (now - timedelta(minutes=5)).isoformat()}))
            self._write_evidence(root, now=now)
            payload = operational_status(self._policy(root), now=now)
        self.assertEqual(payload["status"], "pass")

    def test_blocks_stale_or_error_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            now = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
            heartbeat = root / "state" / "heartbeats" / "dataset_a.json"
            heartbeat.parent.mkdir(parents=True)
            heartbeat.write_text(json.dumps({"status": "error", "updated_at": (now - timedelta(minutes=30)).isoformat()}))
            self._write_evidence(root, now=now)
            payload = operational_status(self._policy(root), now=now)
        heartbeat_check = next(check for check in payload["checks"] if check["name"] == "expected_heartbeats")
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(heartbeat_check["heartbeats"][0]["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
