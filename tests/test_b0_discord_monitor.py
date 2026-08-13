from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from collectors import b0_discord_monitor


class TestB0DiscordMonitor(unittest.TestCase):
    def _policy(self, root: Path) -> dict:
        return {
            "runtime": {"root": str(root)},
            "monitoring": {
                "free_disk_low_water_gib": 0,
                "inode_low_water_pct": 0,
                "heartbeat_max_age_minutes": 20,
                "expected_heartbeat_datasets": [],
                "discord_monitor_state_relative_path": "state/monitoring/discord_monitor.json",
                "discord_monitor_max_age_minutes": 3,
            },
        }

    def test_rejects_non_discord_or_malformed_webhook_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            secret = Path(tmp) / "webhook"
            secret.write_text("https://example.test/api/webhooks/not-allowed", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "allowed HTTPS Discord"):
                b0_discord_monitor.load_discord_webhook_url(secret)
            secret.write_text("https://discord.com:bad/api/webhooks/not-allowed", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "allowed HTTPS Discord"):
                b0_discord_monitor.load_discord_webhook_url(secret)

    def test_test_alerts_records_no_webhook_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            policy = self._policy(root)
            state_file = root / "state" / "monitoring" / "discord_monitor.json"
            delivered: list[str] = []

            def sender(_: str, message: str) -> None:
                delivered.append(message)

            with patch.object(b0_discord_monitor, "load_discord_webhook_url", return_value="https://discord.com/api/webhooks/unit/token"):
                ok = b0_discord_monitor.run_cycle(policy, state_file=state_file, test_alerts=True, sender=sender)

            self.assertTrue(ok)
            self.assertEqual(len(delivered), len(b0_discord_monitor.ALERT_CATEGORIES))
            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "pass")
            self.assertEqual(set(state["alert_categories"]), set(b0_discord_monitor.ALERT_CATEGORIES))
            self.assertNotIn("https://discord.com/api/webhooks/unit/token", json.dumps(state))


if __name__ == "__main__":
    unittest.main()
