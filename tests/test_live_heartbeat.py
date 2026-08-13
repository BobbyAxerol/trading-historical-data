from __future__ import annotations

import unittest
from unittest.mock import Mock, call, patch

from collectors.common.manifest import sleep_with_heartbeat


class TestLiveHeartbeat(unittest.TestCase):
    def test_sleep_with_heartbeat_refreshes_healthy_live_wait(self) -> None:
        heartbeat = Mock()
        heartbeat.state.read.return_value = {"status": "ok"}

        with patch("collectors.common.manifest.time.sleep") as sleep:
            sleep_with_heartbeat(heartbeat, 601, heartbeat_interval_seconds=300, schedule="16:30")

        self.assertEqual(sleep.call_args_list, [call(300.0), call(300.0), call(1.0)])
        self.assertEqual(heartbeat.beat.call_args_list, [call(status="sleeping", schedule="16:30")] * 3)

    def test_sleep_with_heartbeat_never_hides_a_collector_error(self) -> None:
        heartbeat = Mock()
        heartbeat.state.read.return_value = {"status": "error"}

        with patch("collectors.common.manifest.time.sleep") as sleep:
            sleep_with_heartbeat(heartbeat, 601, heartbeat_interval_seconds=300)

        self.assertEqual(sleep.call_args_list, [call(300.0), call(300.0), call(1.0)])
        heartbeat.beat.assert_not_called()

    def test_sleep_with_heartbeat_rejects_nonpositive_interval(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            sleep_with_heartbeat(Mock(), 1, heartbeat_interval_seconds=0)


if __name__ == "__main__":
    unittest.main()
