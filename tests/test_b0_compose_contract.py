from __future__ import annotations

import unittest

import yaml

from collectors.production_preflight import REPO_ROOT


class TestB0ComposeContract(unittest.TestCase):
    def test_collector_vendor_home_is_ephemeral_and_not_a_runtime_data_mount(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
        environment = compose["x-collector-gate-environment"]
        expected_home = "/tmp/primus-hmd-home"
        self.assertEqual(environment["HOME"], expected_home)
        self.assertEqual(environment["XDG_CONFIG_HOME"], f"{expected_home}/.config")
        self.assertEqual(environment["XDG_CACHE_HOME"], f"{expected_home}/.cache")
        self.assertEqual(environment["MPLCONFIGDIR"], f"{expected_home}/.config/matplotlib")


if __name__ == "__main__":
    unittest.main()
