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

    def test_staged_crypto_approval_is_not_a_global_writer_flag(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
        common_environment = compose["x-collector-gate-environment"]
        self.assertNotIn("PRIMUS_HMD_B0_APPROVED", common_environment)
        self.assertNotIn("PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED", common_environment)

        services = compose["services"]
        crypto_environment = services["crypto-1m-live"]["environment"]
        self.assertEqual(
            crypto_environment["PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED"],
            "${PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED:-}",
        )
        self.assertEqual(
            services["crypto-1m-live"]["command"],
            ["python", "-m", "collectors.crypto_1m", "--mode", "live", "--symbols", "BTCUSDT"],
        )
        for service_name, service in services.items():
            if service_name == "crypto-1m-live":
                continue
            self.assertNotIn("PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED", service.get("environment", {}), service_name)


if __name__ == "__main__":
    unittest.main()
