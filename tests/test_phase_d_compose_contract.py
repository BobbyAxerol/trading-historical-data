from __future__ import annotations

import unittest

import yaml

from collectors.production_preflight import REPO_ROOT


SERVICE = "phase-d-binance-usdm-perpetual-1m"
APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_USDM_PERPETUAL_1M_APPROVED"
COMMAND = [
    "python",
    "-m",
    "collectors.binance_usdm_perpetual_1m",
    "--mode",
    "once",
    "--symbols",
    "BTCUSDT",
    "--start-month",
    "2020-01",
    "--daily-bridge-days",
    "35",
    "--rest-bridge-days",
    "35",
    "--rest-window-minutes",
    "10080",
]


class TestPhaseDComposeContract(unittest.TestCase):
    def test_phase_d_service_is_one_shot_exact_and_service_scoped(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        service = compose["services"][SERVICE]
        self.assertEqual(service["profiles"], ["phase-d"])
        self.assertEqual(service["restart"], "no")
        self.assertEqual(service["command"], COMMAND)
        self.assertEqual(service["pids_limit"], 256)
        self.assertEqual(service["cpus"], 1.0)
        self.assertEqual(service["mem_limit"], "1536m")
        self.assertEqual(service["environment"][APPROVAL], f"${{{APPROVAL}:-}}")

        common = compose["x-collector-gate-environment"]
        self.assertNotIn(APPROVAL, common)
        for name, candidate in compose["services"].items():
            if name != SERVICE:
                self.assertNotIn(APPROVAL, candidate.get("environment", {}), name)

    def test_phase_d_policy_matches_the_compose_command_and_disables_deribit(self) -> None:
        policy = yaml.safe_load((REPO_ROOT / "configs" / "primus_hmd_phase_d.yml").read_text(encoding="utf-8"))
        approval = policy["phase_d_approval"]
        self.assertEqual(approval["status"], "approved")
        self.assertEqual(approval["runtime_profile"], {"max_concurrent_historical_jobs": 1, "cpus": 1.0, "memory_mib": 1536, "pids": 256, "restart": "no"})
        self.assertEqual(approval["services"][SERVICE]["dataset_id"], "crypto_binance_futures_1m")
        self.assertEqual(approval["services"][SERVICE]["command"], COMMAND)
        self.assertEqual(policy["deribit"]["status"], "disabled_by_owner")


if __name__ == "__main__":
    unittest.main()
