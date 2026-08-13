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
SPOT_SERVICE = "phase-d-binance-spot-1m"
SPOT_APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_SPOT_1M_APPROVED"
SPOT_COMMAND = [
    "python",
    "-m",
    "collectors.binance_spot_1m",
    "--mode",
    "once",
    "--symbols",
    "BTCUSDT",
    "--backfill-start",
    "2018-01-01",
    "--max-workers",
    "1",
    "--repair-gaps",
]
VNDIRECT_SERVICE = "phase-d-vn30f1m-vndirect-daily"
VNDIRECT_APPROVAL = "PRIMUS_HMD_PHASE_D_VN30F1M_VNDIRECT_DAILY_APPROVED"
VNDIRECT_COMMAND = [
    "python",
    "-m",
    "collectors.vn_derivatives",
    "sync-vndirect",
    "--resolution",
    "1d",
    "--mode",
    "once",
    "--start",
    "2017-08-10",
    "--overlap-days",
    "14",
    "--audit-phase-d",
    "--json",
]
METRICS_SERVICE = "phase-d-binance-futures-metrics-5m"
METRICS_APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_FUTURES_METRICS_5M_APPROVED"
METRICS_COMMAND = [
    "python",
    "-m",
    "collectors.binance_futures_metrics_5m",
    "--mode",
    "once",
    "--symbols",
    "BTCUSDT",
    "--start-date",
    "2020-01-01",
    "--max-workers",
    "2",
    "--no-legacy",
    "--rest-tail-days",
    "7",
    "--rest-overlap-hours",
    "24",
    "--audit-phase-d",
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
        self.assertEqual(approval["services"][SPOT_SERVICE]["dataset_id"], "crypto_binance_spot_1m")
        self.assertEqual(approval["services"][SPOT_SERVICE]["command"], SPOT_COMMAND)
        self.assertEqual(approval["services"][VNDIRECT_SERVICE]["dataset_id"], "vn30f1m_vndirect_dchart_1d")
        self.assertEqual(approval["services"][VNDIRECT_SERVICE]["command"], VNDIRECT_COMMAND)
        self.assertEqual(approval["services"][METRICS_SERVICE]["dataset_id"], "crypto_binance_futures_metrics_5m")
        self.assertEqual(approval["services"][METRICS_SERVICE]["command"], METRICS_COMMAND)
        self.assertEqual(policy["deribit"]["status"], "disabled_by_owner")

    def test_phase_d_spot_service_is_one_shot_and_has_its_own_approval(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        service = compose["services"][SPOT_SERVICE]
        self.assertEqual(service["profiles"], ["phase-d"])
        self.assertEqual(service["restart"], "no")
        self.assertEqual(service["command"], SPOT_COMMAND)
        self.assertEqual(service["pids_limit"], 256)
        self.assertEqual(service["cpus"], 1.0)
        self.assertEqual(service["mem_limit"], "1536m")
        self.assertEqual(service["environment"][SPOT_APPROVAL], f"${{{SPOT_APPROVAL}:-}}")
        self.assertNotIn(SPOT_APPROVAL, compose["x-collector-gate-environment"])
        self.assertNotIn(APPROVAL, service["environment"])

    def test_phase_d_vndirect_service_is_one_shot_and_has_its_own_approval(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        service = compose["services"][VNDIRECT_SERVICE]
        self.assertEqual(service["profiles"], ["phase-d"])
        self.assertEqual(service["restart"], "no")
        self.assertEqual(service["command"], VNDIRECT_COMMAND)
        self.assertEqual(service["pids_limit"], 128)
        self.assertEqual(service["cpus"], 0.5)
        self.assertEqual(service["mem_limit"], "512m")
        self.assertEqual(service["environment"][VNDIRECT_APPROVAL], f"${{{VNDIRECT_APPROVAL}:-}}")
        self.assertNotIn(VNDIRECT_APPROVAL, compose["x-collector-gate-environment"])
        self.assertNotIn(SPOT_APPROVAL, service["environment"])

    def test_phase_d_metrics_service_is_one_shot_and_has_its_own_approval(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        service = compose["services"][METRICS_SERVICE]
        self.assertEqual(service["profiles"], ["phase-d"])
        self.assertEqual(service["restart"], "no")
        self.assertEqual(service["command"], METRICS_COMMAND)
        self.assertEqual(service["pids_limit"], 256)
        self.assertEqual(service["cpus"], 1.0)
        self.assertEqual(service["mem_limit"], "1536m")
        self.assertEqual(service["environment"][METRICS_APPROVAL], f"${{{METRICS_APPROVAL}:-}}")
        self.assertNotIn(METRICS_APPROVAL, compose["x-collector-gate-environment"])
        self.assertNotIn(VNDIRECT_APPROVAL, service["environment"])


if __name__ == "__main__":
    unittest.main()
