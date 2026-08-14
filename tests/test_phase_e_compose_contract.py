from __future__ import annotations

import unittest

import yaml

from collectors.production_preflight import REPO_ROOT


PHASE_E_APPROVALS = {
    "phase-e-binance-usdm-core-perpetual-1m": "PRIMUS_HMD_PHASE_E_BINANCE_USDM_CORE_PERPETUAL_1M_APPROVED",
    "phase-e-binance-orderbook-history-1h": "PRIMUS_HMD_PHASE_E_BINANCE_ORDERBOOK_HISTORY_1H_APPROVED",
    "phase-e-vn-daily-universe-1d": "PRIMUS_HMD_PHASE_E_VN_DAILY_UNIVERSE_1D_APPROVED",
    "phase-e-vn30f1m-vndirect-1m": "PRIMUS_HMD_PHASE_E_VN30F1M_VNDIRECT_1M_APPROVED",
    "phase-e-vn30-contract-source-probe": "PRIMUS_HMD_PHASE_E_VN30_CONTRACT_SOURCE_PROBE_APPROVED",
}
PHASE_E_TAIL_APPROVALS = {
    "crypto-1m-core-live": "PRIMUS_HMD_STAGED_CRYPTO_CORE_1M_APPROVED",
    "binance-usdm-quarterly-next-1m": "PRIMUS_HMD_STAGED_BINANCE_USDM_QUARTERLY_NEXT_1M_APPROVED",
    "binance-orderbook-expanded-1h": "PRIMUS_HMD_STAGED_BINANCE_ORDERBOOK_EXPANDED_1H_APPROVED",
    "vn30f1m-vndirect-1m": "PRIMUS_HMD_STAGED_VN30F1M_VNDIRECT_1M_APPROVED",
}


class TestPhaseEComposeContract(unittest.TestCase):
    def setUp(self) -> None:
        self.compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        self.policy = yaml.safe_load((REPO_ROOT / "configs" / "primus_hmd_phase_e.yml").read_text(encoding="utf-8"))

    def test_phase_e_one_shots_match_policy_and_are_service_scoped(self) -> None:
        approval = self.policy["phase_e_approval"]
        self.assertEqual(approval["status"], "approved")
        self.assertEqual(approval["runtime_profile"]["max_concurrent_historical_jobs"], 1)
        common = self.compose["x-collector-gate-environment"]
        for service_name, approval_name in PHASE_E_APPROVALS.items():
            with self.subTest(service=service_name):
                service = self.compose["services"][service_name]
                self.assertEqual(service["profiles"], ["phase-e"])
                self.assertEqual(service["restart"], "no")
                self.assertEqual(service["command"], approval["services"][service_name]["command"])
                self.assertEqual(service["environment"][approval_name], f"${{{approval_name}:-}}")
                self.assertNotIn(approval_name, common)
                for candidate_name, candidate in self.compose["services"].items():
                    if candidate_name != service_name:
                        self.assertNotIn(approval_name, candidate.get("environment", {}), candidate_name)

    def test_phase_e_tails_match_policy_and_stay_independently_scoped(self) -> None:
        approval = self.policy["phase_e_approval"]
        for service_name, approval_name in PHASE_E_TAIL_APPROVALS.items():
            with self.subTest(service=service_name):
                service = self.compose["services"][service_name]
                self.assertEqual(service["command"], approval["staged_live_tails"][service_name]["command"])
                self.assertEqual(service["environment"][approval_name], f"${{{approval_name}:-}}")
                self.assertEqual(service["pids_limit"], 128)
                self.assertEqual(service["cpus"], 0.5)
                self.assertEqual(service["mem_limit"], "512m")

    def test_phase_e_keeps_contract_publish_and_deribit_out_of_scope(self) -> None:
        services = self.policy["phase_e_approval"]["services"]
        self.assertIn("phase-e-vn30-contract-source-probe", services)
        self.assertFalse(any("contract-backfill" in name for name in services))
        self.assertEqual(self.policy["deribit"]["status"], "disabled_by_owner")


if __name__ == "__main__":
    unittest.main()
