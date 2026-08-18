from __future__ import annotations

import unittest

import yaml

from collectors.production_preflight import REPO_ROOT


PHASE_F_APPROVALS = {
    "phase-f-vn30f1m-dnse-probe": "PRIMUS_HMD_PHASE_F_VN30F1M_DNSE_PROBE_APPROVED",
    "phase-f-vn30f1m-dnse-backfill": "PRIMUS_HMD_PHASE_F_VN30F1M_DNSE_BACKFILL_APPROVED",
    "phase-f-vn30f1m-csv-bridge": "PRIMUS_HMD_PHASE_F_VN30F1M_CSV_BRIDGE_APPROVED",
}


class TestPhaseFComposeContract(unittest.TestCase):
    def setUp(self) -> None:
        self.compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        self.policy = yaml.safe_load((REPO_ROOT / "configs" / "primus_hmd_phase_f.yml").read_text(encoding="utf-8"))

    def test_phase_f_services_match_the_owner_approved_policy(self) -> None:
        approval = self.policy["phase_f_approval"]
        self.assertEqual(approval["status"], "approved")
        self.assertEqual(approval["runtime_profile"]["max_concurrent_historical_jobs"], 1)
        common = self.compose["x-collector-gate-environment"]
        for service_name, approval_name in PHASE_F_APPROVALS.items():
            with self.subTest(service=service_name):
                service = self.compose["services"][service_name]
                self.assertEqual(service["profiles"], ["phase-f"])
                self.assertEqual(service["restart"], "no")
                self.assertEqual(service["command"], approval["services"][service_name]["command"])
                self.assertEqual(service["environment"][approval_name], f"${{{approval_name}:-}}")
                self.assertNotIn(approval_name, common)
                self.assertEqual(service["pids_limit"], 128)
                self.assertEqual(service["cpus"], 0.5)
                self.assertEqual(service["mem_limit"], "768m")

        bridge_volumes = self.compose["services"]["phase-f-vn30f1m-csv-bridge"]["volumes"]
        self.assertTrue(any(str(item).endswith("/storage:/app/storage") for item in bridge_volumes))
        self.assertTrue(any(str(item).endswith("/state:/app/state") for item in bridge_volumes))
        self.assertTrue(any(str(item).endswith("/logs:/app/logs") for item in bridge_volumes))
        self.assertTrue(any(str(item).endswith("/releases:/app/releases:ro") for item in bridge_volumes))
        self.assertTrue(any(str(item).endswith("/state/migration_inputs:/input:ro") for item in bridge_volumes))

    def test_phase_f_keeps_reader_promotion_and_deribit_out_of_scope(self) -> None:
        scope = self.policy["phase_f_approval"]["scope"].lower()
        self.assertIn("consumer release promotion", scope)
        self.assertEqual(self.policy["deribit"]["status"], "disabled_by_owner")


if __name__ == "__main__":
    unittest.main()
