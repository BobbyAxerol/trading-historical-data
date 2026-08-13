from __future__ import annotations

import unittest

import yaml

from collectors.production_preflight import REPO_ROOT


STAGED_TAILS = {
    "crypto-1m-live": {
        "approval": "PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED",
        "dataset_id": "crypto_binance_futures_1m",
        "command": ["python", "-m", "collectors.crypto_1m", "--mode", "live", "--symbols", "BTCUSDT"],
    },
    "binance-usdm-quarterly-1m": {
        "approval": "PRIMUS_HMD_STAGED_BINANCE_USDM_QUARTERLY_1M_APPROVED",
        "dataset_id": "crypto_binance_usdm_quarterly_1m",
        "command": ["python", "-m", "collectors.binance_usdm_quarterly_1m", "--mode", "live", "--pairs", "BTCUSDT", "--symbols", "BTCUSDT_260925", "--no-archive-discovery", "--no-monthly", "--no-daily", "--sleep", "21600"],
    },
    "binance-spot-1m": {
        "approval": "PRIMUS_HMD_STAGED_BINANCE_SPOT_1M_APPROVED",
        "dataset_id": "crypto_binance_spot_1m",
        "command": ["python", "-m", "collectors.binance_spot_1m", "--mode", "live", "--symbols", "BTCUSDT", "--no-monthly", "--no-daily", "--no-validate", "--sleep", "75"],
    },
    "binance-orderbook-snapshot-1h": {
        "approval": "PRIMUS_HMD_STAGED_BINANCE_ORDERBOOK_SNAPSHOT_1H_APPROVED",
        "dataset_id": "crypto_binance_orderbook_snapshot_1h",
        "command": ["python", "-m", "collectors.binance_orderbook_snapshot_1h", "--mode", "live", "--symbols", "BTCUSDT", "--no-vision", "--no-validate", "--sleep", "3600"],
    },
    "binance-futures-metrics-5m": {
        "approval": "PRIMUS_HMD_STAGED_BINANCE_FUTURES_METRICS_5M_APPROVED",
        "dataset_id": "crypto_binance_futures_metrics_5m",
        "command": ["python", "-m", "collectors.binance_futures_metrics_5m", "--mode", "live", "--symbols", "BTCUSDT", "--no-legacy", "--no-vision", "--rest-tail-days", "1", "--rest-overlap-hours", "1", "--no-validate", "--sleep", "21600"],
    },
    "vn-daily": {
        "approval": "PRIMUS_HMD_STAGED_VN_DAILY_APPROVED",
        "dataset_id": "vn_equity_1d",
        "command": ["python", "-m", "collectors.vn_daily", "--mode", "live", "--symbols", "FPT", "--schedule", "16:30", "--skip-derived"],
    },
    "vn30f1m-vndirect": {
        "approval": "PRIMUS_HMD_STAGED_VN30F1M_VNDIRECT_APPROVED",
        "dataset_id": "vn30f1m_vndirect",
        "command": ["python", "-m", "collectors.vn_derivatives", "sync-vndirect", "--resolution", "1d", "--mode", "live", "--schedule", "16:30", "--overlap-days", "14"],
    },
}

class TestB0ComposeContract(unittest.TestCase):
    def test_collector_vendor_home_is_ephemeral_and_not_a_runtime_data_mount(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
        environment = compose["x-collector-gate-environment"]
        expected_home = "/tmp/primus-hmd-home"
        self.assertEqual(environment["HOME"], expected_home)
        self.assertEqual(environment["XDG_CONFIG_HOME"], f"{expected_home}/.config")
        self.assertEqual(environment["XDG_CACHE_HOME"], f"{expected_home}/.cache")
        self.assertEqual(environment["MPLCONFIGDIR"], f"{expected_home}/.config/matplotlib")

    def test_staged_tail_approvals_are_service_scoped(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
        common_environment = compose["x-collector-gate-environment"]
        self.assertNotIn("PRIMUS_HMD_B0_APPROVED", common_environment)
        for spec in STAGED_TAILS.values():
            self.assertNotIn(spec["approval"], common_environment)

        services = compose["services"]
        approval_names = {spec["approval"] for spec in STAGED_TAILS.values()}
        for service_name, spec in STAGED_TAILS.items():
            environment = services[service_name]["environment"]
            self.assertEqual(environment[spec["approval"]], f"${{{spec['approval']}:-}}")
            self.assertEqual(services[service_name]["command"], spec["command"])
            self.assertEqual(services[service_name]["pids_limit"], 128)
            self.assertEqual(services[service_name]["cpus"], 0.5)
            self.assertEqual(services[service_name]["mem_limit"], "512m")
            present_approvals = {name for name in approval_names if name in environment}
            self.assertEqual(present_approvals, {spec["approval"]}, service_name)

        for service_name, service in services.items():
            if service_name in STAGED_TAILS:
                continue
            environment = service.get("environment", {})
            for approval_name in approval_names:
                self.assertNotIn(approval_name, environment, service_name)

    def test_staged_tail_policy_matches_the_compose_contract(self) -> None:
        compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
        policy = yaml.safe_load((REPO_ROOT / "configs" / "primus_hmd_b0.yml").read_text())
        approval = policy["staged_non_deribit_tail_approval"]
        profile = approval["runtime_profile"]

        self.assertEqual(approval["status"], "approved")
        self.assertEqual(profile["first_cycles"], "sequential")
        self.assertEqual(profile["max_resident_tail_services"], len(STAGED_TAILS))
        self.assertEqual(profile["heavy_historical_jobs"], "prohibited")
        self.assertEqual(profile["per_service_limits"], {"cpus": 0.5, "memory_mib": 512, "pids": 128})
        self.assertEqual(set(approval["services"]), set(STAGED_TAILS))

        for service_name, spec in STAGED_TAILS.items():
            self.assertEqual(approval["services"][service_name]["dataset_id"], spec["dataset_id"])
            self.assertEqual(approval["services"][service_name]["command"], spec["command"])
            self.assertEqual(compose["services"][service_name]["command"], spec["command"])


if __name__ == "__main__":
    unittest.main()
