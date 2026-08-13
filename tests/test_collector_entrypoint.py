from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from collectors.production_preflight import REPO_ROOT


ENTRYPOINT = REPO_ROOT / "docker" / "entrypoint.sh"
STAGED_TAIL_COMMANDS = {
    "PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED": [
        "python",
        "-m",
        "collectors.crypto_1m",
        "--mode",
        "live",
        "--symbols",
        "BTCUSDT",
    ],
    "PRIMUS_HMD_STAGED_BINANCE_USDM_QUARTERLY_1M_APPROVED": [
        "python",
        "-m",
        "collectors.binance_usdm_quarterly_1m",
        "--mode",
        "live",
        "--pairs",
        "BTCUSDT",
        "--symbols",
        "BTCUSDT_260925",
        "--no-archive-discovery",
        "--no-monthly",
        "--no-daily",
        "--sleep",
        "21600",
    ],
    "PRIMUS_HMD_STAGED_BINANCE_SPOT_1M_APPROVED": [
        "python",
        "-m",
        "collectors.binance_spot_1m",
        "--mode",
        "live",
        "--symbols",
        "BTCUSDT",
        "--no-monthly",
        "--no-daily",
        "--no-validate",
        "--sleep",
        "75",
    ],
    "PRIMUS_HMD_STAGED_BINANCE_ORDERBOOK_SNAPSHOT_1H_APPROVED": [
        "python",
        "-m",
        "collectors.binance_orderbook_snapshot_1h",
        "--mode",
        "live",
        "--symbols",
        "BTCUSDT",
        "--no-vision",
        "--no-validate",
        "--sleep",
        "3600",
    ],
    "PRIMUS_HMD_STAGED_BINANCE_FUTURES_METRICS_5M_APPROVED": [
        "python",
        "-m",
        "collectors.binance_futures_metrics_5m",
        "--mode",
        "live",
        "--symbols",
        "BTCUSDT",
        "--no-legacy",
        "--no-vision",
        "--rest-tail-days",
        "1",
        "--rest-overlap-hours",
        "1",
        "--no-validate",
        "--sleep",
        "21600",
    ],
    "PRIMUS_HMD_STAGED_VN_DAILY_APPROVED": [
        "python",
        "-m",
        "collectors.vn_daily",
        "--mode",
        "live",
        "--symbols",
        "FPT",
        "--schedule",
        "16:30",
        "--skip-derived",
    ],
    "PRIMUS_HMD_STAGED_VN30F1M_VNDIRECT_APPROVED": [
        "python",
        "-m",
        "collectors.vn_derivatives",
        "sync-vndirect",
        "--resolution",
        "1d",
        "--mode",
        "live",
        "--schedule",
        "16:30",
        "--overlap-days",
        "14",
    ],
}
STAGED_CRYPTO_COMMAND = STAGED_TAIL_COMMANDS["PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED"]
PHASE_D_APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_USDM_PERPETUAL_1M_APPROVED"
PHASE_D_COMMAND = [
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
PHASE_D_SPOT_APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_SPOT_1M_APPROVED"
PHASE_D_SPOT_COMMAND = [
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
PHASE_D_VNDIRECT_APPROVAL = "PRIMUS_HMD_PHASE_D_VN30F1M_VNDIRECT_DAILY_APPROVED"
PHASE_D_VNDIRECT_COMMAND = [
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
PHASE_D_METRICS_APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_FUTURES_METRICS_5M_APPROVED"
PHASE_D_METRICS_COMMAND = [
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
PHASE_D_QUARTERLY_APPROVAL = "PRIMUS_HMD_PHASE_D_BINANCE_USDM_QUARTERLY_1M_APPROVED"
PHASE_D_QUARTERLY_COMMAND = [
    "python",
    "-m",
    "collectors.binance_usdm_quarterly_1m",
    "--mode",
    "once",
    "--pairs",
    "BTCUSDT",
    "--start-month",
    "2021-02",
    "--repair-gaps",
    "--max-gap-minutes",
    "5",
    "--audit-phase-d",
]


class TestCollectorEntrypoint(unittest.TestCase):
    def run_entrypoint(self, *arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as tmp:
            fake_python = Path(tmp) / "python"
            fake_python.write_text("#!/bin/sh\nprintf 'fake-python:%s\\n' \"$*\"\n", encoding="utf-8")
            fake_python.chmod(0o755)
            env = dict(os.environ)
            env["PATH"] = f"{tmp}{os.pathsep}{env['PATH']}"
            for name in [
                "PRIMUS_HMD_B0_APPROVED",
                "PRIMUS_HMD_B0_SEED_APPROVED",
                "PRIMUS_HMD_B0_SEED_RUNNER",
                PHASE_D_APPROVAL,
                PHASE_D_SPOT_APPROVAL,
                PHASE_D_VNDIRECT_APPROVAL,
                PHASE_D_METRICS_APPROVAL,
                PHASE_D_QUARTERLY_APPROVAL,
                *STAGED_TAIL_COMMANDS,
            ]:
                env.pop(name, None)
            env.update(environment or {})
            return subprocess.run(
                [str(ENTRYPOINT), *arguments],
                check=False,
                capture_output=True,
                text=True,
                env=env,
            )

    def test_legacy_global_approval_cannot_start_a_writer(self) -> None:
        result = self.run_entrypoint(
            *STAGED_CRYPTO_COMMAND,
            environment={"PRIMUS_HMD_B0_APPROVED": "approved"},
        )

        self.assertEqual(result.returncode, 64)
        self.assertIn("no service-scoped staged authorization", result.stderr)

    def test_each_staged_approval_accepts_only_its_exact_tail_command(self) -> None:
        for approval_name, command in STAGED_TAIL_COMMANDS.items():
            with self.subTest(approval_name=approval_name):
                approved = {approval_name: "approved"}
                permitted = self.run_entrypoint(*command, environment=approved)
                self.assertEqual(permitted.returncode, 0)
                self.assertEqual(permitted.stdout, f"fake-python:{' '.join(command[1:])}\n")

                extra_argument = self.run_entrypoint(*command, "--unreviewed", environment=approved)
                self.assertEqual(extra_argument.returncode, 64)

        approved = {"PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED": "approved"}

        missing_symbol_scope = self.run_entrypoint(
            "python",
            "-m",
            "collectors.crypto_1m",
            "--mode",
            "live",
            environment=approved,
        )
        self.assertEqual(missing_symbol_scope.returncode, 64)

        wrong_symbol_scope = self.run_entrypoint(
            "python",
            "-m",
            "collectors.crypto_1m",
            "--mode",
            "live",
            "--symbols",
            "ETHUSDT",
            environment=approved,
        )
        self.assertEqual(wrong_symbol_scope.returncode, 64)

        wrong_module = self.run_entrypoint(
            "python",
            "-m",
            "collectors.binance_spot_1m",
            "--mode",
            "live",
            environment=approved,
        )
        self.assertEqual(wrong_module.returncode, 64)

        spot_with_crypto_approval = self.run_entrypoint(
            *STAGED_TAIL_COMMANDS["PRIMUS_HMD_STAGED_BINANCE_SPOT_1M_APPROVED"],
            environment=approved,
        )
        self.assertEqual(spot_with_crypto_approval.returncode, 64)

    def test_bounded_seed_exception_remains_exact(self) -> None:
        approved = {
            "PRIMUS_HMD_B0_SEED_APPROVED": "approved",
            "PRIMUS_HMD_B0_SEED_RUNNER": "bounded-v1",
        }
        permitted = self.run_entrypoint("python", "-m", "collectors.b0_bounded_seed", environment=approved)
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, "fake-python:-m collectors.b0_bounded_seed\n")

        altered = self.run_entrypoint(
            "python",
            "-m",
            "collectors.b0_bounded_seed",
            "--unreviewed",
            environment=approved,
        )
        self.assertEqual(altered.returncode, 64)

    def test_phase_d_approval_accepts_only_its_exact_archive_command(self) -> None:
        permitted = self.run_entrypoint(*PHASE_D_COMMAND, environment={PHASE_D_APPROVAL: "approved"})
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, f"fake-python:{' '.join(PHASE_D_COMMAND[1:])}\n")

        changed_symbol = list(PHASE_D_COMMAND)
        changed_symbol[6] = "ETHUSDT"
        rejected_symbol = self.run_entrypoint(*changed_symbol, environment={PHASE_D_APPROVAL: "approved"})
        self.assertEqual(rejected_symbol.returncode, 64)

        rejected_extra = self.run_entrypoint(*PHASE_D_COMMAND, "--no-validate", environment={PHASE_D_APPROVAL: "approved"})
        self.assertEqual(rejected_extra.returncode, 64)

        staged_only = self.run_entrypoint(*PHASE_D_COMMAND, environment={"PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED": "approved"})
        self.assertEqual(staged_only.returncode, 64)

    def test_phase_d_spot_approval_accepts_only_its_exact_archive_command(self) -> None:
        permitted = self.run_entrypoint(*PHASE_D_SPOT_COMMAND, environment={PHASE_D_SPOT_APPROVAL: "approved"})
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, f"fake-python:{' '.join(PHASE_D_SPOT_COMMAND[1:])}\n")

        changed_start = list(PHASE_D_SPOT_COMMAND)
        changed_start[8] = "2019-01-01"
        rejected_start = self.run_entrypoint(*changed_start, environment={PHASE_D_SPOT_APPROVAL: "approved"})
        self.assertEqual(rejected_start.returncode, 64)

        wrong_phase_approval = self.run_entrypoint(*PHASE_D_SPOT_COMMAND, environment={PHASE_D_APPROVAL: "approved"})
        self.assertEqual(wrong_phase_approval.returncode, 64)

    def test_phase_d_vndirect_approval_accepts_only_its_exact_history_command(self) -> None:
        permitted = self.run_entrypoint(*PHASE_D_VNDIRECT_COMMAND, environment={PHASE_D_VNDIRECT_APPROVAL: "approved"})
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, f"fake-python:{' '.join(PHASE_D_VNDIRECT_COMMAND[1:])}\n")

        changed_start = list(PHASE_D_VNDIRECT_COMMAND)
        changed_start[9] = "2018-01-01"
        rejected_start = self.run_entrypoint(*changed_start, environment={PHASE_D_VNDIRECT_APPROVAL: "approved"})
        self.assertEqual(rejected_start.returncode, 64)

        wrong_phase_approval = self.run_entrypoint(*PHASE_D_VNDIRECT_COMMAND, environment={PHASE_D_SPOT_APPROVAL: "approved"})
        self.assertEqual(wrong_phase_approval.returncode, 64)

    def test_phase_d_metrics_approval_accepts_only_its_exact_history_command(self) -> None:
        permitted = self.run_entrypoint(*PHASE_D_METRICS_COMMAND, environment={PHASE_D_METRICS_APPROVAL: "approved"})
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, f"fake-python:{' '.join(PHASE_D_METRICS_COMMAND[1:])}\n")

        changed_workers = list(PHASE_D_METRICS_COMMAND)
        changed_workers[10] = "8"
        rejected_workers = self.run_entrypoint(*changed_workers, environment={PHASE_D_METRICS_APPROVAL: "approved"})
        self.assertEqual(rejected_workers.returncode, 64)

        wrong_phase_approval = self.run_entrypoint(*PHASE_D_METRICS_COMMAND, environment={PHASE_D_VNDIRECT_APPROVAL: "approved"})
        self.assertEqual(wrong_phase_approval.returncode, 64)

    def test_phase_d_quarterly_approval_accepts_only_its_exact_history_command(self) -> None:
        permitted = self.run_entrypoint(*PHASE_D_QUARTERLY_COMMAND, environment={PHASE_D_QUARTERLY_APPROVAL: "approved"})
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, f"fake-python:{' '.join(PHASE_D_QUARTERLY_COMMAND[1:])}\n")

        changed_pair = list(PHASE_D_QUARTERLY_COMMAND)
        changed_pair[7] = "ETHUSDT"
        rejected_pair = self.run_entrypoint(*changed_pair, environment={PHASE_D_QUARTERLY_APPROVAL: "approved"})
        self.assertEqual(rejected_pair.returncode, 64)

        wrong_phase_approval = self.run_entrypoint(*PHASE_D_QUARTERLY_COMMAND, environment={PHASE_D_METRICS_APPROVAL: "approved"})
        self.assertEqual(wrong_phase_approval.returncode, 64)


if __name__ == "__main__":
    unittest.main()
