from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from collectors.production_preflight import REPO_ROOT


ENTRYPOINT = REPO_ROOT / "docker" / "entrypoint.sh"
STAGED_CRYPTO_COMMAND = [
    "python",
    "-m",
    "collectors.crypto_1m",
    "--mode",
    "live",
    "--symbols",
    "BTCUSDT",
]


class TestCollectorEntrypoint(unittest.TestCase):
    def run_entrypoint(self, *arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as tmp:
            fake_python = Path(tmp) / "python"
            fake_python.write_text("#!/bin/sh\nprintf 'fake-python:%s\\n' \"$*\"\n", encoding="utf-8")
            fake_python.chmod(0o755)
            env = dict(os.environ)
            env["PATH"] = f"{tmp}{os.pathsep}{env['PATH']}"
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

    def test_staged_approval_accepts_only_exact_crypto_live_command(self) -> None:
        approved = {"PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED": "approved"}
        permitted = self.run_entrypoint(*STAGED_CRYPTO_COMMAND, environment=approved)
        self.assertEqual(permitted.returncode, 0)
        self.assertEqual(permitted.stdout, "fake-python:-m collectors.crypto_1m --mode live --symbols BTCUSDT\n")

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

        extra_argument = self.run_entrypoint(*STAGED_CRYPTO_COMMAND, "--repair-gaps", environment=approved)
        self.assertEqual(extra_argument.returncode, 64)

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


if __name__ == "__main__":
    unittest.main()
