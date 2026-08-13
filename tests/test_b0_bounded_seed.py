from __future__ import annotations

import unittest

from collectors import b0_bounded_seed
from collectors.b0_seed_evidence import SEED_IDS


class TestB0BoundedSeed(unittest.TestCase):
    def test_fixed_plan_has_no_broad_or_deribit_command(self) -> None:
        plan, steps = b0_bounded_seed._fixed_steps()
        self.assertEqual(tuple(seed_id for seed_id, _ in steps), SEED_IDS)
        self.assertIn("Deribit backfill", plan["prohibited"])
        rendered = " ".join(" ".join(command) for _, command in steps).lower()
        self.assertNotIn("deribit", rendered)
        self.assertIn("--no-archive-discovery", rendered)
        self.assertIn("--skip-derived", rendered)
        self.assertNotIn("--mode live", rendered)


if __name__ == "__main__":
    unittest.main()
