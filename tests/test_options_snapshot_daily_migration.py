import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tools.migrate_options_snapshot_daily import run_migration


class TestOptionsSnapshotDailyMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        root = Path(self.tmp.name)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        self.month_dir = (
            Path(os.environ["DATA_ROOT"])
            / "options"
            / "binance"
            / "snapshot_5m"
            / "underlying=BTC"
            / "year=2026"
            / "month=07"
        )
        self.month_dir.mkdir(parents=True, exist_ok=True)
        self.monthly_path = self.month_dir / "part.parquet"
        pd.DataFrame(
            {
                "snapshot_time": pd.to_datetime(["2026-07-01 00:00:00", "2026-07-01 00:05:00", "2026-07-02 00:00:00"]),
                "underlying": ["BTC", "BTC", "BTC"],
                "symbol": ["BTC-260925-100000-C", "BTC-260925-100000-P", "BTC-260925-100000-C"],
                "mark_price": [1.0, 2.0, 3.0],
            }
        ).to_parquet(self.monthly_path, index=False, engine="pyarrow", compression="zstd")

    def tearDown(self):
        if self.old_data_root is None:
            os.environ.pop("DATA_ROOT", None)
        else:
            os.environ["DATA_ROOT"] = self.old_data_root
        if self.old_state_root is None:
            os.environ.pop("STATE_ROOT", None)
        else:
            os.environ["STATE_ROOT"] = self.old_state_root
        self.tmp.cleanup()

    def test_split_monthly_options_to_daily_and_cleanup(self):
        dry = run_migration(dry_run=True, cleanup_monthly=False, confirm=False)
        self.assertEqual(dry["total_monthly_files"], 1)
        self.assertEqual(dry["rows"], 3)

        report = run_migration(dry_run=False, cleanup_monthly=False, confirm=False)
        self.assertEqual(report["error_files"], 0)
        self.assertEqual(report["converted_files"], 1)
        day_1 = self.month_dir / "day=01" / "part.parquet"
        day_2 = self.month_dir / "day=02" / "part.parquet"
        self.assertTrue(day_1.exists())
        self.assertTrue(day_2.exists())
        self.assertTrue(self.monthly_path.exists())

        cleanup = run_migration(dry_run=False, cleanup_monthly=True, confirm=True)
        self.assertEqual(cleanup["error_files"], 0)
        self.assertEqual(cleanup["deleted_monthly_files"], 1)
        self.assertFalse(self.monthly_path.exists())
        self.assertEqual(len(pd.read_parquet(day_1)), 2)
        self.assertEqual(len(pd.read_parquet(day_2)), 1)


if __name__ == "__main__":
    unittest.main()
