import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tools.cleanup_csv_gz_after_parquet import run_cleanup


class TestParquetCleanup(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        root = Path(self.tmp.name)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        self.root = Path(os.environ["DATA_ROOT"])

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

    def _write_pair(self, *, missing_parquet_key: bool = False) -> Path:
        path = self.root / "crypto" / "test" / "1m" / "symbol=BTCUSDT" / "year=2026" / "month=07"
        path.mkdir(parents=True, exist_ok=True)
        csv_df = pd.DataFrame(
            {
                "time": ["2026-07-01 00:00:00", "2026-07-01 00:01:00"],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "close": [100.0, 101.0],
                "source": ["csv", "csv"],
            }
        )
        parquet_df = csv_df.iloc[1:].copy() if missing_parquet_key else csv_df.copy()
        csv_path = path / "part.csv.gz"
        parquet_path = path / "part.parquet"
        csv_df.to_csv(csv_path, index=False, compression="gzip")
        parquet_df["time"] = pd.to_datetime(parquet_df["time"])
        parquet_df.to_parquet(parquet_path, index=False)
        newer = csv_path.stat().st_mtime + 2
        os.utime(parquet_path, (newer, newer))
        return csv_path

    def test_dry_run_keeps_csv(self):
        csv_path = self._write_pair()
        report = run_cleanup(dataset_prefix="crypto/test/1m", workers=1, dry_run=True, sample_rows=2, allow_warnings=True)
        self.assertEqual(report["dry_run_delete_files"], 1)
        self.assertTrue(csv_path.exists())

    def test_confirm_deletes_csv_after_validation(self):
        csv_path = self._write_pair()
        report = run_cleanup(dataset_prefix="crypto/test/1m", workers=1, dry_run=False, sample_rows=2, allow_warnings=True)
        self.assertEqual(report["deleted_files"], 1)
        self.assertFalse(csv_path.exists())

    def test_validation_error_blocks_delete(self):
        csv_path = self._write_pair(missing_parquet_key=True)
        report = run_cleanup(dataset_prefix="crypto/test/1m", workers=1, dry_run=False, sample_rows=2, allow_warnings=True)
        self.assertEqual(report["blocked_files"], 1)
        self.assertTrue(csv_path.exists())


if __name__ == "__main__":
    unittest.main()
