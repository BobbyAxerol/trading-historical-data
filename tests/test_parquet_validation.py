import os
import tempfile
import time
import unittest
from pathlib import Path

import pandas as pd

from tools.validate_parquet_migration import run_validation


class TestParquetMigrationValidation(unittest.TestCase):
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

    def _write_pair(self, parquet_extra: pd.DataFrame | None = None, drop_first_parquet_row: bool = False) -> None:
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
        parquet_df = csv_df.copy()
        if drop_first_parquet_row:
            parquet_df = parquet_df.iloc[1:].reset_index(drop=True)
        if parquet_extra is not None:
            parquet_df = pd.concat([parquet_df, parquet_extra], ignore_index=True)
        csv_df.to_csv(path / "part.csv.gz", index=False, compression="gzip")
        parquet_df["time"] = pd.to_datetime(parquet_df["time"])
        parquet_df.to_parquet(path / "part.parquet", index=False)

    def _overwrite_parquet_close(self, close_value: float) -> None:
        path = self.root / "crypto" / "test" / "1m" / "symbol=BTCUSDT" / "year=2026" / "month=07" / "part.parquet"
        df = pd.read_parquet(path)
        df.loc[df["time"] == pd.Timestamp("2026-07-01 00:00:00"), "close"] = close_value
        df.to_parquet(path, index=False)
        newer = time.time() + 2
        os.utime(path, (newer, newer))

    def test_exact_partition_passes(self):
        self._write_pair()
        report = run_validation(dataset_prefix="crypto/test/1m", workers=1, sample_rows=2)
        self.assertEqual(report["error_files"], 0)
        self.assertEqual(report["ok_files"], 1)

    def test_parquet_superset_passes_for_stale_csv(self):
        extra = pd.DataFrame(
            {
                "time": ["2026-07-01 00:02:00"],
                "symbol": ["BTCUSDT"],
                "close": [102.0],
                "source": ["parquet"],
            }
        )
        self._write_pair(parquet_extra=extra)
        report = run_validation(dataset_prefix="crypto/test/1m", workers=1, sample_rows=2)
        self.assertEqual(report["error_files"], 0)
        self.assertEqual(report["row_delta"], 1)

    def test_newer_parquet_value_mismatch_is_warning(self):
        self._write_pair()
        self._overwrite_parquet_close(999.0)
        report = run_validation(dataset_prefix="crypto/test/1m", workers=1, sample_rows=2)
        self.assertEqual(report["error_files"], 0)
        self.assertEqual(report["warning_files"], 1)
        self.assertIn("sample_value_mismatch_parquet_newer_than_csv", report["warnings"][0]["warnings"])

    def test_missing_csv_key_fails(self):
        self._write_pair(drop_first_parquet_row=True)
        report = run_validation(dataset_prefix="crypto/test/1m", workers=1, sample_rows=2)
        self.assertEqual(report["error_files"], 1)
        self.assertIn("csv_keys_missing_in_parquet", report["errors"][0]["errors"])


if __name__ == "__main__":
    unittest.main()
