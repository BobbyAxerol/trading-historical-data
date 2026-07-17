import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tools.convert_csv_gz_to_parquet import run_conversion


class TestCsvGzToParquetConverter(unittest.TestCase):
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

    def _write_csv_part(self, relative_dir: str) -> Path:
        path = self.root / relative_dir / "part.csv.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(
            {
                "time": ["2026-07-01 00:00:00", "2026-07-01 00:01:00"],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "close": [100.0, 101.0],
                "source": ["test", "test"],
            }
        )
        df.to_csv(path, index=False, compression="gzip")
        return path

    def test_dry_run_does_not_write_parquet(self):
        csv_path = self._write_csv_part("crypto/test/1m/symbol=BTCUSDT/year=2026/month=07")
        report = run_conversion(dataset_prefix="crypto/test/1m", workers=2, overwrite=False, dry_run=True, compression="zstd")
        self.assertEqual(report["total_files"], 1)
        self.assertEqual(report["dry_run_files"], 1)
        self.assertFalse(csv_path.with_name("part.parquet").exists())

    def test_convert_and_skip_up_to_date(self):
        csv_path = self._write_csv_part("crypto/test/1m/symbol=BTCUSDT/year=2026/month=07")
        report = run_conversion(dataset_prefix="crypto/test/1m", workers=1, overwrite=False, dry_run=False, compression="zstd")
        parquet_path = csv_path.with_name("part.parquet")
        self.assertEqual(report["converted_files"], 1)
        self.assertTrue(parquet_path.exists())

        loaded = pd.read_parquet(parquet_path)
        self.assertEqual(list(loaded.columns), ["time", "symbol", "close", "source"])
        self.assertEqual(len(loaded), 2)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(loaded["time"]))

        second = run_conversion(dataset_prefix="crypto/test/1m", workers=1, overwrite=False, dry_run=False, compression="zstd")
        self.assertEqual(second["skipped_files"], 1)
        self.assertEqual(second["converted_files"], 0)
        report_path = Path(os.environ["STATE_ROOT"]) / "parquet_migration_report.json"
        self.assertTrue(report_path.exists())


if __name__ == "__main__":
    unittest.main()
