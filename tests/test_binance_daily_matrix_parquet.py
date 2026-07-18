import os
import tempfile
import time
import unittest
from pathlib import Path

import pandas as pd

from tools.migrate_binance_daily_matrix_parquet import run_migration


class TestBinanceDailyMatrixParquet(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        root = Path(self.tmp.name)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        self.matrix_dir = Path(os.environ["DATA_ROOT"]) / "crypto" / "binance_daily_matrix"
        self.matrix_dir.mkdir(parents=True, exist_ok=True)

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

    def _write_csv_matrices(self) -> None:
        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        for feature in ("open", "high", "low", "close", "volume"):
            df = pd.DataFrame({"BTCUSDT": [1, 2], "ETHUSDT": [3, 4]}, index=index)
            if feature != "volume":
                df = df.astype("float64")
            path = self.matrix_dir / f"{feature}.csv.gz"
            df.to_csv(path, compression="gzip")

    def test_convert_csv_matrices_to_parquet(self):
        self._write_csv_matrices()
        report = run_migration(dry_run=False, overwrite=False, cleanup_csv=False, confirm=False)
        self.assertEqual(report["error_features"], 0)
        self.assertEqual(report["converted_features"], 5)
        for feature in ("open", "high", "low", "close", "volume"):
            self.assertTrue((self.matrix_dir / f"{feature}.parquet").exists())
            self.assertTrue((self.matrix_dir / f"{feature}.csv.gz").exists())

    def test_cleanup_removes_csv_after_validation(self):
        self._write_csv_matrices()
        run_migration(dry_run=False, overwrite=False, cleanup_csv=False, confirm=False)
        report = run_migration(dry_run=False, overwrite=False, cleanup_csv=True, confirm=True)
        self.assertEqual(report["error_features"], 0)
        self.assertEqual(report["deleted_csv_features"], 5)
        self.assertFalse(any(self.matrix_dir.glob("*.csv.gz")))

    def test_newer_csv_reconverts_before_cleanup(self):
        self._write_csv_matrices()
        run_migration(dry_run=False, overwrite=False, cleanup_csv=False, confirm=False)
        path = self.matrix_dir / "close.csv.gz"
        df = pd.read_csv(path, compression="gzip", index_col=0)
        df.loc["2026-01-02", "BTCUSDT"] = 99.0
        df.to_csv(path, compression="gzip")
        newer = time.time() + 2
        os.utime(path, (newer, newer))

        report = run_migration(dry_run=False, overwrite=False, cleanup_csv=True, confirm=True)
        self.assertEqual(report["error_features"], 0)
        loaded = pd.read_parquet(self.matrix_dir / "close.parquet")
        self.assertEqual(float(loaded.loc[pd.Timestamp("2026-01-02"), "BTCUSDT"]), 99.0)
        self.assertFalse(path.exists())

    def test_vn_daily_matrix_dataset_cleanup(self):
        self.matrix_dir = Path(os.environ["DATA_ROOT"]) / "vn" / "equity" / "daily_matrix"
        self.matrix_dir.mkdir(parents=True, exist_ok=True)
        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        for feature in ("open", "high", "low", "close", "volume"):
            pd.DataFrame({"FPT": [1, 2], "VCB": [3, 4]}, index=index).to_csv(
                self.matrix_dir / f"{feature}.csv.gz",
                compression="gzip",
            )

        run_migration(dry_run=False, overwrite=False, cleanup_csv=False, confirm=False, dataset="vn_daily_matrix")
        report = run_migration(dry_run=False, overwrite=False, cleanup_csv=True, confirm=True, dataset="vn_daily_matrix")

        self.assertEqual(report["dataset"], "vn_daily_matrix")
        self.assertEqual(report["error_features"], 0)
        self.assertEqual(report["deleted_csv_features"], 5)
        self.assertFalse(any(self.matrix_dir.glob("*.csv.gz")))


if __name__ == "__main__":
    unittest.main()
