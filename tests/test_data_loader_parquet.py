import os
import tempfile
import time
import unittest
from pathlib import Path

import pandas as pd

import data_loader
from data_loader import CryptoDailyMatrix, MarketDataLoaderBase, VNDailyMatrix


class TempParquetLoader(MarketDataLoaderBase):
    DATASET_NAME = "temp_parquet_loader"
    NEW_PATH_PARTS = ("crypto", "test", "1m")
    TZ_INFO = "UTC"


class TestDataLoaderParquetFirst(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_storage_dir = data_loader.STORAGE_DIR
        data_loader.STORAGE_DIR = Path(self.tmp.name) / "storage"
        self.part_dir = (
            data_loader.STORAGE_DIR
            / "crypto"
            / "test"
            / "1m"
            / "symbol=BTCUSDT"
            / "year=2026"
            / "month=07"
        )
        self.part_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        self.tmp.cleanup()

    def _write_csv(self, close: float) -> Path:
        path = self.part_dir / "part.csv.gz"
        df = pd.DataFrame(
            {
                "time": ["2026-07-01 00:00:00"],
                "symbol": ["BTCUSDT"],
                "open": [close],
                "high": [close],
                "low": [close],
                "close": [close],
                "volume": [1],
            }
        )
        df.to_csv(path, index=False, compression="gzip")
        return path

    def _write_parquet(self, close: float) -> Path:
        path = self.part_dir / "part.parquet"
        df = pd.DataFrame(
            {
                "time": [pd.Timestamp("2026-07-01 00:00:00")],
                "symbol": ["BTCUSDT"],
                "open": [close],
                "high": [close],
                "low": [close],
                "close": [close],
                "volume": [1],
            }
        )
        df.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
        return path

    def test_prefers_fresh_parquet_over_csv(self):
        csv_path = self._write_csv(1.0)
        time.sleep(0.01)
        parquet_path = self._write_parquet(2.0)
        self.assertGreaterEqual(parquet_path.stat().st_mtime, csv_path.stat().st_mtime)

        df = TempParquetLoader().load(symbols="BTCUSDT", check_val=False)
        self.assertEqual(df.loc[0, "close"], 2.0)
        self.assertIsNone(df["time"].dt.tz)

    def test_uses_csv_when_csv_is_newer_than_parquet(self):
        parquet_path = self._write_parquet(2.0)
        time.sleep(0.01)
        csv_path = self._write_csv(3.0)
        self.assertGreater(csv_path.stat().st_mtime, parquet_path.stat().st_mtime)

        df = TempParquetLoader().load(symbols="BTCUSDT", check_val=False)
        self.assertEqual(df.loc[0, "close"], 3.0)

    def test_falls_back_to_csv_when_parquet_read_fails(self):
        csv_path = self._write_csv(4.0)
        parquet_path = self.part_dir / "part.parquet"
        parquet_path.write_text("not parquet")
        now = time.time() + 1
        os.utime(parquet_path, (now, now))
        self.assertGreater(parquet_path.stat().st_mtime, csv_path.stat().st_mtime)

        df = TempParquetLoader().load(symbols="BTCUSDT", check_val=False)
        self.assertEqual(df.loc[0, "close"], 4.0)


class TestCryptoDailyMatrixParquetFirst(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_storage_dir = data_loader.STORAGE_DIR
        data_loader.STORAGE_DIR = Path(self.tmp.name) / "storage"
        self.matrix_dir = data_loader.STORAGE_DIR / "crypto" / "binance_daily_matrix"
        self.matrix_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        self.tmp.cleanup()

    def _matrix(self, value: float) -> pd.DataFrame:
        return pd.DataFrame({"BTCUSDT": [value]}, index=pd.to_datetime(["2026-01-01"]))

    def test_prefers_matrix_parquet_over_csv(self):
        csv_path = self.matrix_dir / "close.csv.gz"
        self._matrix(1.0).to_csv(csv_path, compression="gzip")
        time.sleep(0.01)
        parquet_path = self.matrix_dir / "close.parquet"
        self._matrix(2.0).to_parquet(parquet_path, engine="pyarrow", compression="zstd")

        df = CryptoDailyMatrix().load("close", check_val=False)
        self.assertEqual(df.loc[pd.Timestamp("2026-01-01"), "BTCUSDT"], 2.0)

    def test_falls_back_to_matrix_csv_when_no_parquet(self):
        csv_path = self.matrix_dir / "close.csv.gz"
        self._matrix(3.0).to_csv(csv_path, compression="gzip")

        df = CryptoDailyMatrix().load("close", check_val=False)
        self.assertEqual(df.loc[pd.Timestamp("2026-01-01"), "BTCUSDT"], 3.0)


class TestVNDailyMatrixParquetFirst(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_storage_dir = data_loader.STORAGE_DIR
        data_loader.STORAGE_DIR = Path(self.tmp.name) / "storage"
        self.matrix_dir = data_loader.STORAGE_DIR / "vn" / "equity" / "daily_matrix"
        self.matrix_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        self.tmp.cleanup()

    def _matrix(self, value: float) -> pd.DataFrame:
        return pd.DataFrame({"FPT": [value]}, index=pd.to_datetime(["2026-01-01"]))

    def test_prefers_matrix_parquet_over_csv(self):
        csv_path = self.matrix_dir / "close.csv.gz"
        self._matrix(1.0).to_csv(csv_path, compression="gzip")
        time.sleep(0.01)
        parquet_path = self.matrix_dir / "close.parquet"
        self._matrix(2.0).to_parquet(parquet_path, engine="pyarrow", compression="zstd")

        df = VNDailyMatrix().load("close", check_val=False)
        self.assertEqual(df.loc[pd.Timestamp("2026-01-01"), "FPT"], 2.0)

    def test_falls_back_to_matrix_csv_when_no_parquet(self):
        csv_path = self.matrix_dir / "close.csv.gz"
        self._matrix(3.0).to_csv(csv_path, compression="gzip")

        df = VNDailyMatrix().load("close", check_val=False)
        self.assertEqual(df.loc[pd.Timestamp("2026-01-01"), "FPT"], 3.0)


if __name__ == "__main__":
    unittest.main()
