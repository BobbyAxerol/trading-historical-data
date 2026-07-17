import os
import tempfile
import time
import unittest
from pathlib import Path

import pandas as pd

import data_loader
from data_loader import MarketDataLoaderBase


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


if __name__ == "__main__":
    unittest.main()
