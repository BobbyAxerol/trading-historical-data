import os
import tempfile
import time
import unittest
from pathlib import Path

import pandas as pd

import data_loader
from data_loader import BinanceOptions5m, CryptoDailyMatrix, MarketDataLoaderBase, VNDailyMatrix
from storage_manifest import StorageCompatibilityError, StorageManifestError, write_release_manifest


class TempParquetLoader(MarketDataLoaderBase):
    DATASET_NAME = "temp_parquet_loader"
    NEW_PATH_PARTS = ("crypto", "test", "1m")
    TZ_INFO = "UTC"


class TempOhlcvLoader(MarketDataLoaderBase):
    DATASET_NAME = "crypto_1m"
    NEW_PATH_PARTS = ("crypto", "test", "1m")
    TZ_INFO = "UTC"
    DEFAULT_COLUMNS = data_loader.OHLCV_COLUMNS
    RESAMPLE_SUPPORTED = True


class TempCryptoBinance1m(data_loader.CryptoBinance1m):
    """Use the real Binance futures reader rules against the temporary fixture."""

    NEW_PATH_PARTS = ("crypto", "test", "1m")


class TempCryptoBinanceQuarterly1m(data_loader.CryptoBinanceQuarterly1m):
    """Use the real quarterly reader rules against the temporary fixture."""

    NEW_PATH_PARTS = ("crypto", "test", "1m")


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


class TestOhlcvProjectionAndResample(unittest.TestCase):
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
        times = pd.date_range("2026-07-01 00:00:00", periods=10, freq="min")
        df = pd.DataFrame(
            {
                "time": times,
                "symbol": ["BTCUSDT"] * len(times),
                "open": [x + 10 for x in range(10)],
                "high": [x + 10.5 for x in range(10)],
                "low": [x + 9.5 for x in range(10)],
                "close": [x + 10.25 for x in range(10)],
                "volume": [1.9] * len(times),
                "source": ["unit_test"] * len(times),
                "ingested_at": ["2026-07-01T00:00:00Z"] * len(times),
            }
        )
        df.to_parquet(self.part_dir / "part.parquet", index=False, engine="pyarrow", compression="zstd")

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        self.tmp.cleanup()

    def test_ohlcv_loader_defaults_to_ohlcv_projection(self):
        df = TempOhlcvLoader().load(symbols="BTCUSDT", check_val=True)
        self.assertEqual(list(df.columns), list(data_loader.OHLCV_COLUMNS))
        self.assertNotIn("source", df.columns)
        self.assertNotIn("ingested_at", df.columns)

    def test_full_and_custom_columns_are_opt_in(self):
        full = TempOhlcvLoader().load(symbols="BTCUSDT", check_val=False, columns="full")
        self.assertIn("source", full.columns)
        self.assertIn("ingested_at", full.columns)

        close_only = TempOhlcvLoader().load(symbols="BTCUSDT", check_val=False, columns=["time", "symbol", "close"])
        self.assertEqual(list(close_only.columns), ["time", "symbol", "close"])

    def test_binance_futures_reader_preserves_fractional_volume(self):
        df = TempCryptoBinance1m().load(symbols="BTCUSDT", check_val=False)

        self.assertEqual(str(df["volume"].dtype), "float64")
        self.assertAlmostEqual(float(df.loc[0, "volume"]), 1.9)

    def test_duckdb_resample_matches_pandas_chunk_fallback(self):
        duck = TempOhlcvLoader().load_resampled(symbols="BTCUSDT", timeframe="5min", check_val=True, engine="duckdb")
        pandas_df = TempOhlcvLoader().load_resampled(symbols="BTCUSDT", timeframe="5min", check_val=True, engine="pandas")

        pd.testing.assert_frame_equal(duck.reset_index(drop=True), pandas_df.reset_index(drop=True), check_dtype=True)
        self.assertEqual(str(duck["time"].dtype), "datetime64[ns]")
        self.assertEqual(len(duck), 2)
        self.assertEqual(float(duck.loc[0, "open"]), 10.0)
        self.assertEqual(float(duck.loc[0, "high"]), 14.5)
        self.assertEqual(float(duck.loc[0, "low"]), 9.5)
        self.assertEqual(float(duck.loc[0, "close"]), 14.25)
        self.assertEqual(float(duck.loc[0, "volume"]), 5.0)
        self.assertEqual(str(duck["volume"].dtype), "int64")


class TestCryptoBinanceFuturesDefaultDiscovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_storage_dir = data_loader.STORAGE_DIR
        data_loader.STORAGE_DIR = Path(self.tmp.name) / "storage"
        for symbol in ("BTCUSDT", "BTCUSDT_260925", "BTCUSDT_261225"):
            part_dir = (
                data_loader.STORAGE_DIR
                / "crypto"
                / "test"
                / "1m"
                / f"symbol={symbol}"
                / "year=2026"
                / "month=08"
            )
            part_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                {
                    "time": [pd.Timestamp("2026-08-01 00:00:00")],
                    "symbol": [symbol],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [1.0],
                }
            ).to_parquet(part_dir / "part.parquet", index=False, engine="pyarrow", compression="zstd")

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        self.tmp.cleanup()

    def test_default_discovery_keeps_perpetual_and_quarterly_readers_disjoint(self):
        perpetual = TempCryptoBinance1m()
        quarterly = TempCryptoBinanceQuarterly1m()

        self.assertEqual(perpetual._discover_symbols(), ["BTCUSDT"])
        self.assertEqual(quarterly._discover_symbols(), ["BTCUSDT_260925", "BTCUSDT_261225"])
        self.assertEqual(set(perpetual.load(check_val=False)["symbol"]), {"BTCUSDT"})
        self.assertEqual(
            set(quarterly.load(check_val=False)["symbol"]),
            {"BTCUSDT_260925", "BTCUSDT_261225"},
        )

    def test_explicit_symbol_queries_remain_compatible(self):
        quarterly_symbol_via_perpetual = TempCryptoBinance1m().load(
            symbols="BTCUSDT_260925",
            check_val=False,
        )
        perpetual_symbol_via_quarterly = TempCryptoBinanceQuarterly1m().load(
            symbols="BTCUSDT",
            check_val=False,
        )

        self.assertEqual(set(quarterly_symbol_via_perpetual["symbol"]), {"BTCUSDT_260925"})
        self.assertEqual(set(perpetual_symbol_via_quarterly["symbol"]), {"BTCUSDT"})


class TestDailyPartitionDiscovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_storage_dir = data_loader.STORAGE_DIR
        data_loader.STORAGE_DIR = Path(self.tmp.name) / "storage"
        self.day_dir = (
            data_loader.STORAGE_DIR
            / "options"
            / "binance"
            / "snapshot_5m"
            / "underlying=BTC"
            / "year=2026"
            / "month=07"
            / "day=01"
        )
        self.day_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "snapshot_time": [pd.Timestamp("2026-07-01 00:00:00")],
                "underlying": ["BTC"],
                "symbol": ["BTC-260925-100000-C"],
                "mark_price": [100.0],
            }
        ).to_parquet(self.day_dir / "part.parquet", index=False, engine="pyarrow", compression="zstd")

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        self.tmp.cleanup()

    def test_loader_discovers_daily_option_partitions(self):
        df = BinanceOptions5m().load(symbols="BTC", start_date="2026-07-01", end_date="2026-07-01 23:59:59", check_val=True)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "symbol"], "BTC-260925-100000-C")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["snapshot_time"]))

        empty = BinanceOptions5m().load(symbols="BTC", start_date="2026-07-02", end_date="2026-07-02 23:59:59", check_val=False)
        self.assertTrue(empty.empty)


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


class TestReaderManifestEnforcement(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_storage_dir = data_loader.STORAGE_DIR
        self.old_root = os.environ.get("HISTORICAL_MARKET_DATA_ROOT")
        data_loader.STORAGE_DIR = Path(self.tmp.name) / "storage"
        os.environ["HISTORICAL_MARKET_DATA_ROOT"] = str(data_loader.STORAGE_DIR)
        self.part_dir = (
            data_loader.STORAGE_DIR
            / "crypto"
            / "binance_futures"
            / "1m"
            / "symbol=BTCUSDT"
            / "year=2026"
            / "month=08"
        )
        self.part_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "time": [pd.Timestamp("2026-08-13 00:00:00")],
                "symbol": ["BTCUSDT"],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1],
            }
        ).to_parquet(self.part_dir / "part.parquet", index=False, engine="pyarrow", compression="zstd")

    def tearDown(self):
        data_loader.STORAGE_DIR = self.old_storage_dir
        if self.old_root is None:
            os.environ.pop("HISTORICAL_MARKET_DATA_ROOT", None)
        else:
            os.environ["HISTORICAL_MARKET_DATA_ROOT"] = self.old_root
        self.tmp.cleanup()

    @staticmethod
    def _manifest(status: str) -> dict:
        return {
            "schema_version": 1,
            "status": status,
            "environment_id": "unit",
            "created_at": "2026-08-13T00:00:00+00:00",
            "source_inventory_reference": "state/bootstrap/source_inventory.json",
            "git": {"commit": "unit", "tag": "unit"},
            "build": {
                "collector_image": "unit@sha256:abc",
                "python_base_image": "python@sha256:def",
                "python": "3.12.13",
                "duckdb": "1.5.5",
                "pyarrow": "20.0.0",
            },
            "storage": {
                "supported_loader_contract_versions": ["hmd-loader-v1"],
                "schema_migration_policy": "additive",
                "incompatible_reader_policy": "raise",
            },
            "datasets": [
                {
                    "dataset_id": "binance_perpetual_spot_quarterly",
                    "canonical_schema_version": "unit-v1",
                    "partition_layout_version": "hive-v1",
                    "supported_loader_contract_versions": ["hmd-loader-v1"],
                    "source_report_reference": "state/bootstrap/source_inventory.json#unit",
                }
            ],
        }

    def test_explicit_reader_root_refuses_missing_or_draft_manifest(self):
        with self.assertRaises(StorageManifestError):
            data_loader.CryptoBinance1m().load(symbols="BTCUSDT", check_val=False)
        write_release_manifest(data_loader.STORAGE_DIR, self._manifest("draft"))
        with self.assertRaises(StorageCompatibilityError):
            data_loader.CryptoBinance1m().load(symbols="BTCUSDT", check_val=False)

    def test_explicit_reader_root_accepts_matching_manifest(self):
        write_release_manifest(data_loader.STORAGE_DIR, self._manifest("pass"))
        df = data_loader.CryptoBinance1m().load(symbols="BTCUSDT", check_val=False)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "symbol"], "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
