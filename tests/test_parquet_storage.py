import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from collectors.common.storage import PartitionedParquetStore


class TestPartitionedParquetStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_data_root = os.environ.get("DATA_ROOT")
        self.old_state_root = os.environ.get("STATE_ROOT")
        root = Path(self.tmp.name)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")

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

    def test_append_dedupe_and_latest_time_month_partition(self):
        store = PartitionedParquetStore(["crypto", "test", "1m"], partition="month")
        first = pd.DataFrame(
            {
                "time": ["2026-07-01 00:00:00", "2026-07-01 00:01:00"],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "close": [100.0, 101.0],
            }
        )
        result = store.append(
            first,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs={"symbol": "BTCUSDT"},
            lock_name="test_parquet/BTCUSDT",
        )
        self.assertEqual(result["rows_written"], 2)
        self.assertEqual(result["latest_time"], "2026-07-01T00:01:00")

        second = pd.DataFrame(
            {
                "time": ["2026-07-01 00:01:00", "2026-07-01 00:02:00"],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "close": [111.0, 102.0],
            }
        )
        result = store.append(
            second,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs={"symbol": "BTCUSDT"},
            lock_name="test_parquet/BTCUSDT",
        )

        path = Path(os.environ["DATA_ROOT"]) / "crypto" / "test" / "1m" / "symbol=BTCUSDT" / "year=2026" / "month=07" / "part.parquet"
        self.assertTrue(path.exists())
        df = pd.read_parquet(path)
        self.assertEqual(len(df), 3)
        self.assertEqual(result["rows_written"], 2)
        self.assertEqual(result["latest_time"], "2026-07-01T00:02:00")
        self.assertEqual(store.latest_time(attrs={"symbol": "BTCUSDT"}, time_col="time"), pd.Timestamp("2026-07-01 00:02:00"))
        self.assertEqual(df.loc[df["time"] == pd.Timestamp("2026-07-01 00:01:00"), "close"].iloc[0], 111.0)

    def test_year_partition_layout(self):
        store = PartitionedParquetStore(["vn", "test", "1d"], partition="year")
        df = pd.DataFrame(
            {
                "time": ["2026-01-02"],
                "symbol": ["FPT"],
                "close": [1000],
            }
        )
        store.append(
            df,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs={"symbol": "FPT"},
            lock_name="test_parquet/FPT",
        )
        path = Path(os.environ["DATA_ROOT"]) / "vn" / "test" / "1d" / "symbol=FPT" / "year=2026" / "part.parquet"
        self.assertTrue(path.exists())
        loaded = pd.read_parquet(path)
        self.assertEqual(str(loaded.loc[0, "time"]), "2026-01-02 00:00:00")


if __name__ == "__main__":
    unittest.main()
