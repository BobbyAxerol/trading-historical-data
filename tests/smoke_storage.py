from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        os.environ["DATA_ROOT"] = str(root / "storage")
        os.environ["STATE_ROOT"] = str(root / "state")
        from collectors.common.manifest import Manifest
        from collectors.common.storage import PartitionedCsvGzStore

        df = pd.DataFrame(
            {
                "time": ["2026-06-01 09:00:00", "2026-06-01 09:00:00", "2026-06-01 09:01:00"],
                "symbol": ["TEST", "TEST", "TEST"],
                "open": [1, 2, 3],
                "high": [1, 2, 3],
                "low": [1, 2, 3],
                "close": [1, 2, 3],
                "volume": [10, 20, 30],
            }
        )
        store = PartitionedCsvGzStore(["unit", "ohlcv", "1m"], partition="month")
        result = store.append(
            df,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs={"symbol": "TEST"},
            lock_name="unit/TEST",
        )
        assert result["rows_written"] == 3
        files = list((root / "storage").rglob("*.csv.gz"))
        assert len(files) == 1
        saved = pd.read_csv(files[0], compression="gzip")
        assert len(saved) == 2
        assert saved.loc[saved["time"] == "2026-06-01 09:00:00", "open"].iloc[0] == 2

        manifest = Manifest("unit_test")
        manifest.update_symbol("TEST", latest_time="2026-06-01T09:01:00")
        assert manifest.symbol_state("TEST")["latest_time"] == "2026-06-01T09:01:00"
    print("storage smoke ok")


if __name__ == "__main__":
    main()

