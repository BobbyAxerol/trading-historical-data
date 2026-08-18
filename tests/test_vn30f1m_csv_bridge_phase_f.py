from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from collectors.vn30f1m_csv_bridge_phase_f import BRIDGE_AUDIT_STATE, SYMBOL, bridge
from collectors.common.manifest import JsonState


class TestVN30F1MCSVBridgePhaseF(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.previous = {name: os.environ.get(name) for name in ("DATA_ROOT", "STATE_ROOT", "LOG_ROOT")}
        os.environ["DATA_ROOT"] = str(self.root / "storage")
        os.environ["STATE_ROOT"] = str(self.root / "state")
        os.environ["LOG_ROOT"] = str(self.root / "logs")
        self.raw = self.root / "vn30f1m_raw_1m.csv"
        self.extended = self.root / "vn30f1m_raw_1m_from_2024.csv"
        self.adjusted = self.root / "vn30f1m_1m.csv"
        raw = pd.DataFrame(
            {
                "Unnamed: 0": [0, 1],
                "datetime": ["2024-01-02 09:00:00", "2024-01-02 09:01:00"],
                "open": [1200.0, 1200.5],
                "high": [1201.0, 1201.5],
                "low": [1199.0, 1200.0],
                "close": [1200.5, 1201.0],
                "volume": [10, 11],
                "value": [1.0, 2.0],
            }
        )
        raw.to_csv(self.raw, index=False)
        adjusted = raw.copy()
        adjusted["close"] = [1100.5, 1101.0]
        adjusted.to_csv(self.adjusted, index=False)
        extended = raw.iloc[[1]].copy()
        extended.loc[:, "close"] = 1201.1
        extended = pd.concat([extended, raw.iloc[[0]].assign(datetime="2024-05-02 09:00:00")], ignore_index=True)
        extended.to_csv(self.extended, index=False)

    def tearDown(self) -> None:
        for name, value in self.previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.temp.cleanup()

    def test_bridge_keeps_base_raw_on_overlap_and_extends_history(self) -> None:
        result = bridge(
            base_raw_path=self.raw,
            extended_raw_path=self.extended,
            adjusted_path=self.adjusted,
            start="2024-01-02",
            end="2024-05-02",
        )
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["stored_raw_rows"], 3)
        self.assertEqual(result["overlap_rows"], 1)
        self.assertEqual(result["overlap_conflict_counts"]["close"], 1)
        self.assertEqual(result["adjusted_evidence"]["shared_rows_with_any_ohlc_difference"], 2)
        part = self.root / "storage" / "vn" / "futures" / "1m" / "symbol=VN30F1M" / "year=2024" / "month=01" / "part.parquet"
        stored = pd.read_parquet(part)
        self.assertEqual(set(stored["source"]), {"legacy_csv_raw_2018"})
        later = self.root / "storage" / "vn" / "futures" / "1m" / "symbol=VN30F1M" / "year=2024" / "month=05" / "part.parquet"
        self.assertEqual(set(pd.read_parquet(later)["source"]), {"legacy_csv_raw_from_2024"})
        self.assertNotIn("value", stored.columns)
        self.assertEqual(JsonState(BRIDGE_AUDIT_STATE).read()["status"], "pass")


if __name__ == "__main__":
    unittest.main()
