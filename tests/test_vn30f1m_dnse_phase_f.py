from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from collectors.common.manifest import JsonState
from collectors.vn30f1m_dnse_phase_f import (
    AUDIT_STATE,
    PROBE_STATE,
    SYMBOL,
    _frame_audit,
    audit_storage,
    iter_windows,
    run_backfill,
    run_probe,
)


def _frame(times: list[str], *, source: str = "dnse") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "time": pd.to_datetime(times),
            "symbol": [SYMBOL] * len(times),
            "open": [1200.0] * len(times),
            "high": [1201.0] * len(times),
            "low": [1199.0] * len(times),
            "close": [1200.5] * len(times),
            "volume": [10] * len(times),
            "source": [source] * len(times),
            "ingested_at": ["2026-08-18T00:00:00+00:00"] * len(times),
        }
    )


class TestVN30F1MDNSEPhaseF(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.previous = {name: os.environ.get(name) for name in ("DATA_ROOT", "STATE_ROOT", "LOG_ROOT")}
        os.environ["DATA_ROOT"] = str(self.root / "storage")
        os.environ["STATE_ROOT"] = str(self.root / "state")
        os.environ["LOG_ROOT"] = str(self.root / "logs")

    def tearDown(self) -> None:
        for name, value in self.previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.temp.cleanup()

    def test_windows_are_bounded_and_contiguous(self) -> None:
        windows = list(iter_windows(pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-12"), window_days=5))
        self.assertEqual([(item.start.strftime("%Y-%m-%d"), item.end.strftime("%Y-%m-%d")) for item in windows], [("2025-01-01", "2025-01-06"), ("2025-01-06", "2025-01-11"), ("2025-01-11", "2025-01-12")])

    def test_frame_audit_rejects_wrong_source_and_invalid_ohlc(self) -> None:
        frame = _frame(["2025-01-06 08:45"])
        frame.loc[0, "source"] = "other"
        frame.loc[0, "high"] = 1198.0
        audit = _frame_audit(frame, symbol=SYMBOL)
        self.assertEqual(audit["status"], "fail")
        self.assertEqual(audit["wrong_source_rows"], 1)
        self.assertEqual(audit["invalid_ohlc_rows"], 1)

    @patch("collectors.vn30f1m_dnse_phase_f.fetch_ohlc")
    def test_probe_writes_passing_evidence_without_storage_write(self, fetch) -> None:
        fetch.return_value = _frame(["2025-01-06 08:45", "2025-01-06 08:46"])
        result = run_probe(symbol=SYMBOL, start="2025-01-06", end="2025-01-06")
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["row_count"], 2)
        self.assertFalse((self.root / "storage" / "vn" / "futures" / "1m").exists())
        self.assertEqual(JsonState(PROBE_STATE).read()["status"], "pass")
        fetch.assert_called_once()

    @patch("collectors.vn30f1m_dnse_phase_f.fetch_ohlc")
    def test_backfill_requires_probe_then_writes_audited_partition(self, fetch) -> None:
        JsonState(PROBE_STATE).write({"status": "pass", "symbol": SYMBOL})
        fetch.return_value = _frame(["2025-01-06 08:45", "2025-01-06 08:46"])
        result = run_backfill(
            symbol=SYMBOL,
            start="2025-01-06",
            end="2025-01-06",
            window_days=5,
            require_probe=True,
            audit_after=True,
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["audit"]["status"], "pass")
        part = self.root / "storage" / "vn" / "futures" / "1m" / "symbol=VN30F1M" / "year=2025" / "month=01" / "part.parquet"
        self.assertTrue(part.exists())
        loaded = pd.read_parquet(part)
        self.assertEqual(len(loaded), 2)
        self.assertEqual(JsonState(AUDIT_STATE).read()["status"], "pass")

    def test_storage_audit_fails_when_a_trading_date_is_missing(self) -> None:
        audit = audit_storage(symbol=SYMBOL, start="2025-01-06", end="2025-01-07")
        self.assertEqual(audit["status"], "fail")
        self.assertEqual(audit["missing_trading_date_count"], 2)


if __name__ == "__main__":
    unittest.main()
