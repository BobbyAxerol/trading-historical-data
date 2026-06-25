import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

from collectors import binance_daily_matrix as matrix


class MockResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class QuietLogger:
    def info(self, *args, **kwargs):
        pass


class TestBinanceDailyMatrixUniverse(unittest.TestCase):
    def test_get_top_symbols_uses_score_not_plain_quote_volume(self):
        now = datetime(2026, 6, 17, tzinfo=timezone.utc)
        active_meta = {
            "OLDUSDT": {"onboardDate": int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)},
            "MIDUSDT": {"onboardDate": int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)},
            "NEWUSDT": {"onboardDate": int(datetime(2025, 6, 17, tzinfo=timezone.utc).timestamp() * 1000)},
        }
        tickers = [
            {"symbol": "OLDUSDT", "quoteVolume": "200"},
            {"symbol": "MIDUSDT", "quoteVolume": "100"},
            {"symbol": "NEWUSDT", "quoteVolume": "300"},
        ]
        stability = {
            "OLDUSDT": 2.0,
            "MIDUSDT": 3.0,
            "NEWUSDT": 1.0,
        }

        with patch.object(matrix.requests, "get", return_value=MockResponse(tickers)):
            with patch.object(matrix, "_fetch_volume_stability", side_effect=lambda symbol: stability[symbol]):
                with patch.object(matrix.time, "sleep"):
                    selected = matrix._get_top_symbols(
                        active_meta,
                        set(active_meta),
                        top_n=1,
                        now_utc=now,
                        logger=QuietLogger(),
                    )

        self.assertEqual(selected, ["OLDUSDT"])

    def test_fetch_volume_stability_scores_nonzero_volume_history(self):
        rows = [[0, "0", "0", "0", "0", str(100 + i)] for i in range(100)]

        with patch.object(matrix.requests, "get", return_value=MockResponse(rows)):
            score = matrix._fetch_volume_stability("BTCUSDT")

        self.assertGreater(score, 0.0)


if __name__ == "__main__":
    unittest.main()
