from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from collectors import b0_source_probe


class FakeResponse:
    def __init__(self, *, status_code: int = 200, payload=None, content: bytes | None = None):
        self.status_code = status_code
        self._payload = payload
        self.content = content if content is not None else b"{}"

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def get(self, url, *, params, timeout, headers):
        del timeout, headers
        if "s3-ap-northeast-1.amazonaws.com" in url:
            prefix = params["prefix"]
            body = f"<ListBucketResult><Contents><Key>{prefix}first.zip</Key></Contents></ListBucketResult>".encode()
            return FakeResponse(content=body)
        if url.endswith("exchangeInfo"):
            return FakeResponse(payload={"symbols": [{"symbol": "BTCUSDT_260925", "pair": "BTCUSDT", "contractType": "CURRENT_QUARTER"}]})
        if url.endswith("depth"):
            return FakeResponse(payload={"bids": [["1", "1"]], "asks": [["2", "1"]]})
        return FakeResponse(payload=[[1, 2, 3]])


class TestB0SourceProbe(unittest.TestCase):
    def test_binance_probe_is_bounded_and_redacted(self) -> None:
        payload = b0_source_probe.probe_binance(session=FakeSession())
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["request_budget"], 8)
        self.assertEqual(payload["actual_request_count"], 8)
        self.assertTrue(payload["non_destructive"])
        self.assertTrue(all(item["status"] == "pass" for item in payload["probes"]))
        self.assertNotIn("raw_response", str(payload))

    def test_binance_probe_blocks_on_endpoint_failure(self) -> None:
        class FailingSession(FakeSession):
            def get(self, url, *, params, timeout, headers):
                if url.endswith("depth"):
                    return FakeResponse(status_code=429, content=b"rate limited")
                return super().get(url, params=params, timeout=timeout, headers=headers)

        payload = b0_source_probe.probe_binance(session=FailingSession())
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["probes"][-1]["http_status"], 429)

    def test_write_probe_uses_state_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(b0_source_probe, "state_root", return_value=Path(tmp)):
                path = b0_source_probe.write_probe("binance", {"status": "pass"})
            self.assertEqual(path, Path(tmp) / "bootstrap/source_probes/binance.json")
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
