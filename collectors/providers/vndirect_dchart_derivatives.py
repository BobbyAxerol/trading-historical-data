from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

import pandas as pd
import requests

from collectors.common.manifest import utc_now_iso

FetchStatus = Literal[
    "success",
    "no_data",
    "rate_limited",
    "http_error",
    "schema_error",
    "network_error",
]


@dataclass(frozen=True)
class DChartFetchResult:
    status: FetchStatus
    data: pd.DataFrame
    requested_start: pd.Timestamp
    requested_end: pd.Timestamp
    first_bar: pd.Timestamp | None
    last_bar: pd.Timestamp | None
    http_status: int | None
    error: str | None

    @property
    def row_count(self) -> int:
        return int(len(self.data)) if isinstance(self.data, pd.DataFrame) else 0


class VndirectDChartProvider:
    BASE_URL = "https://dchart-api.vndirect.com.vn/dchart/history"
    SYMBOL = "VN30F1M"
    SOURCE = "vndirect_dchart"
    RESOLUTIONS = {
        "1m": "1",
        "1d": "D",
    }

    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 trading-historical-data/vn30f1m-research",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://dchart.vndirect.com.vn/",
            }
        )

    def fetch(
        self,
        *,
        start: pd.Timestamp,
        end: pd.Timestamp,
        resolution: Literal["1m", "1d"],
    ) -> DChartFetchResult:
        requested_start = pd.Timestamp(start)
        requested_end = pd.Timestamp(end)
        start_utc = self._to_utc(requested_start)
        end_utc = self._to_utc(requested_end)
        params = {
            "resolution": self.RESOLUTIONS[resolution],
            "symbol": self.SYMBOL,
            "from": int(start_utc.timestamp()),
            "to": int(end_utc.timestamp()),
        }

        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout, allow_redirects=True)
        except requests.RequestException as exc:
            return self._failure("network_error", requested_start, requested_end, None, f"{type(exc).__name__}: {exc}")

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            return self._failure("rate_limited", requested_start, requested_end, response.status_code, retry_after)
        if response.status_code != 200:
            return self._failure("http_error", requested_start, requested_end, response.status_code, response.text[:500])

        try:
            payload = response.json()
        except ValueError as exc:
            return self._failure("schema_error", requested_start, requested_end, response.status_code, f"invalid JSON: {exc}")

        if payload.get("s") == "no_data":
            return DChartFetchResult(
                status="no_data",
                data=self._empty_frame(),
                requested_start=requested_start,
                requested_end=requested_end,
                first_bar=None,
                last_bar=None,
                http_status=response.status_code,
                error=None,
            )
        if payload.get("s") != "ok":
            return self._failure("schema_error", requested_start, requested_end, response.status_code, f"unexpected status: {payload.get('s')!r}")

        required = ("t", "o", "h", "l", "c", "v")
        missing = [key for key in required if key not in payload]
        if missing:
            return self._failure("schema_error", requested_start, requested_end, response.status_code, f"missing fields: {missing}")
        lengths = {len(payload[key]) for key in required}
        if len(lengths) != 1:
            return self._failure("schema_error", requested_start, requested_end, response.status_code, f"array lengths differ: {sorted(lengths)}")
        if lengths == {0}:
            return DChartFetchResult(
                status="no_data",
                data=self._empty_frame(),
                requested_start=requested_start,
                requested_end=requested_end,
                first_bar=None,
                last_bar=None,
                http_status=response.status_code,
                error=None,
            )

        frame = pd.DataFrame(
            {
                "time": pd.to_datetime(payload["t"], unit="s", utc=True).tz_convert("Asia/Ho_Chi_Minh"),
                "open": payload["o"],
                "high": payload["h"],
                "low": payload["l"],
                "close": payload["c"],
                "volume": payload["v"],
            }
        )
        try:
            frame = self._normalize_and_validate(frame, resolution=resolution)
        except ValueError as exc:
            return self._failure("schema_error", requested_start, requested_end, response.status_code, str(exc))

        return DChartFetchResult(
            status="success",
            data=frame,
            requested_start=requested_start,
            requested_end=requested_end,
            first_bar=frame["time"].min() if not frame.empty else None,
            last_bar=frame["time"].max() if not frame.empty else None,
            http_status=response.status_code,
            error=None,
        )

    @staticmethod
    def _to_utc(value: pd.Timestamp) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("Asia/Ho_Chi_Minh")
        return ts.tz_convert("UTC")

    @classmethod
    def _empty_frame(cls) -> pd.DataFrame:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "source", "source_symbol", "quality_flags", "ingested_at"])

    @classmethod
    def _failure(
        cls,
        status: FetchStatus,
        start: pd.Timestamp,
        end: pd.Timestamp,
        http_status: int | None,
        error: str | None,
    ) -> DChartFetchResult:
        return DChartFetchResult(
            status=status,
            data=cls._empty_frame(),
            requested_start=start,
            requested_end=end,
            first_bar=None,
            last_bar=None,
            http_status=http_status,
            error=error,
        )

    @classmethod
    def _normalize_and_validate(cls, frame: pd.DataFrame, *, resolution: Literal["1m", "1d"]) -> pd.DataFrame:
        if frame.empty:
            return cls._empty_frame()
        work = frame.copy()
        work["time"] = pd.to_datetime(work["time"], errors="coerce")
        work = work.dropna(subset=["time"])
        for col in ["open", "high", "low", "close", "volume"]:
            work[col] = pd.to_numeric(work[col], errors="coerce")
        work = work.dropna(subset=["open", "high", "low", "close", "volume"])
        if work.empty:
            raise ValueError("all rows invalid after numeric/time normalization")

        for col in ["open", "high", "low", "close", "volume"]:
            if not work[col].map(lambda value: math.isfinite(float(value))).all():
                raise ValueError(f"non-finite values in {col}")

        invalid_ohlc = (
            (work["high"] < work[["open", "close", "low"]].max(axis=1))
            | (work["low"] > work[["open", "close", "high"]].min(axis=1))
            | (work["volume"] < 0)
        )
        if invalid_ohlc.any():
            raise ValueError(f"invalid OHLC/volume rows: {int(invalid_ohlc.sum())}")

        duplicate_rows = int(work.duplicated(subset=["time"]).sum())
        if duplicate_rows:
            raise ValueError(f"duplicate time rows: {duplicate_rows}")
        work = work.sort_values("time").reset_index(drop=True)
        if resolution == "1d":
            local_dates = work["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.normalize()
            if local_dates.duplicated().any():
                raise ValueError(f"duplicate local trading dates: {int(local_dates.duplicated().sum())}")

        work["source"] = cls.SOURCE
        work["source_symbol"] = cls.SYMBOL
        work["quality_flags"] = "CONTINUOUS_ALIAS"
        work["ingested_at"] = utc_now_iso()
        return work[["time", "open", "high", "low", "close", "volume", "source", "source_symbol", "quality_flags", "ingested_at"]]
