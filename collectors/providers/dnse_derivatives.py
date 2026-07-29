from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd
import requests

from collectors.common.env import GET_DATA_ROOT
from collectors.common.manifest import utc_now_iso
from collectors.common.retry import retry_sync

BASE_URL = "https://openapi.dnse.com.vn"
AssetType = Literal["stock", "derivative"]


def _build_headers(path: str) -> dict[str, str]:
    sys.path.insert(0, str(GET_DATA_ROOT))
    from openapi_sdk.python.dnse.common import build_signature

    api_key = os.getenv("DNSE_API_KEY")
    api_secret = os.getenv("DNSE_API_SECRET_KEY")
    if not api_key or not api_secret:
        raise RuntimeError("Missing DNSE_API_KEY or DNSE_API_SECRET_KEY")

    date_value = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
    nonce = uuid.uuid4().hex
    headers_list, signature = build_signature(
        api_secret,
        "GET",
        path,
        date_value,
        algorithm="hmac-sha256",
        nonce=nonce,
        header_name="X-Aux-Date",
    )
    return {
        "X-API-Key": api_key,
        "X-Aux-Date": date_value,
        "X-Signature": (
            f'Signature keyId="{api_key}",'
            f'algorithm="hmac-sha256",'
            f'headers="{headers_list}",'
            f'signature="{signature}",'
            f'nonce="{nonce}"'
        ),
        "Accept": "application/json",
    }


def _unix(date_or_ts: str | pd.Timestamp) -> int:
    ts = pd.Timestamp(date_or_ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Ho_Chi_Minh")
    return int(ts.tz_convert("UTC").timestamp())


def _parse_ohlc(data: dict[str, Any], provider_symbol: str, *, resolution: str) -> pd.DataFrame:
    if "t" in data:
        df = pd.DataFrame(
            {
                "time": pd.to_datetime(data.get("t", []), unit="s", utc=True).tz_convert("Asia/Ho_Chi_Minh").tz_localize(None),
                "open": data.get("o", []),
                "high": data.get("h", []),
                "low": data.get("l", []),
                "close": data.get("c", []),
                "volume": data.get("v", []),
            }
        )
    else:
        candles = data.get("data", data.get("candles", data.get("ohlc", [])))
        df = pd.DataFrame(candles)
        df = df.rename(columns={"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        if "time" in df.columns and pd.api.types.is_numeric_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)

    if df.empty or "time" not in df.columns:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"])

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    try:
        if df["time"].dt.tz is not None:
            df["time"] = df["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except Exception:
        pass
    df = df.dropna(subset=["time"]).sort_values("time")

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    if "volume" not in df.columns:
        df["volume"] = 0
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("float64")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df[df["volume"] >= 0]
    if resolution.lower() in {"d", "1d", "day"}:
        df["time"] = df["time"].dt.normalize()

    df["provider_symbol"] = provider_symbol
    df["source"] = "dnse"
    df["ingested_at"] = utc_now_iso()
    return df[["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"]].sort_values("time").reset_index(drop=True)


def fetch_ohlc(
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    resolution: str,
    *,
    asset_type: AssetType,
) -> pd.DataFrame:
    """Fetch DNSE OHLC with explicit asset type.

    This avoids the old alias-only routing that treated concrete contracts like
    ``VN30F2503`` as stocks.
    """
    path = "/price/ohlc"
    bar_type = "DERIVATIVE" if asset_type == "derivative" else "STOCK"

    def call() -> pd.DataFrame:
        response = requests.get(
            f"{BASE_URL}{path}",
            params={
                "symbol": symbol,
                "type": bar_type,
                "resolution": resolution,
                "from": str(_unix(start)),
                "to": str(_unix(end)),
            },
            headers=_build_headers(path),
            timeout=30,
        )
        if response.status_code in {418, 429} or response.status_code >= 500:
            raise RuntimeError(f"DNSE retryable HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        return _parse_ohlc(response.json(), symbol, resolution=resolution)

    return retry_sync(call, attempts=5, base_sleep=2)

