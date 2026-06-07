from __future__ import annotations

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from collectors.common.calendar_vn import is_derivative_session, seconds_until_next_session, vn_now
from collectors.common.config import load_yaml
from collectors.common.discovery import latest_time_from_files, max_timestamp
from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, Manifest, utc_now_iso
from collectors.common.retry import SlidingWindowRateLimiter, retry_sync
from collectors.common.storage import PartitionedCsvGzStore

DATASET = "vn_futures_dnse_1m"
BASE_URL = "https://openapi.dnse.com.vn"
DERIVATIVE_SYMBOLS = {"VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"}


def _build_headers(path: str) -> dict[str, str]:
    import sys

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


def _parse_ohlc(data: dict[str, Any], symbol: str) -> pd.DataFrame:
    from collectors.common.calendar_vn import filter_trading_hours

    if "t" in data:
        rows = {
            "time": pd.to_datetime(data.get("t", []), unit="s", utc=True).tz_convert("Asia/Ho_Chi_Minh").tz_localize(None),
            "open": data.get("o", []),
            "high": data.get("h", []),
            "low": data.get("l", []),
            "close": data.get("c", []),
            "volume": data.get("v", []),
        }
        df = pd.DataFrame(rows)
    else:
        candles = data.get("data", data.get("candles", data.get("ohlc", [])))
        df = pd.DataFrame(candles)
        df = df.rename(columns={"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        if "time" in df.columns and pd.api.types.is_numeric_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)

    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    if df["time"].dt.tz is not None:
        df["time"] = df["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    df = df.dropna(subset=["time"]).sort_values("time")

    df["symbol"] = symbol
    df["open"] = pd.to_numeric(df["open"], errors="coerce").astype("float64")
    df["high"] = pd.to_numeric(df["high"], errors="coerce").astype("float64")
    df["low"] = pd.to_numeric(df["low"], errors="coerce").astype("float64")
    df["close"] = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    df["source"] = "dnse"
    df["ingested_at"] = utc_now_iso()

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df[df["volume"] >= 0]

    cols = ["time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]
    df = df[cols]

    derivative = symbol in DERIVATIVE_SYMBOLS
    return filter_trading_hours(df, derivative=derivative)



def fetch_ohlc(symbol: str, start: pd.Timestamp, end: pd.Timestamp, resolution: str = "1") -> pd.DataFrame:
    path = "/price/ohlc"
    bar_type = "DERIVATIVE" if symbol in DERIVATIVE_SYMBOLS else "STOCK"

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
        return _parse_ohlc(response.json(), symbol)

    return retry_sync(call, attempts=5, base_sleep=2)


def run_symbol(symbol: str, *, start_default: str, limiter: SlidingWindowRateLimiter, logger) -> None:
    manifest = Manifest(DATASET)
    state = manifest.symbol_state(symbol)
    dataset_parts = ["vn", "futures" if symbol in DERIVATIVE_SYMBOLS else "equity", "1m"]
    store = PartitionedCsvGzStore(dataset_parts, partition="month")
    storage_latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    legacy_base = "futures" if symbol in DERIVATIVE_SYMBOLS else "stocks"
    legacy_latest = latest_time_from_files(
        [
            GET_DATA_ROOT / "data_stock" / "_intraday_storage" / legacy_base / f"{symbol}_1m.csv.gz",
            GET_DATA_ROOT / "data_stock" / "_intraday_storage" / legacy_base / f"{symbol}_1m.parquet",
            GET_DATA_ROOT.parent / "data_stock" / "_intraday_storage" / legacy_base / f"{symbol}_1m.csv.gz",
            GET_DATA_ROOT.parent / "data_stock" / "_intraday_storage" / legacy_base / f"{symbol}_1m.parquet",
        ],
        ["time"],
    )
    discovered_latest = max_timestamp(state.get("latest_time"), storage_latest, legacy_latest)
    if discovered_latest is not None:
        start = discovered_latest - pd.Timedelta(minutes=5)
        if not state.get("latest_time") or discovered_latest > pd.Timestamp(state["latest_time"]):
            manifest.update_symbol(
                symbol,
                latest_time=discovered_latest.isoformat(),
                discovered_from_tail=True,
                legacy_latest=legacy_latest.isoformat() if legacy_latest is not None else None,
                storage_latest=storage_latest.isoformat() if storage_latest is not None else None,
            )
    else:
        start = pd.Timestamp(start_default)
    end = pd.Timestamp.now()

    limiter.wait()
    logger.info("Fetching DNSE %s %s -> %s", symbol, start, end)
    df = fetch_ohlc(symbol, start, end)
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_response", last_success_at=utc_now_iso())
        logger.warning("%s DNSE returned no rows", symbol)
        return

    result = store.append(
        df,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": symbol},
        lock_name=f"{DATASET}/{symbol}",
    )
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        last_success_at=utc_now_iso(),
        rows_written=result["rows_written"],
        source="dnse",
        last_error=None,
    )
    logger.info("%s DNSE wrote %s rows latest=%s", symbol, result["rows_written"], result["latest_time"])


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default="VN30F1M")
    parser.add_argument("--backfill-start", default=None)
    args = parser.parse_args()

    config = load_yaml("symbols.vn_intraday.yml")
    start_default = args.backfill_start or config.get("futures_backfill_start", "2024-05-01")
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)
    limiter = SlidingWindowRateLimiter(max_calls=config.get("dnse_max_calls_per_hour", 900), period_seconds=3600)

    while True:
        if args.mode == "live" and not is_derivative_session(vn_now()):
            sleep_for = seconds_until_next_session(derivative=True)
            logger.info("VN derivatives market closed, sleeping %ss", sleep_for)
            heartbeat.beat(status="sleeping", sleep_seconds=sleep_for)
            time.sleep(sleep_for)
            continue
        for symbol in symbols:
            try:
                run_symbol(symbol, start_default=start_default, limiter=limiter, logger=logger)
                heartbeat.beat(symbol=symbol)
            except Exception as exc:
                Manifest(DATASET).update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
                logger.exception("%s DNSE failed", symbol)
                heartbeat.beat(status="error", symbol=symbol, error=str(exc))
        if args.mode != "live":
            break
        time.sleep(65 + random.randint(0, 5))


if __name__ == "__main__":
    main()
