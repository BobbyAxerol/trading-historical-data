from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from collectors.common.config import load_yaml
from collectors.common.discovery import latest_time_from_files, max_timestamp
from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, Manifest, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.storage import PartitionedParquetStore

DATASET = "crypto_binance_futures_1m"
BINANCE_FAPI = "https://fapi.binance.com"


def _ms(value: pd.Timestamp) -> int:
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    return int(value.timestamp() * 1000)


def _closed_until() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("min") - pd.Timedelta(minutes=1)


def _request_klines(symbol: str, start_ms: int, end_ms: int, limit: int = 1500) -> list[list[Any]]:
    def call() -> list[list[Any]]:
        response = requests.get(
            f"{BINANCE_FAPI}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": limit,
            },
            timeout=30,
        )
        if response.status_code in {418, 429} or response.status_code >= 500:
            raise RuntimeError(f"Binance retryable HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        return response.json()

    return retry_sync(call, attempts=5)


def fetch_1m(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: list[list[Any]] = []
    cursor = _ms(start)
    end_ms = _ms(end)

    while cursor <= end_ms:
        batch = _request_klines(symbol, cursor, end_ms)
        if not batch:
            break
        rows.extend(batch)
        next_cursor = int(batch[-1][0]) + 60_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(None)
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce").astype("Int64")
    df["symbol"] = symbol
    df["source"] = "binance_futures"
    df["ingested_at"] = utc_now_iso()
    return df[
        [
            "time",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "source",
            "ingested_at",
        ]
    ].dropna(subset=["time", "open", "high", "low", "close"])


def run_symbol(symbol: str, mode: str, backfill_start: str, logger) -> None:
    manifest = Manifest(DATASET)
    state = manifest.symbol_state(symbol)
    store = PartitionedParquetStore(["crypto", "binance_futures", "1m"], partition="month")
    storage_latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    legacy_latest = latest_time_from_files(
        [
            GET_DATA_ROOT / "crypto_1m_data" / f"{symbol.lower()}_perpetual_1m.csv.gz",
            GET_DATA_ROOT / "crypto_1m_data" / f"{symbol}_1m.csv.gz",
            GET_DATA_ROOT / f"{symbol}_1m.csv.gz",
        ],
        ["time", "open_time"],
    )
    discovered_latest = max_timestamp(state.get("latest_time"), storage_latest, legacy_latest)

    if mode == "backfill":
        start = pd.Timestamp(backfill_start, tz="UTC")
    elif discovered_latest is not None:
        start = discovered_latest.tz_localize("UTC") - pd.Timedelta(minutes=10)
        if not state.get("latest_time") or discovered_latest > pd.Timestamp(state["latest_time"]):
            manifest.update_symbol(
                symbol,
                latest_time=discovered_latest.isoformat(),
                discovered_from_tail=True,
                legacy_latest=legacy_latest.isoformat() if legacy_latest is not None else None,
                storage_latest=storage_latest.isoformat() if storage_latest is not None else None,
            )
    else:
        start = pd.Timestamp(backfill_start, tz="UTC")

    end = _closed_until()
    if start > end:
        logger.info("%s already current: start=%s end=%s", symbol, start, end)
        return

    logger.info("Fetching %s %s -> %s", symbol, start, end)
    df = fetch_1m(symbol, start, end)
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_response", last_success_at=utc_now_iso())
        logger.warning("%s returned no rows", symbol)
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
        source="binance_futures",
        rows_written=result["rows_written"],
        last_error=None,
    )
    logger.info("%s wrote %s rows latest=%s", symbol, result["rows_written"], result["latest_time"])


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "once", "backfill"], default="once")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols override")
    parser.add_argument("--sleep", type=int, default=70)
    args = parser.parse_args()

    config = load_yaml("symbols.crypto.yml")
    symbols = args.symbols.split(",") if args.symbols else config.get("symbols", ["BTCUSDT", "ETHUSDT"])
    backfill_start = config.get("backfill_start", "2020-01-01")

    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)

    while True:
        for symbol in symbols:
            try:
                run_symbol(symbol.strip().upper(), args.mode, backfill_start, logger)
                heartbeat.beat(symbol=symbol)
            except Exception as exc:
                Manifest(DATASET).update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
                logger.exception("%s failed", symbol)
                heartbeat.beat(status="error", symbol=symbol, error=str(exc))
        if args.mode != "live":
            break
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()
