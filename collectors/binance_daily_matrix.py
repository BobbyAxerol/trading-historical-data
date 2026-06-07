from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from collectors.common.env import GET_DATA_ROOT, load_environment, data_root
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.locks import FileLock

DATASET = "binance_daily_matrix"
BINANCE_FAPI = "https://fapi.binance.com"


def _get_active_symbols() -> set[str]:
    """Retrieve active trading symbols ending in USDT from Binance USD-M Futures exchangeInfo."""
    def call() -> dict[str, Any]:
        res = requests.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo", timeout=30)
        res.raise_for_status()
        return res.json()

    data = retry_sync(call, attempts=5)
    symbols = {
        s["symbol"] for s in data.get("symbols", [])
        if s.get("status") == "TRADING" and s.get("symbol", "").endswith("USDT")
    }
    return symbols


def _get_top_symbols(active_set: set[str], top_n: int = 400) -> list[str]:
    """Retrieve top N active trading symbols sorted by 24h quoteVolume."""
    def call() -> list[dict[str, Any]]:
        res = requests.get(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", timeout=30)
        res.raise_for_status()
        return res.json()

    tickers = retry_sync(call, attempts=5)
    
    # Filter for active USDT symbols and parse quoteVolume
    valid_tickers = []
    for t in tickers:
        symbol = t.get("symbol", "")
        if symbol in active_set:
            try:
                volume = float(t.get("quoteVolume", 0.0))
            except Exception:
                volume = 0.0
            valid_tickers.append((symbol, volume))

    # Sort descending by volume
    valid_tickers.sort(key=lambda x: x[1], reverse=True)
    return [item[0] for item in valid_tickers[:top_n]]


def update_master_symbol_list(logger) -> list[str]:
    """Update master symbol list monthly: only add new top symbols, do not remove unless delisted."""
    state_handler = JsonState("binance_daily_matrix_symbols.json")
    state = state_handler.read()

    existing_symbols = state.get("symbols", [])
    last_updated_month = state.get("last_updated_month", "")

    now_utc = datetime.now(timezone.utc)
    current_month = now_utc.strftime("%Y-%m")

    # Get currently active trading symbols
    try:
        active_set = _get_active_symbols()
    except Exception as exc:
        logger.error("Failed to fetch active symbols: %s", exc)
        return existing_symbols

    # Monthly update or first-time generation
    if not existing_symbols or current_month != last_updated_month:
        logger.info("Updating master symbol list for month: %s", current_month)
        try:
            top_symbols = _get_top_symbols(active_set, top_n=400)
            # Merge: add new top symbols to existing ones
            combined_set = set(existing_symbols) | set(top_symbols)
            existing_symbols = sorted(list(combined_set))
            last_updated_month = current_month
        except Exception as exc:
            logger.error("Failed to fetch top 24h ticker info: %s", exc)

    # Filter: remove any symbols that are no longer active/delisted
    final_symbols = [s for s in existing_symbols if s in active_set]

    # Write state back
    state_handler.write({
        "last_updated_month": last_updated_month,
        "symbols": final_symbols,
        "updated_at": utc_now_iso(),
    })

    logger.info("Master symbols count: %d (active and tracked)", len(final_symbols))
    return final_symbols


def fetch_daily_klines(symbol: str, start: pd.Timestamp, end: pd.Timestamp, logger) -> pd.DataFrame:
    """Fetch daily klines for a single symbol from start to end (end inclusive)."""
    rows = []
    cursor = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    while cursor <= end_ms:
        def call() -> list[list[Any]]:
            res = requests.get(
                f"{BINANCE_FAPI}/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": "1d",
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1500,
                },
                timeout=30,
            )
            if res.status_code in {418, 429} or res.status_code >= 500:
                raise RuntimeError(f"Binance retryable HTTP {res.status_code}")
            res.raise_for_status()
            return res.json()

        batch = retry_sync(call, attempts=5)
        if not batch:
            break
        rows.extend(batch)
        next_cursor = int(batch[-1][0]) + 86400_000  # 1 day in ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume", 
        "close_time", "quote_volume", "number_of_trades", 
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None).dt.strftime("%Y-%m-%d")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["symbol"] = symbol
    return df[["time", "symbol", "open", "high", "low", "close", "volume"]].dropna(subset=["time"])


def run_pipeline(backfill_start_str: str, logger) -> None:
    # 1. Update/get master symbols list
    symbols = update_master_symbol_list(logger)
    if not symbols:
        logger.warning("No symbols to fetch.")
        return

    # Define matrix directories & file paths
    matrix_dir = data_root() / "crypto" / "binance_daily_matrix"
    matrix_dir.mkdir(parents=True, exist_ok=True)
    
    features = ["open", "high", "low", "close", "volume"]
    paths = {f: matrix_dir / f"{f}.csv.gz" for f in features}

    # 2. Determine latest timestamp from existing matrices
    latest_date: pd.Timestamp | None = None
    existing_cols: set[str] = set()

    open_path = paths["open"]
    if open_path.exists():
        try:
            existing_df = pd.read_csv(open_path, compression="gzip", index_col=0)
            if not existing_df.empty:
                latest_date = pd.to_datetime(existing_df.index.max())
                existing_cols = set(existing_df.columns)
        except Exception as exc:
            logger.error("Failed to read existing matrix files: %s", exc)

    now_utc = pd.Timestamp.now(tz="UTC").tz_convert(None)
    
    # 3. Fetch data for each symbol
    dfs = []
    logger.info("Fetching daily klines for %d symbols...", len(symbols))
    
    for i, symbol in enumerate(symbols):
        # If the symbol already exists in matrix, we fetch incrementally
        if latest_date is not None and symbol in existing_cols:
            start = latest_date - pd.Timedelta(days=5)
        else:
            # New symbol or first-time backfill
            start = pd.Timestamp(backfill_start_str)

        if start >= now_utc:
            continue

        try:
            df = fetch_daily_klines(symbol, start, now_utc, logger)
            if not df.empty:
                dfs.append(df)
            if (i + 1) % 50 == 0:
                logger.info("Fetched %d/%d symbols...", i + 1, len(symbols))
        except Exception as exc:
            logger.error("Failed to fetch daily klines for %s: %s", symbol, exc)

    if not dfs:
        logger.info("No new daily data fetched.")
        return

    # 4. Concatenate and Pivot
    all_df = pd.concat(dfs, ignore_index=True)
    
    # Process each feature matrix under lock
    with FileLock("binance_daily_matrix"):
        for feature in features:
            path = paths[feature]
            pivoted_new = all_df.pivot(index="time", columns="symbol", values=feature)

            # Cast data types properly
            if feature == "volume":
                pivoted_new = pivoted_new.fillna(0).astype("int64")
            else:
                pivoted_new = pivoted_new.astype("float64")

            if path.exists():
                try:
                    existing_df = pd.read_csv(path, compression="gzip", index_col=0)
                    # Align indices & columns, combining df with prioritized new data
                    combined = pivoted_new.combine_first(existing_df)
                except Exception as exc:
                    logger.error("Error loading %s, overwriting: %s", path.name, exc)
                    combined = pivoted_new
            else:
                combined = pivoted_new

            # Sort columns and index
            combined = combined.sort_index()
            combined = combined.reindex(sorted(combined.columns), axis=1)

            # Write atomically
            tmp = path.with_suffix(".tmp")
            combined.to_csv(tmp, compression="gzip")
            tmp.replace(path)
            
            logger.info("Wrote matrix %s: shape=%s", path.name, combined.shape)


def should_run_utc(schedule_hhmm: str, last_run_date: str | None) -> bool:
    now = datetime.now(timezone.utc)
    if last_run_date == now.strftime("%Y-%m-%d"):
        return False
    hh, mm = [int(x) for x in schedule_hhmm.split(":")]
    return now.hour > hh or (now.hour == hh and now.minute >= mm)


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Binance Futures Top 400 Daily Matrix Collector")
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--schedule", default=None, help="Schedule run daily at HH:MM UTC")
    parser.add_argument("--backfill-start", default="2020-01-01")
    args = parser.parse_args()

    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)
    last_run_date: str | None = None

    while True:
        if args.schedule:
            if should_run_utc(args.schedule, last_run_date):
                try:
                    logger.info("Starting daily matrix pipeline...")
                    run_pipeline(args.backfill_start, logger)
                    heartbeat.beat(status="success")
                    logger.info("Daily matrix pipeline finished successfully.")
                except Exception as exc:
                    logger.exception("Daily matrix pipeline failed")
                    heartbeat.beat(status="error", error=str(exc))
                last_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if args.mode != "live":
                break
            time.sleep(300)
            continue

        # Mode once / run immediately
        try:
            run_pipeline(args.backfill_start, logger)
            heartbeat.beat(status="success")
        except Exception as exc:
            logger.exception("Matrix pipeline failed")
            heartbeat.beat(status="error", error=str(exc))
        break


if __name__ == "__main__":
    main()
