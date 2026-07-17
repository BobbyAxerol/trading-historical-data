from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta

import pandas as pd

from collectors.common.config import load_yaml
from collectors.common.discovery import latest_time_from_files, max_timestamp
from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, Manifest, utc_now_iso
from collectors.common.retry import SlidingWindowRateLimiter, retry_sync
from collectors.common.storage import PartitionedParquetStore
from collectors.vn_daily_matrix import build_matrix

DATASET = "vn_equity_1d"


def default_symbols() -> list[str]:
    try:
        from get_multiple_stock_1d import HOSE_300_SYMBOLS

        return list(HOSE_300_SYMBOLS)
    except Exception:
        return ["FPT", "VCB", "HPG", "SSI", "MBB", "TCB"]


def _effective_end_date() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def fetch_symbol(symbol: str, start: str, end: str) -> pd.DataFrame:
    from vnstock.explorer.vci import Quote as VCIQuote

    def call() -> pd.DataFrame:
        quote = VCIQuote(symbol, show_log=False)
        return quote.history(start=start, end=end, interval="1D", show_log=False)

    df = retry_sync(call, attempts=5, base_sleep=2)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    start_ts = pd.to_datetime(start, errors="coerce").normalize()
    end_ts = pd.to_datetime(end, errors="coerce").normalize()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")
    df["time"] = df["time"].dt.normalize()
    if pd.notna(start_ts):
        df = df[df["time"] >= start_ts]
    if pd.notna(end_ts):
        df = df[df["time"] <= end_ts]
    if df.empty:
        return pd.DataFrame()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    prices = df[["open", "high", "low", "close"]]
    df["high"] = prices.max(axis=1, skipna=False)
    df["low"] = prices.min(axis=1, skipna=False)
    df["symbol"] = symbol
    df["source"] = "vnstock_vci"
    df["ingested_at"] = utc_now_iso()
    return df[["time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]]


def run_symbol(symbol: str, *, start_default: str, end: str, limiter: SlidingWindowRateLimiter, logger) -> None:
    manifest = Manifest(DATASET)
    state = manifest.symbol_state(symbol)
    store = PartitionedParquetStore(["vn", "equity", "1d"], partition="year")
    storage_latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    legacy_latest = latest_time_from_files(
        [
            GET_DATA_ROOT / "data_stock" / f"{symbol}_1d_max.csv.gz",
            GET_DATA_ROOT.parent / "data_stock" / f"{symbol}_1d_max.csv.gz",
        ],
        ["time"],
    )
    discovered_latest = max_timestamp(state.get("latest_time"), storage_latest, legacy_latest)
    if discovered_latest is not None:
        start = (discovered_latest - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
        if not state.get("latest_time") or discovered_latest > pd.Timestamp(state["latest_time"]):
            manifest.update_symbol(
                symbol,
                latest_time=discovered_latest.isoformat(),
                discovered_from_tail=True,
                legacy_latest=legacy_latest.isoformat() if legacy_latest is not None else None,
                storage_latest=storage_latest.isoformat() if storage_latest is not None else None,
            )
    else:
        start = start_default

    if start > end:
        logger.info("%s daily already current", symbol)
        return

    limiter.wait()
    logger.info("Fetching %s daily %s -> %s", symbol, start, end)
    df = fetch_symbol(symbol, start, end)
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_response", last_success_at=utc_now_iso())
        logger.warning("%s daily returned no rows", symbol)
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
        source="vnstock_vci",
        last_error=None,
    )
    logger.info("%s daily wrote %s rows latest=%s", symbol, result["rows_written"], result["latest_time"])


def should_run(schedule_hhmm: str, last_run_date: str | None) -> bool:
    now = datetime.now()
    if last_run_date == now.strftime("%Y-%m-%d"):
        return False
    hh, mm = [int(x) for x in schedule_hhmm.split(":")]
    return now.hour > hh or (now.hour == hh and now.minute >= mm)


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--schedule", default="16:30")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--backfill-start", default=None)
    args = parser.parse_args()

    config = load_yaml("symbols.vn_daily.yml")
    symbols = args.symbols.split(",") if args.symbols else config.get("symbols") or default_symbols()
    if args.max_symbols:
        symbols = symbols[: args.max_symbols]
    start_default = args.backfill_start or config.get("backfill_start", "2016-01-01")
    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)
    limiter = SlidingWindowRateLimiter(max_calls=config.get("max_calls_per_minute", 18), period_seconds=60)
    schedule_state = JsonState("vn_daily_schedule.json")
    schedule = schedule_state.read()
    last_run_date: str | None = schedule.get("last_run_date")

    while True:
        if args.mode == "once" or should_run(args.schedule, last_run_date):
            end = _effective_end_date()
            for symbol in symbols:
                try:
                    run_symbol(symbol.strip().upper(), start_default=start_default, end=end, limiter=limiter, logger=logger)
                    heartbeat.beat(symbol=symbol)
                except Exception as exc:
                    Manifest(DATASET).update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
                    logger.exception("%s daily failed", symbol)
                    heartbeat.beat(status="error", symbol=symbol, error=str(exc))
            try:
                build_matrix(symbols=[s.strip().upper() for s in symbols], logger=logger)
            except Exception as exc:
                logger.exception("VN daily matrix build failed")
                heartbeat.beat(status="error", error=f"matrix_build_failed: {exc}")
            last_run_date = datetime.now().strftime("%Y-%m-%d")
            schedule_state.write({
                "last_run_date": last_run_date,
                "updated_at": utc_now_iso(),
                "symbols_count": len(symbols),
            })
        if args.mode != "live":
            break
        time.sleep(300)


if __name__ == "__main__":
    main()
