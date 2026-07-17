from __future__ import annotations

import argparse
import os
import time
from datetime import datetime

import pandas as pd

from collectors.common.calendar_vn import is_stock_session, seconds_until_next_session, vn_now
from collectors.common.config import load_yaml
from collectors.common.discovery import latest_time_from_files, max_timestamp
from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, Manifest, utc_now_iso
from collectors.common.retry import SlidingWindowRateLimiter, retry_sync
from collectors.common.storage import PartitionedParquetStore

DATASET = "vn_equity_1m"


def default_symbols() -> list[str]:
    return [
        "FPT",
        "ACB",
        "TCB",
        "BID",
        "CTG",
        "VCB",
        "HDB",
        "MBB",
        "TPB",
        "STB",
        "VIB",
        "VHM",
        "VIC",
        "VRE",
        "POW",
        "DIG",
        "HPG",
        "HSG",
        "PDR",
        "DGC",
    ]


def authenticate() -> None:
    try:
        from vnstock import register_user

        api_key = os.getenv("VNSTOCK_API_KEY")
        if api_key:
            register_user(api_key=api_key)
        else:
            register_user()
    except Exception:
        pass


def fetch_symbol(symbol: str, start: str, end: str, source: str) -> pd.DataFrame:
    from vnstock import Quote
    from collectors.common.calendar_vn import filter_trading_hours

    def call() -> pd.DataFrame:
        quote = Quote(symbol=symbol, source=source, show_log=False)
        return quote.history(start=start, end=end, interval="1m", show_log=False)

    df = retry_sync(call, attempts=3, base_sleep=2)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()

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
    df["source"] = f"vnstock_{source.lower()}"
    df["ingested_at"] = utc_now_iso()

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df[df["volume"] >= 0]

    cols = ["time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]
    df = df[cols]

    return filter_trading_hours(df, derivative=False)



def run_symbol(symbol: str, *, source: str, start_default: str, limiter: SlidingWindowRateLimiter, logger) -> None:
    manifest = Manifest(DATASET)
    state = manifest.symbol_state(symbol)
    store = PartitionedParquetStore(["vn", "equity", "1m"], partition="month")
    storage_latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    legacy_latest = latest_time_from_files(
        [
            GET_DATA_ROOT / "data_stock" / "_intraday_storage" / "stocks" / f"{symbol}_1m.csv.gz",
            GET_DATA_ROOT.parent / "data_stock" / "_intraday_storage" / "stocks" / f"{symbol}_1m.csv.gz",
        ],
        ["time"],
    )
    discovered_latest = max_timestamp(state.get("latest_time"), storage_latest, legacy_latest)
    if discovered_latest is not None:
        start = (discovered_latest - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
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
    end = datetime.now().strftime("%Y-%m-%d")

    limiter.wait()
    logger.info("Fetching %s 1m %s -> %s source=%s", symbol, start, end, source)
    df = fetch_symbol(symbol, start, end, source)
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_response", last_success_at=utc_now_iso())
        logger.warning("%s 1m returned no rows", symbol)
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
        source=f"vnstock_{source.lower()}",
        last_error=None,
    )
    logger.info("%s 1m wrote %s rows latest=%s", symbol, result["rows_written"], result["latest_time"])


def should_run(schedule_hhmm: str, last_run_date: str | None) -> bool:
    now = vn_now()
    if last_run_date == now.strftime("%Y-%m-%d"):
        return False
    hh, mm = [int(x) for x in schedule_hhmm.split(":")]
    return now.hour > hh or (now.hour == hh and now.minute >= mm)


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--source", default="vci")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--backfill-start", default=None)
    parser.add_argument("--schedule", default=None)
    args = parser.parse_args()

    config = load_yaml("symbols.vn_intraday.yml")
    symbols = args.symbols.split(",") if args.symbols else config.get("stocks") or default_symbols()
    if args.max_symbols:
        symbols = symbols[: args.max_symbols]
    start_default = args.backfill_start or config.get("backfill_start", "2020-01-01")
    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)
    limiter = SlidingWindowRateLimiter(max_calls=config.get("vnstock_max_calls_per_minute", 18), period_seconds=60)
    authenticate()
    last_run_date: str | None = None

    while True:
        if args.schedule:
            if should_run(args.schedule, last_run_date):
                for symbol in symbols:
                    try:
                        run_symbol(symbol.strip().upper(), source=args.source, start_default=start_default, limiter=limiter, logger=logger)
                        heartbeat.beat(symbol=symbol)
                    except Exception as exc:
                        Manifest(DATASET).update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
                        logger.exception("%s 1m failed", symbol)
                        heartbeat.beat(status="error", symbol=symbol, error=str(exc))
                last_run_date = vn_now().strftime("%Y-%m-%d")
            if args.mode != "live":
                break
            time.sleep(300)
            continue

        if args.mode == "live" and not is_stock_session(vn_now()):
            sleep_for = seconds_until_next_session()
            logger.info("VN stock market closed, sleeping %ss", sleep_for)
            heartbeat.beat(status="sleeping", sleep_seconds=sleep_for)
            time.sleep(sleep_for)
            continue

        for symbol in symbols:
            try:
                run_symbol(symbol.strip().upper(), source=args.source, start_default=start_default, limiter=limiter, logger=logger)
                heartbeat.beat(symbol=symbol)
            except Exception as exc:
                Manifest(DATASET).update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
                logger.exception("%s 1m failed", symbol)
                heartbeat.beat(status="error", symbol=symbol, error=str(exc))
        if args.mode != "live":
            break
        time.sleep(60)



if __name__ == "__main__":
    main()
