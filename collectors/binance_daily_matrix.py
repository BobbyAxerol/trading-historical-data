from __future__ import annotations

import argparse
import math
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from collectors.common.env import load_environment, data_root
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.locks import FileLock

DATASET = "binance_daily_matrix"
BINANCE_FAPI = "https://fapi.binance.com"
FEATURES = ["open", "high", "low", "close", "volume"]
DEFAULT_OVERLAP_DAYS = 5
DEFAULT_MIN_HISTORY_DAYS = 365
VOLUME_STABILITY_LOOKBACK_DAYS = 180
MIN_STABILITY_OBSERVATIONS = 90
UNIVERSE_SCORE_WEIGHTS = {
    "quote_volume": 0.50,
    "age": 0.30,
    "volume_stability": 0.20,
}
EXCLUDED_UNDERLYING_SUBTYPES = {"Alpha", "Index", "TradFi"}
CORE_SYMBOL_ORDER = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
    "DOTUSDT", "LINKUSDT", "TRXUSDT", "MATICUSDT", "SHIBUSDT", "ATOMUSDT", "UNIUSDT", "TONUSDT",
    "APTUSDT", "NEARUSDT", "ALGOUSDT", "FTMUSDT", "PEPEUSDT", "WBTCUSDT", "BCHUSDT", "LTCUSDT",
    "KASUSDT", "ICPUSDT", "XMRUSDT", "ETCUSDT", "HBARUSDT", "STXUSDT", "XLMUSDT", "RNDRUSDT",
    "CROUSDT", "ARBUSDT", "IMXUSDT", "MKRUSDT", "FILUSDT", "INJUSDT", "OPUSDT", "VETUSDT",
    "MNTUSDT", "ARUSDT", "GRTUSDT", "FLOKIUSDT", "THETAUSDT", "RUNEUSDT", "LDOUSDT", "COREUSDT",
    "BONKUSDT", "AAVEUSDT",
]


def _get_active_symbol_meta() -> dict[str, dict[str, Any]]:
    """Retrieve active USD-M crypto perpetual metadata from Binance exchangeInfo."""
    def call() -> dict[str, Any]:
        res = requests.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo", timeout=30)
        res.raise_for_status()
        return res.json()

    data = retry_sync(call, attempts=5)
    return {
        s["symbol"]: s
        for s in data.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("symbol", "").endswith("USDT")
        and s.get("quoteAsset") == "USDT"
        and s.get("marginAsset") == "USDT"
    }


def _is_eligible_crypto_symbol(meta: dict[str, Any], *, min_history_days: int, now_utc: datetime) -> tuple[bool, str]:
    """Return whether a Binance USD-M symbol belongs in the crypto backtest universe."""
    if meta.get("contractType") != "PERPETUAL":
        return False, f"contractType={meta.get('contractType')}"
    if meta.get("underlyingType") != "COIN":
        return False, f"underlyingType={meta.get('underlyingType')}"

    subtypes = set(meta.get("underlyingSubType") or [])
    excluded = sorted(subtypes & EXCLUDED_UNDERLYING_SUBTYPES)
    if excluded:
        return False, f"underlyingSubType={','.join(excluded)}"

    onboard_ms = meta.get("onboardDate")
    if min_history_days > 0 and onboard_ms:
        onboard_dt = datetime.fromtimestamp(int(onboard_ms) / 1000, tz=timezone.utc)
        age_days = (now_utc - onboard_dt).days
        if age_days < min_history_days:
            return False, f"history_days={age_days}<min_history_days={min_history_days}"

    return True, "eligible"


def _eligible_symbol_sets(
    active_meta: dict[str, dict[str, Any]],
    *,
    min_history_days: int,
    now_utc: datetime,
) -> tuple[set[str], dict[str, str]]:
    eligible: set[str] = set()
    rejected: dict[str, str] = {}
    for symbol, meta in active_meta.items():
        ok, reason = _is_eligible_crypto_symbol(meta, min_history_days=min_history_days, now_utc=now_utc)
        if ok:
            eligible.add(symbol)
        else:
            rejected[symbol] = reason
    return eligible, rejected


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _symbol_age_days(meta: dict[str, Any], now_utc: datetime) -> int:
    onboard_ms = meta.get("onboardDate")
    if not onboard_ms:
        return 0
    onboard_dt = datetime.fromtimestamp(int(onboard_ms) / 1000, tz=timezone.utc)
    return max((now_utc - onboard_dt).days, 0)


def _fetch_volume_stability(symbol: str) -> float:
    """Score recent daily volume consistency using the legacy universe script's idea."""
    def call() -> list[list[Any]]:
        res = requests.get(
            f"{BINANCE_FAPI}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "1d",
                "limit": VOLUME_STABILITY_LOOKBACK_DAYS,
            },
            timeout=30,
        )
        if res.status_code in {418, 429} or res.status_code >= 500:
            raise RuntimeError(f"Binance retryable HTTP {res.status_code}")
        res.raise_for_status()
        return res.json()

    try:
        data = retry_sync(call, attempts=5)
    except Exception:
        return 0.0

    volume = pd.Series([_safe_float(row[5]) for row in data if len(row) > 5])
    volume = volume[volume > 0]
    if len(volume) < MIN_STABILITY_OBSERVATIONS:
        return 0.0

    log_volume = volume.map(math.log1p)
    std = float(log_volume.std())
    if math.isnan(std):
        return 0.0
    return float(log_volume.mean() / (std + 1e-9))


def _get_top_symbols(
    active_meta: dict[str, dict[str, Any]],
    eligible_set: set[str],
    *,
    top_n: int = 400,
    now_utc: datetime,
    logger,
) -> list[str]:
    """Retrieve top symbols using liquidity, age, and recent volume stability."""
    def call() -> list[dict[str, Any]]:
        res = requests.get(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", timeout=30)
        res.raise_for_status()
        return res.json()

    tickers = retry_sync(call, attempts=5)

    rows = []
    for t in tickers:
        symbol = t.get("symbol", "")
        if symbol not in eligible_set:
            continue

        quote_volume = _safe_float(t.get("quoteVolume"))
        if quote_volume <= 0:
            continue

        rows.append({
            "symbol": symbol,
            "quote_volume": quote_volume,
            "age_days": _symbol_age_days(active_meta.get(symbol, {}), now_utc),
            "volume_stability": _fetch_volume_stability(symbol),
        })
        time.sleep(0.05)

    if not rows:
        return []

    df = pd.DataFrame(rows)
    df["quote_volume_rank"] = df["quote_volume"].rank(pct=True)
    df["age_rank"] = df["age_days"].rank(pct=True)
    df["volume_stability_rank"] = df["volume_stability"].rank(pct=True)
    df["score"] = (
        df["quote_volume_rank"] * UNIVERSE_SCORE_WEIGHTS["quote_volume"]
        + df["age_rank"] * UNIVERSE_SCORE_WEIGHTS["age"]
        + df["volume_stability_rank"] * UNIVERSE_SCORE_WEIGHTS["volume_stability"]
    )
    df = df.sort_values(["score", "quote_volume"], ascending=False)

    selected = df["symbol"].head(top_n).tolist()
    logger.info(
        "Selected %d Binance daily symbols by score; top=%s",
        len(selected),
        df[["symbol", "quote_volume", "age_days", "volume_stability", "score"]].head(10).to_dict("records"),
    )
    return selected


def _ordered_symbols(top_symbols: list[str], existing_symbols: list[str], eligible_set: set[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    for symbol in CORE_SYMBOL_ORDER:
        if symbol in eligible_set and symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)

    for symbol in top_symbols:
        if symbol in eligible_set and symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)

    for symbol in existing_symbols:
        if symbol in eligible_set and symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)

    return ordered


def update_master_symbol_list(logger, *, top_n: int = 400, min_history_days: int = DEFAULT_MIN_HISTORY_DAYS) -> list[str]:
    """Update master symbol list monthly: only add new eligible crypto symbols.

    The state keeps old symbols so existing matrix columns remain queryable for
    backtests. Symbols outside the crypto-perpetual universe are rejected and
    removed from the daily crypto matrix because they violate the dataset schema.
    """
    state_handler = JsonState("binance_daily_matrix_symbols.json")
    state = state_handler.read()

    existing_symbols = [str(symbol).upper() for symbol in state.get("symbols", [])]
    last_updated_month = state.get("last_updated_month", "")

    now_utc = datetime.now(timezone.utc)
    current_month = now_utc.strftime("%Y-%m")

    # Get currently active trading metadata and derive the eligible crypto universe.
    try:
        active_meta = _get_active_symbol_meta()
    except Exception as exc:
        logger.error("Failed to fetch active symbols: %s", exc)
        return existing_symbols

    eligible_set, rejected_reasons = _eligible_symbol_sets(
        active_meta,
        min_history_days=min_history_days,
        now_utc=now_utc,
    )
    rejected_existing = {symbol: rejected_reasons[symbol] for symbol in existing_symbols if symbol in rejected_reasons}
    eligible_existing = [symbol for symbol in existing_symbols if symbol in eligible_set]
    universe_policy = {
        "top_n": top_n,
        "rank_by": "score",
        "score_weights": UNIVERSE_SCORE_WEIGHTS,
        "score_inputs": {
            "quote_volume": "24h quoteVolume",
            "age": "onboardDate age_days",
            "volume_stability": f"{VOLUME_STABILITY_LOOKBACK_DAYS}d log1p(base_volume) mean/std",
        },
        "min_stability_observations": MIN_STABILITY_OBSERVATIONS,
        "order": "core_symbols_then_score_desc_then_existing",
        "core_symbols": CORE_SYMBOL_ORDER,
        "contractType": "PERPETUAL",
        "underlyingType": "COIN",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "excluded_underlyingSubTypes": sorted(EXCLUDED_UNDERLYING_SUBTYPES),
        "min_history_days": min_history_days,
    }
    policy_changed = state.get("universe_policy") != universe_policy

    # Monthly update or first-time generation
    if policy_changed or not eligible_existing or current_month != last_updated_month:
        logger.info("Updating master symbol list for month: %s", current_month)
        try:
            top_symbols = _get_top_symbols(
                active_meta,
                eligible_set,
                top_n=top_n,
                now_utc=now_utc,
                logger=logger,
            )
            existing_symbols = _ordered_symbols(top_symbols, eligible_existing, eligible_set)
            last_updated_month = current_month
        except Exception as exc:
            logger.error("Failed to score Binance daily universe: %s", exc)
            existing_symbols = eligible_existing
    else:
        existing_symbols = eligible_existing

    active_symbols = [s for s in existing_symbols if s in eligible_set]
    inactive_symbols = sorted(set(existing_symbols) - eligible_set)

    # Write state back
    state_handler.write({
        "last_updated_month": last_updated_month,
        "symbols": existing_symbols,
        "active_symbols": active_symbols,
        "inactive_symbols": inactive_symbols,
        "rejected_symbols": rejected_existing,
        "universe_policy": universe_policy,
        "updated_at": utc_now_iso(),
    })

    logger.info(
        "Master symbols count: %d tracked, %d active, %d inactive, %d rejected existing",
        len(existing_symbols),
        len(active_symbols),
        len(inactive_symbols),
        len(rejected_existing),
    )
    return active_symbols


def fetch_daily_klines(symbol: str, start: pd.Timestamp, end: pd.Timestamp, logger) -> pd.DataFrame:
    """Fetch daily klines for a single symbol from start to end (end inclusive)."""
    rows = []
    start = pd.Timestamp(start).normalize()
    end = pd.Timestamp(end).normalize()
    start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
    end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
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
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None).dt.normalize()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["symbol"] = symbol
    return df[["time", "symbol", "open", "high", "low", "close", "volume"]].dropna(subset=["time"])


def _closed_daily_until() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").normalize().tz_convert(None) - pd.Timedelta(days=1)


def _load_matrix(path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, compression="gzip", index_col=0)
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()]
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def _symbol_fetch_start(
    existing_df: pd.DataFrame,
    symbol: str,
    *,
    backfill_start: pd.Timestamp,
    end: pd.Timestamp,
    overlap_days: int,
) -> pd.Timestamp | None:
    if symbol not in existing_df.columns:
        return backfill_start

    series = pd.to_numeric(existing_df[symbol], errors="coerce").dropna()
    if series.empty:
        return backfill_start

    first = pd.Timestamp(series.index.min()).normalize()
    latest = pd.Timestamp(series.index.max()).normalize()

    if first > backfill_start:
        return backfill_start

    gaps = series.index.to_series().sort_values().diff().dropna()
    big_gaps = gaps[gaps > pd.Timedelta(days=1)]
    if not big_gaps.empty:
        first_gap_idx = big_gaps.index[0]
        prev_pos = series.index.get_loc(first_gap_idx) - 1
        return max(backfill_start, pd.Timestamp(series.index[prev_pos]).normalize() - pd.Timedelta(days=overlap_days))

    if latest < end:
        return max(backfill_start, latest - pd.Timedelta(days=overlap_days))

    return max(backfill_start, latest - pd.Timedelta(days=overlap_days))


def run_pipeline(
    backfill_start_str: str,
    logger,
    *,
    top_n: int = 400,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
) -> None:
    # 1. Update/get master symbols list
    symbols = update_master_symbol_list(logger, top_n=top_n, min_history_days=min_history_days)
    if not symbols:
        logger.warning("No symbols to fetch.")
        return

    # Define matrix directories & file paths
    matrix_dir = data_root() / "crypto" / "binance_daily_matrix"
    matrix_dir.mkdir(parents=True, exist_ok=True)
    
    paths = {f: matrix_dir / f"{f}.csv.gz" for f in FEATURES}

    # 2. Determine per-symbol fetch windows from existing open matrix
    existing_open = _load_matrix(paths["open"])
    backfill_start = pd.Timestamp(backfill_start_str).normalize()
    end = _closed_daily_until()

    if backfill_start > end:
        logger.info("No closed daily candles to fetch: backfill_start=%s end=%s", backfill_start, end)
        return
    
    # 3. Fetch data for each symbol
    dfs = []
    logger.info("Fetching daily klines for %d symbols...", len(symbols))
    
    for i, symbol in enumerate(symbols):
        start = _symbol_fetch_start(
            existing_open,
            symbol,
            backfill_start=backfill_start,
            end=end,
            overlap_days=overlap_days,
        )
        if start is None or start > end:
            continue

        try:
            df = fetch_daily_klines(symbol, start, end, logger)
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
    all_df["time"] = pd.to_datetime(all_df["time"], errors="coerce").dt.normalize()
    all_df = all_df.dropna(subset=["time", "symbol"])
    all_df = all_df[all_df["time"] <= end]
    all_df = all_df.drop_duplicates(subset=["time", "symbol"], keep="last")
    
    # Process each feature matrix under lock
    with FileLock("binance_daily_matrix"):
        for feature in FEATURES:
            path = paths[feature]
            pivoted_new = all_df.pivot(index="time", columns="symbol", values=feature)

            # Cast data types properly
            if feature == "volume":
                pivoted_new = pivoted_new.fillna(0).astype("int64")
            else:
                pivoted_new = pivoted_new.astype("float64")

            if path.exists():
                try:
                    existing_df = _load_matrix(path)
                    # Align indices & columns, combining df with prioritized new data
                    combined = pivoted_new.combine_first(existing_df)
                except Exception as exc:
                    logger.error("Error loading %s, overwriting: %s", path.name, exc)
                    combined = pivoted_new
            else:
                combined = pivoted_new

            # Sort columns and index
            combined.index = pd.to_datetime(combined.index, errors="coerce")
            combined = combined[~combined.index.isna()]
            combined = combined[combined.index <= end]
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
            combined = combined.reindex(symbols, axis=1)
            combined.index = combined.index.strftime("%Y-%m-%d")

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
    parser.add_argument("--top-n", type=int, default=400)
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS)
    parser.add_argument("--min-history-days", type=int, default=DEFAULT_MIN_HISTORY_DAYS)
    args = parser.parse_args()

    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)
    last_run_date: str | None = None

    while True:
        if args.schedule:
            if should_run_utc(args.schedule, last_run_date):
                try:
                    logger.info("Starting daily matrix pipeline...")
                    run_pipeline(
                        args.backfill_start,
                        logger,
                        top_n=args.top_n,
                        overlap_days=args.overlap_days,
                        min_history_days=args.min_history_days,
                    )
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
            run_pipeline(
                args.backfill_start,
                logger,
                top_n=args.top_n,
                overlap_days=args.overlap_days,
                min_history_days=args.min_history_days,
            )
            heartbeat.beat(status="success")
        except Exception as exc:
            logger.exception("Matrix pipeline failed")
            heartbeat.beat(status="error", error=str(exc))
        break


if __name__ == "__main__":
    main()
