from __future__ import annotations

import argparse
import hashlib
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
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
    return pd.Timestamp.now(tz="UTC").normalize().tz_convert(None) - timedelta(days=1)


def _load_matrix(path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix == ".parquet":
        df = pd.read_parquet(path, engine="pyarrow")
    else:
        df = pd.read_csv(path, compression="gzip", index_col=0)
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()]
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def _select_matrix_path(matrix_dir, feature: str) -> Any:
    parquet_path = matrix_dir / f"{feature}.parquet"
    csv_path = matrix_dir / f"{feature}.csv.gz"
    if parquet_path.exists():
        if not csv_path.exists() or parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
            return parquet_path
    if csv_path.exists():
        return csv_path
    return parquet_path


def _atomic_write_matrix(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if path.suffix == ".parquet":
        df.to_parquet(tmp, engine="pyarrow", compression="zstd")
    else:
        df.to_csv(tmp, compression="gzip")
    os.replace(tmp, path)


def _sha256_file(path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_matrix_audit_report(
    matrix_dir,
    *,
    universe_state: dict[str, Any],
    backfill_start: pd.Timestamp,
    closed_end: pd.Timestamp,
    top_n: int,
    min_history_days: int,
) -> dict[str, Any]:
    """Validate the five persisted matrix features without inventing coverage.

    A missing history before an instrument's first direct Binance candle is
    valid listing evidence.  Once an OHLC candle exists, however, all OHLC
    values and every intervening UTC day through the closed-day tail must be
    present.  The collector writes this report before a consumer manifest can
    declare the matrix.
    """

    errors: list[str] = []
    matrices: dict[str, pd.DataFrame] = {}
    files: dict[str, dict[str, Any]] = {}
    backfill_start = pd.Timestamp(backfill_start).normalize()
    closed_end = pd.Timestamp(closed_end).normalize()

    for feature in FEATURES:
        path = matrix_dir / f"{feature}.parquet"
        file_info: dict[str, Any] = {"path": str(path), "exists": path.exists()}
        if not path.exists():
            errors.append(f"missing feature file: {feature}")
            files[feature] = file_info
            continue
        try:
            frame = pd.read_parquet(path, engine="pyarrow")
        except Exception as exc:
            errors.append(f"cannot read {feature}: {type(exc).__name__}: {exc}")
            files[feature] = file_info
            continue

        converted_index = pd.to_datetime(frame.index, errors="coerce")
        invalid_index_count = int(pd.isna(converted_index).sum())
        frame = frame.copy()
        frame.index = pd.DatetimeIndex(converted_index)
        if invalid_index_count:
            errors.append(f"{feature} has {invalid_index_count} invalid timestamp(s)")
        if frame.index.has_duplicates:
            errors.append(f"{feature} has duplicate timestamps")
        if not frame.index.is_monotonic_increasing:
            errors.append(f"{feature} timestamps are not monotonic")
        non_numeric_columns = [str(column) for column in frame.columns if not pd.api.types.is_numeric_dtype(frame[column])]
        if non_numeric_columns:
            errors.append(f"{feature} has non-numeric columns: {non_numeric_columns[:5]}")
        try:
            infinite_count = int(np.isinf(frame.to_numpy(dtype="float64", na_value=np.nan)).sum())
        except (TypeError, ValueError):
            infinite_count = -1
        if infinite_count > 0:
            errors.append(f"{feature} has {infinite_count} infinite value(s)")
        if frame.empty:
            errors.append(f"{feature} is empty")

        matrices[feature] = frame
        file_info.update(
            {
                "sha256": _sha256_file(path),
                "rows": int(len(frame)),
                "columns": int(len(frame.columns)),
                "first_time": frame.index.min().isoformat() if not frame.empty else None,
                "last_time": frame.index.max().isoformat() if not frame.empty else None,
                "invalid_index_count": invalid_index_count,
                "infinite_count": infinite_count,
            }
        )
        files[feature] = file_info

    base = matrices.get("open")
    if base is not None:
        for feature, frame in matrices.items():
            if not frame.index.equals(base.index):
                errors.append(f"{feature} index differs from open")
            if list(frame.columns) != list(base.columns):
                errors.append(f"{feature} columns differ from open")

    policy = universe_state.get("universe_policy") if isinstance(universe_state.get("universe_policy"), dict) else {}
    active_symbols = [str(symbol).upper() for symbol in universe_state.get("active_symbols", [])]
    tracked_symbols = [str(symbol).upper() for symbol in universe_state.get("symbols", [])]
    expected_policy = {
        "top_n": top_n,
        "contractType": "PERPETUAL",
        "underlyingType": "COIN",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "min_history_days": min_history_days,
    }
    for key, expected in expected_policy.items():
        if policy.get(key) != expected:
            errors.append(f"universe policy {key}={policy.get(key)!r}, expected {expected!r}")
    if not active_symbols:
        errors.append("universe has no active symbols")
    if len(active_symbols) != len(set(active_symbols)):
        errors.append("universe active_symbols contains duplicates")
    if tracked_symbols != active_symbols:
        errors.append("universe tracked symbols differ from active symbols")
    if any(not symbol.endswith("USDT") for symbol in active_symbols):
        errors.append("universe contains a non-USDT symbol")
    if base is not None and list(base.columns) != active_symbols:
        errors.append("matrix columns differ from the persisted eligible active universe")

    quality: dict[str, Any] = {
        "incomplete_ohlc_count": 0,
        "negative_price_count": 0,
        "ohlc_bound_error_count": 0,
        "negative_volume_count": 0,
        "zero_volume_count": 0,
        "continuity_gap_count": 0,
        "missing_tail_symbol_count": 0,
        "continuity_gap_examples": [],
        "head_after_backfill_symbol_count": 0,
    }
    if all(feature in matrices for feature in FEATURES):
        open_frame = matrices["open"]
        high_frame = matrices["high"]
        low_frame = matrices["low"]
        close_frame = matrices["close"]
        volume_frame = matrices["volume"]
        complete_ohlc = open_frame.notna() & high_frame.notna() & low_frame.notna() & close_frame.notna()
        observed_ohlc = open_frame.notna() | high_frame.notna() | low_frame.notna() | close_frame.notna()
        incomplete = observed_ohlc & ~complete_ohlc
        quality["incomplete_ohlc_count"] = int(incomplete.to_numpy().sum())
        quality["negative_price_count"] = int(
            ((open_frame < 0) | (high_frame < 0) | (low_frame < 0) | (close_frame < 0)).to_numpy().sum()
        )
        invalid_bounds = complete_ohlc & (
            (high_frame < open_frame)
            | (high_frame < close_frame)
            | (high_frame < low_frame)
            | (low_frame > open_frame)
            | (low_frame > close_frame)
            | (low_frame > high_frame)
        )
        quality["ohlc_bound_error_count"] = int(invalid_bounds.to_numpy().sum())
        quality["negative_volume_count"] = int((volume_frame < 0).to_numpy().sum())
        quality["zero_volume_count"] = int((volume_frame == 0).to_numpy().sum())

        for symbol in open_frame.columns:
            observed_dates = complete_ohlc.index[complete_ohlc[symbol]]
            if observed_dates.empty:
                errors.append(f"{symbol} has no complete OHLC history")
                continue
            first = pd.Timestamp(observed_dates.min()).normalize()
            last = pd.Timestamp(observed_dates.max()).normalize()
            if first > backfill_start:
                quality["head_after_backfill_symbol_count"] += 1
            tail_lag_days = int((closed_end - last).days)
            if tail_lag_days != 0:
                quality["missing_tail_symbol_count"] += 1
                if quality["missing_tail_symbol_count"] <= 10:
                    quality.setdefault("missing_tail_examples", []).append(
                        {"symbol": str(symbol), "last_time": last.isoformat(), "tail_lag_days": tail_lag_days}
                    )
            gaps = observed_dates[1:] - observed_dates[:-1]
            gap_positions = np.flatnonzero(gaps > pd.Timedelta(days=1))
            quality["continuity_gap_count"] += int(len(gap_positions))
            for position in gap_positions[: max(0, 20 - len(quality["continuity_gap_examples"]))]:
                quality["continuity_gap_examples"].append(
                    {
                        "symbol": str(symbol),
                        "after": pd.Timestamp(observed_dates[position]).isoformat(),
                        "before": pd.Timestamp(observed_dates[position + 1]).isoformat(),
                        "gap_days": int(gaps[position] / pd.Timedelta(days=1)),
                    }
                )

        for key in ("incomplete_ohlc_count", "negative_price_count", "ohlc_bound_error_count", "negative_volume_count", "continuity_gap_count", "missing_tail_symbol_count"):
            if quality[key]:
                errors.append(f"quality {key}={quality[key]}")

    return {
        "dataset": DATASET,
        "phase": "phase_e_supplemental",
        "status": "pass" if not errors else "fail",
        "audited_at": utc_now_iso(),
        "backfill_start": backfill_start.isoformat(),
        "closed_daily_end": closed_end.isoformat(),
        "files": files,
        "universe": {
            "state_updated_at": universe_state.get("updated_at"),
            "tracked_symbol_count": len(tracked_symbols),
            "active_symbol_count": len(active_symbols),
            "matrix_column_count": int(len(base.columns)) if base is not None else 0,
            "policy": policy,
        },
        "quality": quality,
        "errors": errors,
    }


def _run_phase_e_audit(
    matrix_dir,
    *,
    backfill_start: pd.Timestamp,
    closed_end: pd.Timestamp,
    top_n: int,
    min_history_days: int,
) -> dict[str, Any]:
    report = _build_matrix_audit_report(
        matrix_dir,
        universe_state=JsonState("binance_daily_matrix_symbols.json").read(),
        backfill_start=backfill_start,
        closed_end=closed_end,
        top_n=top_n,
        min_history_days=min_history_days,
    )
    JsonState("audits/binance_daily_matrix_phase_e.json").write(report)
    if report["status"] != "pass":
        raise RuntimeError("Binance daily matrix audit failed: " + "; ".join(report["errors"][:5]))
    return report


def _symbol_fetch_start(
    existing_df: pd.DataFrame,
    symbol: str,
    *,
    backfill_start: pd.Timestamp,
    end: pd.Timestamp,
    overlap_days: int,
    feature: str = "ohlcv",
    reference_df: pd.DataFrame | None = None,
) -> pd.Timestamp | None:
    """Find the earliest date that needs a source refresh for one feature.

    Matrix files have a shared daily index, so a zero in the volume matrix can
    mean either a real zero or a missing cell written by an older collector.
    Binance USD-M daily candles normally have positive volume; for the volume
    integrity scan, positive values are therefore the observed coverage. The
    reference OHLC matrix limits that scan to dates on which the symbol itself
    existed, avoiding repeated backfills before a symbol's listing date.
    """
    backfill_start = pd.Timestamp(backfill_start).normalize()
    end = pd.Timestamp(end).normalize()
    expected_start = backfill_start

    if reference_df is not None and symbol in reference_df.columns:
        reference = pd.to_numeric(reference_df[symbol], errors="coerce")
        reference.index = pd.to_datetime(reference.index, errors="coerce").normalize()
        reference = reference[~reference.index.isna()].dropna()
        if not reference.empty:
            expected_start = max(expected_start, pd.Timestamp(reference.index.min()).normalize())

    if symbol not in existing_df.columns:
        return expected_start

    series = pd.to_numeric(existing_df[symbol], errors="coerce")
    series.index = pd.to_datetime(series.index, errors="coerce").normalize()
    series = series[~series.index.isna()]
    series = series[~series.index.duplicated(keep="last")].sort_index()

    if feature.lower() == "volume":
        # A legacy run converted missing pivot cells to zero. Treat positive
        # volume as the reliable persisted coverage signal for repair.
        series = series.where(series > 0)
    series = series.dropna()
    if series.empty:
        return expected_start

    dates = pd.DatetimeIndex(series.index).normalize().drop_duplicates().sort_values()
    first = pd.Timestamp(dates[0]).normalize()
    latest = pd.Timestamp(dates[-1]).normalize()

    if first > expected_start:
        return expected_start

    overlap = timedelta(days=int(overlap_days))
    gaps = dates[1:] - dates[:-1]
    for position, gap in enumerate(gaps, start=1):
        if gap > timedelta(days=1):
            return max(expected_start, pd.Timestamp(dates[position - 1]).normalize() - overlap)

    if latest < end:
        return max(expected_start, latest - overlap)

    return max(expected_start, latest - overlap)


def _merge_feature_matrix(
    pivoted_new: pd.DataFrame,
    existing_df: pd.DataFrame,
    *,
    feature: str,
    symbols: list[str],
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Merge fetched cells without missing pivot cells overwriting storage."""
    feature = feature.lower()
    new = pivoted_new.copy()
    new.index = pd.to_datetime(new.index, errors="coerce").normalize()
    new = new[~new.index.isna()]
    if feature == "volume":
        new = new.apply(pd.to_numeric, errors="coerce")
    else:
        new = new.astype("float64")

    existing = existing_df.copy()
    if not existing.empty:
        existing.index = pd.to_datetime(existing.index, errors="coerce").normalize()
        existing = existing[~existing.index.isna()]
        existing = existing[~existing.index.duplicated(keep="last")]
        existing = existing.apply(pd.to_numeric, errors="coerce")

    # combine_first must happen before volume's final dense-matrix fill. A
    # missing symbol/date in pivoted_new is not a source observation.
    combined = new.combine_first(existing)
    combined = combined[combined.index <= pd.Timestamp(end).normalize()]
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    combined = combined.reindex(symbols, axis=1)
    combined.index.name = "time"

    # Preserve the long-standing matrix dtype/loader contract. The fill is
    # intentionally after the merge, so it cannot overwrite existing values.
    if feature == "volume":
        combined = combined.fillna(0).astype("int64")
    else:
        combined = combined.astype("float64")
    return combined


def run_pipeline(
    backfill_start_str: str,
    logger,
    *,
    top_n: int = 400,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
    phase_e_audit: bool = False,
) -> dict[str, Any] | None:
    # 1. Update/get master symbols list
    symbols = update_master_symbol_list(logger, top_n=top_n, min_history_days=min_history_days)
    if not symbols:
        logger.warning("No symbols to fetch.")
        return None

    # Define matrix directories & file paths
    matrix_dir = data_root() / "crypto" / "binance_daily_matrix"
    matrix_dir.mkdir(parents=True, exist_ok=True)
    
    paths = {f: matrix_dir / f"{f}.parquet" for f in FEATURES}

    # 2. Determine per-symbol fetch windows from every feature matrix. Using
    # only open previously hid a corrupted volume matrix from backfill.
    existing_matrices: dict[str, pd.DataFrame] = {}
    for feature in FEATURES:
        try:
            existing_matrices[feature] = _load_matrix(_select_matrix_path(matrix_dir, feature))
        except Exception as exc:
            logger.error("Error loading existing %s matrix; repairing from source: %s", feature, exc)
            existing_matrices[feature] = pd.DataFrame()
    existing_open = existing_matrices["open"]
    backfill_start = pd.Timestamp(backfill_start_str).normalize()
    end = _closed_daily_until()

    if backfill_start > end:
        logger.info("No closed daily candles to fetch: backfill_start=%s end=%s", backfill_start, end)
        return None
    
    # 3. Fetch data for each symbol
    dfs = []
    logger.info("Fetching daily klines for %d symbols...", len(symbols))
    
    for i, symbol in enumerate(symbols):
        feature_starts = {
            feature: _symbol_fetch_start(
                existing_matrices[feature],
                symbol,
                backfill_start=backfill_start,
                end=end,
                overlap_days=overlap_days,
                feature=feature,
                reference_df=existing_open if feature == "volume" else None,
            )
            for feature in FEATURES
        }
        starts = [value for value in feature_starts.values() if value is not None]
        start = min(starts) if starts else None
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
        if phase_e_audit:
            report = _run_phase_e_audit(
                matrix_dir,
                backfill_start=backfill_start,
                closed_end=end,
                top_n=top_n,
                min_history_days=min_history_days,
            )
            logger.info("Binance daily matrix audit passed: files=%s universe=%s", len(report["files"]), report["universe"]["active_symbol_count"])
            return report
        return None

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

            try:
                combined = _merge_feature_matrix(
                    pivoted_new,
                    existing_matrices[feature],
                    feature=feature,
                    symbols=symbols,
                    end=end,
                )
            except Exception as exc:
                logger.error("Error merging %s matrix; writing fetched cells only: %s", feature, exc)
                combined = _merge_feature_matrix(
                    pivoted_new,
                    pd.DataFrame(),
                    feature=feature,
                    symbols=symbols,
                    end=end,
                )

            # Write atomically
            _atomic_write_matrix(combined, path)
            
            logger.info("Wrote matrix %s: shape=%s", path.name, combined.shape)

    if phase_e_audit:
        report = _run_phase_e_audit(
            matrix_dir,
            backfill_start=backfill_start,
            closed_end=end,
            top_n=top_n,
            min_history_days=min_history_days,
        )
        logger.info("Binance daily matrix audit passed: files=%s universe=%s", len(report["files"]), report["universe"]["active_symbol_count"])
        return report
    return None


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
    parser.add_argument("--phase-e-audit", action="store_true", help="Write and enforce the strict five-feature Phase E audit.")
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
                        phase_e_audit=args.phase_e_audit,
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
                phase_e_audit=args.phase_e_audit,
            )
            heartbeat.beat(status="success")
        except Exception as exc:
            logger.exception("Matrix pipeline failed")
            heartbeat.beat(status="error", error=str(exc))
            if args.mode == "once":
                raise
        break


if __name__ == "__main__":
    main()
