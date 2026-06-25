from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from collectors.common.config import load_yaml
from collectors.common.env import data_root, load_environment
from collectors.common.locks import FileLock
from collectors.common.logging import setup_logging
from collectors.common.manifest import JsonState, utc_now_iso

DATASET = "vn_daily_matrix"
FEATURES = ["open", "high", "low", "close", "volume"]


def _configured_symbols() -> list[str]:
    config = load_yaml("symbols.vn_daily.yml")
    symbols = config.get("symbols") or []
    return [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]


def _discover_symbols(raw_root: Path) -> list[str]:
    if not raw_root.exists():
        return []
    return sorted(
        path.name.split("=", 1)[1].upper()
        for path in raw_root.glob("symbol=*")
        if path.is_dir() and "=" in path.name
    )


def _ordered_symbols(raw_root: Path, symbols: Iterable[str] | None = None) -> list[str]:
    configured = list(symbols) if symbols is not None else _configured_symbols()
    configured = [str(symbol).strip().upper() for symbol in configured if str(symbol).strip()]
    discovered = _discover_symbols(raw_root)

    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in configured + discovered:
        if symbol not in seen:
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


def _read_symbol(raw_root: Path, symbol: str, start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> pd.DataFrame:
    symbol_root = raw_root / f"symbol={symbol}"
    if not symbol_root.exists():
        return pd.DataFrame()

    frames = []
    for path in sorted(symbol_root.glob("year=*/part.csv.gz")):
        try:
            year = int(path.parent.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        if start_ts is not None and year < start_ts.year:
            continue
        if end_ts is not None and year > end_ts.year:
            continue
        try:
            frames.append(pd.read_csv(path, compression="gzip"))
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    if "time" not in df.columns:
        return pd.DataFrame()

    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["time"])
    if start_ts is not None:
        df = df[df["time"] >= start_ts]
    if end_ts is not None:
        df = df[df["time"] <= end_ts]
    if df.empty:
        return pd.DataFrame()

    df["symbol"] = symbol
    for feature in FEATURES:
        if feature in df.columns:
            df[feature] = pd.to_numeric(df[feature], errors="coerce")
    prices = df[["open", "high", "low", "close"]]
    df["high"] = prices.max(axis=1, skipna=False)
    df["low"] = prices.min(axis=1, skipna=False)
    return (
        df[["time", "symbol", *FEATURES]]
        .drop_duplicates(subset=["time", "symbol"], keep="last")
        .sort_values(["time", "symbol"])
        .reset_index(drop=True)
    )


def build_matrix(
    *,
    symbols: Iterable[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    logger=None,
) -> dict[str, object]:
    """Build pivoted VN daily OHLCV matrices from canonical partitioned storage."""
    raw_root = data_root() / "vn" / "equity" / "1d"
    matrix_root = data_root() / "vn" / "equity" / "daily_matrix"
    matrix_root.mkdir(parents=True, exist_ok=True)

    start_ts = pd.to_datetime(start_date).normalize() if start_date else None
    end_ts = pd.to_datetime(end_date).normalize() if end_date else None
    symbol_list = _ordered_symbols(raw_root, symbols)

    frames = []
    missing_symbols: list[str] = []
    for symbol in symbol_list:
        df = _read_symbol(raw_root, symbol, start_ts, end_ts)
        if df.empty:
            missing_symbols.append(symbol)
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError(f"No VN daily data found under {raw_root}")

    all_df = pd.concat(frames, ignore_index=True)
    all_df = all_df.drop_duplicates(subset=["time", "symbol"], keep="last")
    active_symbols = [symbol for symbol in symbol_list if symbol not in set(missing_symbols)]

    with FileLock(DATASET):
        for feature in FEATURES:
            matrix = all_df.pivot(index="time", columns="symbol", values=feature)
            matrix = matrix.reindex(columns=active_symbols)
            matrix.index = pd.to_datetime(matrix.index, errors="coerce")
            matrix = matrix[~matrix.index.isna()].sort_index()
            matrix.index = matrix.index.strftime("%Y-%m-%d")
            tmp = matrix_root / f"{feature}.csv.tmp"
            out = matrix_root / f"{feature}.csv.gz"
            matrix.to_csv(tmp, compression="gzip")
            tmp.replace(out)
            if logger:
                logger.info("Wrote VN daily matrix %s.csv.gz: shape=%s", feature, matrix.shape)

    state = JsonState("vn_daily_matrix_symbols.json")
    state.write({
        "symbols": active_symbols,
        "missing_symbols": missing_symbols,
        "features": FEATURES,
        "storage": str(matrix_root),
        "updated_at": utc_now_iso(),
        "source": "storage/vn/equity/1d",
    })

    return {
        "symbols": active_symbols,
        "missing_symbols": missing_symbols,
        "rows": int(all_df["time"].nunique()),
        "features": FEATURES,
        "path": str(matrix_root),
    }


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols. Defaults to configs/symbols.vn_daily.yml plus storage discovery.")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args()

    logger = setup_logging(DATASET)
    symbols = args.symbols.split(",") if args.symbols else None
    result = build_matrix(symbols=symbols, start_date=args.start_date, end_date=args.end_date, logger=logger)
    logger.info("VN daily matrix build finished: %s", result)


if __name__ == "__main__":
    main()
