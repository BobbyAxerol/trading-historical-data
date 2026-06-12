from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd

from collectors.common.discovery import latest_time_from_files
from collectors.common.env import GET_DATA_ROOT, data_root, load_environment


DATASET_PATHS = {
    "crypto": ("crypto", "binance_futures", "1m"),
    "vn-daily": ("vn", "equity", "1d"),
    "vn-intraday": ("vn", "equity", "1m"),
    "vn-futures": ("vn", "futures", "1m"),
    "options": ("options", "binance", "snapshot_5m"),
}


def _paths(patterns: list[Path]) -> list[Path]:
    found: list[Path] = []
    for pattern in patterns:
        found.extend(Path(path) for path in glob.glob(str(pattern)))
    return found


def _read_times(path: Path, time_cols: list[str]) -> pd.Series:
    if path.suffix == ".parquet":
        for time_col in time_cols:
            try:
                df = pd.read_parquet(path, columns=[time_col])
                values = pd.to_datetime(df[time_col], errors="coerce").dropna()
                try:
                    values = values.dt.tz_convert(None)
                except Exception:
                    try:
                        values = values.dt.tz_localize(None)
                    except Exception:
                        pass
                return values.sort_values()
            except Exception:
                continue
        raise ValueError(f"none of time columns found: {time_cols}")
    else:
        header = pd.read_csv(path, compression="gzip", nrows=0)
        for time_col in time_cols:
            if time_col in header.columns:
                df = pd.read_csv(path, compression="gzip", usecols=[time_col])
                values = pd.to_datetime(df[time_col], errors="coerce").dropna()
                try:
                    values = values.dt.tz_convert(None)
                except Exception:
                    try:
                        values = values.dt.tz_localize(None)
                    except Exception:
                        pass
                return values.sort_values()
        raise ValueError(f"none of time columns found: {time_cols}")


def _discover_symbols(root: Path, dataset: str) -> list[str]:
    dataset_root = root.joinpath(*DATASET_PATHS[dataset])
    if not dataset_root.exists():
        return []
    prefix = "underlying=" if dataset == "options" else "symbol="
    symbols = []
    for path in dataset_root.iterdir():
        if path.is_dir() and path.name.startswith(prefix):
            symbols.append(path.name.split("=", 1)[1])
    return sorted(symbols)


def audit_symbol(name: str, paths: list[Path], time_cols: list[str], expected: pd.Timedelta | None) -> bool:
    files = [p for p in paths if p.exists()]
    if not files:
        print(f"{name}: missing")
        return False

    latest = latest_time_from_files(files, time_cols)
    all_times: list[pd.Series] = []

    for path in files:
        try:
            times = _read_times(path, time_cols)
        except Exception as exc:
            print(f"{name}: read_fail {path}: {exc}")
            return False
        if not times.empty:
            all_times.append(times)

    if not all_times:
        print(f"{name}: files={len(files)} empty")
        return False

    combined = pd.concat(all_times, ignore_index=True).dropna().sort_values().drop_duplicates().reset_index(drop=True)
    first = combined.min()
    max_gap = None
    gap_count = 0
    examples: list[str] = []

    if expected is not None:
        diffs = combined.diff().dropna()
        big = diffs[diffs > expected]
        gap_count = len(big)
        if not big.empty:
            max_gap = big.max()
            for idx in big.index[:5]:
                prev_time = combined.iloc[idx - 1]
                next_time = combined.iloc[idx]
                examples.append(f"{prev_time} -> {next_time} ({next_time - prev_time})")

    suffix = f" examples={examples}" if examples else ""
    print(f"{name}: files={len(files)} rows={len(combined)} first={first} latest={latest} gaps>{expected}={gap_count} max_gap={max_gap}{suffix}")
    return gap_count == 0


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--dataset", choices=["crypto", "vn-daily", "vn-intraday", "vn-futures", "options"], required=True)
    parser.add_argument("--include-existing-files", action="store_true")
    parser.add_argument("--all-symbols", action="store_true")
    args = parser.parse_args()

    root = data_root()
    symbols = _discover_symbols(root, args.dataset) if args.all_symbols else args.symbol or ["BTCUSDT", "ETHUSDT", "FPT", "VN30F1M", "BTC"]
    ok = True
    for symbol in symbols:
        if args.dataset == "crypto":
            paths = _paths([root / "crypto" / "binance_futures" / "1m" / f"symbol={symbol}" / "year=*" / "month=*" / "part.csv.gz"])
            if args.include_existing_files:
                paths += [
                    GET_DATA_ROOT / "crypto_1m_data" / f"{symbol.lower()}_perpetual_1m.csv.gz",
                    GET_DATA_ROOT / "crypto_1m_data" / f"{symbol}_1m.csv.gz",
                ]
            ok &= audit_symbol(f"crypto:{symbol}", paths, ["time", "open_time"], pd.Timedelta(minutes=1))
        elif args.dataset == "vn-daily":
            paths = _paths([root / "vn" / "equity" / "1d" / f"symbol={symbol}" / "year=*" / "part.csv.gz"])
            if args.include_existing_files:
                paths += [GET_DATA_ROOT / "data_stock" / f"{symbol}_1d_max.csv.gz"]
            ok &= audit_symbol(f"vn-daily:{symbol}", paths, ["time"], None)
        elif args.dataset == "vn-intraday":
            paths = _paths([root / "vn" / "equity" / "1m" / f"symbol={symbol}" / "year=*" / "month=*" / "part.csv.gz"])
            if args.include_existing_files:
                paths += [GET_DATA_ROOT / "data_stock" / "_intraday_storage" / "stocks" / f"{symbol}_1m.csv.gz"]
            ok &= audit_symbol(f"vn-intraday:{symbol}", paths, ["time"], None)
        elif args.dataset == "vn-futures":
            paths = _paths([root / "vn" / "futures" / "1m" / f"symbol={symbol}" / "year=*" / "month=*" / "part.csv.gz"])
            if args.include_existing_files:
                paths += [
                    GET_DATA_ROOT / "data_stock" / "_intraday_storage" / "futures" / f"{symbol}_1m.csv.gz",
                    GET_DATA_ROOT / "data_stock" / "_intraday_storage" / "futures" / f"{symbol}_1m.parquet",
                ]
            ok &= audit_symbol(f"vn-futures:{symbol}", paths, ["time"], None)
        else:
            paths = _paths([root / "options" / "binance" / "snapshot_5m" / f"underlying={symbol}" / "year=*" / "month=*" / "part.csv.gz"])
            if args.include_existing_files:
                paths += [GET_DATA_ROOT / "options_full_history.csv.gz", GET_DATA_ROOT / "option_data" / "options_full_history.csv.gz"]
            ok &= audit_symbol(f"options:{symbol}", paths, ["snapshot_time", "time"], pd.Timedelta(minutes=5))

    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
