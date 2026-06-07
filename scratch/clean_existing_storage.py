import os
import glob
import pandas as pd
from pathlib import Path
from collectors.common.calendar_vn import filter_trading_hours
from collectors.common.manifest import Manifest
from collectors.common.discovery import latest_time_from_files

def clean_file(path: Path, is_derivative: bool) -> int:
    try:
        df = pd.read_csv(path, compression='gzip')
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return 0

    original_len = len(df)
    if original_len == 0:
        return 0

    # Clean time & timezone
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    if df["time"].dt.tz is not None:
        df["time"] = df["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)

    # Filter out of hours / holidays
    df = filter_trading_hours(df, derivative=is_derivative)

    if df.empty:
        print(f"File {path} is empty after filtering. Deleting.")
        path.unlink()
        return original_len

    # Schema normalization
    df["symbol"] = df["symbol"].astype(str)
    df["open"] = pd.to_numeric(df["open"], errors="coerce").astype("float64")
    df["high"] = pd.to_numeric(df["high"], errors="coerce").astype("float64")
    df["low"] = pd.to_numeric(df["low"], errors="coerce").astype("float64")
    df["close"] = pd.to_numeric(df["close"], errors="coerce").astype("float64")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    if "source" in df.columns:
        df["source"] = df["source"].astype(str)
    else:
        df["source"] = "dnse" if is_derivative else "vnstock_kbs"
    if "ingested_at" not in df.columns:
        df["ingested_at"] = pd.Timestamp.now().isoformat()
    df["ingested_at"] = df["ingested_at"].astype(str)

    df = df.dropna(subset=["time", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["symbol", "time"]).sort_values(["time"]).reset_index(drop=True)

    cols = ["time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]
    df = df[cols]

    # Write atomically
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False, compression="gzip")
    os.replace(tmp, path)

    removed = original_len - len(df)
    if removed > 0:
        print(f"Cleaned {path.name} in {path.parent.relative_to(path.parents[4])}: removed {removed} out-of-hours/holiday/dup rows.")
    return removed

def main():
    storage_root = Path('/root/bobby/pool_alpha/alphas_storage/_get_data/storage/vn')
    
    # 1. Clean equity 1m
    equity_files = list((storage_root / 'equity' / '1m').glob('**/part.csv.gz'))
    print(f"Scanning {len(equity_files)} equity 1m files...")
    total_eq_removed = 0
    for path in equity_files:
        if path.exists():
            total_eq_removed += clean_file(path, is_derivative=False)
    print(f"Total equity rows removed: {total_eq_removed}")

    # 2. Clean futures 1m
    futures_files = list((storage_root / 'futures' / '1m').glob('**/part.csv.gz'))
    print(f"\nScanning {len(futures_files)} futures 1m files...")
    total_fut_removed = 0
    for path in futures_files:
        if path.exists():
            total_fut_removed += clean_file(path, is_derivative=True)
    print(f"Total futures rows removed: {total_fut_removed}")

    # 3. Update manifests to correct latest_time
    print("\nUpdating manifests with correct latest_time...")
    
    # Equity manifest
    eq_manifest = Manifest("vn_equity_1m")
    eq_state = eq_manifest.read()
    for symbol in eq_state.get("symbols", {}):
        # find files for this symbol
        symbol_files = list((storage_root / 'equity' / '1m' / f"symbol={symbol}").glob('**/part.csv.gz'))
        latest_ts = latest_time_from_files(symbol_files, ["time"])
        if latest_ts:
            eq_manifest.update_symbol(symbol, latest_time=latest_ts.isoformat())
            print(f"Updated equity manifest for {symbol}: latest_time = {latest_ts.isoformat()}")

    # Futures manifest
    fut_manifest = Manifest("vn_futures_dnse_1m")
    fut_state = fut_manifest.read()
    for symbol in fut_state.get("symbols", {}):
        symbol_files = list((storage_root / 'futures' / '1m' / f"symbol={symbol}").glob('**/part.csv.gz'))
        latest_ts = latest_time_from_files(symbol_files, ["time"])
        if latest_ts:
            fut_manifest.update_symbol(symbol, latest_time=latest_ts.isoformat())
            print(f"Updated futures manifest for {symbol}: latest_time = {latest_ts.isoformat()}")

if __name__ == '__main__':
    main()
