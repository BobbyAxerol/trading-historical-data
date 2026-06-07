from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.manifest import Manifest, utc_now_iso
from collectors.common.storage import PartitionedCsvGzStore

CHUNK_ROWS = 250_000


def _read_csv_chunks(path: Path, chunksize: int) -> Iterable[pd.DataFrame]:
    yield from pd.read_csv(path, compression="gzip", chunksize=chunksize)


def _read_parquet_chunks(path: Path) -> Iterable[pd.DataFrame]:
    yield pd.read_parquet(path)


def _chunks(path: Path, chunksize: int) -> Iterable[pd.DataFrame]:
    if path.suffix == ".parquet":
        yield from _read_parquet_chunks(path)
    else:
        yield from _read_csv_chunks(path, chunksize)


def _base_ohlcv(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    work["symbol"] = symbol
    work["source"] = source
    work["ingested_at"] = utc_now_iso()
    keep = ["time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]
    return work[[col for col in keep if col in work.columns]].dropna(subset=["time", "open", "high", "low", "close"])


def _crypto_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    work = df.copy()
    if "time" not in work.columns:
        work["time"] = work.get("open_time")
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    if "quote_asset_volume" in work.columns and "quote_volume" not in work.columns:
        work = work.rename(columns={"quote_asset_volume": "quote_volume"})
    if "taker_buy_base_asset_volume" in work.columns and "taker_buy_base_volume" not in work.columns:
        work = work.rename(columns={"taker_buy_base_asset_volume": "taker_buy_base_volume"})
    if "taker_buy_quote_asset_volume" in work.columns and "taker_buy_quote_volume" not in work.columns:
        work = work.rename(columns={"taker_buy_quote_asset_volume": "taker_buy_quote_volume"})
    if "close_time" in work.columns:
        close_raw = work["close_time"]
        numeric_close = pd.to_numeric(close_raw, errors="coerce")
        parsed_numeric = pd.to_datetime(numeric_close, unit="ms", errors="coerce")
        parsed_text = pd.to_datetime(close_raw, errors="coerce")
        work["close_time"] = parsed_text.fillna(parsed_numeric)

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    work["symbol"] = symbol
    work["source"] = "binance_futures"
    work["ingested_at"] = utc_now_iso()
    keep = [
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
    return work[[col for col in keep if col in work.columns]].dropna(subset=["time", "open", "high", "low", "close"])


def _option_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "snapshot_time" not in work.columns and "time" in work.columns:
        work["snapshot_time"] = work["time"]
    work["snapshot_time"] = pd.to_datetime(work["snapshot_time"], errors="coerce")
    if "underlying" not in work.columns and "symbol" in work.columns:
        work["underlying"] = work["symbol"].astype(str).str.split("-").str[0]
    if "source" not in work.columns:
        work["source"] = "binance_options"
    work["ingested_at"] = utc_now_iso()
    return work.dropna(subset=["snapshot_time", "symbol", "underlying"])


def _append_chunks(
    *,
    paths: list[Path],
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    symbol_key: str,
    attrs_key: str,
    normalizer,
    time_col: str,
    dedupe_cols: list[str],
    lock_prefix: str,
    chunksize: int,
    dry_run: bool,
) -> tuple[int, int]:
    files_seen = 0
    rows_seen = 0
    for path in paths:
        if not path.exists():
            continue
        files_seen += 1
        symbol = symbol_key_from_path(path) if symbol_key == "__path_symbol__" else symbol_key
        print(f"seed {lock_prefix}:{symbol} <- {path}")
        for chunk in _chunks(path, chunksize):
            normalized = normalizer(chunk, symbol) if symbol_key != "__options__" else normalizer(chunk)
            if normalized.empty:
                continue
            rows_seen += len(normalized)
            if dry_run:
                continue
            attrs = {attrs_key: symbol} if symbol_key != "__options__" else {attrs_key: normalized[attrs_key].iloc[0]}
            if symbol_key == "__options__":
                grouped = normalized.groupby(attrs_key)
            else:
                grouped = [(symbol, normalized)]
            for group_value, part in grouped:
                attrs = {attrs_key: str(group_value)}
                result = store.append(
                    part,
                    time_col=time_col,
                    dedupe_cols=dedupe_cols,
                    attrs=attrs,
                    lock_name=f"{lock_prefix}/{group_value}",
                )
                latest = result.get("latest_time")
                manifest.update_symbol(
                    str(group_value),
                    latest_time=str(latest) if latest else None,
                    last_success_at=utc_now_iso(),
                    rows_seeded=int(result.get("rows_written", 0)),
                    seeded_from=str(path),
                    source="existing_files",
                    last_error=None,
                )
    return files_seen, rows_seen


def symbol_key_from_path(path: Path) -> str:
    name = path.name
    for suffix in ["_perpetual_1m.csv.gz", "_1m.csv.gz", "_1m.parquet", "_1d_max.csv.gz"]:
        if name.endswith(suffix):
            return name[: -len(suffix)].upper()
    return path.stem.upper()


def seed_crypto(symbols: list[str] | None, chunksize: int, dry_run: bool) -> None:
    paths = sorted((GET_DATA_ROOT / "crypto_1m_data").glob("*_perpetual_1m.csv.gz"))
    if symbols:
        keep = {s.upper() for s in symbols}
        paths = [p for p in paths if symbol_key_from_path(p) in keep]
    store = PartitionedCsvGzStore(["crypto", "binance_futures", "1m"], partition="month")
    files_seen, rows_seen = _append_chunks(
        paths=paths,
        store=store,
        manifest=Manifest("crypto_binance_futures_1m"),
        symbol_key="__path_symbol__",
        attrs_key="symbol",
        normalizer=_crypto_ohlcv,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        lock_prefix="seed_crypto_binance_futures_1m",
        chunksize=chunksize,
        dry_run=dry_run,
    )
    print(f"crypto seed files={files_seen} normalized_rows={rows_seen}")


def seed_vn_daily(symbols: list[str] | None, chunksize: int, dry_run: bool) -> None:
    paths = sorted((GET_DATA_ROOT / "data_stock").glob("*_1d_max.csv.gz"))
    if symbols:
        keep = {s.upper() for s in symbols}
        paths = [p for p in paths if symbol_key_from_path(p) in keep]
    store = PartitionedCsvGzStore(["vn", "equity", "1d"], partition="year")
    files_seen, rows_seen = _append_chunks(
        paths=paths,
        store=store,
        manifest=Manifest("vn_equity_1d"),
        symbol_key="__path_symbol__",
        attrs_key="symbol",
        normalizer=lambda chunk, symbol: _base_ohlcv(chunk, symbol, "existing_vn_daily"),
        time_col="time",
        dedupe_cols=["symbol", "time"],
        lock_prefix="seed_vn_equity_1d",
        chunksize=chunksize,
        dry_run=dry_run,
    )
    print(f"vn_daily seed files={files_seen} normalized_rows={rows_seen}")


def seed_vn_intraday(symbols: list[str] | None, chunksize: int, dry_run: bool) -> None:
    paths = sorted((GET_DATA_ROOT / "data_stock" / "_intraday_storage" / "stocks").glob("*_1m.csv.gz"))
    if symbols:
        keep = {s.upper() for s in symbols}
        paths = [p for p in paths if symbol_key_from_path(p) in keep]
    store = PartitionedCsvGzStore(["vn", "equity", "1m"], partition="month")
    files_seen, rows_seen = _append_chunks(
        paths=paths,
        store=store,
        manifest=Manifest("vn_equity_1m"),
        symbol_key="__path_symbol__",
        attrs_key="symbol",
        normalizer=lambda chunk, symbol: _base_ohlcv(chunk, symbol, "existing_vn_intraday"),
        time_col="time",
        dedupe_cols=["symbol", "time"],
        lock_prefix="seed_vn_equity_1m",
        chunksize=chunksize,
        dry_run=dry_run,
    )
    print(f"vn_intraday seed files={files_seen} normalized_rows={rows_seen}")


def seed_vn_futures(symbols: list[str] | None, chunksize: int, dry_run: bool) -> None:
    base = GET_DATA_ROOT / "data_stock" / "_intraday_storage" / "futures"
    paths = sorted(base.glob("*_1m.csv.gz")) + sorted(base.glob("*_1m.parquet"))
    if symbols:
        keep = {s.upper() for s in symbols}
        paths = [p for p in paths if symbol_key_from_path(p) in keep]
    store = PartitionedCsvGzStore(["vn", "futures", "1m"], partition="month")
    files_seen, rows_seen = _append_chunks(
        paths=paths,
        store=store,
        manifest=Manifest("vn_futures_dnse_1m"),
        symbol_key="__path_symbol__",
        attrs_key="symbol",
        normalizer=lambda chunk, symbol: _base_ohlcv(chunk, symbol, "existing_vn_futures"),
        time_col="time",
        dedupe_cols=["symbol", "time"],
        lock_prefix="seed_vn_futures_1m",
        chunksize=chunksize,
        dry_run=dry_run,
    )
    print(f"vn_futures seed files={files_seen} normalized_rows={rows_seen}")


def seed_options(chunksize: int, dry_run: bool) -> None:
    paths = [
        GET_DATA_ROOT / "options_full_history.csv.gz",
        GET_DATA_ROOT / "option_data" / "options_full_history.csv.gz",
    ]
    store = PartitionedCsvGzStore(["options", "binance", "snapshot_5m"], partition="month")
    files_seen, rows_seen = _append_chunks(
        paths=paths,
        store=store,
        manifest=Manifest("options_binance_snapshot_5m"),
        symbol_key="__options__",
        attrs_key="underlying",
        normalizer=_option_snapshot,
        time_col="snapshot_time",
        dedupe_cols=["snapshot_time", "symbol"],
        lock_prefix="seed_options_binance_snapshot_5m",
        chunksize=chunksize,
        dry_run=dry_run,
    )
    print(f"options seed files={files_seen} normalized_rows={rows_seen}")


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Seed canonical storage from old tested CSV/Parquet files.")
    parser.add_argument(
        "--dataset",
        choices=["all", "crypto", "vn-daily", "vn-intraday", "vn-futures", "options"],
        default="all",
    )
    parser.add_argument("--symbols", default=None, help="Comma-separated symbol filter.")
    parser.add_argument("--chunksize", type=int, default=CHUNK_ROWS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    if args.dataset in {"all", "crypto"}:
        seed_crypto(symbols, args.chunksize, args.dry_run)
    if args.dataset in {"all", "vn-daily"}:
        seed_vn_daily(symbols, args.chunksize, args.dry_run)
    if args.dataset in {"all", "vn-intraday"}:
        seed_vn_intraday(symbols, args.chunksize, args.dry_run)
    if args.dataset in {"all", "vn-futures"}:
        seed_vn_futures(symbols, args.chunksize, args.dry_run)
    if args.dataset in {"all", "options"}:
        seed_options(args.chunksize, args.dry_run)


if __name__ == "__main__":
    main()
