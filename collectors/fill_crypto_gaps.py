from __future__ import annotations

import argparse

import pandas as pd

from collectors.common.config import load_yaml
from collectors.common.env import load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Manifest, utc_now_iso
from collectors.common.storage import PartitionedParquetStore as PartitionedCsvGzStore
from collectors.common.storage import read_partition_file
from collectors.crypto_1m import DATASET, fetch_1m


EXPECTED_STEP = pd.Timedelta(minutes=1)


def _symbol_times(store: PartitionedCsvGzStore, symbol: str) -> pd.Series:
    chunks: list[pd.Series] = []
    for path in sorted(store.files({"symbol": symbol})):
        try:
            values = read_partition_file(path, usecols=["time"])
            if "time" not in values.columns:
                continue
            chunks.append(pd.to_datetime(values["time"], errors="coerce"))
        except Exception as exc:
            raise RuntimeError(f"read failed {path}: {exc}") from exc
    if not chunks:
        return pd.Series(dtype="datetime64[ns]")
    return pd.concat(chunks, ignore_index=True).dropna().sort_values().drop_duplicates().reset_index(drop=True)


def _find_gaps(times: pd.Series, *, since: pd.Timestamp | None, until: pd.Timestamp | None) -> pd.DataFrame:
    if times.empty:
        return pd.DataFrame(columns=["prev_time", "next_time", "missing_start", "missing_end", "gap"])
    work = times
    if since is not None:
        work = work[work >= since]
    if until is not None:
        work = work[work <= until]
    work = work.sort_values().drop_duplicates().reset_index(drop=True)
    if len(work) < 2:
        return pd.DataFrame(columns=["prev_time", "next_time", "missing_start", "missing_end", "gap"])

    diffs = work.diff()
    gap_rows = []
    for idx, gap in diffs[diffs > EXPECTED_STEP].items():
        prev_time = work.iloc[idx - 1]
        next_time = work.iloc[idx]
        gap_rows.append(
            {
                "prev_time": prev_time,
                "next_time": next_time,
                "missing_start": prev_time + EXPECTED_STEP,
                "missing_end": next_time - EXPECTED_STEP,
                "gap": gap,
            }
        )
    return pd.DataFrame(gap_rows)


def repair_symbol(
    symbol: str,
    *,
    store: PartitionedCsvGzStore,
    since: pd.Timestamp | None,
    until: pd.Timestamp | None,
    max_gaps: int,
    dry_run: bool,
    logger,
) -> int:
    times = _symbol_times(store, symbol)
    gaps = _find_gaps(times, since=since, until=until)
    if gaps.empty:
        logger.info("%s has no continuity gaps", symbol)
        return 0

    if max_gaps > 0:
        gaps = gaps.head(max_gaps)

    logger.warning("%s has %d gaps to repair", symbol, len(gaps))
    repaired_rows = 0
    manifest = Manifest(DATASET)

    for row in gaps.itertuples(index=False):
        start = pd.Timestamp(row.missing_start)
        end = pd.Timestamp(row.missing_end)
        start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
        end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
        logger.info("%s repair gap %s -> %s gap=%s", symbol, start, end, row.gap)
        if dry_run:
            continue

        df = fetch_1m(symbol, start, end)
        if df.empty:
            manifest.update_symbol(symbol, last_error=f"empty_gap_fill {start} {end}", last_failed_at=utc_now_iso())
            logger.warning("%s returned empty for gap %s -> %s", symbol, start, end)
            continue

        result = store.append(
            df,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs={"symbol": symbol},
            lock_name=f"{DATASET}/{symbol}",
        )
        repaired_rows += int(result["rows_written"])
        manifest.update_symbol(
            symbol,
            latest_time=str(result["latest_time"]),
            last_success_at=utc_now_iso(),
            source="binance_futures_gap_fill",
            rows_gap_filled=int(result["rows_written"]),
            last_gap_start=start.isoformat(),
            last_gap_end=end.isoformat(),
            last_error=None,
        )
        logger.info("%s filled rows=%s latest=%s", symbol, result["rows_written"], result["latest_time"])

    return repaired_rows


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Find and fill missing Binance Futures 1m gaps in canonical storage.")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols. Defaults to configs/symbols.crypto.yml.")
    parser.add_argument("--since", default=None)
    parser.add_argument("--until", default=None)
    parser.add_argument("--max-gaps", type=int, default=0, help="0 means no limit.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = load_yaml("symbols.crypto.yml")
    symbols = args.symbols.split(",") if args.symbols else config.get("symbols", ["BTCUSDT", "ETHUSDT"])
    symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    since = pd.Timestamp(args.since) if args.since else None
    until = pd.Timestamp(args.until) if args.until else None

    logger = setup_logging("crypto_gap_fill")
    store = PartitionedCsvGzStore(["crypto", "binance_futures", "1m"], partition="month")
    total = 0
    for symbol in symbols:
        total += repair_symbol(
            symbol,
            store=store,
            since=since,
            until=until,
            max_gaps=args.max_gaps,
            dry_run=args.dry_run,
            logger=logger,
        )
    logger.info("gap fill completed total_rows=%s dry_run=%s", total, args.dry_run)


if __name__ == "__main__":
    main()
