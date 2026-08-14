from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from collectors.common.config import load_yaml
from collectors.common.discovery import latest_time_from_files, max_timestamp
from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, Manifest, sleep_with_heartbeat, utc_now_iso
from collectors.common.retry import SlidingWindowRateLimiter, retry_sync
from collectors.common.storage import PartitionedParquetStore, read_partition_file, release_unused_memory
from collectors.vn_daily_matrix import build_matrix
from collectors.vn_daily_universe import build_universe_report, configured_equity_symbols, configured_external_symbols

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


def run_symbol(
    symbol: str,
    *,
    start_default: str,
    end: str,
    limiter: SlidingWindowRateLimiter,
    logger,
    force_history: bool = False,
) -> None:
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
    if force_history:
        # The bounded B0 seed may already have a tail.  An owner-approved
        # historical rebuild must not mistake that tail for complete history.
        start = start_default
    elif discovered_latest is not None:
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


def _audit_symbol_files(store: PartitionedParquetStore, symbol: str) -> dict[str, Any]:
    required = ["time", "symbol", "open", "high", "low", "close", "volume", "source"]
    files = store.files({"symbol": symbol})
    rows = duplicate_rows = invalid_time_rows = invalid_numeric_rows = 0
    ohlc_bad_rows = negative_rows = source_mismatch_rows = 0
    first: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None
    file_errors: list[str] = []
    seen_times: set[int] = set()
    for path in files:
        try:
            frame = read_partition_file(path, usecols=required)
        except Exception as exc:
            file_errors.append(f"{Path(path).name}: {type(exc).__name__}: {exc}")
            continue
        rows += int(len(frame))
        times = pd.to_datetime(frame["time"], errors="coerce").dt.normalize()
        invalid_time_rows += int(times.isna().sum())
        numeric = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
        invalid_numeric_rows += int(numeric.isna().any(axis=1).sum())
        valid = times.notna() & numeric.notna().all(axis=1)
        if valid.any():
            valid_times = times.loc[valid]
            work_numeric = numeric.loc[valid]
            ohlc_bad_rows += int(
                (
                    (work_numeric["high"] < work_numeric[["open", "close", "low"]].max(axis=1))
                    | (work_numeric["low"] > work_numeric[["open", "close", "high"]].min(axis=1))
                ).sum()
            )
            negative_rows += int((work_numeric < 0).any(axis=1).sum())
            for timestamp in valid_times:
                key = int(timestamp.value)
                if key in seen_times:
                    duplicate_rows += 1
                seen_times.add(key)
            part_first = valid_times.min()
            part_latest = valid_times.max()
            first = part_first if first is None or part_first < first else first
            latest = part_latest if latest is None or part_latest > latest else latest
        source_mismatch_rows += int((frame["source"].astype(str) != "vnstock_vci").sum())
        del frame, times, numeric
        release_unused_memory()
    integrity_errors = (
        len(file_errors)
        + duplicate_rows
        + invalid_time_rows
        + invalid_numeric_rows
        + ohlc_bad_rows
        + negative_rows
        + source_mismatch_rows
    )
    return {
        "symbol": symbol,
        "status": "pass" if files and rows and integrity_errors == 0 else "requires_repair",
        "files": len(files),
        "rows": rows,
        "first": first.isoformat() if first is not None else None,
        "latest": latest.isoformat() if latest is not None else None,
        "duplicate_rows": duplicate_rows,
        "invalid_time_rows": invalid_time_rows,
        "invalid_numeric_rows": invalid_numeric_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "negative_rows": negative_rows,
        "source_mismatch_rows": source_mismatch_rows,
        "file_errors": file_errors,
    }


def audit_configured_symbols(symbols: list[str]) -> dict[str, Any]:
    """Write Phase E raw-VN evidence without assuming every stock shares one listing date."""

    store = PartitionedParquetStore(["vn", "equity", "1d"], partition="year")
    reports = [_audit_symbol_files(store, symbol.strip().upper()) for symbol in symbols if symbol.strip()]
    failed = [report["symbol"] for report in reports if report["status"] != "pass"]
    payload: dict[str, Any] = {
        "dataset": DATASET,
        "service": "phase_e_vn_daily_universe_1d",
        "status": "pass" if not failed else "requires_repair",
        "configured_symbol_count": len(reports),
        "passing_symbol_count": len(reports) - len(failed),
        "failed_symbols": failed,
        "symbols": reports,
        "validated_at": utc_now_iso(),
        "continuity_note": "listing dates, delistings, and the VN exchange calendar are source availability facts; this audit does not fabricate daily bars",
    }
    JsonState("audits/vn_equity_1d_phase_e.json").write(payload)
    release_unused_memory()
    return payload


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
    parser.add_argument(
        "--configured-universe",
        action="store_true",
        help="Require the repository's configured equity universe rather than an implicit fallback list.",
    )
    parser.add_argument(
        "--force-history",
        action="store_true",
        help="Fetch from --backfill-start even when the new runtime already has a bounded tail.",
    )
    parser.add_argument(
        "--audit-phase-e",
        action="store_true",
        help="Write and enforce durable Phase E raw-equity audit evidence after a one-shot run.",
    )
    parser.add_argument(
        "--fail-on-symbol-error",
        action="store_true",
        help="Return non-zero when any configured symbol fails; required by the Phase E gate.",
    )
    parser.add_argument(
        "--skip-derived",
        action="store_true",
        help="Skip universe-report and matrix rebuilds; used by the bounded B0 source seed.",
    )
    args = parser.parse_args()

    config = load_yaml("symbols.vn_daily.yml")
    if args.symbols:
        symbols = args.symbols.split(",")
    elif args.configured_universe:
        symbols = configured_equity_symbols(config)
        if not symbols:
            raise RuntimeError("configured VN daily universe is empty")
    else:
        symbols = configured_equity_symbols(config) or default_symbols()
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
            failures: list[str] = []
            for symbol in symbols:
                try:
                    run_symbol(
                        symbol.strip().upper(),
                        start_default=start_default,
                        end=end,
                        limiter=limiter,
                        logger=logger,
                        force_history=args.force_history,
                    )
                    heartbeat.beat(symbol=symbol)
                except Exception as exc:
                    Manifest(DATASET).update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
                    logger.exception("%s daily failed", symbol)
                    heartbeat.beat(status="error", symbol=symbol, error=str(exc))
                    failures.append(f"{symbol}: {type(exc).__name__}: {exc}")
            if not args.skip_derived:
                try:
                    report = build_universe_report(
                        equity_symbols=[s.strip().upper() for s in symbols],
                        external_symbols=configured_external_symbols(config),
                        as_of_date=end,
                        write=True,
                    )
                    logger.info("VN daily universe report wrote %s rows", len(report))
                except Exception as exc:
                    logger.exception("VN daily universe report failed")
                    heartbeat.beat(status="error", error=f"universe_report_failed: {exc}")
                    failures.append(f"universe_report: {type(exc).__name__}: {exc}")
                try:
                    build_matrix(symbols=[s.strip().upper() for s in symbols], logger=logger)
                except Exception as exc:
                    logger.exception("VN daily matrix build failed")
                    heartbeat.beat(status="error", error=f"matrix_build_failed: {exc}")
                    failures.append(f"matrix: {type(exc).__name__}: {exc}")
            else:
                logger.info("VN daily bounded seed skipped derived universe and matrix outputs")
            if args.audit_phase_e:
                audit = audit_configured_symbols([symbol.strip().upper() for symbol in symbols])
                logger.info(
                    "VN daily Phase E audit status=%s passing=%s/%s",
                    audit["status"],
                    audit["passing_symbol_count"],
                    audit["configured_symbol_count"],
                )
                if audit["status"] != "pass":
                    failures.append(f"phase_e_audit={audit['status']}: {audit['failed_symbols'][:20]}")
            if args.fail_on_symbol_error and failures:
                raise RuntimeError("; ".join(failures))
            last_run_date = datetime.now().strftime("%Y-%m-%d")
            schedule_state.write({
                "last_run_date": last_run_date,
                "updated_at": utc_now_iso(),
                "symbols_count": len(symbols),
            })
        if args.mode != "live":
            break
        sleep_with_heartbeat(
            heartbeat,
            300,
            schedule=args.schedule,
            last_run_date=last_run_date,
        )


if __name__ == "__main__":
    main()
