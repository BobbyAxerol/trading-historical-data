"""Resumable Binance USD-M perpetual 1m archive rebuild.

This collector is deliberately separate from the B0 live tail.  It downloads
one completed Vision archive or one bounded REST window at a time, writes the
partition atomically, then releases the working frame.  It therefore never
holds the complete historical range in memory.
"""

from __future__ import annotations

import argparse
import io
import time
import zipfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from collectors.binance_usdm_quarterly_1m import (
    KLINE_COLUMNS,
    OUTPUT_COLUMNS,
    S3_BASE_URL,
    VISION_BASE_URL,
    _date_from_key,
    _month_from_key,
    _request_bytes,
    _request_json,
    _s3_keys,
    normalize_kline_frame,
)
from collectors.common.config import load_yaml
from collectors.common.env import load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, Manifest, utc_now_iso
from collectors.common.storage import PartitionedParquetStore, read_partition_file, release_unused_memory
from collectors.crypto_1m import BINANCE_FAPI, _closed_until, fetch_1m


DATASET = "crypto_binance_futures_1m"
SERVICE = "phase_d_binance_usdm_perpetual_1m"
STORE_PARTS = ["crypto", "binance_futures", "1m"]
ONE_MINUTE = pd.Timedelta(minutes=1)
MAX_DAILY_VISION_REPAIR_DAYS = 31


def read_vision_zip(content: bytes, *, symbol: str, source: str) -> pd.DataFrame:
    """Read both old headerless and current headered Binance Vision CSVs."""

    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not csv_names:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)
        with archive.open(csv_names[0]) as handle:
            raw = pd.read_csv(handle, header=None)

    if raw.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    if str(raw.iloc[0, 0]).strip().lower() in {"open_time", "open time", "timestamp"}:
        raw = raw.iloc[1:].reset_index(drop=True)
    raw.columns = KLINE_COLUMNS[: len(raw.columns)]
    return normalize_kline_frame(raw, symbol=symbol, source=source)


def discover_active_perpetuals(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """Fail closed unless each explicitly requested symbol is a live USD-M perpetual."""

    requested = {symbol.upper() for symbol in symbols}
    payload = _request_json(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    active: dict[str, dict[str, Any]] = {}
    for item in payload.get("symbols", []):
        symbol = str(item.get("symbol", "")).upper()
        if symbol not in requested:
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != "USDT" or item.get("marginAsset") != "USDT":
            continue
        if item.get("status") != "TRADING":
            continue
        onboard = item.get("onboardDate")
        active[symbol] = {
            "symbol": symbol,
            "pair": item.get("pair"),
            "contract_type": item.get("contractType"),
            "status": item.get("status"),
            "onboard_time": pd.to_datetime(onboard, unit="ms", utc=True).isoformat() if onboard else None,
        }

    missing = sorted(requested - set(active))
    if missing:
        raise RuntimeError(f"requested symbols are not active USD-M USDT perpetuals: {','.join(missing)}")
    return active


def vision_monthly_keys(symbol: str, *, interval: str, start_month: str, s3_base_url: str) -> list[str]:
    prefix = f"data/futures/um/monthly/klines/{symbol}/{interval}/"
    keys = []
    for key in _s3_keys(prefix, s3_base_url=s3_base_url):
        month = _month_from_key(key)
        if key.endswith(".zip") and month and month >= start_month:
            keys.append(key)
    return sorted(keys)


def _append(store: PartitionedParquetStore, df: pd.DataFrame, symbol: str) -> dict[str, object]:
    return store.append(
        df,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": symbol},
        # Match the B0 live-tail lock so a tail and archive writer cannot
        # replace the same current-month partition concurrently.
        lock_name=f"{DATASET}/{symbol}",
    )


def _month_partition_exists(store: PartitionedParquetStore, symbol: str, month: str) -> bool:
    year, month_number = month.split("-", 1)
    directory = store.root / f"symbol={symbol}" / f"year={int(year):04d}" / f"month={int(month_number):02d}"
    return (directory / "part.parquet").exists() or (directory / "part.csv.gz").exists()


def _day_complete(store: PartitionedParquetStore, symbol: str, day: pd.Timestamp) -> bool:
    """Return true only for a complete 1,440-minute UTC calendar day.

    A B0 tail can have some rows in a current day; treating any row as a
    complete day would silently preserve a gap before the tail began.
    """

    day = pd.Timestamp(day).tz_localize(None).normalize()
    directory = store.root / f"symbol={symbol}" / f"year={day.year:04d}" / f"month={day.month:02d}"
    path = directory / "part.parquet"
    if not path.exists():
        path = directory / "part.csv.gz"
    if not path.exists():
        return False
    try:
        frame = read_partition_file(path, usecols=["time"])
        values = pd.to_datetime(frame["time"], errors="coerce").dropna()
        values = values[values.dt.normalize() == day].drop_duplicates().sort_values()
    except Exception:
        return False
    finally:
        release_unused_memory()
    if len(values) != 1440:
        return False
    return values.iloc[0] == day and values.iloc[-1] == day + pd.Timedelta(minutes=1439)


def sync_vision_file(
    *,
    key: str,
    symbol: str,
    store: PartitionedParquetStore,
    manifest: Manifest,
    vision_base_url: str,
    source: str,
    logger,
) -> dict[str, object]:
    content = _request_bytes(f"{vision_base_url.rstrip('/')}/{key}")
    if content is None:
        manifest.update_symbol(symbol, last_missing_vision_key=key, last_error="vision_404")
        logger.warning("%s missing Vision file: %s", symbol, key)
        return {"rows_written": 0, "latest_time": None, "missing": True}

    frame = read_vision_zip(content, symbol=symbol, source=source)
    del content
    if frame.empty:
        manifest.update_symbol(symbol, last_empty_vision_key=key, last_error="empty_vision_file")
        logger.warning("%s empty Vision file: %s", symbol, key)
        release_unused_memory()
        return {"rows_written": 0, "latest_time": None, "missing": False}

    result = _append(store, frame, symbol)
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        last_success_at=utc_now_iso(),
        last_vision_key=key,
        rows_written=result["rows_written"],
        source=source,
        last_error=None,
    )
    logger.info("%s Vision wrote rows=%s latest=%s key=%s", symbol, result["rows_written"], result["latest_time"], Path(key).name)
    del frame
    release_unused_memory()
    return {**result, "missing": False}


def sync_monthly_archives(
    *,
    symbol: str,
    interval: str,
    start_month: str,
    store: PartitionedParquetStore,
    manifest: Manifest,
    vision_base_url: str,
    s3_base_url: str,
    logger,
) -> dict[str, object]:
    total_rows = 0
    downloaded = 0
    skipped = 0
    latest_time = None
    keys = vision_monthly_keys(symbol, interval=interval, start_month=start_month, s3_base_url=s3_base_url)
    if not keys:
        raise RuntimeError(f"no USD-M monthly Vision archive keys found for {symbol} from {start_month}")

    for key in keys:
        month = _month_from_key(key)
        if month and _month_partition_exists(store, symbol, month):
            skipped += 1
            continue
        result = sync_vision_file(
            key=key,
            symbol=symbol,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            source="binance_vision_futures_um_monthly",
            logger=logger,
        )
        if result["missing"]:
            raise RuntimeError(f"monthly Vision archive disappeared during Phase D: {key}")
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time
        downloaded += 1
        time.sleep(0.05)

    return {
        "rows_written": total_rows,
        "latest_time": latest_time,
        "archive_keys": len(keys),
        "downloaded_archives": downloaded,
        "skipped_existing_archives": skipped,
    }


def _daily_bridge_days(days: int) -> list[pd.Timestamp]:
    if days <= 0:
        return []
    yesterday = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize() - pd.Timedelta(days=1)
    start = yesterday - pd.Timedelta(days=days - 1)
    return list(pd.date_range(start, yesterday, freq="D"))


def _daily_vision_key(symbol: str, interval: str, day: pd.Timestamp) -> str:
    date_text = pd.Timestamp(day).tz_localize(None).strftime("%Y-%m-%d")
    return f"data/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_text}.zip"


def sync_daily_bridge(
    *,
    symbol: str,
    interval: str,
    days: int,
    store: PartitionedParquetStore,
    manifest: Manifest,
    vision_base_url: str,
    logger,
) -> dict[str, object]:
    total_rows = 0
    downloaded = 0
    skipped = 0
    missing = 0
    latest_time = None
    for day in _daily_bridge_days(days):
        if _day_complete(store, symbol, day):
            skipped += 1
            continue
        key = _daily_vision_key(symbol, interval, day)
        result = sync_vision_file(
            key=key,
            symbol=symbol,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            source="binance_vision_futures_um_daily_bridge",
            logger=logger,
        )
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time
        missing += int(bool(result["missing"]))
        downloaded += 1
        time.sleep(0.05)
    return {
        "rows_written": total_rows,
        "latest_time": latest_time,
        "daily_files_requested": downloaded,
        "skipped_complete_days": skipped,
        "missing_daily_files": missing,
    }


def _utc_day(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.normalize()


def _audited_gap_days(audit: dict[str, object]) -> list[pd.Timestamp]:
    """Expand persisted gap examples to exact UTC dates, without guessing bars."""
    days: set[pd.Timestamp] = set()
    for example in audit.get("gap_examples", []):
        if not isinstance(example, dict) or not example.get("start") or not example.get("end"):
            continue
        start = _utc_day(example["start"])
        end = _utc_day(example["end"])
        if end < start:
            continue
        days.update(pd.date_range(start, end, freq="D"))
    return sorted(days)


def repair_audited_vision_gaps(
    *,
    symbol: str,
    interval: str,
    audit: dict[str, object],
    store: PartitionedParquetStore,
    manifest: Manifest,
    vision_base_url: str,
    logger,
    max_repair_days: int = MAX_DAILY_VISION_REPAIR_DAYS,
) -> dict[str, object]:
    """Repair only explicitly audited historical gaps from official daily Vision files."""
    if max_repair_days <= 0:
        raise ValueError("max_repair_days must be positive")
    days = _audited_gap_days(audit)
    if not days:
        return {"status": "not_required", "requested_days": [], "rows_written": 0, "missing_days": [], "unrepaired_days": []}
    if len(days) > max_repair_days:
        return {
            "status": "bounded_refusal",
            "requested_days": [day.date().isoformat() for day in days],
            "rows_written": 0,
            "missing_days": [],
            "unrepaired_days": [day.date().isoformat() for day in days],
            "max_repair_days": max_repair_days,
        }

    total_rows = 0
    skipped_days: list[str] = []
    missing_days: list[str] = []
    unrepaired_days: list[str] = []
    for day in days:
        day_text = day.date().isoformat()
        if _day_complete(store, symbol, day):
            skipped_days.append(day_text)
            continue
        result = sync_vision_file(
            key=_daily_vision_key(symbol, interval, day),
            symbol=symbol,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            source="binance_vision_futures_um_daily_repair",
            logger=logger,
        )
        total_rows += int(result.get("rows_written") or 0)
        if result["missing"]:
            missing_days.append(day_text)
        elif not _day_complete(store, symbol, day):
            unrepaired_days.append(day_text)
        time.sleep(0.05)

    return {
        "status": "pass" if not missing_days and not unrepaired_days else "source_unavailable",
        "requested_days": [day.date().isoformat() for day in days],
        "skipped_complete_days": skipped_days,
        "rows_written": total_rows,
        "missing_days": missing_days,
        "unrepaired_days": unrepaired_days,
        "source": "binance_vision_futures_um_daily_repair",
    }


def sync_rest_bridge(
    *,
    symbol: str,
    days: int,
    window_minutes: int,
    store: PartitionedParquetStore,
    manifest: Manifest,
    logger,
) -> dict[str, object]:
    if days <= 0:
        return {"rows_written": 0, "latest_time": None, "windows": 0}
    if window_minutes <= 0:
        raise ValueError("rest window minutes must be positive")

    end = _closed_until()
    start = end - pd.Timedelta(days=days)
    cursor = start
    total_rows = 0
    latest_time = None
    windows = 0
    while cursor <= end:
        window_end = min(cursor + pd.Timedelta(minutes=window_minutes - 1), end)
        logger.info("%s REST bridge %s -> %s", symbol, cursor, window_end)
        frame = fetch_1m(symbol, cursor, window_end)
        if frame.empty:
            raise RuntimeError(f"empty REST bridge response for {symbol}: {cursor} -> {window_end}")
        frame = frame.copy()
        frame["source"] = "binance_futures_rest_bridge"
        result = _append(store, frame, symbol)
        manifest.update_symbol(
            symbol,
            latest_time=str(result["latest_time"]),
            last_success_at=utc_now_iso(),
            rows_written=result["rows_written"],
            source="binance_futures_rest_bridge",
            last_error=None,
        )
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time
        windows += 1
        del frame
        release_unused_memory()
        cursor = window_end + ONE_MINUTE
    return {"rows_written": total_rows, "latest_time": latest_time, "windows": windows}


def validate_symbol(
    *,
    store: PartitionedParquetStore,
    symbol: str,
    expected_start: str | None,
) -> dict[str, object]:
    """Validate one partition at a time, including cross-partition continuity."""

    paths = store.files({"symbol": symbol})
    expected = pd.Timestamp(expected_start, tz="UTC") if expected_start else None
    previous: pd.Timestamp | None = None
    first: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None
    rows = 0
    duplicate_rows = 0
    ohlc_bad_rows = 0
    negative_rows = 0
    gap_count = 0
    max_gap_minutes = 0
    examples: list[dict[str, object]] = []

    for path in paths:
        frame = read_partition_file(
            path,
            usecols=["time", "symbol", "open", "high", "low", "close", "volume", "quote_volume"],
        )
        rows += len(frame)
        frame["time"] = pd.to_datetime(frame["time"], errors="coerce", utc=True)
        frame = frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        duplicate_rows += int(frame.duplicated(subset=["symbol", "time"]).sum())
        times = frame["time"].drop_duplicates().sort_values().reset_index(drop=True)
        if times.empty:
            del frame
            release_unused_memory()
            continue

        prices = frame[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
        ohlc_bad_rows += int(
            ((prices["high"] < prices[["open", "low", "close"]].max(axis=1)) | (prices["low"] > prices[["open", "high", "close"]].min(axis=1))).sum()
        )
        numeric = frame[["open", "high", "low", "close", "volume", "quote_volume"]].apply(pd.to_numeric, errors="coerce")
        negative_rows += int((numeric < 0).any(axis=1).sum())

        if first is None:
            first = times.iloc[0]
            if expected is not None and first > expected:
                gap = first - expected
                gap_count += 1
                max_gap_minutes = max(max_gap_minutes, int(gap.total_seconds() // 60))
                examples.append({"start": expected.isoformat(), "end": (first - ONE_MINUTE).isoformat(), "minutes": int(gap.total_seconds() // 60)})
        if previous is not None:
            boundary_gap = times.iloc[0] - previous
            if boundary_gap > ONE_MINUTE:
                gap_count += 1
                max_gap_minutes = max(max_gap_minutes, int(boundary_gap.total_seconds() // 60) - 1)
                if len(examples) < 10:
                    examples.append({"start": (previous + ONE_MINUTE).isoformat(), "end": (times.iloc[0] - ONE_MINUTE).isoformat(), "minutes": int(boundary_gap.total_seconds() // 60) - 1})
            elif boundary_gap <= pd.Timedelta(0):
                duplicate_rows += 1

        diffs = times.diff().dropna()
        for index in diffs[diffs > ONE_MINUTE].index:
            gap = diffs.loc[index]
            gap_count += 1
            max_gap_minutes = max(max_gap_minutes, int(gap.total_seconds() // 60) - 1)
            if len(examples) < 10:
                examples.append({"start": (times.loc[index - 1] + ONE_MINUTE).isoformat(), "end": (times.loc[index] - ONE_MINUTE).isoformat(), "minutes": int(gap.total_seconds() // 60) - 1})
        previous = times.iloc[-1]
        latest = previous
        del frame, times
        release_unused_memory()

    closed_until = _closed_until()
    tail_lag_minutes = None if latest is None else max(0, int((closed_until - latest).total_seconds() // 60))
    valid = bool(
        paths
        and first is not None
        and latest is not None
        and duplicate_rows == 0
        and ohlc_bad_rows == 0
        and negative_rows == 0
        and gap_count == 0
        and tail_lag_minutes is not None
        and tail_lag_minutes <= 5
    )
    return {
        "status": "pass" if valid else "requires_repair",
        "symbol": symbol,
        "files": len(paths),
        "rows": rows,
        "first": None if first is None else first.isoformat(),
        "latest": None if latest is None else latest.isoformat(),
        "duplicate_rows": duplicate_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "negative_rows": negative_rows,
        "gap_count": gap_count,
        "max_gap_minutes": max_gap_minutes,
        "gap_examples": examples,
        "tail_lag_minutes": tail_lag_minutes,
        "validated_at": utc_now_iso(),
    }


def sync_symbol(
    *,
    symbol: str,
    interval: str,
    start_month: str,
    daily_bridge_days: int,
    rest_bridge_days: int,
    rest_window_minutes: int,
    store: PartitionedParquetStore,
    manifest: Manifest,
    vision_base_url: str,
    s3_base_url: str,
    logger,
) -> dict[str, object]:
    monthly = sync_monthly_archives(
        symbol=symbol,
        interval=interval,
        start_month=start_month,
        store=store,
        manifest=manifest,
        vision_base_url=vision_base_url,
        s3_base_url=s3_base_url,
        logger=logger,
    )
    daily = sync_daily_bridge(
        symbol=symbol,
        interval=interval,
        days=daily_bridge_days,
        store=store,
        manifest=manifest,
        vision_base_url=vision_base_url,
        logger=logger,
    )
    rest = sync_rest_bridge(
        symbol=symbol,
        days=rest_bridge_days,
        window_minutes=rest_window_minutes,
        store=store,
        manifest=manifest,
        logger=logger,
    )
    return {"monthly": monthly, "daily": daily, "rest": rest}


def _symbols_from_args(args: argparse.Namespace, config: dict[str, Any]) -> list[str]:
    values = args.symbols.split(",") if args.symbols else config.get("symbols", [])
    symbols = [str(value).strip().upper() for value in values if str(value).strip()]
    if not symbols:
        raise ValueError("at least one explicitly configured perpetual symbol is required")
    return symbols


def run(args: argparse.Namespace) -> dict[str, object]:
    phase_label = str(getattr(args, "phase_label", "d")).strip().lower()
    if phase_label not in {"d", "e"}:
        raise ValueError("phase_label must be d or e")
    service = f"phase_{phase_label}_binance_usdm_perpetual_1m"
    config = load_yaml("symbols.binance_usdm_perpetual.yml")
    symbols = _symbols_from_args(args, config)
    interval = str(config.get("interval", "1m"))
    start_month = str(args.start_month or config.get("start_month", "2020-01"))
    daily_bridge_days = int(args.daily_bridge_days if args.daily_bridge_days is not None else config.get("daily_bridge_days", 35))
    rest_bridge_days = int(args.rest_bridge_days if args.rest_bridge_days is not None else config.get("rest_bridge_days", 35))
    rest_window_minutes = int(args.rest_window_minutes if args.rest_window_minutes is not None else config.get("rest_window_minutes", 10080))
    logger = setup_logging(service)
    active = discover_active_perpetuals(symbols)
    store = PartitionedParquetStore(STORE_PARTS, partition="month")
    manifest = Manifest(DATASET)
    heartbeat = Heartbeat(service)
    results: dict[str, object] = {}
    failures: list[str] = []

    JsonState(f"phase_{phase_label}/{service}.json").write(
        {
            "status": "running",
            "phase": phase_label,
            "service": service,
            "dataset": DATASET,
            "symbols": symbols,
            "active_contracts": active,
            "start_month": start_month,
            "daily_bridge_days": daily_bridge_days,
            "rest_bridge_days": rest_bridge_days,
            "rest_window_minutes": rest_window_minutes,
            "started_at": utc_now_iso(),
        }
    )

    for symbol in symbols:
        try:
            result = sync_symbol(
                symbol=symbol,
                interval=interval,
                start_month=start_month,
                daily_bridge_days=daily_bridge_days,
                rest_bridge_days=rest_bridge_days,
                rest_window_minutes=rest_window_minutes,
                store=store,
                manifest=manifest,
                vision_base_url=str(config.get("vision_base_url", VISION_BASE_URL)),
                s3_base_url=str(config.get("s3_base_url", S3_BASE_URL)),
                logger=logger,
            )
            audit = {
                "status": "skipped"
            } if args.no_validate else validate_symbol(
                store=store,
                symbol=symbol,
                expected_start=None if getattr(args, "allow_later_start", False) else f"{start_month}-01",
            )
            repair = {"status": "not_required", "requested_days": [], "rows_written": 0, "missing_days": [], "unrepaired_days": []}
            if audit["status"] != "pass" and int(audit.get("gap_count") or 0) > 0:
                repair = repair_audited_vision_gaps(
                    symbol=symbol,
                    interval=interval,
                    audit=audit,
                    store=store,
                    manifest=manifest,
                    vision_base_url=str(config.get("vision_base_url", VISION_BASE_URL)),
                    logger=logger,
                )
                audit = validate_symbol(
                    store=store,
                    symbol=symbol,
                    expected_start=None if getattr(args, "allow_later_start", False) else f"{start_month}-01",
                )
            results[symbol] = {**result, "repair": repair, "validation": audit}
            JsonState(f"audits/{DATASET}_{symbol}_phase_{phase_label}.json").write(
                {
                    "dataset": DATASET,
                    "phase": phase_label,
                    "service": service,
                    "symbol": symbol,
                    "expected_start_policy": "source_listing_allowed" if getattr(args, "allow_later_start", False) else "exact_configured_start",
                    "repair": repair,
                    **audit,
                }
            )
            manifest.update_symbol(
                symbol,
                **{
                    f"phase_{phase_label}_validation": audit,
                    f"phase_{phase_label}_last_run_at": utc_now_iso(),
                    "last_error": None if audit["status"] == "pass" else f"phase_{phase_label}_validation_requires_repair",
                },
            )
            if audit["status"] != "pass":
                failures.append(f"{symbol} validation={audit['status']}")
                heartbeat.beat(status="error", symbol=symbol, error=f"phase_{phase_label}_validation_requires_repair", validation=audit)
            else:
                heartbeat.beat(status="ok", symbol=symbol, validation=audit)
            logger.info("%s Phase %s result: %s", symbol, phase_label.upper(), results[symbol])
        except Exception as exc:
            failures.append(f"{symbol}: {exc}")
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))
            logger.exception("%s Phase D sync failed", symbol)

    status = "pass" if not failures else "requires_repair"
    payload = {
        "status": status,
        "phase": phase_label,
        "service": service,
        "dataset": DATASET,
        "symbols": symbols,
        "results": results,
        "failures": failures,
        "completed_at": utc_now_iso(),
    }
    JsonState(f"phase_{phase_label}/{service}.json").write(payload)
    if failures:
        raise RuntimeError("; ".join(failures))
    return payload


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Run an append-only, bounded-memory USD-M perpetual 1m controlled rebuild.")
    parser.add_argument("--mode", choices=["once"], default="once")
    parser.add_argument("--symbols", default=None, help="Comma-separated explicit perpetual symbols; no default-universe expansion.")
    parser.add_argument("--start-month", default=None, help="First UTC month of completed Vision archive history, e.g. 2020-01.")
    parser.add_argument("--daily-bridge-days", type=int, default=None, help="Completed UTC days to bridge from daily Vision archives.")
    parser.add_argument("--rest-bridge-days", type=int, default=None, help="Most recent UTC days to bridge from REST in bounded windows.")
    parser.add_argument("--rest-window-minutes", type=int, default=None, help="Maximum REST rows held before an immediate partition append.")
    parser.add_argument("--phase-label", default="d", choices=["d", "e"], help="Evidence namespace; Phase D remains the compatibility default.")
    parser.add_argument(
        "--allow-later-start",
        action="store_true",
        help="Accept a later first bar when Binance listing/archive availability begins after --start-month; still require strict continuity from the first source bar onward.",
    )
    parser.add_argument("--no-validate", action="store_true", help="Reserved for isolated debugging; not permitted by the Phase D entrypoint.")
    args = parser.parse_args()
    result = run(args)
    print(result)


if __name__ == "__main__":
    main()
