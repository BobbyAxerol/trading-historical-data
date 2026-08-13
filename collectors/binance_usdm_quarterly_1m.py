from __future__ import annotations

import argparse
import io
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pandas as pd
import requests

from collectors.common.config import load_yaml
from collectors.common.env import load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, Manifest, sleep_with_heartbeat, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.storage import PartitionedParquetStore as PartitionedCsvGzStore
from collectors.common.storage import read_partition_file, release_unused_memory
from collectors.crypto_1m import BINANCE_FAPI, _closed_until, fetch_1m

DATASET = "crypto_binance_usdm_quarterly_1m"
SERVICE = "phase_d_binance_usdm_quarterly_1m"
STORE_PARTS = ["crypto", "binance_futures", "1m"]
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
VISION_BASE_URL = "https://data.binance.vision"
S3_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
USER_AGENT = {"User-Agent": "pool-alpha-get-data/1.0"}
QUARTERLY_TYPES = {"CURRENT_QUARTER", "NEXT_QUARTER"}
KLINE_COLUMNS = [
    "open_time",
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
    "ignore",
]
OUTPUT_COLUMNS = [
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
ONE_MINUTE = pd.Timedelta(minutes=1)
NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]
# The staged tail predates Phase D and uses the same direct Binance REST
# source.  It remains an allowed provenance only for its overlapping active
# contract partition; no old-VPS or synthetic source is accepted here.
PHASE_D_SOURCES = {
    "binance_vision_futures_um_monthly",
    "binance_vision_futures_um_daily",
    "binance_vision_futures_um_daily_gap_repair",
    "binance_futures_rest",
    "binance_futures_rest_gap_repair",
    "binance_futures",
}


def _request_json(url: str, *, params: dict[str, Any] | None = None) -> Any:
    def call() -> Any:
        response = requests.get(url, params=params, timeout=60, headers=USER_AGENT)
        if response.status_code in {418, 429} or response.status_code >= 500:
            raise RuntimeError(f"Retryable HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        return response.json()

    return retry_sync(call, attempts=5)


def _request_bytes(url: str) -> bytes | None:
    def call() -> bytes | None:
        response = requests.get(url, timeout=120, headers=USER_AGENT)
        if response.status_code == 404:
            return None
        if response.status_code in {418, 429} or response.status_code >= 500:
            raise RuntimeError(f"Retryable HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        return response.content

    return retry_sync(call, attempts=5)


def _s3_get(params: dict[str, str], s3_base_url: str) -> ET.Element:
    def call() -> ET.Element:
        response = requests.get(s3_base_url, params=params, timeout=60, headers=USER_AGENT)
        if response.status_code in {418, 429} or response.status_code >= 500:
            raise RuntimeError(f"Retryable S3 HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        return ET.fromstring(response.text)

    return retry_sync(call, attempts=5)


def _s3_common_prefixes(prefix: str, *, s3_base_url: str) -> list[str]:
    prefixes: list[str] = []
    marker: str | None = None
    while True:
        params = {"delimiter": "/", "prefix": prefix}
        if marker:
            params["marker"] = marker
        root = _s3_get(params, s3_base_url)
        batch = [node.find("s3:Prefix", S3_NS).text for node in root.findall("s3:CommonPrefixes", S3_NS)]
        batch = [item for item in batch if item]
        prefixes.extend(batch)
        if root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS) != "true":
            break
        marker = root.findtext("s3:NextMarker", namespaces=S3_NS) or (batch[-1] if batch else None)
        if not marker:
            break
    return prefixes


def _s3_keys(prefix: str, *, s3_base_url: str) -> list[str]:
    keys: list[str] = []
    marker: str | None = None
    while True:
        params = {"prefix": prefix}
        if marker:
            params["marker"] = marker
        root = _s3_get(params, s3_base_url)
        batch = [node.findtext("s3:Key", namespaces=S3_NS) for node in root.findall("s3:Contents", S3_NS)]
        batch = [item for item in batch if item]
        keys.extend(batch)
        if root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS) != "true":
            break
        marker = batch[-1] if batch else None
        if not marker:
            break
    return keys


def _ms_to_timestamp(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    unit = "us" if values.dropna().gt(10**14).any() else "ms"
    return pd.to_datetime(values, unit=unit, errors="coerce", utc=True).dt.tz_convert(None)


def _delivery_from_symbol(symbol: str) -> str | None:
    match = re.search(r"_(\d{6})$", symbol)
    if not match:
        return None
    code = match.group(1)
    return f"20{code[:2]}-{code[2:4]}-{code[4:6]}"


def normalize_kline_frame(df: pd.DataFrame, *, symbol: str, source: str) -> pd.DataFrame:
    if "open_time" not in df.columns:
        df = df.copy()
        df.columns = KLINE_COLUMNS[: len(df.columns)]

    df = df.rename(
        columns={
            "count": "number_of_trades",
            "taker_buy_volume": "taker_buy_base_volume",
        }
    )
    for col in KLINE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    result = df[KLINE_COLUMNS].copy()
    result["time"] = _ms_to_timestamp(result["open_time"])
    result["close_time"] = _ms_to_timestamp(result["close_time"])
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    for col in numeric_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    prices = result[["open", "high", "low", "close"]]
    result["high"] = prices.max(axis=1, skipna=False)
    result["low"] = prices.min(axis=1, skipna=False)
    result["number_of_trades"] = pd.to_numeric(result["number_of_trades"], errors="coerce").astype("Int64")
    result["symbol"] = symbol.upper()
    result["source"] = source
    result["ingested_at"] = utc_now_iso()
    result = result[OUTPUT_COLUMNS].dropna(subset=["time", "open", "high", "low", "close"])
    result = result.drop_duplicates(subset=["symbol", "time"], keep="last").sort_values(["symbol", "time"])
    return result.reset_index(drop=True)


def read_vision_zip(content: bytes, *, symbol: str, source: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not csv_names:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)
        with archive.open(csv_names[0]) as handle:
            # Vision has published both headered and headerless Kline CSVs.
            # Reading with a default header would silently discard the first
            # candle of headerless archives.
            df = pd.read_csv(handle, header=None)
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    if str(df.iloc[0, 0]).strip().lower() in {"open_time", "open time", "timestamp"}:
        df = df.iloc[1:].reset_index(drop=True)
    df.columns = KLINE_COLUMNS[: len(df.columns)]
    return normalize_kline_frame(df, symbol=symbol, source=source)


def discover_active_contracts(pairs: list[str]) -> dict[str, dict[str, Any]]:
    payload = _request_json(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    pair_set = {pair.upper() for pair in pairs}
    active: dict[str, dict[str, Any]] = {}
    for item in payload.get("symbols", []):
        if item.get("contractType") not in QUARTERLY_TYPES:
            continue
        if item.get("quoteAsset") != "USDT" or item.get("marginAsset") != "USDT":
            continue
        if item.get("pair", "").upper() not in pair_set:
            continue
        symbol = item["symbol"].upper()
        active[symbol] = {
            "symbol": symbol,
            "pair": item.get("pair"),
            "contract_type": item.get("contractType"),
            "status": item.get("status"),
            "onboard_time": pd.to_datetime(item.get("onboardDate"), unit="ms", utc=True).isoformat(),
            "delivery_time": pd.to_datetime(item.get("deliveryDate"), unit="ms", utc=True).isoformat(),
        }
    return active


def discover_archive_symbols(pairs: list[str], *, s3_base_url: str) -> list[str]:
    pair_set = {pair.upper() for pair in pairs}
    base_prefix = "data/futures/um/monthly/klines/"
    symbols = []
    for prefix in _s3_common_prefixes(base_prefix, s3_base_url=s3_base_url):
        symbol = prefix.rstrip("/").split("/")[-1].upper()
        if not re.match(r"^[A-Z0-9]+USDT_\d{6}$", symbol):
            continue
        if symbol.split("_", 1)[0] in pair_set:
            symbols.append(symbol)
    return sorted(symbols)


def _month_from_key(key: str) -> str | None:
    match = re.search(r"-(\d{4}-\d{2})\.zip$", key)
    return match.group(1) if match else None


def _date_from_key(key: str) -> str | None:
    match = re.search(r"-(\d{4}-\d{2}-\d{2})\.zip$", key)
    return match.group(1) if match else None


def vision_monthly_keys(symbol: str, *, interval: str, start_month: str, s3_base_url: str) -> list[str]:
    prefix = f"data/futures/um/monthly/klines/{symbol}/{interval}/"
    keys = []
    for key in _s3_keys(prefix, s3_base_url=s3_base_url):
        month = _month_from_key(key)
        if key.endswith(".zip") and month and month >= start_month:
            keys.append(key)
    return sorted(keys)


def vision_daily_keys(symbol: str, *, interval: str, s3_base_url: str) -> list[str]:
    prefix = f"data/futures/um/daily/klines/{symbol}/{interval}/"
    return sorted(key for key in _s3_keys(prefix, s3_base_url=s3_base_url) if key.endswith(".zip") and _date_from_key(key))


def _append(store: PartitionedCsvGzStore, df: pd.DataFrame, symbol: str) -> dict[str, object]:
    return store.append(
        df,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": symbol},
        lock_name=f"{DATASET}/{symbol}",
    )


def _month_partition_exists(store: PartitionedCsvGzStore, symbol: str, month: str) -> bool:
    year, month_num = month.split("-", 1)
    partition_dir = store.root / f"symbol={symbol}" / f"year={int(year):04d}" / f"month={int(month_num):02d}"
    return (partition_dir / "part.parquet").exists() or (partition_dir / "part.csv.gz").exists()


def _date_exists(store: PartitionedCsvGzStore, symbol: str, date_text: str) -> bool:
    date = pd.Timestamp(date_text)
    partition_dir = store.root / f"symbol={symbol}" / f"year={date.year:04d}" / f"month={date.month:02d}"
    path = partition_dir / "part.parquet"
    if not path.exists():
        path = partition_dir / "part.csv.gz"
    if not path.exists():
        return False
    try:
        df = read_partition_file(path, usecols=["time"])
    except Exception:
        return False
    times = pd.to_datetime(df["time"], errors="coerce")
    return bool((times.dt.normalize() == date.normalize()).any())


def small_gap_repair_dates(store: PartitionedCsvGzStore, symbol: str, *, max_gap_minutes: int) -> list[str]:
    return sorted({end.strftime("%Y-%m-%d") for _, end in small_gap_ranges(store, symbol, max_gap_minutes=max_gap_minutes)})


def small_gap_ranges(store: PartitionedCsvGzStore, symbol: str, *, max_gap_minutes: int) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Find only bounded gaps while retaining one partition at a time."""

    ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    previous: pd.Timestamp | None = None
    max_delta = pd.Timedelta(minutes=max_gap_minutes + 1)

    for path in sorted(store.files({"symbol": symbol})):
        try:
            frame = read_partition_file(path, usecols=["time"])
        except Exception:
            continue
        times = (
            pd.to_datetime(frame["time"], errors="coerce", utc=True)
            .dropna()
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        )
        if times.empty:
            del frame, times
            release_unused_memory()
            continue

        if previous is not None:
            boundary = times.iloc[0] - previous
            if ONE_MINUTE < boundary <= max_delta:
                ranges.append((previous + ONE_MINUTE, times.iloc[0] - ONE_MINUTE))

        diffs = times.diff()
        for index in diffs[(diffs > ONE_MINUTE) & (diffs <= max_delta)].index:
            ranges.append((times.iloc[index - 1] + ONE_MINUTE, times.iloc[index] - ONE_MINUTE))

        previous = pd.Timestamp(times.iloc[-1])
        del frame, times, diffs
        release_unused_memory()

    return ranges


def repair_small_gaps(
    *,
    symbol: str,
    interval: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    vision_base_url: str,
    max_gap_minutes: int,
    logger,
) -> int:
    repaired_rows = 0
    for date_text in small_gap_repair_dates(store, symbol, max_gap_minutes=max_gap_minutes):
        key = f"data/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_text}.zip"
        result = sync_vision_file(
            key=key,
            symbol=symbol,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            source="binance_vision_futures_um_daily_gap_repair",
            logger=logger,
        )
        repaired_rows += int(result.get("rows_written") or 0)

    for start, end in small_gap_ranges(store, symbol, max_gap_minutes=max_gap_minutes):
        logger.info("%s REST gap repair %s -> %s", symbol, start, end)
        start_utc = pd.Timestamp(start)
        end_utc = pd.Timestamp(end)
        start_utc = start_utc.tz_localize("UTC") if start_utc.tzinfo is None else start_utc.tz_convert("UTC")
        end_utc = end_utc.tz_localize("UTC") if end_utc.tzinfo is None else end_utc.tz_convert("UTC")
        df = fetch_1m(symbol, start_utc, end_utc)
        if df.empty:
            manifest.update_symbol(symbol, last_error="empty_rest_gap_repair", last_gap_start=str(start), last_gap_end=str(end))
            continue
        df = df.copy()
        df["source"] = "binance_futures_rest_gap_repair"
        result = _append(store, df, symbol)
        repaired_rows += int(result.get("rows_written") or 0)
        manifest.update_symbol(
            symbol,
            latest_time=str(result["latest_time"]),
            last_success_at=utc_now_iso(),
            last_gap_start=str(start),
            last_gap_end=str(end),
            source="binance_futures_rest_gap_repair",
            last_error=None,
        )
    return repaired_rows


def sync_vision_file(
    *,
    key: str,
    symbol: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    vision_base_url: str,
    source: str,
    logger,
) -> dict[str, object]:
    url = f"{vision_base_url.rstrip('/')}/{key}"
    content = _request_bytes(url)
    if content is None:
        manifest.update_symbol(symbol, last_missing_vision_key=key, last_error="vision_404")
        logger.warning("%s missing Vision file: %s", symbol, key)
        return {"rows_written": 0, "latest_time": None, "outcome": "missing"}
    df = read_vision_zip(content, symbol=symbol, source=source)
    if df.empty:
        manifest.update_symbol(symbol, last_empty_vision_key=key, last_error="empty_vision_file")
        logger.warning("%s empty Vision file: %s", symbol, key)
        return {"rows_written": 0, "latest_time": None, "outcome": "empty"}
    result = _append(store, df, symbol)
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
    return {**result, "outcome": "written"}


def sync_rest_tail(
    *,
    symbol: str,
    meta: dict[str, Any] | None,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    overlap_minutes: int,
    rest_start: str | None,
    logger,
) -> dict[str, object]:
    latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    if rest_start:
        explicit_start = pd.Timestamp(rest_start, tz="UTC")
        start = explicit_start if latest is None else max(explicit_start, latest.tz_localize("UTC") - timedelta(minutes=int(overlap_minutes)))
    elif latest is None:
        onboard = meta.get("onboard_time") if meta else None
        start = pd.Timestamp(onboard).tz_convert("UTC") if onboard else pd.Timestamp(_delivery_from_symbol(symbol) or "2021-01-01", tz="UTC")
    else:
        start = latest.tz_localize("UTC") - timedelta(minutes=int(overlap_minutes))

    end = _closed_until()
    if meta and meta.get("delivery_time"):
        delivery = pd.Timestamp(meta["delivery_time"]).tz_convert("UTC") - timedelta(minutes=1)
        end = min(end, delivery)
    if start > end:
        logger.info("%s REST tail current: start=%s end=%s", symbol, start, end)
        return {"rows_written": 0, "latest_time": latest.isoformat() if latest is not None else None}

    logger.info("Fetching %s REST tail %s -> %s", symbol, start, end)
    df = fetch_1m(symbol, start, end)
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_rest_response", last_success_at=utc_now_iso())
        logger.warning("%s REST tail returned no rows", symbol)
        return {"rows_written": 0, "latest_time": latest.isoformat() if latest is not None else None}
    df = df.copy()
    df["source"] = "binance_futures_rest"
    result = _append(store, df, symbol)
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        last_success_at=utc_now_iso(),
        rows_written=result["rows_written"],
        source="binance_futures_rest",
        last_error=None,
    )
    logger.info("%s REST wrote rows=%s latest=%s", symbol, result["rows_written"], result["latest_time"])
    return result


def audit_symbol(
    store: PartitionedCsvGzStore,
    symbol: str,
    *,
    is_active: bool,
    expected_first_archive_month: str | None,
    expected_last_archive_month: str | None,
) -> dict[str, object]:
    """Audit one quarterly contract without loading its complete history.

    Contract boundaries are intentionally assessed per concrete delivery symbol:
    there is no fabricated continuity assertion between two different quarterly
    contracts.  Within a contract, every canonical minute must still be
    continuous after its first observed source candle.
    """

    paths = sorted(store.files({"symbol": symbol}))
    previous: pd.Timestamp | None = None
    first: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None
    rows = 0
    duplicate_rows = 0
    invalid_time_rows = 0
    invalid_numeric_rows = 0
    ohlc_bad_rows = 0
    negative_rows = 0
    source_mismatch_rows = 0
    symbol_mismatch_rows = 0
    post_delivery_date_rows = 0
    out_of_order_partition_rows = 0
    gap_count = 0
    max_gap_minutes = 0
    gap_examples: list[dict[str, object]] = []
    file_errors: list[str] = []
    delivery_text = _delivery_from_symbol(symbol)
    delivery_date = pd.Timestamp(delivery_text, tz="UTC").normalize() if delivery_text else None

    for path in paths:
        try:
            frame = read_partition_file(
                path,
                usecols=["time", "symbol", *NUMERIC_COLUMNS, "source"],
            )
        except Exception as exc:
            file_errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
            continue

        rows += int(len(frame))
        times_raw = pd.to_datetime(frame["time"], errors="coerce", utc=True)
        invalid_time_rows += int(times_raw.isna().sum())
        valid_time = times_raw.notna()
        numeric = frame[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
        invalid_numeric_rows += int(numeric.isna().any(axis=1).sum())
        negative_rows += int((numeric < 0).any(axis=1).sum())
        prices = numeric[["open", "high", "low", "close"]]
        ohlc_bad_rows += int(
            (
                (prices["high"] < prices[["open", "low", "close"]].max(axis=1))
                | (prices["low"] > prices[["open", "high", "close"]].min(axis=1))
            ).sum()
        )
        symbol_mismatch_rows += int((frame["symbol"].astype(str) != symbol.upper()).sum())
        source_mismatch_rows += int((~frame["source"].astype(str).isin(PHASE_D_SOURCES)).sum())
        if delivery_date is not None:
            post_delivery_date_rows += int((times_raw.loc[valid_time].dt.normalize() > delivery_date).sum())

        times = times_raw.loc[valid_time].sort_values().reset_index(drop=True)
        duplicate_rows += int(times.duplicated().sum())
        times = times.drop_duplicates().reset_index(drop=True)
        if not times.empty:
            if previous is not None:
                boundary = times.iloc[0] - previous
                if boundary == pd.Timedelta(0):
                    duplicate_rows += 1
                elif boundary < pd.Timedelta(0):
                    out_of_order_partition_rows += 1
                elif boundary > ONE_MINUTE:
                    gap_count += 1
                    missing_minutes = int(boundary.total_seconds() // 60) - 1
                    max_gap_minutes = max(max_gap_minutes, missing_minutes)
                    if len(gap_examples) < 10:
                        gap_examples.append(
                            {
                                "start": (previous + ONE_MINUTE).isoformat(),
                                "end": (times.iloc[0] - ONE_MINUTE).isoformat(),
                                "minutes": missing_minutes,
                            }
                        )

            diffs = times.diff()
            for index in diffs[diffs > ONE_MINUTE].index:
                gap = diffs.loc[index]
                missing_minutes = int(gap.total_seconds() // 60) - 1
                gap_count += 1
                max_gap_minutes = max(max_gap_minutes, missing_minutes)
                if len(gap_examples) < 10:
                    gap_examples.append(
                        {
                            "start": (times.iloc[index - 1] + ONE_MINUTE).isoformat(),
                            "end": (times.iloc[index] - ONE_MINUTE).isoformat(),
                            "minutes": missing_minutes,
                        }
                    )
            first = times.iloc[0] if first is None else min(first, times.iloc[0])
            latest = times.iloc[-1] if latest is None else max(latest, times.iloc[-1])
            previous = times.iloc[-1]

        del frame, times_raw, numeric, prices, times
        release_unused_memory()

    archive_start_covered = (
        expected_first_archive_month is None
        or (first is not None and first.strftime("%Y-%m") <= expected_first_archive_month)
    )
    archive_end_covered = (
        expected_last_archive_month is None
        or (latest is not None and latest.strftime("%Y-%m") >= expected_last_archive_month)
    )
    tail_lag_minutes = None
    if is_active and latest is not None:
        tail_lag_minutes = max(0, int((_closed_until() - latest).total_seconds() // 60))

    integrity_errors = (
        len(file_errors)
        + duplicate_rows
        + invalid_time_rows
        + invalid_numeric_rows
        + ohlc_bad_rows
        + negative_rows
        + source_mismatch_rows
        + symbol_mismatch_rows
        + post_delivery_date_rows
        + out_of_order_partition_rows
    )
    tail_current = not is_active or (tail_lag_minutes is not None and tail_lag_minutes <= 5)
    status = "pass" if (
        paths
        and first is not None
        and latest is not None
        and archive_start_covered
        and archive_end_covered
        and tail_current
        and integrity_errors == 0
        and gap_count == 0
    ) else "requires_repair"
    return {
        "status": status,
        "symbol": symbol,
        "active": is_active,
        "files": len(paths),
        "rows": rows,
        "first": first.isoformat() if first is not None else None,
        "latest": latest.isoformat() if latest is not None else None,
        "expected_first_archive_month": expected_first_archive_month,
        "expected_last_archive_month": expected_last_archive_month,
        "archive_start_covered": archive_start_covered,
        "archive_end_covered": archive_end_covered,
        "duplicate_rows": duplicate_rows,
        "invalid_time_rows": invalid_time_rows,
        "invalid_numeric_rows": invalid_numeric_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "negative_rows": negative_rows,
        "source_mismatch_rows": source_mismatch_rows,
        "symbol_mismatch_rows": symbol_mismatch_rows,
        "post_delivery_date_rows": post_delivery_date_rows,
        "out_of_order_partition_rows": out_of_order_partition_rows,
        "gap_count": gap_count,
        "max_gap_minutes": max_gap_minutes,
        "gap_examples": gap_examples,
        "tail_lag_minutes": tail_lag_minutes,
        "file_errors": file_errors,
        "validated_at": utc_now_iso(),
    }


def sync_symbol(
    *,
    symbol: str,
    meta: dict[str, Any] | None,
    interval: str,
    start_month: str,
    include_monthly: bool,
    include_daily: bool,
    include_rest: bool,
    refresh_archive: bool,
    repair_gaps: bool,
    max_gap_minutes: int,
    active_symbols: set[str],
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    vision_base_url: str,
    s3_base_url: str,
    overlap_minutes: int,
    rest_start: str | None,
    logger,
) -> dict[str, object]:
    total_rows = 0
    latest_time = None
    monthly_keys: list[str] = []
    unavailable_vision_keys: list[str] = []
    if include_monthly:
        monthly_keys = vision_monthly_keys(symbol, interval=interval, start_month=start_month, s3_base_url=s3_base_url)
        for key in monthly_keys:
            month = _month_from_key(key)
            if month and not refresh_archive and _month_partition_exists(store, symbol, month):
                logger.debug("%s skip existing monthly partition %s", symbol, month)
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
            total_rows += int(result.get("rows_written") or 0)
            latest_time = result.get("latest_time") or latest_time
            if result.get("outcome") in {"missing", "empty"}:
                unavailable_vision_keys.append(key)
            time.sleep(0.05)

    if include_daily and symbol in active_symbols:
        for key in vision_daily_keys(symbol, interval=interval, s3_base_url=s3_base_url):
            date_text = _date_from_key(key)
            if date_text and not refresh_archive and _date_exists(store, symbol, date_text):
                logger.debug("%s skip existing daily date %s", symbol, date_text)
                continue
            result = sync_vision_file(
                key=key,
                symbol=symbol,
                store=store,
                manifest=manifest,
                vision_base_url=vision_base_url,
                source="binance_vision_futures_um_daily",
                logger=logger,
            )
            total_rows += int(result.get("rows_written") or 0)
            latest_time = result.get("latest_time") or latest_time
            if result.get("outcome") in {"missing", "empty"}:
                unavailable_vision_keys.append(key)
            time.sleep(0.05)

    if include_rest and symbol in active_symbols:
        result = sync_rest_tail(
            symbol=symbol,
            meta=meta,
            store=store,
            manifest=manifest,
            overlap_minutes=overlap_minutes,
            rest_start=rest_start,
            logger=logger,
        )
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time

    if repair_gaps:
        total_rows += repair_small_gaps(
            symbol=symbol,
            interval=interval,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            max_gap_minutes=max_gap_minutes,
            logger=logger,
        )

    archive_months = [month for key in monthly_keys if (month := _month_from_key(key))]
    return {
        "rows_written": total_rows,
        "latest_time": latest_time,
        "archive_key_count": len(monthly_keys),
        "first_archive_month": archive_months[0] if archive_months else None,
        "last_archive_month": archive_months[-1] if archive_months else None,
        "unavailable_vision_keys": unavailable_vision_keys,
    }


def sync_all(args: argparse.Namespace, logger) -> dict[str, Any]:
    config = load_yaml("symbols.binance_usdm_quarterly.yml")
    pairs = [item.strip().upper() for item in (args.pairs.split(",") if args.pairs else config.get("pairs", ["BTCUSDT", "ETHUSDT"]))]
    interval = args.interval or config.get("interval", "1m")
    start_month = args.start_month or config.get("start_month", "2021-02")
    vision_base_url = config.get("vision_base_url", VISION_BASE_URL)
    s3_base_url = config.get("s3_base_url", S3_BASE_URL)
    overlap_minutes = int(args.rest_overlap_minutes or config.get("rest_overlap_minutes", 10))

    active = discover_active_contracts(pairs)
    archive_symbols = [] if args.no_archive_discovery else discover_archive_symbols(pairs, s3_base_url=s3_base_url)
    if args.symbols:
        symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    else:
        symbols = sorted(set(archive_symbols) | set(active))
    if args.max_symbols:
        symbols = symbols[: args.max_symbols]

    state = JsonState("binance_usdm_quarterly_contracts.json")
    state.write(
        {
            "dataset": DATASET,
            "storage": "storage/crypto/binance_futures/1m",
            "pairs": pairs,
            "interval": interval,
            "start_month": start_month,
            "active_contracts": active,
            "archive_symbols": archive_symbols,
            "selected_symbols": symbols,
            "updated_at": utc_now_iso(),
        }
    )

    store = PartitionedCsvGzStore(STORE_PARTS, partition="month")
    manifest = Manifest(DATASET)
    heartbeat = Heartbeat(SERVICE if args.audit_phase_d else DATASET)
    total_rows = 0
    results: dict[str, dict[str, object]] = {}
    phase_d_failures: list[str] = []
    if args.audit_phase_d:
        JsonState(f"phase_d/{SERVICE}.json").write(
            {
                "status": "running",
                "dataset": DATASET,
                "pairs": pairs,
                "symbols": symbols,
                "start_month": start_month,
                "max_gap_minutes": args.max_gap_minutes,
                "started_at": utc_now_iso(),
            }
        )

    for symbol in symbols:
        try:
            result = sync_symbol(
                symbol=symbol,
                meta=active.get(symbol),
                interval=interval,
                start_month=start_month,
                include_monthly=not args.no_monthly,
                include_daily=not args.no_daily,
                include_rest=not args.no_rest,
                refresh_archive=args.refresh_archive,
                repair_gaps=args.repair_gaps,
                max_gap_minutes=args.max_gap_minutes,
                active_symbols=set(active),
                store=store,
                manifest=manifest,
                vision_base_url=vision_base_url,
                s3_base_url=s3_base_url,
                overlap_minutes=overlap_minutes,
                rest_start=args.rest_start,
                logger=logger,
            )
            total_rows += int(result.get("rows_written") or 0)
            if args.audit_phase_d:
                audit = audit_symbol(
                    store,
                    symbol,
                    is_active=symbol in active,
                    expected_first_archive_month=str(result.get("first_archive_month")) if result.get("first_archive_month") else None,
                    expected_last_archive_month=str(result.get("last_archive_month")) if result.get("last_archive_month") else None,
                )
                results[symbol] = {**result, "validation": audit}
                JsonState(f"audits/{DATASET}_{symbol}_phase_d.json").write(
                    {
                        "dataset": DATASET,
                        "service": SERVICE,
                        "symbol": symbol,
                        **audit,
                    }
                )
                unavailable = list(result.get("unavailable_vision_keys") or [])
                manifest.update_symbol(
                    symbol,
                    phase_d_validation=audit,
                    phase_d_last_run_at=utc_now_iso(),
                    last_error=None if audit["status"] == "pass" and not unavailable else "phase_d_validation_requires_repair",
                )
                if unavailable:
                    phase_d_failures.append(f"{symbol}: unavailable Vision keys={len(unavailable)}")
                if audit["status"] != "pass":
                    phase_d_failures.append(f"{symbol}: validation={audit['status']}")
                    heartbeat.beat(status="error", symbol=symbol, error="phase_d_validation_requires_repair", validation=audit)
                else:
                    heartbeat.beat(status="ok", symbol=symbol, latest_time=result.get("latest_time"), validation=audit)
            else:
                heartbeat.beat(symbol=symbol, latest_time=result.get("latest_time"))
        except Exception as exc:
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            logger.exception("%s quarterly sync failed", symbol)
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))
            if args.audit_phase_d:
                phase_d_failures.append(f"{symbol}: {type(exc).__name__}: {exc}")

    payload: dict[str, Any] = {
        "status": "pass" if not phase_d_failures else "requires_repair",
        "dataset": DATASET,
        "pairs": pairs,
        "symbols": symbols,
        "active_symbols": sorted(active),
        "rows_written": total_rows,
        "results": results,
        "failures": phase_d_failures,
        "completed_at": utc_now_iso(),
    }
    if args.audit_phase_d:
        JsonState(f"phase_d/{SERVICE}.json").write(payload)
        if phase_d_failures:
            message = "Phase D Binance quarterly validation failed: " + "; ".join(phase_d_failures)
            heartbeat.beat(status="error", error=message)
            raise RuntimeError(message)
    return payload


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default=None, help="Comma-separated concrete contracts, e.g. BTCUSDT_240329")
    parser.add_argument("--pairs", default=None, help="Comma-separated underlying pairs, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--interval", default=None)
    parser.add_argument("--start-month", default=None)
    parser.add_argument("--sleep", type=int, default=None)
    parser.add_argument("--rest-overlap-minutes", type=int, default=None)
    parser.add_argument(
        "--rest-start",
        default=None,
        help="Explicit UTC lower bound for REST tail collection; intended for a bounded seed on an empty runtime.",
    )
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--no-monthly", action="store_true")
    parser.add_argument("--no-daily", action="store_true")
    parser.add_argument("--no-rest", action="store_true")
    parser.add_argument(
        "--no-archive-discovery",
        action="store_true",
        help="Skip the historical archive listing and operate only on explicit/current active contracts.",
    )
    parser.add_argument("--refresh-archive", action="store_true", help="Re-download Vision archive files even when local partitions already exist.")
    parser.add_argument("--repair-gaps", action="store_true", help="Fetch daily Vision files for small internal gaps after monthly sync.")
    parser.add_argument("--max-gap-minutes", type=int, default=5)
    parser.add_argument(
        "--audit-phase-d",
        action="store_true",
        help="Write a durable, fail-closed Phase D audit after the exact historical rebuild.",
    )
    args = parser.parse_args()

    config = load_yaml("symbols.binance_usdm_quarterly.yml")
    sleep_seconds = args.sleep or int(config.get("sleep_seconds", 21600))
    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)

    while True:
        result = sync_all(args, logger)
        logger.info("USD-M quarterly sync finished: %s", result)
        if args.mode != "live":
            break
        sleep_with_heartbeat(heartbeat, sleep_seconds)


if __name__ == "__main__":
    main()
