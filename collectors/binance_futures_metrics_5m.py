from __future__ import annotations

import argparse
import io
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from collectors.common.storage import read_partition_file, release_unused_memory, write_partition_file

DATASET = "crypto_binance_futures_metrics_5m"
STORE_PARTS = ["crypto", "binance_futures_metrics", "5m"]
BINANCE_FAPI = "https://fapi.binance.com"
VISION_BASE_URL = "https://data.binance.vision"
S3_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
USER_AGENT = {"User-Agent": "pool-alpha-get-data/1.0"}
METRIC_COLUMNS = [
    "time",
    "market",
    "symbol",
    "contract_type",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
    "source",
    "ingested_at",
]
NUMERIC_METRIC_COLUMNS = [
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
]
PHASE_D_METRICS_SOURCES = {
    "binance_vision_usdm_metrics",
    "binance_futures_data_rest",
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


def discover_active_contracts(pairs: list[str], *, include_current: bool, include_next: bool) -> dict[str, dict[str, Any]]:
    payload = _request_json(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    allowed = set()
    if include_current:
        allowed.add("CURRENT_QUARTER")
    if include_next:
        allowed.add("NEXT_QUARTER")
    pair_set = {pair.upper() for pair in pairs}
    active: dict[str, dict[str, Any]] = {}
    for item in payload.get("symbols", []):
        contract_type = item.get("contractType")
        if contract_type not in allowed:
            continue
        if item.get("quoteAsset") != "USDT" or item.get("marginAsset") != "USDT":
            continue
        if item.get("pair", "").upper() not in pair_set:
            continue
        symbol = item["symbol"].upper()
        active[symbol] = {
            "symbol": symbol,
            "pair": item.get("pair"),
            "contract_type": contract_type,
            "status": item.get("status"),
        }
    return active


def build_symbol_meta(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    meta = {
        symbol.strip().upper(): {"contract_type": "PERPETUAL", "pair": symbol.strip().upper()}
        for symbol in config.get("symbols", [])
    }
    meta.update(
        discover_active_contracts(
            [pair.strip().upper() for pair in config.get("quarterly_pairs", [])],
            include_current=bool(config.get("include_current_quarter", True)),
            include_next=bool(config.get("include_next_quarter", True)),
        )
    )
    return meta


def normalize_metrics_frame(raw: pd.DataFrame, *, symbol: str, contract_type: str, source: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=METRIC_COLUMNS)
    df = raw.copy()
    df.columns = [str(col).strip() for col in df.columns]
    if "time" not in df.columns:
        if "create_time" in df.columns:
            df = df.rename(columns={"create_time": "time"})
        else:
            return pd.DataFrame(columns=METRIC_COLUMNS)
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True).dt.tz_convert(None).dt.floor("5min")
    df["symbol"] = symbol.upper()
    df["market"] = "usdm_futures"
    df["contract_type"] = contract_type
    for col in [
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # Vision may emit more than one raw observation in a five-minute bucket.
    # Selecting the final row can discard a valid value when only that raw row
    # is null.  Coalesce each metric independently from direct source rows;
    # this never invents a value and preserves a genuine upstream null when
    # every observation in the bucket is absent.
    def last_non_null(values: pd.Series):
        present = values.dropna()
        return present.iloc[-1] if not present.empty else pd.NA

    grouped = (
        df.dropna(subset=["time"])
        .groupby("time", as_index=False, sort=True)[NUMERIC_METRIC_COLUMNS]
        .agg(last_non_null)
    )
    grouped["symbol"] = symbol.upper()
    grouped["market"] = "usdm_futures"
    grouped["contract_type"] = contract_type
    grouped["source"] = source
    grouped["ingested_at"] = utc_now_iso()
    return grouped[METRIC_COLUMNS].sort_values(["symbol", "time"]).reset_index(drop=True)


def read_vision_metrics_zip(content: bytes, *, symbol: str, contract_type: str, source: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not csv_names:
            return pd.DataFrame(columns=METRIC_COLUMNS)
        with archive.open(csv_names[0]) as handle:
            raw = pd.read_csv(handle)
    return normalize_metrics_frame(raw, symbol=symbol, contract_type=contract_type, source=source)


def append_metrics(store: PartitionedCsvGzStore, df: pd.DataFrame, symbol: str) -> dict[str, object]:
    return store.append(
        df,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": symbol},
        lock_name=f"{DATASET}/{symbol}",
    )


def _date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    if start > end:
        return []
    return list(pd.date_range(start.normalize(), end.normalize(), freq="D"))


def _date_from_key(key: str) -> str | None:
    match = re.search(r"metrics-(\d{4}-\d{2}-\d{2})\.zip$", Path(key).name)
    return match.group(1) if match else None


def _vision_key(symbol: str, day: pd.Timestamp) -> str:
    date_text = day.strftime("%Y-%m-%d")
    return f"data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{date_text}.zip"


def vision_metric_keys(symbol: str, *, s3_base_url: str) -> list[str]:
    prefix = f"data/futures/um/daily/metrics/{symbol}/"
    return sorted(key for key in _s3_keys(prefix, s3_base_url=s3_base_url) if key.endswith(".zip") and _date_from_key(key))


def _parse_config_start(start_date: str | None) -> pd.Timestamp | None:
    if start_date is None:
        return None
    if str(start_date).strip().lower() in {"", "auto", "none", "null"}:
        return None
    return pd.Timestamp(start_date).normalize()


def effective_start_day(keys: list[str], start_date: str | None) -> pd.Timestamp | None:
    key_days = [pd.Timestamp(day) for key in keys if (day := _date_from_key(key))]
    if not key_days:
        return _parse_config_start(start_date)
    earliest = min(key_days).normalize()
    configured = _parse_config_start(start_date)
    if configured is None:
        return earliest
    return max(earliest, configured)


def _symbol_day_quality(store: PartitionedCsvGzStore, symbol: str) -> tuple[dict[str, int], dict[str, int]]:
    """Return per-day row and nullable-metric counts without whole-history RAM."""

    day_counts: dict[str, int] = {}
    nullable_metric_rows: dict[str, int] = {}
    for path in store.files({"symbol": symbol}):
        try:
            frame = read_partition_file(path, usecols=["time", *NUMERIC_METRIC_COLUMNS])
        except Exception:
            continue
        times = pd.to_datetime(frame["time"], errors="coerce").dt.floor("5min")
        valid = times.notna()
        if valid.any():
            per_day = times.loc[valid].dt.strftime("%Y-%m-%d")
            for day, count in per_day.value_counts().items():
                day_counts[str(day)] = day_counts.get(str(day), 0) + int(count)
            numeric = frame.loc[valid, NUMERIC_METRIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
            missing = numeric.isna().any(axis=1)
            for day, count in per_day.loc[missing].value_counts().items():
                nullable_metric_rows[str(day)] = nullable_metric_rows.get(str(day), 0) + int(count)
            del numeric, missing, per_day
        del frame, times, valid
        release_unused_memory()
    return day_counts, nullable_metric_rows


def expected_rows_for_day(day: pd.Timestamp, first_available_day: pd.Timestamp, min_rows_per_full_day: int) -> int:
    if day.normalize() == first_available_day.normalize():
        return int(min_rows_per_full_day) - 1
    return int(min_rows_per_full_day)


def missing_coverage_key_days(
    *,
    available_days: list[pd.Timestamp],
    local_day_counts: dict[str, int],
    effective_start: pd.Timestamp,
    min_rows_per_full_day: int,
    nullable_metric_rows: dict[str, int] | None = None,
) -> tuple[list[pd.Timestamp], list[dict[str, Any]]]:
    if not available_days:
        return [], []

    available_set = {day.normalize() for day in available_days}
    first_available = min(available_set)
    latest_full_day = max(available_set)
    missing_days: list[dict[str, Any]] = []
    key_days: set[pd.Timestamp] = set()
    nullable_metric_rows = nullable_metric_rows or {}

    for day in _date_range(effective_start, latest_full_day):
        expected = expected_rows_for_day(day, first_available, min_rows_per_full_day)
        actual = int(local_day_counts.get(day.strftime("%Y-%m-%d"), 0))
        nullable_rows = int(nullable_metric_rows.get(day.strftime("%Y-%m-%d"), 0))
        if actual >= expected and nullable_rows == 0:
            continue
        item: dict[str, Any] = {"date": day.strftime("%Y-%m-%d"), "rows": actual, "expected_rows": expected}
        if nullable_rows:
            item["nullable_metric_rows"] = nullable_rows
        missing_days.append(item)
        for candidate in (day - pd.Timedelta(1, unit="D"), day):
            candidate = candidate.normalize()
            if candidate in available_set:
                key_days.add(candidate)

    return sorted(key_days), missing_days


def _download_vision_day(
    *,
    symbol: str,
    contract_type: str,
    day: pd.Timestamp,
    vision_base_url: str,
) -> tuple[str, pd.DataFrame]:
    key = _vision_key(symbol, day)
    content = _request_bytes(f"{vision_base_url.rstrip('/')}/{key}")
    if content is None:
        return key, pd.DataFrame(columns=METRIC_COLUMNS)
    return key, read_vision_metrics_zip(
        content,
        symbol=symbol,
        contract_type=contract_type,
        source="binance_vision_usdm_metrics",
    )


def _download_vision_key(
    *,
    key: str,
    symbol: str,
    contract_type: str,
    vision_base_url: str,
) -> tuple[str, pd.DataFrame]:
    content = _request_bytes(f"{vision_base_url.rstrip('/')}/{key}")
    if content is None:
        return key, pd.DataFrame(columns=METRIC_COLUMNS)
    return key, read_vision_metrics_zip(
        content,
        symbol=symbol,
        contract_type=contract_type,
        source="binance_vision_usdm_metrics",
    )


def seed_legacy_metrics(
    *,
    symbol: str,
    contract_type: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    legacy_seed_dir: str | None,
    logger,
) -> int:
    if not legacy_seed_dir:
        return 0
    path = Path(legacy_seed_dir) / f"{symbol}_metrics_synced.csv.gz"
    if not path.exists():
        return 0
    stat = path.stat()
    state = manifest.symbol_state(symbol)
    if state.get("legacy_seed_path") == str(path) and state.get("legacy_seed_mtime_ns") == stat.st_mtime_ns:
        return 0
    try:
        raw = pd.read_csv(path, compression="gzip")
    except Exception as exc:
        logger.warning("%s failed to read legacy metrics %s: %s", symbol, path, exc)
        return 0
    df = normalize_metrics_frame(raw, symbol=symbol, contract_type=contract_type, source="legacy_binance_vision_metrics")
    if df.empty:
        return 0
    result = append_metrics(store, df, symbol)
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        legacy_seed_path=str(path),
        legacy_seed_mtime_ns=stat.st_mtime_ns,
        legacy_rows=len(df),
        rows_written=result["rows_written"],
        source="legacy_binance_vision_metrics",
        last_success_at=utc_now_iso(),
        last_error=None,
    )
    logger.info("%s legacy metrics seeded rows=%s latest=%s", symbol, result["rows_written"], result["latest_time"])
    return int(result.get("rows_written") or 0)


def normalize_existing_partitions(
    store: PartitionedCsvGzStore,
    symbol: str,
    *,
    contract_type: str,
    logger,
) -> dict[str, int]:
    touched = 0
    rows_removed = 0
    for path in store.files({"symbol": symbol}):
        try:
            df = read_partition_file(path)
        except Exception as exc:
            logger.warning("%s failed to read metrics partition %s: %s", symbol, path, exc)
            continue
        if df.empty or "time" not in df.columns:
            continue

        before = len(df)
        missing_cols = [col for col in METRIC_COLUMNS if col not in df.columns]
        work = df.copy()
        parsed_time = pd.to_datetime(work["time"], errors="coerce")
        invalid_times = int(parsed_time.isna().sum())
        off_bucket = int((parsed_time.dropna().dt.floor("5min") != parsed_time.dropna()).sum())
        work["time"] = parsed_time.dt.floor("5min")
        duplicate_rows = int(work.duplicated(subset=["symbol", "time"]).sum()) if "symbol" in work.columns else 0
        work["symbol"] = symbol.upper()
        work["market"] = work.get("market", "usdm_futures")
        work["contract_type"] = work.get("contract_type", contract_type)
        for col in METRIC_COLUMNS:
            if col not in work.columns:
                work[col] = pd.NA
        for col in [
            "sum_open_interest",
            "sum_open_interest_value",
            "count_toptrader_long_short_ratio",
            "sum_toptrader_long_short_ratio",
            "count_long_short_ratio",
            "sum_taker_long_short_vol_ratio",
        ]:
            work[col] = pd.to_numeric(work[col], errors="coerce")

        work = (
            work[METRIC_COLUMNS]
            .dropna(subset=["time"])
            .drop_duplicates(subset=["symbol", "time"], keep="last")
            .sort_values(["symbol", "time"])
            .reset_index(drop=True)
        )
        rows_removed += before - len(work)
        needs_write = bool(missing_cols or invalid_times or off_bucket or duplicate_rows or before != len(work))
        if needs_write:
            comparable = work.copy()
            comparable["time"] = comparable["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            write_partition_file(comparable, path)
            touched += 1

    if touched:
        logger.info("%s normalized metrics partitions: touched=%s rows_removed=%s", symbol, touched, rows_removed)
    return {"touched": touched, "rows_removed": rows_removed}


def sync_vision_metrics(
    *,
    symbol: str,
    contract_type: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    start_date: str,
    vision_overlap_days: int,
    min_rows_per_full_day: int,
    max_workers: int,
    vision_base_url: str,
    s3_base_url: str,
    logger,
) -> int:
    keys = vision_metric_keys(symbol, s3_base_url=s3_base_url)
    if not keys:
        manifest.update_symbol(symbol, last_error="no_vision_metric_keys", last_success_at=utc_now_iso())
        logger.warning("%s no Binance Vision metrics keys discovered", symbol)
        return 0

    key_by_day = {pd.Timestamp(day).normalize(): key for key in keys if (day := _date_from_key(key))}
    available_days = sorted(key_by_day)
    configured_start = effective_start_day(keys, start_date)
    if configured_start is None:
        return 0

    local_counts, nullable_metric_rows = _symbol_day_quality(store, symbol)
    key_days, missing_days = missing_coverage_key_days(
        available_days=available_days,
        local_day_counts=local_counts,
        effective_start=configured_start,
        min_rows_per_full_day=min_rows_per_full_day,
        nullable_metric_rows=nullable_metric_rows,
    )

    latest_available = available_days[-1]
    overlap_start = max(configured_start, latest_available - pd.Timedelta(int(vision_overlap_days), unit="D"))
    key_days = sorted(set(key_days) | {day for day in available_days if day >= overlap_start})
    if not key_days:
        manifest.update_symbol(
            symbol,
            effective_start=str(configured_start.date()),
            vision_first_key_date=str(available_days[0].date()),
            vision_last_key_date=str(available_days[-1].date()),
            missing_day_count=0,
            last_success_at=utc_now_iso(),
            last_error=None,
        )
        return 0

    total = 0
    workers = max(1, int(max_workers))
    keys_to_fetch = [key_by_day[day] for day in key_days]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _download_vision_key,
                key=key,
                symbol=symbol,
                contract_type=contract_type,
                vision_base_url=vision_base_url,
            )
            for key in keys_to_fetch
        ]
        for future in as_completed(futures):
            key, df = future.result()
            if df.empty:
                manifest.update_symbol(symbol, last_missing_vision_key=key, last_error="empty_or_missing_vision_metrics")
                logger.debug("%s missing/empty Vision metrics key=%s", symbol, key)
                continue
            df = df[df["time"] >= configured_start]
            if df.empty:
                continue
            result = append_metrics(store, df, symbol)
            total += int(result.get("rows_written") or 0)
            manifest.update_symbol(
                symbol,
                latest_time=str(result["latest_time"]),
                last_success_at=utc_now_iso(),
                last_vision_key=key,
                rows_written=result["rows_written"],
                source="binance_vision_usdm_metrics",
                effective_start=str(configured_start.date()),
                vision_first_key_date=str(available_days[0].date()),
                vision_last_key_date=str(available_days[-1].date()),
                missing_day_count=len(missing_days),
                last_error=None,
            )
            logger.info("%s Vision metrics wrote rows=%s key=%s", symbol, result["rows_written"], Path(key).name)
    if missing_days:
        JsonState(f"audits/{DATASET}_{symbol}_missing_days.json").write(
            {
                "dataset": DATASET,
                "symbol": symbol,
                "updated_at": utc_now_iso(),
                "effective_start": str(configured_start.date()),
                "vision_last_key_date": str(available_days[-1].date()),
                "missing_day_count": len(missing_days),
                "missing_days_sample": missing_days[:200],
            }
        )
    return total


def _ms(value: pd.Timestamp) -> int:
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    return int(value.timestamp() * 1000)


def _closed_5m_until() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("5min") - pd.Timedelta(5, unit="min")


def _request_futures_data(endpoint: str, *, params: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _request_json(f"{BINANCE_FAPI}/futures/data/{endpoint}", params=params)
    return payload if isinstance(payload, list) else []


def _fetch_rest_metric(endpoint: str, *, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cursor = start
    end_ms = _ms(end)
    while _ms(cursor) <= end_ms:
        params: dict[str, Any] = {
            "period": "5m",
            "startTime": _ms(cursor),
            "endTime": end_ms,
            "limit": 500,
        }
        # These are USD-M endpoints.  Unlike their COIN-M counterparts, every
        # endpoint here expects the concrete USD-M ``symbol`` parameter.
        # Sending ``pair`` yields HTTP 400 / -1121 and silently drops the four
        # long/short ratio series while open interest still succeeds.
        params["symbol"] = symbol
        batch = _request_futures_data(endpoint, params=params)
        if not batch:
            break
        rows.extend(batch)
        timestamps = [int(item["timestamp"]) for item in batch if "timestamp" in item]
        if not timestamps:
            break
        next_cursor = pd.to_datetime(max(timestamps), unit="ms", utc=True) + pd.Timedelta(5, unit="min")
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.05)
    return pd.DataFrame(rows)


def fetch_rest_metrics_tail(
    symbol: str,
    *,
    contract_type: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    logger=None,
) -> pd.DataFrame:
    if contract_type != "PERPETUAL" or "_" in symbol:
        return pd.DataFrame(columns=METRIC_COLUMNS)

    frames: list[pd.DataFrame] = []
    endpoint_map = {
        "openInterestHist": {
            "sumOpenInterest": "sum_open_interest",
            "sumOpenInterestValue": "sum_open_interest_value",
        },
        "topLongShortAccountRatio": {"longShortRatio": "count_toptrader_long_short_ratio"},
        "topLongShortPositionRatio": {"longShortRatio": "sum_toptrader_long_short_ratio"},
        "globalLongShortAccountRatio": {"longShortRatio": "count_long_short_ratio"},
        "takerlongshortRatio": {"buySellRatio": "sum_taker_long_short_vol_ratio"},
    }
    for endpoint, columns in endpoint_map.items():
        try:
            raw = _fetch_rest_metric(endpoint, symbol=symbol, start=start, end=end)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code in {400, 404}:
                if logger is not None:
                    logger.warning(
                        "%s REST metric endpoint %s unavailable status=%s; keeping other metrics",
                        symbol,
                        endpoint,
                        status_code,
                    )
                continue
            raise
        if raw.empty or "timestamp" not in raw.columns:
            continue
        frame = pd.DataFrame(
            {
                "time": pd.to_datetime(raw["timestamp"], unit="ms", errors="coerce", utc=True)
                .dt.tz_convert(None)
                .dt.floor("5min")
            }
        )
        for src, dst in columns.items():
            frame[dst] = pd.to_numeric(raw.get(src), errors="coerce")
        frames.append(frame.dropna(subset=["time"]).drop_duplicates(subset=["time"], keep="last"))

    if not frames:
        return pd.DataFrame(columns=METRIC_COLUMNS)
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="time", how="outer")
    merged["symbol"] = symbol.upper()
    merged["market"] = "usdm_futures"
    merged["contract_type"] = contract_type
    merged["source"] = "binance_futures_data_rest"
    merged["ingested_at"] = utc_now_iso()
    for col in METRIC_COLUMNS:
        if col not in merged.columns:
            merged[col] = pd.NA
    return normalize_metrics_frame(
        merged,
        symbol=symbol,
        contract_type=contract_type,
        source="binance_futures_data_rest",
    )


def sync_rest_tail(
    *,
    symbol: str,
    contract_type: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    rest_tail_days: int,
    rest_overlap_hours: int,
    logger,
) -> int:
    if contract_type != "PERPETUAL" or "_" in symbol:
        return 0
    end = _closed_5m_until()
    latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    start_by_window = end - pd.Timedelta(int(rest_tail_days), unit="D")
    if latest is not None:
        start = max(start_by_window, latest.tz_localize("UTC") - pd.Timedelta(int(rest_overlap_hours), unit="h"))
    else:
        start = start_by_window
    if start > end:
        return 0
    df = fetch_rest_metrics_tail(symbol, contract_type=contract_type, start=start, end=end, logger=logger)
    if df.empty:
        manifest.update_symbol(symbol, last_rest_error="empty_rest_metrics_tail", last_rest_start=str(start), last_rest_end=str(end))
        logger.warning("%s REST metrics tail returned no rows %s -> %s", symbol, start, end)
        return 0
    # A partial REST response must not replace a complete Vision row through
    # the generic append's last-row dedupe.  Keep it as observable source
    # evidence and let the next bounded overlap retry it instead.
    incomplete_rows = int(df[NUMERIC_METRIC_COLUMNS].isna().any(axis=1).sum())
    if incomplete_rows:
        manifest.update_symbol(symbol, last_rest_partial_rows=incomplete_rows)
        logger.warning("%s REST metrics skipped partial rows=%s", symbol, incomplete_rows)
        df = df.dropna(subset=NUMERIC_METRIC_COLUMNS).reset_index(drop=True)
    if df.empty:
        return 0
    result = append_metrics(store, df, symbol)
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        last_success_at=utc_now_iso(),
        last_rest_start=str(start),
        last_rest_end=str(end),
        rows_written=result["rows_written"],
        source="binance_futures_data_rest",
        last_error=None,
    )
    logger.info("%s REST metrics wrote rows=%s latest=%s", symbol, result["rows_written"], result["latest_time"])
    return int(result.get("rows_written") or 0)


def audit_symbol(
    store: PartitionedCsvGzStore,
    symbol: str,
    *,
    effective_start: pd.Timestamp | None = None,
    expected_end: pd.Timestamp | None = None,
    min_rows_per_full_day: int = 288,
) -> dict[str, Any]:
    """Audit one metrics symbol without concatenating its full history.

    Phase D must keep the historical data path bounded even after six or more
    years of 5-minute rows.  Monthly files are chronological canonical
    partitions, so continuity can be checked with only the previous timestamp
    and daily counters in memory.
    """

    files = store.files({"symbol": symbol})
    rows = 0
    duplicate_rows = 0
    invalid_time_rows = 0
    off_bucket_rows = 0
    invalid_numeric_rows = 0
    nullable_metric_rows = 0
    nullable_metric_values = {column: 0 for column in NUMERIC_METRIC_COLUMNS}
    nullable_metric_rows_by_source: dict[str, int] = {}
    negative_metric_rows = 0
    source_mismatch_rows = 0
    market_mismatch_rows = 0
    contract_type_mismatch_rows = 0
    symbol_mismatch_rows = 0
    file_errors: list[str] = []
    day_counts: dict[str, int] = {}
    previous_time: pd.Timestamp | None = None
    first: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None
    gap_count = 0
    max_gap: pd.Timedelta | None = None

    for path in files:
        try:
            frame = read_partition_file(path, usecols=METRIC_COLUMNS)
        except Exception as exc:
            file_errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
            continue

        rows += int(len(frame))
        parsed_time = pd.to_datetime(frame["time"], errors="coerce")
        bucketed_time = parsed_time.dt.floor("5min")
        invalid_time_rows += int(bucketed_time.isna().sum())
        valid_time = bucketed_time.notna()
        off_bucket_rows += int((parsed_time.loc[valid_time] != bucketed_time.loc[valid_time]).sum())

        numeric = frame[NUMERIC_METRIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
        source_values = frame["source"].astype(str)
        null_values = numeric.isna()
        non_null_raw = frame[NUMERIC_METRIC_COLUMNS].notna()
        invalid_numeric_rows += int((null_values & non_null_raw).any(axis=1).sum())
        nullable_rows = null_values.any(axis=1)
        nullable_metric_rows += int(nullable_rows.sum())
        for column in NUMERIC_METRIC_COLUMNS:
            nullable_metric_values[column] += int(null_values[column].sum())
        for source, count in source_values.loc[nullable_rows].value_counts().items():
            nullable_metric_rows_by_source[str(source)] = nullable_metric_rows_by_source.get(str(source), 0) + int(count)
        negative_metric_rows += int((numeric < 0).any(axis=1).sum())
        symbol_mismatch_rows += int((frame["symbol"].astype(str) != symbol.upper()).sum())
        market_mismatch_rows += int((frame["market"].astype(str) != "usdm_futures").sum())
        contract_type_mismatch_rows += int((frame["contract_type"].astype(str) != "PERPETUAL").sum())
        source_mismatch_rows += int((~frame["source"].astype(str).isin(PHASE_D_METRICS_SOURCES)).sum())

        times = bucketed_time.loc[valid_time].sort_values().reset_index(drop=True)
        if not times.empty:
            duplicate_rows += int(times.duplicated().sum())
            unique_times = times.drop_duplicates().reset_index(drop=True)
            if previous_time is not None:
                boundary_gap = unique_times.iloc[0] - previous_time
                if boundary_gap == pd.Timedelta(0):
                    duplicate_rows += 1
                elif boundary_gap > pd.Timedelta(5, unit="min"):
                    gap_count += 1
                    max_gap = boundary_gap if max_gap is None or boundary_gap > max_gap else max_gap
            diffs = unique_times.diff().dropna()
            gaps = diffs[diffs > pd.Timedelta(5, unit="min")]
            gap_count += int(len(gaps))
            if not gaps.empty:
                local_max_gap = gaps.max()
                max_gap = local_max_gap if max_gap is None or local_max_gap > max_gap else max_gap
            for date, count in unique_times.dt.strftime("%Y-%m-%d").value_counts().items():
                day_counts[str(date)] = day_counts.get(str(date), 0) + int(count)
            partition_first = pd.Timestamp(unique_times.iloc[0])
            partition_latest = pd.Timestamp(unique_times.iloc[-1])
            first = partition_first if first is None or partition_first < first else first
            latest = partition_latest if latest is None or partition_latest > latest else latest
            previous_time = partition_latest

        del frame, numeric, source_values, null_values, non_null_raw, nullable_rows, parsed_time, bucketed_time
        release_unused_memory()

    partial_days: list[dict[str, Any]] = []
    if effective_start is not None and expected_end is not None and first is not None:
        first_day = min(effective_start.normalize(), first.normalize())
        for day in _date_range(effective_start, expected_end):
            expected = expected_rows_for_day(day, first_day, min_rows_per_full_day)
            actual = int(day_counts.get(day.strftime("%Y-%m-%d"), 0))
            if actual < expected:
                partial_days.append({"date": day.strftime("%Y-%m-%d"), "rows": actual, "expected_rows": expected})

    integrity_errors = (
        len(file_errors)
        + duplicate_rows
        + invalid_time_rows
        + off_bucket_rows
        + invalid_numeric_rows
        + negative_metric_rows
        + source_mismatch_rows
        + market_mismatch_rows
        + contract_type_mismatch_rows
        + symbol_mismatch_rows
    )
    structurally_valid = bool(
        files
        and effective_start is not None
        and expected_end is not None
        and latest is not None
        and latest.normalize() >= expected_end.normalize()
        and integrity_errors == 0
    )
    source_gap_present = bool(nullable_metric_rows or gap_count or partial_days)
    status = "pass" if structurally_valid and not source_gap_present else "pass_with_documented_source_gaps" if structurally_valid else "fail"
    return {
        "status": status,
        "files": len(files),
        "rows": rows,
        "min_time": first.isoformat() if first is not None else None,
        "max_time": latest.isoformat() if latest is not None else None,
        "effective_start": effective_start.isoformat() if effective_start is not None else None,
        "expected_end": expected_end.isoformat() if expected_end is not None else None,
        "duplicate_rows": duplicate_rows,
        "invalid_time_rows": invalid_time_rows,
        "off_bucket_rows": off_bucket_rows,
        "invalid_numeric_rows": invalid_numeric_rows,
        "nullable_metric_rows": nullable_metric_rows,
        "nullable_metric_values": nullable_metric_values,
        "nullable_metric_rows_by_source": nullable_metric_rows_by_source,
        "negative_metric_rows": negative_metric_rows,
        "source_mismatch_rows": source_mismatch_rows,
        "market_mismatch_rows": market_mismatch_rows,
        "contract_type_mismatch_rows": contract_type_mismatch_rows,
        "symbol_mismatch_rows": symbol_mismatch_rows,
        "gap_count": gap_count,
        "max_gap": str(max_gap) if max_gap is not None else None,
        "partial_day_count": len(partial_days),
        "partial_days_sample": partial_days[:50],
        "file_errors": file_errors,
    }


def sync_all(args: argparse.Namespace, logger, *, run_audit: bool = True) -> dict[str, Any]:
    config = load_yaml("symbols.binance_futures_metrics.yml")
    symbol_meta = build_symbol_meta(config)
    if args.symbols:
        requested = {item.strip().upper() for item in args.symbols.split(",")}
        symbol_meta = {symbol: meta for symbol, meta in symbol_meta.items() if symbol in requested}

    start_date = args.start_date if args.start_date is not None else config.get("start_date")
    vision_overlap_days = int(args.vision_overlap_days or config.get("vision_overlap_days", 7))
    max_workers = int(args.max_workers or config.get("max_workers", 8))
    min_rows_per_full_day = int(config.get("min_rows_per_full_day", 288))
    legacy_seed_dir = config.get("legacy_seed_dir")
    vision_base_url = config.get("vision_base_url", VISION_BASE_URL)
    s3_base_url = config.get("s3_base_url", S3_BASE_URL)
    include_rest_tail = bool(config.get("include_rest_tail", True)) and not args.no_rest
    rest_tail_days = int(args.rest_tail_days or config.get("rest_tail_days", 3))
    rest_overlap_hours = int(args.rest_overlap_hours or config.get("rest_overlap_hours", 12))

    JsonState("binance_futures_metrics_5m_symbols.json").write(
        {
            "dataset": DATASET,
            "storage": "storage/crypto/binance_futures_metrics/5m",
            "symbols": sorted(symbol_meta),
            "start_date": start_date or "auto",
            "vision_overlap_days": vision_overlap_days,
            "include_rest_tail": include_rest_tail,
            "rest_tail_days": rest_tail_days,
            "updated_at": utc_now_iso(),
        }
    )

    store = PartitionedCsvGzStore(STORE_PARTS, partition="month")
    manifest = Manifest(DATASET)
    heartbeat = Heartbeat(DATASET)
    total_rows = 0
    audits = {}
    phase_d_failures: list[str] = []

    for symbol, meta in sorted(symbol_meta.items()):
        try:
            if not args.no_legacy:
                total_rows += seed_legacy_metrics(
                    symbol=symbol,
                    contract_type=meta.get("contract_type", ""),
                    store=store,
                    manifest=manifest,
                    legacy_seed_dir=legacy_seed_dir,
                    logger=logger,
                )
            normalize_existing_partitions(
                store,
                symbol,
                contract_type=meta.get("contract_type", ""),
                logger=logger,
            )
            if not args.no_vision:
                total_rows += sync_vision_metrics(
                    symbol=symbol,
                    contract_type=meta.get("contract_type", ""),
                    store=store,
                    manifest=manifest,
                    start_date=start_date,
                    vision_overlap_days=vision_overlap_days,
                    min_rows_per_full_day=min_rows_per_full_day,
                    max_workers=max_workers,
                    vision_base_url=vision_base_url,
                    s3_base_url=s3_base_url,
                    logger=logger,
                )
            if include_rest_tail:
                total_rows += sync_rest_tail(
                    symbol=symbol,
                    contract_type=meta.get("contract_type", ""),
                    store=store,
                    manifest=manifest,
                    rest_tail_days=rest_tail_days,
                    rest_overlap_hours=rest_overlap_hours,
                    logger=logger,
                )
            state = manifest.symbol_state(symbol)
            effective_start = pd.Timestamp(state["effective_start"]) if state.get("effective_start") else None
            expected_end = pd.Timestamp(state["vision_last_key_date"]) if state.get("vision_last_key_date") else None
            audit = (
                audit_symbol(
                    store,
                    symbol,
                    effective_start=effective_start,
                    expected_end=expected_end,
                    min_rows_per_full_day=min_rows_per_full_day,
                )
                if run_audit
                else {}
            )
            if audit:
                audits[symbol] = audit
                JsonState(f"audits/{DATASET}_{symbol}.json").write({"dataset": DATASET, "symbol": symbol, "updated_at": utc_now_iso(), **audit})
                audit_phase = "phase_d" if args.audit_phase_d else "phase_g" if args.audit_phase_g else None
                if audit_phase:
                    JsonState(f"audits/{DATASET}_{symbol}_{audit_phase}.json").write(
                        {
                            "dataset": DATASET,
                            "symbol": symbol,
                            "service": f"{audit_phase}_binance_futures_metrics_5m",
                            "validated_at": utc_now_iso(),
                            **audit,
                        }
                    )
                    if audit.get("status") not in {"pass", "pass_with_documented_source_gaps"}:
                        phase_d_failures.append(f"{symbol}: audit status={audit.get('status')}")
            heartbeat.beat(symbol=symbol, latest_time=manifest.symbol_state(symbol).get("latest_time"))
        except Exception as exc:
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            logger.exception("%s futures metrics sync failed", symbol)
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))
            if args.audit_phase_d or args.audit_phase_g:
                phase_d_failures.append(f"{symbol}: {type(exc).__name__}: {exc}")

    if (args.audit_phase_d or args.audit_phase_g) and phase_d_failures:
        phase_name = "Phase D" if args.audit_phase_d else "Phase G"
        message = f"{phase_name} Binance futures metrics validation failed: " + "; ".join(phase_d_failures)
        heartbeat.beat(status="error", error=message)
        raise RuntimeError(message)

    return {"symbols": sorted(symbol_meta), "rows_written": total_rows, "audits": audits}


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--vision-overlap-days", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--sleep", type=int, default=None)
    parser.add_argument("--no-legacy", action="store_true")
    parser.add_argument("--no-vision", action="store_true")
    parser.add_argument("--no-rest", action="store_true")
    parser.add_argument("--rest-tail-days", type=int, default=None)
    parser.add_argument("--rest-overlap-hours", type=int, default=None)
    parser.add_argument("--no-validate", action="store_true")
    parser.add_argument("--audit-phase-d", action="store_true", help="Write a durable, fail-closed Phase D audit for the reviewed source scope.")
    parser.add_argument("--audit-phase-g", action="store_true", help="Write a durable, fail-closed Phase G audit for the ETHUSDT expansion scope.")
    args = parser.parse_args()

    config = load_yaml("symbols.binance_futures_metrics.yml")
    sleep_seconds = int(args.sleep or config.get("sleep_seconds", 21600))
    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)
    while True:
        result = sync_all(args, logger, run_audit=not args.no_validate)
        logger.info("Binance futures metrics 5m sync finished: %s", result)
        if args.mode != "live":
            break
        sleep_with_heartbeat(heartbeat, sleep_seconds)


if __name__ == "__main__":
    main()
