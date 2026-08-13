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
from collectors.common.storage import read_partition_file, write_partition_file

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
    df["source"] = source
    df["ingested_at"] = utc_now_iso()
    return (
        df[METRIC_COLUMNS]
        .dropna(subset=["time", "symbol"])
        .drop_duplicates(subset=["symbol", "time"], keep="last")
        .sort_values(["symbol", "time"])
        .reset_index(drop=True)
    )


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


def _symbol_day_counts(store: PartitionedCsvGzStore, symbol: str) -> dict[str, int]:
    frames = []
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(read_partition_file(path, usecols=["time"]))
        except Exception:
            continue
    if not frames:
        return {}
    times = pd.concat(frames, ignore_index=True)["time"]
    times = pd.to_datetime(times, errors="coerce").dropna().dt.floor("5min").drop_duplicates()
    if times.empty:
        return {}
    return times.groupby(times.dt.strftime("%Y-%m-%d")).size().astype(int).to_dict()


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
) -> tuple[list[pd.Timestamp], list[dict[str, Any]]]:
    if not available_days:
        return [], []

    available_set = {day.normalize() for day in available_days}
    first_available = min(available_set)
    latest_full_day = max(available_set)
    missing_days: list[dict[str, Any]] = []
    key_days: set[pd.Timestamp] = set()

    for day in _date_range(effective_start, latest_full_day):
        expected = expected_rows_for_day(day, first_available, min_rows_per_full_day)
        actual = int(local_day_counts.get(day.strftime("%Y-%m-%d"), 0))
        if actual >= expected:
            continue
        missing_days.append({"date": day.strftime("%Y-%m-%d"), "rows": actual, "expected_rows": expected})
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

    local_counts = _symbol_day_counts(store, symbol)
    key_days, missing_days = missing_coverage_key_days(
        available_days=available_days,
        local_day_counts=local_counts,
        effective_start=configured_start,
        min_rows_per_full_day=min_rows_per_full_day,
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
    return merged[METRIC_COLUMNS].dropna(subset=["time"]).drop_duplicates(subset=["symbol", "time"], keep="last").sort_values(["symbol", "time"]).reset_index(drop=True)


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
    frames = []
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(read_partition_file(path, usecols=["time", "symbol"]))
        except Exception:
            continue
    if not frames:
        return {"rows": 0, "duplicate_rows": 0, "gap_count": 0}
    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.floor("5min")
    df = df.dropna(subset=["time"]).sort_values("time")
    times = df["time"].drop_duplicates().sort_values().reset_index(drop=True)
    diffs = times.diff().dropna()
    gaps = diffs[diffs > pd.Timedelta(5, unit="min")]
    partial_days = []
    if effective_start is not None and expected_end is not None and not times.empty:
        counts = times.groupby(times.dt.strftime("%Y-%m-%d")).size().astype(int).to_dict()
        first_day = min(effective_start.normalize(), times.min().normalize())
        for day in _date_range(effective_start, expected_end):
            expected = expected_rows_for_day(day, first_day, min_rows_per_full_day)
            actual = int(counts.get(day.strftime("%Y-%m-%d"), 0))
            if actual < expected:
                partial_days.append({"date": day.strftime("%Y-%m-%d"), "rows": actual, "expected_rows": expected})
    return {
        "rows": int(len(df)),
        "min_time": str(times.min()) if not times.empty else None,
        "max_time": str(times.max()) if not times.empty else None,
        "duplicate_rows": int(df.duplicated(subset=["symbol", "time"]).sum()),
        "gap_count": int(len(gaps)),
        "max_gap": str(gaps.max()) if not gaps.empty else None,
        "partial_day_count": len(partial_days),
        "partial_days_sample": partial_days[:50],
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
            heartbeat.beat(symbol=symbol, latest_time=manifest.symbol_state(symbol).get("latest_time"))
        except Exception as exc:
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            logger.exception("%s futures metrics sync failed", symbol)
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))

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
