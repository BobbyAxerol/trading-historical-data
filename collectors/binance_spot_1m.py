from __future__ import annotations

import argparse
import io
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pandas as pd
import requests

from collectors.common.config import load_yaml
from collectors.common.env import load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, Manifest, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.storage import PartitionedParquetStore as PartitionedCsvGzStore
from collectors.common.storage import read_partition_file, write_partition_file

DATASET = "crypto_binance_spot_1m"
STORE_PARTS = ["crypto", "binance_spot", "1m"]
BINANCE_API = "https://api.binance.com"
VISION_BASE_URL = "https://data.binance.vision"
S3_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
USER_AGENT = {"User-Agent": "pool-alpha-get-data/1.0"}
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
FUTURES_STORE_PARTS = ["crypto", "binance_futures", "1m"]
PROXY_FILL_SOURCE = "binance_usdm_futures_proxy_gap_fill"


def _ms(value: pd.Timestamp) -> int:
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    return int(value.timestamp() * 1000)


def _closed_until() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("min") - pd.Timedelta(1, unit="min")


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


def _ms_to_timestamp(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    unit = "us" if values.dropna().gt(10**14).any() else "ms"
    return pd.to_datetime(values, unit=unit, errors="coerce", utc=True).dt.tz_convert(None)


def normalize_kline_frame(df: pd.DataFrame, *, symbol: str, source: str) -> pd.DataFrame:
    work = df.copy()
    if "open_time" not in work.columns:
        work.columns = KLINE_COLUMNS[: len(work.columns)]

    for col in KLINE_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA

    result = work[KLINE_COLUMNS].copy()
    result["time"] = _ms_to_timestamp(result["open_time"]).dt.floor("min")
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
            raw = pd.read_csv(handle, header=None)
    if not raw.empty and str(raw.iloc[0, 0]).strip().lower() == "open_time":
        raw = raw.iloc[1:].reset_index(drop=True)
    raw.columns = KLINE_COLUMNS[: len(raw.columns)]
    return normalize_kline_frame(raw, symbol=symbol, source=source)


def _month_from_key(key: str) -> str | None:
    stem = Path(key).name
    marker = stem.rsplit("-", 2)
    if len(marker) < 3:
        return None
    month = f"{marker[-2]}-{marker[-1].replace('.zip', '')}"
    return month if len(month) == 7 else None


def _date_from_key(key: str) -> str | None:
    stem = Path(key).name
    if not stem.endswith(".zip"):
        return None
    date = stem.replace(".zip", "").rsplit("-", 3)
    if len(date) < 4:
        return None
    return f"{date[-3]}-{date[-2]}-{date[-1]}"


def vision_monthly_keys(symbol: str, *, interval: str, start_month: str, s3_base_url: str) -> list[str]:
    prefix = f"data/spot/monthly/klines/{symbol}/{interval}/"
    keys = []
    for key in _s3_keys(prefix, s3_base_url=s3_base_url):
        month = _month_from_key(key)
        if key.endswith(".zip") and month and month >= start_month:
            keys.append(key)
    return sorted(keys)


def vision_daily_keys(
    symbol: str,
    *,
    interval: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    s3_base_url: str,
) -> list[str]:
    prefix = f"data/spot/daily/klines/{symbol}/{interval}/"
    keys = []
    start_day = start_date.strftime("%Y-%m-%d")
    end_day = end_date.strftime("%Y-%m-%d")
    for key in _s3_keys(prefix, s3_base_url=s3_base_url):
        date_text = _date_from_key(key)
        if key.endswith(".zip") and date_text and start_day <= date_text <= end_day:
            keys.append(key)
    return sorted(keys)


def _request_klines(symbol: str, start_ms: int, end_ms: int, limit: int = 1000) -> list[list[Any]]:
    return _request_json(
        f"{BINANCE_API}/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        },
    )


def fetch_spot_1m(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: list[list[Any]] = []
    cursor = _ms(start)
    end_ms = _ms(end)

    while cursor <= end_ms:
        batch = _request_klines(symbol, cursor, end_ms)
        if not batch:
            break
        rows.extend(batch)
        next_cursor = int(batch[-1][0]) + 60_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(rows, columns=KLINE_COLUMNS)
    return normalize_kline_frame(df, symbol=symbol, source="binance_spot_rest")


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
        return {"rows_written": 0, "latest_time": None}
    df = read_vision_zip(content, symbol=symbol, source=source)
    if df.empty:
        manifest.update_symbol(symbol, last_empty_vision_key=key, last_error="empty_vision_file")
        logger.warning("%s empty Vision file: %s", symbol, key)
        return {"rows_written": 0, "latest_time": None}
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
    return result


def _download_vision_file(key: str, *, symbol: str, vision_base_url: str, source: str) -> tuple[str, pd.DataFrame]:
    content = _request_bytes(f"{vision_base_url.rstrip('/')}/{key}")
    if content is None:
        return key, pd.DataFrame(columns=OUTPUT_COLUMNS)
    return key, read_vision_zip(content, symbol=symbol, source=source)


def sync_vision_parallel(
    *,
    keys: list[str],
    symbol: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    vision_base_url: str,
    source: str,
    max_workers: int,
    logger,
) -> dict[str, object]:
    if not keys:
        return {"rows_written": 0, "latest_time": None}

    total_rows = 0
    latest_time = None
    workers = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_download_vision_file, key, symbol=symbol, vision_base_url=vision_base_url, source=source)
            for key in keys
        ]
        for future in as_completed(futures):
            key, df = future.result()
            if df.empty:
                manifest.update_symbol(symbol, last_empty_vision_key=key, last_error="empty_or_missing_vision_file")
                logger.warning("%s empty/missing Vision file: %s", symbol, key)
                continue
            result = _append(store, df, symbol)
            total_rows += int(result.get("rows_written") or 0)
            latest_time = result.get("latest_time") or latest_time
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
    return {"rows_written": total_rows, "latest_time": latest_time}


def sync_rest_tail(
    *,
    symbol: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    backfill_start: str,
    overlap_minutes: int,
    logger,
) -> dict[str, object]:
    latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    if latest is None:
        start = pd.Timestamp(backfill_start, tz="UTC")
    else:
        start = latest.tz_localize("UTC") - timedelta(minutes=int(overlap_minutes))

    end = _closed_until()
    if start > end:
        logger.info("%s REST tail current: start=%s end=%s", symbol, start, end)
        return {"rows_written": 0, "latest_time": latest.isoformat() if latest is not None else None}

    logger.info("Fetching %s spot REST tail %s -> %s", symbol, start, end)
    df = fetch_spot_1m(symbol, start, end)
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_rest_response", last_success_at=utc_now_iso())
        logger.warning("%s REST tail returned no rows", symbol)
        return {"rows_written": 0, "latest_time": latest.isoformat() if latest is not None else None}

    result = _append(store, df, symbol)
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        last_success_at=utc_now_iso(),
        rows_written=result["rows_written"],
        source="binance_spot_rest",
        last_error=None,
    )
    logger.info("%s REST wrote rows=%s latest=%s", symbol, result["rows_written"], result["latest_time"])
    return result


def audit_symbol(store: PartitionedCsvGzStore, symbol: str, *, expected_start: str | None = None) -> dict[str, Any]:
    frames = []
    audit_cols = ["time", "symbol", "open", "high", "low", "close", "volume", "quote_volume"]
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(read_partition_file(path, usecols=audit_cols))
        except Exception:
            continue
    if not frames:
        return {"rows": 0, "gaps": [], "duplicate_rows": 0, "ohlc_bad_rows": 0, "negative_rows": 0}

    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"])
    df = df.sort_values("time").reset_index(drop=True)
    dup_rows = int(df.duplicated(subset=["symbol", "time"]).sum())
    ohlc_bad = int(((df["high"] < df[["open", "low", "close"]].max(axis=1)) | (df["low"] > df[["open", "high", "close"]].min(axis=1))).sum())
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume"]
    negative_rows = int((df[numeric_cols].apply(pd.to_numeric, errors="coerce") < 0).any(axis=1).sum())

    times = df["time"].drop_duplicates().sort_values().reset_index(drop=True)
    gaps: list[dict[str, str]] = []
    if len(times) > 1:
        diffs = times.diff()
        one_minute = pd.Timedelta(1, unit="min")
        gap_mask = diffs > one_minute
        for idx in gap_mask[gap_mask].index:
            gaps.append(
                {
                    "start": str(times.iloc[idx - 1] + one_minute),
                    "end": str(times.iloc[idx] - one_minute),
                    "minutes": str(int((times.iloc[idx] - times.iloc[idx - 1]).total_seconds() // 60) - 1),
                }
            )

    if expected_start and not times.empty:
        start_ts = pd.Timestamp(expected_start)
        if times.iloc[0] > start_ts:
            gaps.insert(
                0,
                {
                    "start": str(start_ts),
                    "end": str(times.iloc[0] - pd.Timedelta(1, unit="min")),
                    "minutes": str(int((times.iloc[0] - start_ts).total_seconds() // 60)),
                },
            )

    return {
        "rows": int(len(df)),
        "min_time": str(times.min()) if not times.empty else None,
        "max_time": str(times.max()) if not times.empty else None,
        "gaps": gaps,
        "duplicate_rows": dup_rows,
        "ohlc_bad_rows": ohlc_bad,
        "negative_rows": negative_rows,
    }


def normalize_existing_partitions(store: PartitionedCsvGzStore, symbol: str, logger) -> dict[str, int]:
    touched = 0
    rows_removed = 0
    second_offset_rows = 0
    for path in store.files({"symbol": symbol}):
        try:
            df = read_partition_file(path)
        except Exception as exc:
            logger.warning("%s failed to read partition for normalization %s: %s", symbol, path, exc)
            continue
        if df.empty or "time" not in df.columns:
            continue

        before_rows = len(df)
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        local_second_offset_rows = int(df["time"].dt.second.fillna(0).ne(0).sum())
        second_offset_rows += local_second_offset_rows
        df["time"] = df["time"].dt.floor("min")
        if "close_time" in df.columns:
            df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce").dt.floor("min")

        for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if {"open", "high", "low", "close"}.issubset(df.columns):
            prices = df[["open", "high", "low", "close"]]
            df["high"] = prices.max(axis=1, skipna=False)
            df["low"] = prices.min(axis=1, skipna=False)
        if "number_of_trades" in df.columns:
            df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce").astype("Int64")
        if "symbol" not in df.columns:
            df["symbol"] = symbol

        df = (
            df.dropna(subset=["time"])
            .drop_duplicates(subset=["symbol", "time"], keep="last")
            .sort_values(["symbol", "time"])
            .reset_index(drop=True)
        )
        df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        if "close_time" in df.columns:
            df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        after_rows = len(df)
        rows_removed += before_rows - after_rows
        if local_second_offset_rows or before_rows != after_rows:
            write_partition_file(df, path)
            touched += 1

    if touched:
        logger.info("%s normalized existing partitions: touched=%s second_offset_rows=%s rows_removed=%s", symbol, touched, second_offset_rows, rows_removed)
    return {"touched": touched, "second_offset_rows": second_offset_rows, "rows_removed": rows_removed}


def load_symbol_frame(store: PartitionedCsvGzStore, symbol: str, *, usecols: list[str] | None = None) -> pd.DataFrame:
    frames = []
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(read_partition_file(path, usecols=usecols))
        except ValueError:
            frames.append(read_partition_file(path))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"])
    return df


def _median_scale(
    spot: pd.DataFrame,
    futures: pd.DataFrame,
    *,
    column: str,
    default: float = 1.0,
) -> float:
    if column not in spot.columns or column not in futures.columns:
        return default
    merged = spot[["time", column]].merge(futures[["time", column]], on="time", suffixes=("_spot", "_futures"))
    if merged.empty:
        return default
    spot_values = pd.to_numeric(merged[f"{column}_spot"], errors="coerce")
    futures_values = pd.to_numeric(merged[f"{column}_futures"], errors="coerce")
    ratios = spot_values / futures_values
    ratios = ratios[ratios.replace([float("inf"), float("-inf")], pd.NA).notna()]
    ratios = ratios[(ratios > 0) & (ratios < 1000)]
    if ratios.empty:
        return default
    return float(ratios.median())


def _build_futures_proxy_rows(
    *,
    symbol: str,
    futures_df: pd.DataFrame,
    spot_context: pd.DataFrame,
    futures_context: pd.DataFrame,
) -> pd.DataFrame:
    proxy = futures_df.copy()
    for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]:
        if col in proxy.columns:
            proxy[col] = pd.to_numeric(proxy[col], errors="coerce")
    proxy["symbol"] = symbol

    scale_cols = ["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]
    for col in scale_cols:
        if col in proxy.columns:
            proxy[col] = proxy[col] * _median_scale(spot_context, futures_context, column=col)

    if "number_of_trades" in proxy.columns:
        trade_scale = _median_scale(spot_context, futures_context, column="number_of_trades")
        proxy["number_of_trades"] = (pd.to_numeric(proxy["number_of_trades"], errors="coerce") * trade_scale).round().astype("Int64")

    proxy["source"] = PROXY_FILL_SOURCE
    proxy["ingested_at"] = utc_now_iso()
    if "close_time" not in proxy.columns:
        proxy["close_time"] = proxy["time"] + pd.Timedelta(seconds=59, milliseconds=999)
    return proxy[OUTPUT_COLUMNS].dropna(subset=["time", "open", "high", "low", "close"]).reset_index(drop=True)


def proxy_fill_gaps_from_futures(
    *,
    symbol: str,
    spot_store: PartitionedCsvGzStore,
    futures_store: PartitionedCsvGzStore,
    manifest: Manifest,
    gaps: list[dict[str, str]],
    max_gap_minutes: int,
    context_hours: int,
    logger,
) -> int:
    rows_written = 0
    spot_context_all = load_symbol_frame(
        spot_store,
        symbol,
        usecols=["time", "volume", "quote_volume", "number_of_trades", "taker_buy_base_volume", "taker_buy_quote_volume"],
    )
    futures_all = load_symbol_frame(futures_store, symbol, usecols=OUTPUT_COLUMNS)
    if futures_all.empty:
        logger.warning("%s skip futures proxy fill: no local futures data", symbol)
        return 0

    for gap in gaps:
        minutes = int(gap.get("minutes") or 0)
        if minutes <= 0 or minutes > max_gap_minutes:
            continue
        start = pd.Timestamp(gap["start"]).tz_localize(None)
        end = pd.Timestamp(gap["end"]).tz_localize(None)
        expected_times = pd.date_range(start, end, freq="min")

        futures_df = futures_all[(futures_all["time"] >= start) & (futures_all["time"] <= end)].copy()
        if len(futures_df) != len(expected_times) or set(futures_df["time"]) != set(expected_times):
            logger.info("%s skip futures proxy gap %s -> %s: futures coverage %s/%s", symbol, start, end, len(futures_df), len(expected_times))
            continue

        context_start = start - pd.Timedelta(int(context_hours), unit="h")
        context_end = end + pd.Timedelta(int(context_hours), unit="h")
        spot_context = spot_context_all[(spot_context_all["time"] >= context_start) & (spot_context_all["time"] <= context_end)].copy()
        futures_context = futures_all[(futures_all["time"] >= context_start) & (futures_all["time"] <= context_end)].copy()
        proxy = _build_futures_proxy_rows(
            symbol=symbol,
            futures_df=futures_df,
            spot_context=spot_context,
            futures_context=futures_context,
        )
        if proxy.empty:
            continue
        result = _append(spot_store, proxy, symbol)
        written = int(result.get("rows_written") or 0)
        rows_written += written
        manifest.update_symbol(
            symbol,
            latest_time=str(result["latest_time"]),
            last_success_at=utc_now_iso(),
            last_gap_start=str(start),
            last_gap_end=str(end),
            source=PROXY_FILL_SOURCE,
            last_error=None,
        )
        logger.info("%s futures proxy filled rows=%s gap=%s -> %s", symbol, written, start, end)
    return rows_written


def _date_texts_between(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
    days = pd.date_range(start.normalize(), end.normalize(), freq="D")
    return [day.strftime("%Y-%m-%d") for day in days]


def repair_gap_ranges(
    *,
    symbol: str,
    interval: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    gaps: list[dict[str, str]],
    vision_base_url: str,
    max_gap_minutes: int,
    logger,
) -> int:
    repaired_rows = 0
    attempted_daily_dates: set[str] = set()
    for gap in gaps:
        minutes = int(gap.get("minutes") or 0)
        if minutes <= 0 or minutes > max_gap_minutes:
            continue
        start = pd.Timestamp(gap["start"], tz="UTC")
        end = pd.Timestamp(gap["end"], tz="UTC")

        for date_text in _date_texts_between(start, end):
            if date_text in attempted_daily_dates:
                continue
            attempted_daily_dates.add(date_text)
            key = f"data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_text}.zip"
            result = sync_vision_file(
                key=key,
                symbol=symbol,
                store=store,
                manifest=manifest,
                vision_base_url=vision_base_url,
                source="binance_vision_spot_daily_gap_repair",
                logger=logger,
            )
            repaired_rows += int(result.get("rows_written") or 0)

        logger.info("%s REST gap repair %s -> %s (%s minutes)", symbol, start, end, minutes)
        df = fetch_spot_1m(symbol, start, end)
        if df.empty:
            logger.warning("%s REST gap repair returned no rows for %s -> %s", symbol, start, end)
            manifest.update_symbol(symbol, last_error="empty_rest_gap_repair", last_gap_start=str(start), last_gap_end=str(end))
            continue
        df = df.copy()
        df["source"] = "binance_spot_rest_gap_repair"
        result = _append(store, df, symbol)
        repaired_rows += int(result.get("rows_written") or 0)
        manifest.update_symbol(
            symbol,
            latest_time=str(result["latest_time"]),
            last_success_at=utc_now_iso(),
            last_gap_start=str(start),
            last_gap_end=str(end),
            source="binance_spot_rest_gap_repair",
            last_error=None,
        )
    return repaired_rows


def sync_symbol(
    *,
    symbol: str,
    interval: str,
    backfill_start: str,
    include_monthly: bool,
    include_daily: bool,
    include_rest: bool,
    refresh_archive: bool,
    run_audit: bool,
    repair_gaps: bool,
    proxy_fill_futures_gaps: bool,
    daily_lookback_days: int,
    max_gap_minutes: int,
    proxy_context_hours: int,
    max_workers: int,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    vision_base_url: str,
    s3_base_url: str,
    overlap_minutes: int,
    logger,
) -> dict[str, object]:
    total_rows = 0
    latest_time = None
    start_month = pd.Timestamp(backfill_start).strftime("%Y-%m")

    if include_monthly:
        monthly_keys = []
        for key in vision_monthly_keys(symbol, interval=interval, start_month=start_month, s3_base_url=s3_base_url):
            month = _month_from_key(key)
            if month and not refresh_archive and _month_partition_exists(store, symbol, month):
                logger.debug("%s skip existing monthly partition %s", symbol, month)
                continue
            monthly_keys.append(key)
        result = sync_vision_parallel(
            keys=monthly_keys,
            symbol=symbol,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            source="binance_vision_spot_monthly",
            max_workers=max_workers,
            logger=logger,
        )
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time

    if include_daily:
        end = _closed_until()
        start = max(pd.Timestamp(backfill_start, tz="UTC"), end - pd.Timedelta(int(daily_lookback_days), unit="D"))
        daily_keys = []
        for key in vision_daily_keys(symbol, interval=interval, start_date=start, end_date=end, s3_base_url=s3_base_url):
            date_text = _date_from_key(key)
            if date_text and not refresh_archive and _date_exists(store, symbol, date_text):
                logger.debug("%s skip existing daily date %s", symbol, date_text)
                continue
            daily_keys.append(key)
        result = sync_vision_parallel(
            keys=daily_keys,
            symbol=symbol,
            store=store,
            manifest=manifest,
            vision_base_url=vision_base_url,
            source="binance_vision_spot_daily",
            max_workers=max_workers,
            logger=logger,
        )
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time

    if include_rest:
        result = sync_rest_tail(
            symbol=symbol,
            store=store,
            manifest=manifest,
            backfill_start=backfill_start,
            overlap_minutes=overlap_minutes,
            logger=logger,
        )
        total_rows += int(result.get("rows_written") or 0)
        latest_time = result.get("latest_time") or latest_time

    audit = None
    if run_audit:
        normalize_existing_partitions(store, symbol, logger)
        audit = audit_symbol(store, symbol, expected_start=backfill_start)
        if repair_gaps and audit["gaps"]:
            total_rows += repair_gap_ranges(
                symbol=symbol,
                interval=interval,
                store=store,
                manifest=manifest,
                gaps=audit["gaps"],
                vision_base_url=vision_base_url,
                max_gap_minutes=max_gap_minutes,
                logger=logger,
            )
            normalize_existing_partitions(store, symbol, logger)
            audit = audit_symbol(store, symbol, expected_start=backfill_start)
        if proxy_fill_futures_gaps and audit["gaps"]:
            futures_store = PartitionedCsvGzStore(FUTURES_STORE_PARTS, partition="month")
            total_rows += proxy_fill_gaps_from_futures(
                symbol=symbol,
                spot_store=store,
                futures_store=futures_store,
                manifest=manifest,
                gaps=audit["gaps"],
                max_gap_minutes=max_gap_minutes,
                context_hours=proxy_context_hours,
                logger=logger,
            )
            normalize_existing_partitions(store, symbol, logger)
            audit = audit_symbol(store, symbol, expected_start=backfill_start)
        JsonState(f"audits/{DATASET}_{symbol}.json").write({"dataset": DATASET, "symbol": symbol, "updated_at": utc_now_iso(), **audit})
        logger.info("%s audit: rows=%s gaps=%s dup=%s ohlc_bad=%s negative=%s", symbol, audit["rows"], len(audit["gaps"]), audit["duplicate_rows"], audit["ohlc_bad_rows"], audit["negative_rows"])

    return {"rows_written": total_rows, "latest_time": latest_time, "audit": audit}


def sync_all(args: argparse.Namespace, logger, *, run_audit: bool) -> dict[str, Any]:
    config = load_yaml("symbols.binance_spot.yml")
    symbols = [item.strip().upper() for item in (args.symbols.split(",") if args.symbols else config.get("symbols", ["BTCUSDT"]))]
    interval = args.interval or config.get("interval", "1m")
    backfill_start = args.backfill_start or config.get("backfill_start", "2018-01-01")
    vision_base_url = config.get("vision_base_url", VISION_BASE_URL)
    s3_base_url = config.get("s3_base_url", S3_BASE_URL)
    overlap_minutes = int(args.rest_overlap_minutes or config.get("rest_overlap_minutes", 10))
    daily_lookback_days = int(args.daily_lookback_days or config.get("daily_lookback_days", 45))
    max_gap_minutes = int(args.max_gap_minutes or config.get("max_gap_minutes", 10080))
    proxy_context_hours = int(args.proxy_context_hours or config.get("proxy_context_hours", 6))
    max_workers = int(args.max_workers or config.get("max_workers", 4))
    proxy_fill_futures_gaps = bool(args.proxy_fill_futures_gaps or config.get("proxy_fill_from_futures", False))

    JsonState("binance_spot_1m_symbols.json").write(
        {
            "dataset": DATASET,
            "storage": "storage/crypto/binance_spot/1m",
            "symbols": symbols,
            "interval": interval,
            "backfill_start": backfill_start,
            "updated_at": utc_now_iso(),
        }
    )

    store = PartitionedCsvGzStore(STORE_PARTS, partition="month")
    manifest = Manifest(DATASET)
    heartbeat = Heartbeat(DATASET)
    total_rows = 0
    audits = {}
    for symbol in symbols:
        try:
            result = sync_symbol(
                symbol=symbol,
                interval=interval,
                backfill_start=backfill_start,
                include_monthly=not args.no_monthly,
                include_daily=not args.no_daily,
                include_rest=not args.no_rest,
                refresh_archive=args.refresh_archive,
                run_audit=run_audit,
                repair_gaps=args.repair_gaps,
                proxy_fill_futures_gaps=proxy_fill_futures_gaps,
                daily_lookback_days=daily_lookback_days,
                max_gap_minutes=max_gap_minutes,
                proxy_context_hours=proxy_context_hours,
                max_workers=max_workers,
                store=store,
                manifest=manifest,
                vision_base_url=vision_base_url,
                s3_base_url=s3_base_url,
                overlap_minutes=overlap_minutes,
                logger=logger,
            )
            total_rows += int(result.get("rows_written") or 0)
            if result.get("audit"):
                audits[symbol] = result["audit"]
            heartbeat.beat(symbol=symbol, latest_time=result.get("latest_time"))
        except Exception as exc:
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            logger.exception("%s spot sync failed", symbol)
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))
    return {"symbols": symbols, "rows_written": total_rows, "audits": audits}


def _selected_symbols(args: argparse.Namespace) -> list[str]:
    config = load_yaml("symbols.binance_spot.yml")
    return [item.strip().upper() for item in (args.symbols.split(",") if args.symbols else config.get("symbols", ["BTCUSDT"]))]


def _audits_are_fresh(symbols: list[str], audit_interval_seconds: int) -> bool:
    now = pd.Timestamp.now(tz="UTC")
    max_age = pd.Timedelta(int(audit_interval_seconds), unit="s")
    for symbol in symbols:
        audit = JsonState(f"audits/{DATASET}_{symbol}.json").read()
        updated_at = audit.get("updated_at")
        if not updated_at:
            return False
        updated = pd.Timestamp(updated_at)
        if updated.tzinfo is None:
            updated = updated.tz_localize("UTC")
        if now - updated > max_age:
            return False
    return True


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--interval", default=None)
    parser.add_argument("--backfill-start", default=None)
    parser.add_argument("--sleep", type=int, default=None)
    parser.add_argument("--rest-overlap-minutes", type=int, default=None)
    parser.add_argument("--daily-lookback-days", type=int, default=None)
    parser.add_argument("--max-gap-minutes", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--audit-interval-seconds", type=int, default=None)
    parser.add_argument("--no-monthly", action="store_true")
    parser.add_argument("--no-daily", action="store_true")
    parser.add_argument("--no-rest", action="store_true")
    parser.add_argument("--refresh-archive", action="store_true")
    parser.add_argument("--repair-gaps", action="store_true")
    parser.add_argument("--proxy-fill-futures-gaps", action="store_true")
    parser.add_argument("--proxy-context-hours", type=int, default=None)
    parser.add_argument("--no-validate", action="store_true")
    args = parser.parse_args()

    config = load_yaml("symbols.binance_spot.yml")
    sleep_seconds = args.sleep or int(config.get("sleep_seconds", 70))
    audit_interval = int(args.audit_interval_seconds or config.get("audit_interval_seconds", 21600))
    logger = setup_logging(DATASET)
    symbols = _selected_symbols(args)
    audit_fresh = args.mode == "live" and not args.no_validate and _audits_are_fresh(symbols, audit_interval)
    last_audit_at = time.time() if audit_fresh else 0.0
    first_loop = not audit_fresh

    while True:
        now = time.time()
        run_audit = not args.no_validate and (args.mode != "live" or first_loop or now - last_audit_at >= audit_interval)
        result = sync_all(args, logger, run_audit=run_audit)
        logger.info("Binance spot 1m sync finished: %s", result)
        if run_audit:
            last_audit_at = now
        first_loop = False
        if args.mode != "live":
            break
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
