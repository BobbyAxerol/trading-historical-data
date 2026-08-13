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
from collectors.common.storage import read_partition_file
from collectors.crypto_1m import BINANCE_FAPI, _closed_until, fetch_1m

DATASET = "crypto_binance_usdm_quarterly_1m"
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
            df = pd.read_csv(handle)
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
    frames = []
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(read_partition_file(path, usecols=["time"]))
        except Exception:
            continue
    if not frames:
        return []
    times = pd.concat(frames, ignore_index=True)["time"]
    times = pd.to_datetime(times, errors="coerce").dropna().drop_duplicates().sort_values().reset_index(drop=True)
    if len(times) < 2:
        return []
    diffs = times.diff()
    mask = (diffs > pd.Timedelta(minutes=1)) & (diffs <= pd.Timedelta(minutes=max_gap_minutes))
    ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for idx in mask[mask].index:
        start = times.iloc[idx - 1] + pd.Timedelta(minutes=1)
        end = times.iloc[idx] - pd.Timedelta(minutes=1)
        if start <= end:
            ranges.append((start, end))
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
        df = fetch_1m(symbol, start.tz_localize("UTC"), end.tz_localize("UTC"))
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
    if include_monthly:
        for key in vision_monthly_keys(symbol, interval=interval, start_month=start_month, s3_base_url=s3_base_url):
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

    return {"rows_written": total_rows, "latest_time": latest_time}


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
    heartbeat = Heartbeat(DATASET)
    total_rows = 0
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
            heartbeat.beat(symbol=symbol, latest_time=result.get("latest_time"))
        except Exception as exc:
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            logger.exception("%s quarterly sync failed", symbol)
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))
    return {"symbols": symbols, "active_symbols": sorted(active), "rows_written": total_rows}


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
