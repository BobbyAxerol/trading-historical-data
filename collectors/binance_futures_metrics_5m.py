from __future__ import annotations

import argparse
import io
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from collectors.common.config import load_yaml
from collectors.common.env import load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, Manifest, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.storage import PartitionedCsvGzStore

DATASET = "crypto_binance_futures_metrics_5m"
STORE_PARTS = ["crypto", "binance_futures_metrics", "5m"]
BINANCE_FAPI = "https://fapi.binance.com"
VISION_BASE_URL = "https://data.binance.vision"
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


def _date_row_count(store: PartitionedCsvGzStore, symbol: str, date_text: str) -> int:
    date = pd.Timestamp(date_text)
    path = store.root / f"symbol={symbol}" / f"year={date.year:04d}" / f"month={date.month:02d}" / "part.csv.gz"
    if not path.exists():
        return 0
    try:
        df = pd.read_csv(path, compression="gzip", usecols=["time"])
    except Exception:
        return 0
    times = pd.to_datetime(df["time"], errors="coerce")
    return int((times.dt.normalize() == date.normalize()).sum())


def _date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    if start > end:
        return []
    return list(pd.date_range(start.normalize(), end.normalize(), freq="D"))


def _vision_key(symbol: str, day: pd.Timestamp) -> str:
    date_text = day.strftime("%Y-%m-%d")
    return f"data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{date_text}.zip"


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
    logger,
) -> int:
    latest = store.latest_time(attrs={"symbol": symbol}, time_col="time")
    configured_start = pd.Timestamp(start_date)
    if latest is not None:
        start = max(configured_start, latest - pd.Timedelta(int(vision_overlap_days), unit="D"))
    else:
        start = configured_start
    scan_start = start - pd.Timedelta(1, unit="D")
    end = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize() - pd.Timedelta(1, unit="D")
    days = []
    for day in _date_range(scan_start, end):
        date_text = day.strftime("%Y-%m-%d")
        if day >= configured_start and _date_row_count(store, symbol, date_text) >= int(min_rows_per_full_day):
            continue
        days.append(day)
    if not days:
        return 0

    total = 0
    workers = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _download_vision_day,
                symbol=symbol,
                contract_type=contract_type,
                day=day,
                vision_base_url=vision_base_url,
            )
            for day in days
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
                last_error=None,
            )
            logger.info("%s Vision metrics wrote rows=%s key=%s", symbol, result["rows_written"], Path(key).name)
    return total


def audit_symbol(store: PartitionedCsvGzStore, symbol: str) -> dict[str, Any]:
    frames = []
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(pd.read_csv(path, compression="gzip", usecols=["time", "symbol"]))
        except Exception:
            continue
    if not frames:
        return {"rows": 0, "duplicate_rows": 0, "gap_count": 0}
    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")
    times = df["time"].drop_duplicates().sort_values().reset_index(drop=True)
    diffs = times.diff().dropna()
    gaps = diffs[diffs > pd.Timedelta(5, unit="min")]
    return {
        "rows": int(len(df)),
        "min_time": str(times.min()) if not times.empty else None,
        "max_time": str(times.max()) if not times.empty else None,
        "duplicate_rows": int(df.duplicated(subset=["symbol", "time"]).sum()),
        "gap_count": int(len(gaps)),
        "max_gap": str(gaps.max()) if not gaps.empty else None,
    }


def sync_all(args: argparse.Namespace, logger, *, run_audit: bool = True) -> dict[str, Any]:
    config = load_yaml("symbols.binance_futures_metrics.yml")
    symbol_meta = build_symbol_meta(config)
    if args.symbols:
        requested = {item.strip().upper() for item in args.symbols.split(",")}
        symbol_meta = {symbol: meta for symbol, meta in symbol_meta.items() if symbol in requested}

    start_date = args.start_date or config.get("start_date", "2023-01-01")
    vision_overlap_days = int(args.vision_overlap_days or config.get("vision_overlap_days", 7))
    max_workers = int(args.max_workers or config.get("max_workers", 8))
    min_rows_per_full_day = int(config.get("min_rows_per_full_day", 280))
    legacy_seed_dir = config.get("legacy_seed_dir")
    vision_base_url = config.get("vision_base_url", VISION_BASE_URL)

    JsonState("binance_futures_metrics_5m_symbols.json").write(
        {
            "dataset": DATASET,
            "storage": "storage/crypto/binance_futures_metrics/5m",
            "symbols": sorted(symbol_meta),
            "start_date": start_date,
            "vision_overlap_days": vision_overlap_days,
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
                    logger=logger,
                )
            audit = audit_symbol(store, symbol) if run_audit else {}
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
    parser.add_argument("--no-validate", action="store_true")
    args = parser.parse_args()

    config = load_yaml("symbols.binance_futures_metrics.yml")
    sleep_seconds = int(args.sleep or config.get("sleep_seconds", 21600))
    logger = setup_logging(DATASET)
    while True:
        result = sync_all(args, logger, run_audit=not args.no_validate)
        logger.info("Binance futures metrics 5m sync finished: %s", result)
        if args.mode != "live":
            break
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
