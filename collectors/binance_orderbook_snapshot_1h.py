from __future__ import annotations

import argparse
import io
import os
import time
import zipfile
from datetime import timezone
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
from collectors.common.storage import PartitionedCsvGzStore

DATASET = "crypto_binance_orderbook_snapshot_1h"
STORE_PARTS = ["crypto", "binance_orderbook_snapshot", "1h"]
BINANCE_FAPI = "https://fapi.binance.com"
VISION_BASE_URL = "https://data.binance.vision"
S3_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
USER_AGENT = {"User-Agent": "pool-alpha-get-data/1.0"}
QUARTERLY_TYPES = {"CURRENT_QUARTER", "NEXT_QUARTER"}
BASE_COLUMNS = [
    "time",
    "sample_time",
    "market",
    "symbol",
    "contract_type",
    "mid_price",
    "best_bid",
    "best_ask",
    "spread",
    "spread_bps",
]
META_COLUMNS = ["depth_limit", "source", "ingested_at"]


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


def _date_from_key(key: str) -> str | None:
    name = Path(key).name
    if not name.endswith(".zip"):
        return None
    parts = name.replace(".zip", "").rsplit("-", 3)
    if len(parts) < 4:
        return None
    return f"{parts[-3]}-{parts[-2]}-{parts[-1]}"


def _band_token(band: float) -> str:
    pct = band * 100
    if float(pct).is_integer():
        return f"{int(pct)}pct"
    text = f"{pct:g}".replace(".", "_")
    return f"{text}pct"


def output_columns(percent_bands: list[float]) -> list[str]:
    cols = list(BASE_COLUMNS)
    for band in percent_bands:
        token = _band_token(band)
        cols.extend(
            [
                f"bid_depth_{token}",
                f"ask_depth_{token}",
                f"q_bid_depth_{token}",
                f"q_ask_depth_{token}",
                f"imbalance_{token}",
            ]
        )
    cols.extend(
        [
            "primary_bid_depth",
            "primary_ask_depth",
            "primary_q_bid_depth",
            "primary_q_ask_depth",
            "primary_imbalance",
        ]
    )
    cols.extend(META_COLUMNS)
    return cols


def discover_active_contracts(pairs: list[str], *, include_current: bool, include_next: bool) -> dict[str, dict[str, Any]]:
    payload = _request_json(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    allowed_types = set()
    if include_current:
        allowed_types.add("CURRENT_QUARTER")
    if include_next:
        allowed_types.add("NEXT_QUARTER")
    pair_set = {pair.upper() for pair in pairs}
    active: dict[str, dict[str, Any]] = {}
    for item in payload.get("symbols", []):
        contract_type = item.get("contractType")
        if contract_type not in allowed_types:
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
    symbols = {
        symbol.strip().upper(): {"contract_type": "PERPETUAL", "pair": symbol.strip().upper()}
        for symbol in config.get("symbols", [])
    }
    active = discover_active_contracts(
        [pair.strip().upper() for pair in config.get("quarterly_pairs", [])],
        include_current=bool(config.get("include_current_quarter", True)),
        include_next=bool(config.get("include_next_quarter", True)),
    )
    symbols.update(active)
    return symbols


def vision_daily_keys(symbol: str, *, start_day: str, end_day: str, s3_base_url: str) -> list[str]:
    prefix = f"data/futures/um/daily/bookDepth/{symbol}/"
    keys = []
    for key in _s3_keys(prefix, s3_base_url=s3_base_url):
        date_text = _date_from_key(key)
        if key.endswith(".zip") and date_text and start_day <= date_text <= end_day:
            keys.append(key)
    return sorted(keys)


def _date_has_rows(store: PartitionedCsvGzStore, symbol: str, date_text: str) -> bool:
    date = pd.Timestamp(date_text)
    path = store.root / f"symbol={symbol}" / f"year={date.year:04d}" / f"month={date.month:02d}" / "part.csv.gz"
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path, compression="gzip", usecols=["time"])
    except Exception:
        return False
    times = pd.to_datetime(df["time"], errors="coerce")
    return bool((times.dt.normalize() == date.normalize()).any())


def read_vision_book_depth_zip(
    content: bytes,
    *,
    symbol: str,
    contract_type: str,
    percent_bands: list[float],
    primary_band: float,
    source: str,
) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not csv_names:
            return pd.DataFrame(columns=output_columns(percent_bands))
        with archive.open(csv_names[0]) as handle:
            raw = pd.read_csv(handle)
    return normalize_vision_book_depth(
        raw,
        symbol=symbol,
        contract_type=contract_type,
        percent_bands=percent_bands,
        primary_band=primary_band,
        source=source,
    )


def normalize_vision_book_depth(
    raw: pd.DataFrame,
    *,
    symbol: str,
    contract_type: str,
    percent_bands: list[float],
    primary_band: float,
    source: str,
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=output_columns(percent_bands))
    df = raw.copy()
    df["sample_time"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert(None)
    df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce")
    df["depth"] = pd.to_numeric(df["depth"], errors="coerce")
    df["notional"] = pd.to_numeric(df["notional"], errors="coerce")
    df = df.dropna(subset=["sample_time", "percentage", "depth", "notional"])
    if df.empty:
        return pd.DataFrame(columns=output_columns(percent_bands))

    df["time"] = df["sample_time"].dt.floor("h")
    latest = df.groupby("time", as_index=False)["sample_time"].max()
    df = df.merge(latest, on=["time", "sample_time"], how="inner")

    rows: list[dict[str, Any]] = []
    now_iso = utc_now_iso()
    for (hour, sample_time), group in df.groupby(["time", "sample_time"], sort=True):
        row = {
            "time": hour,
            "sample_time": sample_time,
            "market": "usdm_futures",
            "symbol": symbol.upper(),
            "contract_type": contract_type,
            "mid_price": pd.NA,
            "best_bid": pd.NA,
            "best_ask": pd.NA,
            "spread": pd.NA,
            "spread_bps": pd.NA,
            "depth_limit": pd.NA,
            "source": source,
            "ingested_at": now_iso,
        }
        by_pct = {round(float(item["percentage"]), 8): item for item in group.to_dict("records")}
        for band in percent_bands:
            token = _band_token(band)
            pct = round(band * 100, 8)
            bid = by_pct.get(round(-pct, 8))
            ask = by_pct.get(round(pct, 8))
            bid_depth = float(bid["depth"]) if bid else pd.NA
            ask_depth = float(ask["depth"]) if ask else pd.NA
            q_bid = float(bid["notional"]) if bid else pd.NA
            q_ask = float(ask["notional"]) if ask else pd.NA
            row[f"bid_depth_{token}"] = bid_depth
            row[f"ask_depth_{token}"] = ask_depth
            row[f"q_bid_depth_{token}"] = q_bid
            row[f"q_ask_depth_{token}"] = q_ask
            row[f"imbalance_{token}"] = _imbalance(q_bid, q_ask)
        _set_primary(row, primary_band)
        rows.append(row)

    result = pd.DataFrame(rows)
    return _finalize(result, percent_bands)


def _imbalance(q_bid: Any, q_ask: Any) -> float | pd._libs.missing.NAType:
    bid = pd.to_numeric(pd.Series([q_bid]), errors="coerce").iloc[0]
    ask = pd.to_numeric(pd.Series([q_ask]), errors="coerce").iloc[0]
    if pd.isna(bid) or pd.isna(ask) or bid + ask == 0:
        return pd.NA
    return float((bid - ask) / (bid + ask))


def _set_primary(row: dict[str, Any], primary_band: float) -> None:
    token = _band_token(primary_band)
    row["primary_bid_depth"] = row.get(f"bid_depth_{token}", pd.NA)
    row["primary_ask_depth"] = row.get(f"ask_depth_{token}", pd.NA)
    row["primary_q_bid_depth"] = row.get(f"q_bid_depth_{token}", pd.NA)
    row["primary_q_ask_depth"] = row.get(f"q_ask_depth_{token}", pd.NA)
    row["primary_imbalance"] = row.get(f"imbalance_{token}", pd.NA)


def _finalize(df: pd.DataFrame, percent_bands: list[float]) -> pd.DataFrame:
    cols = output_columns(percent_bands)
    if df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.floor("h")
    df["sample_time"] = pd.to_datetime(df["sample_time"], errors="coerce")
    df = df.dropna(subset=["time", "symbol"])
    numeric_cols = [col for col in cols if col not in {"time", "sample_time", "market", "symbol", "contract_type", "source", "ingested_at"}]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[cols].drop_duplicates(subset=["symbol", "time"], keep="last").sort_values(["symbol", "time"]).reset_index(drop=True)


def fetch_rest_depth(symbol: str, *, depth_limit: int) -> dict[str, Any]:
    return _request_json(f"{BINANCE_FAPI}/fapi/v1/depth", params={"symbol": symbol, "limit": depth_limit})


def normalize_rest_depth(
    payload: dict[str, Any],
    *,
    symbol: str,
    contract_type: str,
    depth_limit: int,
    percent_bands: list[float],
    primary_band: float,
    source: str,
) -> pd.DataFrame:
    bids = [(float(price), float(qty)) for price, qty in payload.get("bids", [])]
    asks = [(float(price), float(qty)) for price, qty in payload.get("asks", [])]
    if not bids or not asks:
        return pd.DataFrame(columns=output_columns(percent_bands))
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2
    row: dict[str, Any] = {
        "time": pd.Timestamp.now(tz="UTC").floor("h").tz_localize(None),
        "sample_time": pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s"),
        "market": "usdm_futures",
        "symbol": symbol.upper(),
        "contract_type": contract_type,
        "mid_price": mid,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": best_ask - best_bid,
        "spread_bps": ((best_ask - best_bid) / mid) * 10_000 if mid else pd.NA,
        "depth_limit": depth_limit,
        "source": source,
        "ingested_at": utc_now_iso(),
    }
    for band in percent_bands:
        token = _band_token(band)
        min_bid = mid * (1 - band)
        max_ask = mid * (1 + band)
        bid_depth = sum(qty for price, qty in bids if price >= min_bid)
        ask_depth = sum(qty for price, qty in asks if price <= max_ask)
        q_bid = sum(price * qty for price, qty in bids if price >= min_bid)
        q_ask = sum(price * qty for price, qty in asks if price <= max_ask)
        row[f"bid_depth_{token}"] = bid_depth
        row[f"ask_depth_{token}"] = ask_depth
        row[f"q_bid_depth_{token}"] = q_bid
        row[f"q_ask_depth_{token}"] = q_ask
        row[f"imbalance_{token}"] = _imbalance(q_bid, q_ask)
    _set_primary(row, primary_band)
    return _finalize(pd.DataFrame([row]), percent_bands)


def append_features(store: PartitionedCsvGzStore, df: pd.DataFrame, symbol: str) -> dict[str, object]:
    return store.append(
        df,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": symbol},
        lock_name=f"{DATASET}/{symbol}",
    )


def prune_lookback(store: PartitionedCsvGzStore, symbol: str, *, lookback_days: int, logger) -> dict[str, int]:
    cutoff = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("h") - pd.Timedelta(int(lookback_days), unit="D")
    touched = 0
    removed = 0
    for path in store.files({"symbol": symbol}):
        try:
            df = pd.read_csv(path, compression="gzip")
        except Exception as exc:
            logger.warning("%s failed to read partition for prune %s: %s", symbol, path, exc)
            continue
        if df.empty or "time" not in df.columns:
            continue
        before = len(df)
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"])
        df = df[df["time"] >= cutoff].copy()
        removed += before - len(df)
        if len(df) == before:
            continue
        touched += 1
        if df.empty:
            path.unlink(missing_ok=True)
            continue
        df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        if "sample_time" in df.columns:
            df["sample_time"] = pd.to_datetime(df["sample_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        tmp = path.with_name(path.name + ".tmp")
        df.to_csv(tmp, index=False, compression="gzip")
        os.replace(tmp, path)
    if removed:
        logger.info("%s pruned orderbook rows=%s cutoff=%s", symbol, removed, cutoff)
    return {"touched": touched, "removed": removed}


def sync_vision_lookback(
    *,
    symbol: str,
    contract_type: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    percent_bands: list[float],
    primary_band: float,
    lookback_days: int,
    vision_base_url: str,
    s3_base_url: str,
    logger,
) -> int:
    end_day = (pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(1, unit="D")).strftime("%Y-%m-%d")
    start_day = (pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(int(lookback_days), unit="D")).strftime("%Y-%m-%d")
    keys = vision_daily_keys(symbol, start_day=start_day, end_day=end_day, s3_base_url=s3_base_url)
    total = 0
    for key in keys:
        date_text = _date_from_key(key)
        if date_text and _date_has_rows(store, symbol, date_text):
            logger.debug("%s skip existing Vision bookDepth date %s", symbol, date_text)
            continue
        content = _request_bytes(f"{vision_base_url.rstrip('/')}/{key}")
        if content is None:
            continue
        df = read_vision_book_depth_zip(
            content,
            symbol=symbol,
            contract_type=contract_type,
            percent_bands=percent_bands,
            primary_band=primary_band,
            source="binance_vision_usdm_bookDepth",
        )
        if df.empty:
            continue
        result = append_features(store, df, symbol)
        total += int(result.get("rows_written") or 0)
        manifest.update_symbol(
            symbol,
            latest_time=str(result["latest_time"]),
            last_success_at=utc_now_iso(),
            last_vision_key=key,
            rows_written=result["rows_written"],
            source="binance_vision_usdm_bookDepth",
            last_error=None,
        )
        logger.info("%s Vision bookDepth wrote rows=%s key=%s", symbol, result["rows_written"], Path(key).name)
    return total


def sync_rest_snapshot(
    *,
    symbol: str,
    contract_type: str,
    store: PartitionedCsvGzStore,
    manifest: Manifest,
    depth_limit: int,
    percent_bands: list[float],
    primary_band: float,
    logger,
) -> int:
    payload = fetch_rest_depth(symbol, depth_limit=depth_limit)
    df = normalize_rest_depth(
        payload,
        symbol=symbol,
        contract_type=contract_type,
        depth_limit=depth_limit,
        percent_bands=percent_bands,
        primary_band=primary_band,
        source="binance_usdm_depth_rest",
    )
    if df.empty:
        manifest.update_symbol(symbol, last_error="empty_rest_depth", last_failed_at=utc_now_iso())
        logger.warning("%s REST depth returned no rows", symbol)
        return 0
    result = append_features(store, df, symbol)
    manifest.update_symbol(
        symbol,
        latest_time=str(result["latest_time"]),
        last_success_at=utc_now_iso(),
        rows_written=result["rows_written"],
        source="binance_usdm_depth_rest",
        last_error=None,
    )
    logger.info("%s REST depth wrote rows=%s latest=%s", symbol, result["rows_written"], result["latest_time"])
    return int(result.get("rows_written") or 0)


def audit_symbol(store: PartitionedCsvGzStore, symbol: str, *, lookback_days: int) -> dict[str, Any]:
    frames = []
    for path in store.files({"symbol": symbol}):
        try:
            frames.append(pd.read_csv(path, compression="gzip"))
        except Exception:
            continue
    if not frames:
        return {"rows": 0, "duplicate_rows": 0, "gap_count": 0}
    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")
    cutoff = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("h") - pd.Timedelta(int(lookback_days), unit="D")
    df = df[df["time"] >= cutoff]
    times = df["time"].drop_duplicates().sort_values().reset_index(drop=True)
    gaps = times.diff().dropna()
    big = gaps[gaps > pd.Timedelta(1, unit="h")]
    return {
        "rows": int(len(df)),
        "min_time": str(times.min()) if not times.empty else None,
        "max_time": str(times.max()) if not times.empty else None,
        "duplicate_rows": int(df.duplicated(subset=["symbol", "time"]).sum()),
        "gap_count": int(len(big)),
        "max_gap": str(big.max()) if not big.empty else None,
    }


def sync_all(args: argparse.Namespace, logger, *, run_audit: bool = True) -> dict[str, Any]:
    config = load_yaml("symbols.binance_orderbook_snapshot.yml")
    symbol_meta = build_symbol_meta(config)
    if args.symbols:
        requested = {item.strip().upper() for item in args.symbols.split(",")}
        symbol_meta = {symbol: meta for symbol, meta in symbol_meta.items() if symbol in requested}
    depth_limit = int(args.depth_limit or config.get("depth_limit", 20))
    lookback_days = int(args.lookback_days or config.get("lookback_days", 30))
    percent_bands = [float(item) for item in config.get("percent_bands", [0.002, 0.01, 0.02, 0.05])]
    primary_band = float(config.get("primary_feature_band", 0.01))
    vision_base_url = config.get("vision_base_url", VISION_BASE_URL)
    s3_base_url = config.get("s3_base_url", S3_BASE_URL)

    JsonState("binance_orderbook_snapshot_1h_symbols.json").write(
        {
            "dataset": DATASET,
            "storage": "storage/crypto/binance_orderbook_snapshot/1h",
            "symbols": sorted(symbol_meta),
            "depth_limit": depth_limit,
            "lookback_days": lookback_days,
            "percent_bands": percent_bands,
            "primary_feature_band": primary_band,
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
            if not args.no_vision:
                total_rows += sync_vision_lookback(
                    symbol=symbol,
                    contract_type=meta.get("contract_type", ""),
                    store=store,
                    manifest=manifest,
                    percent_bands=percent_bands,
                    primary_band=primary_band,
                    lookback_days=lookback_days,
                    vision_base_url=vision_base_url,
                    s3_base_url=s3_base_url,
                    logger=logger,
                )
            if not args.no_rest:
                total_rows += sync_rest_snapshot(
                    symbol=symbol,
                    contract_type=meta.get("contract_type", ""),
                    store=store,
                    manifest=manifest,
                    depth_limit=depth_limit,
                    percent_bands=percent_bands,
                    primary_band=primary_band,
                    logger=logger,
                )
            prune_lookback(store, symbol, lookback_days=lookback_days, logger=logger)
            audit = audit_symbol(store, symbol, lookback_days=lookback_days) if run_audit else {}
            if audit:
                audits[symbol] = audit
                JsonState(f"audits/{DATASET}_{symbol}.json").write({"dataset": DATASET, "symbol": symbol, "updated_at": utc_now_iso(), **audit})
            heartbeat.beat(symbol=symbol, latest_time=manifest.symbol_state(symbol).get("latest_time"))
        except Exception as exc:
            manifest.update_symbol(symbol, last_error=str(exc), last_failed_at=utc_now_iso())
            logger.exception("%s orderbook snapshot sync failed", symbol)
            heartbeat.beat(status="error", symbol=symbol, error=str(exc))

    return {"symbols": sorted(symbol_meta), "rows_written": total_rows, "audits": audits}


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "live"], default="once")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--depth-limit", type=int, default=None)
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--sleep", type=int, default=None)
    parser.add_argument("--no-vision", action="store_true")
    parser.add_argument("--no-rest", action="store_true")
    parser.add_argument("--no-validate", action="store_true")
    args = parser.parse_args()

    config = load_yaml("symbols.binance_orderbook_snapshot.yml")
    sleep_seconds = int(args.sleep or config.get("sleep_seconds", 3600))
    logger = setup_logging(DATASET)
    while True:
        result = sync_all(args, logger, run_audit=not args.no_validate)
        logger.info("Binance orderbook snapshot 1h sync finished: %s", result)
        if args.mode != "live":
            break
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
