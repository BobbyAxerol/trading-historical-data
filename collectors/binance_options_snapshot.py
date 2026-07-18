from __future__ import annotations

import argparse
import re
import time
from datetime import timezone
from typing import Any

import pandas as pd
import requests

from collectors.common.config import load_yaml
from collectors.common.discovery import latest_time_from_files, max_timestamp
from collectors.common.env import GET_DATA_ROOT, load_environment
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, Manifest, utc_now_iso
from collectors.common.retry import retry_sync
from collectors.common.storage import PartitionedParquetStore

DATASET = "options_binance_snapshot_5m"
EAPI = "https://eapi.binance.com"
SPOT_API = "https://api.binance.com"
OPTION_RE = re.compile(r"^(?P<underlying>[A-Z]+)-(?P<expiry>\d{6})-(?P<strike>[0-9.]+)-(?P<type>[CP])$")


def _get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    def call() -> Any:
        response = requests.get(url, params=params or {}, timeout=30)
        if response.status_code in {418, 429} or response.status_code >= 500:
            raise RuntimeError(f"Retryable HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        return response.json()

    return retry_sync(call, attempts=5)


def _snapshot_time(interval_minutes: int) -> pd.Timestamp:
    now = pd.Timestamp.now(tz="UTC")
    minute = (now.minute // interval_minutes) * interval_minutes
    return now.replace(minute=minute, second=0, microsecond=0).tz_convert(None)


def _spot(underlying: str) -> float | None:
    try:
        data = _get_json(f"{SPOT_API}/api/v3/ticker/price", {"symbol": f"{underlying}USDT"})
        return float(data["price"])
    except Exception:
        return None


def _parse_symbol(symbol: str) -> dict[str, Any] | None:
    match = OPTION_RE.match(symbol)
    if not match:
        return None
    payload = match.groupdict()
    payload["strike"] = float(payload["strike"])
    expiry = pd.to_datetime(payload["expiry"], format="%y%m%d", errors="coerce")
    payload["expiry_date"] = expiry.strftime("%Y-%m-%d") if not pd.isna(expiry) else None
    return payload


def fetch_snapshot(
    underlyings: list[str],
    *,
    interval_minutes: int,
    max_expiries: int,
    min_abs_delta: float,
    max_abs_delta: float,
) -> pd.DataFrame:
    marks = _get_json(f"{EAPI}/eapi/v1/mark")
    tickers = _get_json(f"{EAPI}/eapi/v1/ticker")
    ticker_map = {row.get("symbol"): row for row in tickers if row.get("symbol")}
    snap_time = _snapshot_time(interval_minutes)

    rows: list[dict[str, Any]] = []
    today = pd.Timestamp.now(tz="UTC").tz_convert(None).normalize()
    underlying_set = {u.upper() for u in underlyings}
    spot_map = {u: _spot(u) for u in underlying_set}

    parsed_marks = []
    for mark in marks:
        symbol = mark.get("symbol", "")
        parsed = _parse_symbol(symbol)
        if not parsed or parsed["underlying"] not in underlying_set:
            continue
        if parsed["expiry_date"]:
            expiry_ts = pd.Timestamp(parsed["expiry_date"])
            if expiry_ts < today:
                continue
        parsed_marks.append((mark, parsed))

    expiry_keep: dict[str, set[str]] = {}
    for underlying in underlying_set:
        expiries = sorted({p["expiry_date"] for _, p in parsed_marks if p["underlying"] == underlying and p["expiry_date"]})
        expiry_keep[underlying] = set(expiries[:max_expiries])

    for mark, parsed in parsed_marks:
        if parsed["expiry_date"] not in expiry_keep.get(parsed["underlying"], set()):
            continue
        try:
            delta = float(mark.get("delta", 0))
        except Exception:
            delta = 0.0
        if not (min_abs_delta <= abs(delta) <= max_abs_delta):
            continue
        ticker = ticker_map.get(mark["symbol"], {})
        row = {
            "snapshot_time": snap_time,
            "underlying": parsed["underlying"],
            "symbol": mark["symbol"],
            "expiry": parsed["expiry_date"],
            "strike": parsed["strike"],
            "type": parsed["type"],
            "spot": spot_map.get(parsed["underlying"]),
            "source": "binance_options",
            "ingested_at": utc_now_iso(),
            **{f"mark_{k}": v for k, v in mark.items() if k != "symbol"},
            **{f"ticker_{k}": v for k, v in ticker.items() if k != "symbol"},
        }
        rows.append(row)

    return pd.DataFrame(rows)


def run_once(args, logger) -> None:
    config = load_yaml("options.yml")
    underlyings = args.underlyings.split(",") if args.underlyings else config.get("underlyings", ["BTC", "ETH"])
    df = fetch_snapshot(
        [u.strip().upper() for u in underlyings],
        interval_minutes=args.interval_minutes,
        max_expiries=config.get("max_expiries", 4),
        min_abs_delta=config.get("min_abs_delta", 0.15),
        max_abs_delta=config.get("max_abs_delta", 0.90),
    )
    manifest = Manifest(DATASET)
    if df.empty:
        manifest.update_symbol("ALL", last_error="empty_response", last_success_at=utc_now_iso())
        logger.warning("No option rows after filter")
        return

    store = PartitionedParquetStore(["options", "binance", "snapshot_5m"], partition="day")
    total = 0
    for underlying, part in df.groupby("underlying"):
        state = manifest.symbol_state(underlying)
        storage_latest = store.latest_time(attrs={"underlying": underlying}, time_col="snapshot_time")
        legacy_latest = latest_time_from_files(
            [
                GET_DATA_ROOT / "options_full_history.csv.gz",
                GET_DATA_ROOT / "option_data" / "options_full_history.csv.gz",
            ],
            ["snapshot_time", "time"],
        )
        discovered_latest = max_timestamp(state.get("latest_time"), storage_latest, legacy_latest)
        result = store.append(
            part,
            time_col="snapshot_time",
            dedupe_cols=["snapshot_time", "symbol"],
            attrs={"underlying": underlying},
            lock_name=f"{DATASET}/{underlying}",
        )
        total += int(result["rows_written"])
        manifest.update_symbol(
            underlying,
            latest_time=max_timestamp(discovered_latest, result["latest_time"]).isoformat(),
            last_success_at=utc_now_iso(),
            rows_written=result["rows_written"],
            source="binance_options",
            last_error=None,
            discovered_from_tail=discovered_latest is not None,
            legacy_latest=legacy_latest.isoformat() if legacy_latest is not None else None,
            storage_latest=storage_latest.isoformat() if storage_latest is not None else None,
        )
    logger.info("Wrote %s option rows", total)


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "once"], default="once")
    parser.add_argument("--interval-minutes", type=int, default=5)
    parser.add_argument("--underlyings", default=None)
    args = parser.parse_args()
    logger = setup_logging(DATASET)
    heartbeat = Heartbeat(DATASET)

    while True:
        try:
            run_once(args, logger)
            heartbeat.beat()
        except Exception as exc:
            logger.exception("Options snapshot failed")
            heartbeat.beat(status="error", error=str(exc))
        if args.mode != "live":
            break
        now = pd.Timestamp.now(tz=timezone.utc)
        seconds = args.interval_minutes * 60 - ((now.minute * 60 + now.second) % (args.interval_minutes * 60)) + 2
        time.sleep(max(10, seconds))


if __name__ == "__main__":
    main()
