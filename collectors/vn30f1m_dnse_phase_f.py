"""Controlled DNSE rebuild for the legacy VN30F1M provider alias.

This module deliberately keeps the existing ``collectors.vn_intraday_dnse``
tail collector intact.  Phase F needs a separately gated, bounded historical
path before its raw ``vn/futures/1m`` layout can be published to readers.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from collectors.common.calendar_vn import VN_HOLIDAYS, filter_trading_hours
from collectors.common.env import data_root, load_environment
from collectors.common.manifest import Heartbeat, JsonState, Manifest, utc_now_iso
from collectors.common.storage import PartitionedParquetStore
from collectors.vn_intraday_dnse import fetch_ohlc


DATASET_ID = "vn_futures_1m"
MANIFEST_DATASET = "vn_futures_dnse_1m"
SYMBOL = "VN30F1M"
PROBE_STATE = "audits/vn30f1m_dnse_phase_f_probe.json"
AUDIT_STATE = "audits/vn30f1m_dnse_1m_phase_f.json"
CANONICAL_COLUMNS = ("time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at")


@dataclass(frozen=True)
class Window:
    start: pd.Timestamp
    end: pd.Timestamp


def _parse_start(value: str) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if result.tzinfo is not None:
        result = result.tz_convert("Asia/Ho_Chi_Minh").tz_localize(None)
    return result.normalize()


def _parse_end_inclusive(value: str) -> pd.Timestamp:
    # CLI date values are trading-date inclusive.  DNSE receives the next
    # local midnight as an exclusive-ish boundary; partition dedupe makes the
    # boundary safe if an upstream endpoint treats it as inclusive.
    return _parse_start(value) + pd.Timedelta(days=1)


def iter_windows(start: pd.Timestamp, end: pd.Timestamp, *, window_days: int) -> Iterable[Window]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if end <= start:
        raise ValueError("end must be later than start")
    cursor = start
    width = pd.Timedelta(days=window_days)
    while cursor < end:
        window_end = min(cursor + width, end)
        yield Window(start=cursor, end=window_end)
        cursor = window_end


def _frame_audit(frame: pd.DataFrame, *, symbol: str) -> dict[str, Any]:
    """Return a serialisable structural audit without mutating *frame*."""

    result: dict[str, Any] = {
        "rows": int(len(frame)),
        "missing_columns": [column for column in CANONICAL_COLUMNS if column not in frame.columns],
        "duplicate_symbol_time_rows": 0,
        "null_required_rows": 0,
        "invalid_ohlc_rows": 0,
        "negative_volume_rows": 0,
        "wrong_symbol_rows": 0,
        "wrong_source_rows": 0,
        "out_of_derivative_session_rows": 0,
        "first_time": None,
        "last_time": None,
        "status": "empty" if frame.empty else "pass",
    }
    if frame.empty:
        return result
    if result["missing_columns"]:
        result["status"] = "fail"
        return result

    work = frame.loc[:, CANONICAL_COLUMNS].copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    result["duplicate_symbol_time_rows"] = int(work.duplicated(["symbol", "time"], keep=False).sum())
    result["null_required_rows"] = int(work[["time", "symbol", "open", "high", "low", "close", "volume"]].isna().any(axis=1).sum())
    result["wrong_symbol_rows"] = int((work["symbol"].astype(str).str.upper() != symbol.upper()).sum())
    result["wrong_source_rows"] = int((work["source"].astype(str).str.lower() != "dnse").sum())
    result["negative_volume_rows"] = int((work["volume"] < 0).fillna(False).sum())
    invalid_ohlc = (
        (work["high"] < work[["open", "close", "low"]].max(axis=1))
        | (work["low"] > work[["open", "close", "high"]].min(axis=1))
        | (work["high"] < work["low"])
    )
    result["invalid_ohlc_rows"] = int(invalid_ohlc.fillna(True).sum())
    session_rows = filter_trading_hours(work.dropna(subset=["time"]), derivative=True)
    result["out_of_derivative_session_rows"] = int(len(work.dropna(subset=["time"])) - len(session_rows))
    result["first_time"] = str(work["time"].min())
    result["last_time"] = str(work["time"].max())
    if any(
        result[name]
        for name in (
            "duplicate_symbol_time_rows",
            "null_required_rows",
            "invalid_ohlc_rows",
            "negative_volume_rows",
            "wrong_symbol_rows",
            "wrong_source_rows",
            "out_of_derivative_session_rows",
        )
    ):
        result["status"] = "fail"
    return result


def _safe_error(exc: Exception) -> str:
    # Provider exception text can be useful for an operator while headers and
    # credentials must never be persisted in a state file.
    text = str(exc).replace("\n", " ")
    return f"{type(exc).__name__}: {text[:240]}"


def _probe_payload(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "dataset_id": DATASET_ID,
        "provider": "dnse",
        "symbol": symbol,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "updated_at": utc_now_iso(),
    }
    try:
        frame = fetch_ohlc(symbol=symbol, start=start, end=end, resolution="1", asset_type="derivative")
    except Exception as exc:
        payload.update(status="blocked", error=_safe_error(exc), row_count=0, audit=None)
        return payload

    audit = _frame_audit(frame, symbol=symbol)
    payload.update(
        status="pass" if audit["status"] == "pass" else "blocked",
        row_count=int(len(frame)),
        audit=audit,
        error=None if audit["status"] == "pass" else "DNSE returned no valid 1m rows for the requested proof window",
    )
    return payload


def run_probe(*, symbol: str, start: str, end: str) -> dict[str, Any]:
    payload = _probe_payload(symbol, _parse_start(start), _parse_end_inclusive(end))
    JsonState(PROBE_STATE).write(payload)
    Heartbeat("vn30f1m_dnse_phase_f_probe").beat(status="ok" if payload["status"] == "pass" else "error", symbol=symbol, probe_status=payload["status"])
    return payload


def _read_raw_storage(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    root = data_root() / "vn" / "futures" / "1m" / f"symbol={symbol.upper()}"
    frames: list[pd.DataFrame] = []
    for path in sorted(root.glob("year=*/month=*/part.parquet")):
        frame = pd.read_parquet(path)
        if "time" not in frame.columns:
            continue
        frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
        frames.append(frame[(frame["time"] >= start) & (frame["time"] < end)])
    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def _expected_trading_dates(start: pd.Timestamp, end: pd.Timestamp) -> set[pd.Timestamp]:
    dates = pd.date_range(start.normalize(), (end - pd.Timedelta(days=1)).normalize(), freq="D")
    return {
        value
        for value in dates
        if value.weekday() < 5 and value.strftime("%Y-%m-%d") not in VN_HOLIDAYS
    }


def audit_storage(*, symbol: str, start: str, end: str) -> dict[str, Any]:
    start_ts = _parse_start(start)
    end_ts = _parse_end_inclusive(end)
    frame = _read_raw_storage(symbol, start_ts, end_ts)
    audit = _frame_audit(frame, symbol=symbol)
    observed_dates = set(pd.to_datetime(frame.get("time", pd.Series(dtype="datetime64[ns]")), errors="coerce").dropna().dt.normalize())
    missing_dates = sorted(value.strftime("%Y-%m-%d") for value in _expected_trading_dates(start_ts, end_ts) - observed_dates)
    audit.update(
        dataset_id=DATASET_ID,
        provider="dnse",
        symbol=symbol,
        requested_start=start_ts.isoformat(),
        requested_end=end_ts.isoformat(),
        missing_trading_dates=missing_dates,
        missing_trading_date_count=len(missing_dates),
        updated_at=utc_now_iso(),
    )
    audit["status"] = "pass" if audit["status"] == "pass" and not missing_dates else "fail"
    JsonState(AUDIT_STATE).write(audit)
    return audit


def _require_probe(*, symbol: str) -> None:
    probe = JsonState(PROBE_STATE).read()
    if probe.get("status") != "pass" or str(probe.get("symbol", "")).upper() != symbol.upper():
        raise RuntimeError("DNSE historical backfill requires a passing Phase F DNSE source proof")


def run_backfill(
    *,
    symbol: str,
    start: str,
    end: str,
    window_days: int,
    require_probe: bool,
    audit_after: bool,
) -> dict[str, Any]:
    if require_probe:
        _require_probe(symbol=symbol)
    start_ts = _parse_start(start)
    end_ts = _parse_end_inclusive(end)
    store = PartitionedParquetStore(["vn", "futures", "1m"], partition="month")
    manifest = Manifest(MANIFEST_DATASET)
    heartbeat = Heartbeat("vn30f1m_dnse_phase_f_backfill")
    windows: list[dict[str, Any]] = []
    total_rows_written = 0

    for window in iter_windows(start_ts, end_ts, window_days=window_days):
        try:
            frame = fetch_ohlc(symbol=symbol, start=window.start, end=window.end, resolution="1", asset_type="derivative")
        except Exception as exc:
            failure = {
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
                "status": "error",
                "error": _safe_error(exc),
            }
            windows.append(failure)
            manifest.update_symbol(symbol, last_error=failure["error"], last_failed_at=utc_now_iso(), source="dnse")
            heartbeat.beat(status="error", symbol=symbol, window=failure)
            raise RuntimeError(f"DNSE window failed {window.start} -> {window.end}: {failure['error']}") from exc

        frame_audit = _frame_audit(frame, symbol=symbol)
        entry: dict[str, Any] = {
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "rows": int(len(frame)),
            "audit": frame_audit,
        }
        if frame_audit["status"] == "empty":
            entry["status"] = "no_data"
        elif frame_audit["status"] != "pass":
            entry["status"] = "invalid"
            windows.append(entry)
            raise RuntimeError(f"DNSE returned invalid OHLCV for {window.start} -> {window.end}: {frame_audit}")
        else:
            result = store.append(
                frame,
                time_col="time",
                dedupe_cols=["symbol", "time"],
                attrs={"symbol": symbol.upper()},
                lock_name=f"{MANIFEST_DATASET}/{symbol.upper()}",
            )
            rows_written = int(result["rows_written"])
            total_rows_written += rows_written
            entry.update(status="written", rows_written=rows_written, latest_time=result.get("latest_time"))
            manifest.update_symbol(
                symbol.upper(),
                latest_time=result.get("latest_time"),
                last_success_at=utc_now_iso(),
                rows_written=rows_written,
                source="dnse",
                last_error=None,
            )
        windows.append(entry)
        heartbeat.beat(status="ok", symbol=symbol, window_status=entry["status"], latest_time=entry.get("latest_time"))

    payload: dict[str, Any] = {
        "status": "ok",
        "dataset_id": DATASET_ID,
        "provider": "dnse",
        "symbol": symbol,
        "requested_start": start_ts.isoformat(),
        "requested_end": end_ts.isoformat(),
        "window_days": window_days,
        "windows": windows,
        "rows_written": total_rows_written,
        "updated_at": utc_now_iso(),
    }
    if audit_after:
        payload["audit"] = audit_storage(symbol=symbol, start=start, end=end)
        if payload["audit"]["status"] != "pass":
            payload["status"] = "blocked"
    JsonState("audits/vn30f1m_dnse_phase_f_backfill.json").write(payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase F controlled DNSE VN30F1M legacy-alias rebuild")
    parser.add_argument("--mode", choices=("probe", "backfill"), required=True)
    parser.add_argument("--symbols", default=SYMBOL)
    parser.add_argument("--probe-start", default="2025-01-06")
    parser.add_argument("--probe-end", default="2025-01-10")
    parser.add_argument("--backfill-start", default="2025-01-01")
    parser.add_argument("--backfill-end", default="2026-08-18")
    parser.add_argument("--window-days", type=int, default=5)
    parser.add_argument("--require-probe", action="store_true")
    parser.add_argument("--audit-phase-f", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main() -> None:
    load_environment()
    args = build_parser().parse_args()
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    if symbols != [SYMBOL]:
        raise ValueError("Phase F DNSE gate is intentionally limited to VN30F1M")
    if args.mode == "probe":
        payload = run_probe(symbol=SYMBOL, start=args.probe_start, end=args.probe_end)
    else:
        payload = run_backfill(
            symbol=SYMBOL,
            start=args.backfill_start,
            end=args.backfill_end,
            window_days=args.window_days,
            require_probe=args.require_probe,
            audit_after=args.audit_phase_f,
        )
    if args.json:
        print(json.dumps(payload, sort_keys=True, default=str))
    if payload["status"] != "pass" and args.mode == "probe":
        raise RuntimeError(str(payload.get("error", "DNSE source proof did not pass")))
    if payload["status"] != "ok":
        raise RuntimeError("DNSE Phase F backfill audit did not pass")


if __name__ == "__main__":
    main()
