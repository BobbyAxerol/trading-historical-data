from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from collectors.common.calendar_vn import vn_now
from collectors.common.env import state_root
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, sleep_with_heartbeat, utc_now_iso
from collectors.common.storage import PartitionedParquetStore, release_unused_memory
from collectors.providers.vndirect_dchart_derivatives import DChartFetchResult, VndirectDChartProvider

VNDIRECT_SOURCE = "vndirect_dchart"
VNDIRECT_SYMBOL = "VN30F1M"


@dataclass(frozen=True)
class VndirectProbeOptions:
    recent_days: int = 5
    old_start: str = "2018-08-01"
    old_end: str = "2018-09-01"
    daily_start: str = "2017-08-10"
    fail_on_gate: bool = True


@dataclass(frozen=True)
class VndirectDailyOptions:
    start: str | None = None
    end: str | None = None
    version: str = "v1"
    overlap_days: int = 14
    update_matrix: bool = False


def vndirect_probe_path() -> Path:
    return state_root() / "vn_derivatives" / "vndirect_dchart_probe.json"


def _daily_state() -> JsonState:
    return JsonState("vn_derivatives/vndirect_dchart_1d.json")


def _result_summary(result: DChartFetchResult) -> dict[str, object]:
    return {
        "status": result.status,
        "row_count": result.row_count,
        "requested_start": result.requested_start,
        "requested_end": result.requested_end,
        "first_bar": result.first_bar,
        "last_bar": result.last_bar,
        "http_status": result.http_status,
        "error": result.error,
    }


def _json_default(value: object) -> str:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def _gate(
    *,
    recent_1m: DChartFetchResult,
    old_1m: DChartFetchResult,
    daily: DChartFetchResult,
) -> tuple[Literal["PASS", "FAIL"], list[str]]:
    errors: list[str] = []
    if recent_1m.status != "success":
        errors.append(f"recent_1m status is {recent_1m.status}")
    elif recent_1m.row_count <= 100:
        errors.append(f"recent_1m row_count <= 100: {recent_1m.row_count}")

    if daily.status != "success":
        errors.append(f"daily status is {daily.status}")
    elif daily.row_count <= 500:
        errors.append(f"daily row_count <= 500: {daily.row_count}")

    if old_1m.status not in {"success", "no_data"}:
        errors.append(f"old_1m status is {old_1m.status}")

    return ("PASS" if not errors else "FAIL", errors)


def run_vndirect_probe(options: VndirectProbeOptions | None = None) -> dict[str, object]:
    opts = options or VndirectProbeOptions()
    provider = VndirectDChartProvider()
    now = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh")
    recent_start = now - pd.Timedelta(days=opts.recent_days)

    recent_1m = provider.fetch(start=recent_start, end=now, resolution="1m")
    old_1m = provider.fetch(start=pd.Timestamp(opts.old_start), end=pd.Timestamp(opts.old_end), resolution="1m")
    daily = provider.fetch(start=pd.Timestamp(opts.daily_start), end=now, resolution="1d")
    production_gate, gate_errors = _gate(recent_1m=recent_1m, old_1m=old_1m, daily=daily)

    payload = {
        "provider": VNDIRECT_SOURCE,
        "symbol": VNDIRECT_SYMBOL,
        "recent_1m": _result_summary(recent_1m),
        "old_1m": _result_summary(old_1m),
        "daily": _result_summary(daily),
        "production_gate": production_gate,
        "gate_errors": gate_errors,
        "updated_at": utc_now_iso(),
    }
    path = vndirect_probe_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default))
    if production_gate != "PASS" and opts.fail_on_gate:
        raise RuntimeError("; ".join(gate_errors) or "VNDIRECT DChart probe failed")
    return payload


def _daily_store() -> PartitionedParquetStore:
    return PartitionedParquetStore(["vn", "futures", "continuous", "1d"], partition="year")


def _daily_attrs(version: str) -> dict[str, str]:
    return {"symbol": VNDIRECT_SYMBOL, "source": VNDIRECT_SOURCE, "version": version}


def _prepare_daily_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["time", "symbol", "open", "high", "low", "close", "volume", "source", "source_symbol", "quality_flags", "ingested_at"])
    work = df.copy()
    time_col = pd.to_datetime(work["time"], errors="coerce")
    try:
        if time_col.dt.tz is not None:
            time_col = time_col.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except Exception:
        pass
    work["time"] = time_col.dt.normalize()
    work["symbol"] = VNDIRECT_SYMBOL
    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["time", "open", "high", "low", "close", "volume"])
    if work.empty:
        return work
    invalid = (
        (work["high"] < work[["open", "close", "low"]].max(axis=1))
        | (work["low"] > work[["open", "close", "high"]].min(axis=1))
        | (work["volume"] < 0)
    )
    if invalid.any():
        raise ValueError(f"invalid VNDIRECT DChart daily rows: {int(invalid.sum())}")
    duplicate_dates = int(work.duplicated(subset=["symbol", "time"]).sum())
    if duplicate_dates:
        raise ValueError(f"duplicate VNDIRECT DChart daily rows: {duplicate_dates}")
    weekend_rows = int(pd.to_datetime(work["time"]).dt.weekday.ge(5).sum())
    if weekend_rows:
        raise ValueError(f"weekend VNDIRECT DChart daily rows: {weekend_rows}")
    return work[["time", "symbol", "open", "high", "low", "close", "volume", "source", "source_symbol", "quality_flags", "ingested_at"]].sort_values(["symbol", "time"]).reset_index(drop=True)


def _frame_checksum(df: pd.DataFrame) -> str:
    if df.empty:
        return hashlib.sha256(b"").hexdigest()
    values = pd.util.hash_pandas_object(df.sort_values(["symbol", "time"]).reset_index(drop=True), index=False).values
    return hashlib.sha256(values.tobytes()).hexdigest()


def _year_windows(start: pd.Timestamp, end: pd.Timestamp) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current = start.normalize()
    final = end.normalize()
    while current <= final:
        year_end = min(pd.Timestamp(year=current.year, month=12, day=31), final)
        windows.append((current, year_end))
        current = year_end + pd.Timedelta(days=1)
    return windows


def _default_daily_start(options: VndirectDailyOptions) -> pd.Timestamp:
    if options.start:
        return pd.Timestamp(options.start).normalize()
    store = _daily_store()
    latest = store.latest_time(attrs=_daily_attrs(options.version), time_col="time")
    if latest is not None:
        return (pd.Timestamp(latest).normalize() - pd.Timedelta(days=options.overlap_days)).normalize()
    return pd.Timestamp("2017-08-10")


def sync_vndirect_daily(options: VndirectDailyOptions | None = None) -> dict[str, object]:
    opts = options or VndirectDailyOptions()
    logger = setup_logging("vn30f1m_vndirect_daily")
    provider = VndirectDChartProvider()
    store = _daily_store()
    state = _daily_state()
    manifest = state.read()
    manifest.setdefault("provider", VNDIRECT_SOURCE)
    manifest.setdefault("symbol", VNDIRECT_SYMBOL)
    manifest.setdefault("version", opts.version)
    manifest.setdefault("windows", {})

    start = _default_daily_start(opts)
    end = pd.Timestamp(opts.end).normalize() if opts.end else pd.Timestamp(vn_now()).tz_localize(None).normalize()
    if start > end:
        payload = {"status": "ok", "reason": "already_current", "start": start.isoformat(), "end": end.isoformat(), "rows_written": 0, "updated_at": utc_now_iso()}
        manifest.update(payload)
        state.write(manifest)
        return payload

    total_rows_written = 0
    positive_windows = 0
    no_data_windows = 0
    windows_payload: list[dict[str, object]] = []
    for window_start, window_end in _year_windows(start, end):
        key = f"{window_start.date()}_{window_end.date()}"
        logger.info("vndirect_dchart_daily_window_start start=%s end=%s", window_start.date(), window_end.date())
        result = provider.fetch(start=window_start, end=window_end, resolution="1d")
        window_record: dict[str, object] = {
            "start": window_start.date().isoformat(),
            "end": window_end.date().isoformat(),
            "fetch_status": result.status,
            "http_status": result.http_status,
            "row_count": result.row_count,
            "first_bar": result.first_bar.isoformat() if result.first_bar is not None else None,
            "last_bar": result.last_bar.isoformat() if result.last_bar is not None else None,
            "attempt_count": int(manifest.get("windows", {}).get(key, {}).get("attempt_count", 0)) + 1,
            "last_error": result.error,
            "updated_at": utc_now_iso(),
        }
        if result.status == "no_data":
            no_data_windows += 1
            window_record["status"] = "no_data"
            manifest["windows"][key] = window_record
            state.write(manifest)
            continue
        if result.status != "success":
            window_record["status"] = "error"
            manifest["windows"][key] = window_record
            manifest["last_error"] = result.error or result.status
            manifest["updated_at"] = utc_now_iso()
            state.write(manifest)
            raise RuntimeError(f"VNDIRECT DChart daily {key} failed: {result.status} {result.error}")

        frame = _prepare_daily_frame(result.data)
        checksum = _frame_checksum(frame)
        append_result = store.append(
            frame,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs=_daily_attrs(opts.version),
            lock_name=f"vn30f1m_vndirect_dchart_1d/{opts.version}",
        )
        positive_windows += 1
        total_rows_written += int(append_result["rows_written"])
        window_record.update(
            {
                "status": "completed",
                "checksum": checksum,
                "rows_written": int(append_result["rows_written"]),
                "latest_time": str(append_result["latest_time"]),
                "output_partition": f"storage/vn/futures/continuous/1d/symbol={VNDIRECT_SYMBOL}/source={VNDIRECT_SOURCE}/version={opts.version}/year={window_start.year:04d}/part.parquet",
                "last_error": None,
            }
        )
        manifest["windows"][key] = window_record
        manifest["latest_time"] = str(append_result["latest_time"])
        manifest["last_success_at"] = utc_now_iso()
        manifest["last_error"] = None
        manifest["updated_at"] = utc_now_iso()
        state.write(manifest)
        windows_payload.append(window_record)
        del frame
        release_unused_memory()

    matrix_result: dict[str, object] = {"status": "skipped"}
    if opts.update_matrix:
        from collectors.vn_derivatives.continuous import update_daily_matrix_from_continuous

        matrix_result = update_daily_matrix_from_continuous()
    payload = {
        "status": "ok",
        "provider": VNDIRECT_SOURCE,
        "symbol": VNDIRECT_SYMBOL,
        "version": opts.version,
        "resolution": "1d",
        "start": start.date().isoformat(),
        "end": end.date().isoformat(),
        "positive_windows": positive_windows,
        "no_data_windows": no_data_windows,
        "rows_written": total_rows_written,
        "windows": windows_payload,
        "matrix": matrix_result,
        "updated_at": utc_now_iso(),
    }
    manifest.update({k: v for k, v in payload.items() if k != "windows"})
    state.write(manifest)
    Heartbeat("vn30f1m_vndirect").beat(
        rows_written=total_rows_written,
        latest_time=manifest.get("latest_time"),
        source=VNDIRECT_SOURCE,
    )
    release_unused_memory()
    return payload


def live_vndirect_daily(*, schedule: str = "16:30", version: str = "v1", overlap_days: int = 14, update_matrix: bool = True) -> None:
    logger = setup_logging("vn30f1m_vndirect_daily_live")
    heartbeat = Heartbeat("vn30f1m_vndirect")
    state = JsonState("vn_derivatives/vndirect_dchart_daily_schedule.json")
    schedule_state = state.read()
    last_run_date = schedule_state.get("last_run_date")
    while True:
        now = vn_now()
        hh, mm = [int(item) for item in schedule.split(":")]
        due = last_run_date != now.strftime("%Y-%m-%d") and (now.hour > hh or (now.hour == hh and now.minute >= mm))
        if due:
            try:
                result = sync_vndirect_daily(VndirectDailyOptions(version=version, overlap_days=overlap_days, update_matrix=update_matrix))
                logger.info("vndirect_dchart_daily_sync_done status=%s rows_written=%s", result.get("status"), result.get("rows_written"))
                heartbeat.beat(status=str(result.get("status", "ok")), rows_written=result.get("rows_written"))
                last_run_date = now.strftime("%Y-%m-%d")
                state.write({"last_run_date": last_run_date, "updated_at": utc_now_iso(), "version": version})
            except Exception as exc:
                logger.exception("vndirect_dchart_daily_sync_failed")
                heartbeat.beat(status="error", error=str(exc))
        sleep_with_heartbeat(
            heartbeat,
            300,
            schedule=schedule,
            last_run_date=last_run_date,
            source=VNDIRECT_SOURCE,
        )
