from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from collectors.common.calendar_vn import is_trading_day, vn_now
from collectors.common.env import state_root
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, sleep_with_heartbeat, utc_now_iso
from collectors.common.storage import PartitionedParquetStore, read_partition_file, release_unused_memory
from collectors.providers.vndirect_dchart_derivatives import DChartFetchResult, VndirectDChartProvider

VNDIRECT_SOURCE = "vndirect_dchart"
VNDIRECT_SYMBOL = "VN30F1M"
PHASE_D_AUDIT_STATE = "audits/vn30f1m_vndirect_dchart_1d_phase_d.json"
PHASE_E_MINUTE_AUDIT_STATE = "audits/vn30f1m_vndirect_dchart_1m_phase_e.json"
CALENDAR_ASSERTION_START = pd.Timestamp("2024-01-01")
ONE_MINUTE = pd.Timedelta(minutes=1)


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
    audit_phase_d: bool = False


@dataclass(frozen=True)
class VndirectMinuteOptions:
    """Bounded, source-proven VNDIRECT continuous-alias minute sync options."""

    start: str | None = None
    end: str | None = None
    version: str = "v1"
    window_days: int = 31
    min_window_days: int = 7
    overlap_minutes: int = 10
    require_source_proof: bool = False
    audit_phase_e: bool = False


def vndirect_probe_path() -> Path:
    return state_root() / "vn_derivatives" / "vndirect_dchart_probe.json"


def _daily_state() -> JsonState:
    return JsonState("vn_derivatives/vndirect_dchart_1d.json")


def _result_summary(result: DChartFetchResult) -> dict[str, object]:
    return {
        "status": result.status,
        "row_count": result.row_count,
        "requested_start": result.requested_start.isoformat() if result.requested_start is not None else None,
        "requested_end": result.requested_end.isoformat() if result.requested_end is not None else None,
        # This summary is embedded in the minute-sync manifest when source
        # proof is required.  Keep it natively JSON serializable: JsonState
        # deliberately does not use a lossy default encoder.
        "first_bar": result.first_bar.isoformat() if result.first_bar is not None else None,
        "last_bar": result.last_bar.isoformat() if result.last_bar is not None else None,
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


def last_closed_vn_daily(now=None) -> pd.Timestamp:
    """Return the latest safe VN derivative daily date for canonical storage.

    DChart can expose an in-session daily bar.  Keep it out until the regular
    market has been closed for a small buffer, and on weekends/holidays return
    the preceding known trading day.  This applies even to an explicit end
    date so an operator cannot accidentally persist a partial current day.
    """

    local_now = now or vn_now()
    candidate = pd.Timestamp(local_now).tz_localize(None).normalize()
    if is_trading_day(local_now) and (local_now.hour, local_now.minute) < (15, 0):
        candidate -= pd.Timedelta(days=1)
    while not is_trading_day(candidate.to_pydatetime()):
        candidate -= pd.Timedelta(days=1)
    return candidate


def audit_vndirect_daily(*, expected_latest: pd.Timestamp | None = None) -> dict[str, object]:
    """Stream the VNDIRECT daily partitions into durable Phase D evidence.

    The repository calendar is complete from 2024 onward, so strict missing
    trading-day assertions intentionally start there.  Earlier provider
    history is still checked for schema, duplicate keys, OHLC, negative
    values, source provenance, and weekend rows without pretending that a
    partial holiday table can prove continuity.
    """

    store = _daily_store()
    attrs = _daily_attrs("v1")
    files = store.files(attrs)
    expected = pd.Timestamp(expected_latest or last_closed_vn_daily()).normalize()
    required_columns = ["time", "symbol", "open", "high", "low", "close", "volume", "source"]
    rows = 0
    duplicate_rows = 0
    invalid_time_rows = 0
    invalid_numeric_rows = 0
    ohlc_bad_rows = 0
    negative_rows = 0
    weekend_rows = 0
    source_mismatch_rows = 0
    symbol_mismatch_rows = 0
    file_errors: list[str] = []
    seen_keys: set[tuple[str, int]] = set()
    observed_dates: set[pd.Timestamp] = set()
    first: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None

    for path in files:
        try:
            frame = read_partition_file(path, usecols=required_columns)
        except Exception as exc:
            file_errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
            continue
        rows += int(len(frame))
        times = pd.to_datetime(frame["time"], errors="coerce")
        try:
            if times.dt.tz is not None:
                times = times.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass
        times = times.dt.normalize()
        invalid_time_rows += int(times.isna().sum())
        numeric = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
        invalid_numeric_rows += int(numeric.isna().any(axis=1).sum())
        valid = times.notna() & numeric.notna().all(axis=1)
        if valid.any():
            work_times = times.loc[valid]
            work_numeric = numeric.loc[valid]
            high = work_numeric["high"]
            low = work_numeric["low"]
            ohlc_bad_rows += int(
                (
                    (high < work_numeric[["open", "close", "low"]].max(axis=1))
                    | (low > work_numeric[["open", "close", "high"]].min(axis=1))
                ).sum()
            )
            negative_rows += int((work_numeric < 0).any(axis=1).sum())
            weekend_rows += int(work_times.dt.weekday.ge(5).sum())
            for symbol, timestamp in zip(frame.loc[valid, "symbol"].astype(str), work_times, strict=True):
                key = (symbol, int(timestamp.value))
                if key in seen_keys:
                    duplicate_rows += 1
                seen_keys.add(key)
            observed_dates.update(pd.Timestamp(value).normalize() for value in work_times)
            partition_first = pd.Timestamp(work_times.min()).normalize()
            partition_latest = pd.Timestamp(work_times.max()).normalize()
            first = partition_first if first is None or partition_first < first else first
            latest = partition_latest if latest is None or partition_latest > latest else latest
        source_mismatch_rows += int((frame["source"].astype(str) != VNDIRECT_SOURCE).sum())
        symbol_mismatch_rows += int((frame["symbol"].astype(str) != VNDIRECT_SYMBOL).sum())
        del frame, numeric, times
        release_unused_memory()

    calendar_start = max(CALENDAR_ASSERTION_START, first) if first is not None else CALENDAR_ASSERTION_START
    expected_days: set[pd.Timestamp] = set()
    if calendar_start <= expected:
        for day in pd.date_range(calendar_start, expected, freq="D"):
            if is_trading_day(day.to_pydatetime()):
                expected_days.add(day.normalize())
    calendar_missing = sorted(expected_days - observed_dates)
    integrity_errors = (
        len(file_errors)
        + duplicate_rows
        + invalid_time_rows
        + invalid_numeric_rows
        + ohlc_bad_rows
        + negative_rows
        + weekend_rows
        + source_mismatch_rows
        + symbol_mismatch_rows
    )
    status = "pass" if files and integrity_errors == 0 and not calendar_missing and latest == expected else "fail"
    payload: dict[str, object] = {
        "dataset": "vn30f1m_vndirect_dchart_1d",
        "service": "phase_d_vn30f1m_vndirect_daily",
        "status": status,
        "files": len(files),
        "rows": rows,
        "first": first.isoformat() if first is not None else None,
        "latest": latest.isoformat() if latest is not None else None,
        "expected_latest": expected.isoformat(),
        "duplicate_rows": duplicate_rows,
        "invalid_time_rows": invalid_time_rows,
        "invalid_numeric_rows": invalid_numeric_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "negative_rows": negative_rows,
        "weekend_rows": weekend_rows,
        "source_mismatch_rows": source_mismatch_rows,
        "symbol_mismatch_rows": symbol_mismatch_rows,
        "calendar_assertion_start": calendar_start.isoformat(),
        "calendar_missing_trading_days": [day.date().isoformat() for day in calendar_missing[:100]],
        "calendar_missing_trading_day_count": len(calendar_missing),
        "pre_2024_continuity": "not_asserted: project VN holiday calendar is intentionally complete only from 2024 onward",
        "file_errors": file_errors,
        "validated_at": utc_now_iso(),
    }
    JsonState(PHASE_D_AUDIT_STATE).write(payload)
    return payload


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
    safe_end = last_closed_vn_daily()
    requested_end = pd.Timestamp(opts.end).normalize() if opts.end else safe_end
    end = min(requested_end, safe_end)
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
    if opts.audit_phase_d:
        audit = audit_vndirect_daily(expected_latest=end)
        payload["audit"] = audit
        if audit["status"] != "pass":
            Heartbeat("vn30f1m_vndirect").beat(status="error", error="phase_d_audit_failed")
            raise RuntimeError(f"VNDIRECT Phase D audit failed: {audit}")
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


def _minute_store() -> PartitionedParquetStore:
    return PartitionedParquetStore(["vn", "futures", "continuous", "1m"], partition="month")


def _minute_attrs(version: str) -> dict[str, str]:
    return {"symbol": VNDIRECT_SYMBOL, "source": VNDIRECT_SOURCE, "version": version}


def _minute_state() -> JsonState:
    return JsonState("vn_derivatives/vndirect_dchart_1m.json")


def last_closed_vn_minute(now=None) -> pd.Timestamp:
    """Return the latest minute that is closed before a VNDIRECT fetch."""

    local_now = now or vn_now()
    timestamp = pd.Timestamp(local_now)
    try:
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert("Asia/Ho_Chi_Minh").tz_localize(None)
    except (AttributeError, TypeError):
        pass
    return timestamp.floor("min") - ONE_MINUTE


def _prepare_minute_frame(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["time", "symbol", "open", "high", "low", "close", "volume", "source", "source_symbol", "quality_flags", "ingested_at"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    work = df.copy()
    times = pd.to_datetime(work["time"], errors="coerce")
    try:
        if times.dt.tz is not None:
            times = times.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass
    work["time"] = times.dt.floor("min")
    work["symbol"] = VNDIRECT_SYMBOL
    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["time", "open", "high", "low", "close", "volume"])
    if work.empty:
        return pd.DataFrame(columns=columns)
    invalid = (
        (work["high"] < work[["open", "close", "low"]].max(axis=1))
        | (work["low"] > work[["open", "close", "high"]].min(axis=1))
        | (work["volume"] < 0)
    )
    if invalid.any():
        raise ValueError(f"invalid VNDIRECT DChart minute rows: {int(invalid.sum())}")
    duplicate_rows = int(work.duplicated(subset=["symbol", "time"]).sum())
    if duplicate_rows:
        raise ValueError(f"duplicate VNDIRECT DChart minute rows: {duplicate_rows}")
    work["source"] = VNDIRECT_SOURCE
    work["source_symbol"] = VNDIRECT_SYMBOL
    work["quality_flags"] = "CONTINUOUS_ALIAS"
    if "ingested_at" not in work:
        work["ingested_at"] = utc_now_iso()
    return work[columns].sort_values(["symbol", "time"]).reset_index(drop=True)


def _minute_windows_descending(start: pd.Timestamp, end: pd.Timestamp, *, window_days: int) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    lower = pd.Timestamp(start).floor("min")
    cursor = pd.Timestamp(end).floor("min")
    windows: list[tuple[pd.Timestamp, pd.Timestamp, int]] = []
    while cursor >= lower:
        window_start = max(lower, cursor - pd.Timedelta(days=window_days) + ONE_MINUTE)
        windows.append((window_start, cursor, window_days))
        cursor = window_start - ONE_MINUTE
    return windows


def _split_minute_window(start: pd.Timestamp, end: pd.Timestamp, *, window_days: int, min_window_days: int) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    """Split an unavailable/truncated window before accepting a source floor."""

    if window_days <= min_window_days:
        return []
    next_days = max(min_window_days, window_days // 2)
    midpoint = (pd.Timestamp(start) + (pd.Timestamp(end) - pd.Timestamp(start)) / 2).floor("min")
    if midpoint <= start or midpoint >= end:
        return []
    # Descending order keeps the newest part first, which preserves a clear
    # source-retention boundary in state evidence.
    return [(midpoint + ONE_MINUTE, end, next_days), (start, midpoint, next_days)]


def _default_minute_start(options: VndirectMinuteOptions, *, historical: bool) -> pd.Timestamp:
    if options.start:
        return pd.Timestamp(options.start).floor("min")
    store = _minute_store()
    latest = store.latest_time(attrs=_minute_attrs(options.version), time_col="time")
    if latest is not None:
        return pd.Timestamp(latest).floor("min") - pd.Timedelta(minutes=max(options.overlap_minutes, 1))
    if historical:
        return pd.Timestamp("2017-08-10")
    return last_closed_vn_minute() - pd.Timedelta(days=max(options.window_days, 1))


def audit_vndirect_minute(*, version: str = "v1", expected_latest: pd.Timestamp | None = None) -> dict[str, object]:
    """Stream VNDIRECT minute partitions into durable, non-fabricating evidence.

    A continuous futures alias has overnight and lunch breaks, so minute gaps
    are measured and retained as source/session evidence instead of being
    filled or treated as synthetic candles.
    """

    store = _minute_store()
    files = store.files(_minute_attrs(version))
    required = ["time", "symbol", "open", "high", "low", "close", "volume", "source"]
    rows = duplicate_rows = invalid_time_rows = invalid_numeric_rows = 0
    ohlc_bad_rows = negative_rows = source_mismatch_rows = symbol_mismatch_rows = 0
    outside_market_window_rows = within_day_gap_count = max_within_day_gap_minutes = 0
    file_errors: list[str] = []
    seen_keys: set[tuple[str, int]] = set()
    first: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None

    for path in files:
        try:
            frame = read_partition_file(path, usecols=required)
        except Exception as exc:
            file_errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
            continue
        rows += int(len(frame))
        times = pd.to_datetime(frame["time"], errors="coerce")
        try:
            if times.dt.tz is not None:
                times = times.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass
        times = times.dt.floor("min")
        invalid_time_rows += int(times.isna().sum())
        numeric = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
        invalid_numeric_rows += int(numeric.isna().any(axis=1).sum())
        valid = times.notna() & numeric.notna().all(axis=1)
        if valid.any():
            work_times = times.loc[valid].sort_values().reset_index(drop=True)
            work_numeric = numeric.loc[valid]
            ohlc_bad_rows += int(
                (
                    (work_numeric["high"] < work_numeric[["open", "close", "low"]].max(axis=1))
                    | (work_numeric["low"] > work_numeric[["open", "close", "high"]].min(axis=1))
                ).sum()
            )
            negative_rows += int((work_numeric < 0).any(axis=1).sum())
            outside_market_window_rows += int(((work_times.dt.hour < 8) | (work_times.dt.hour >= 16)).sum())
            for symbol, timestamp in zip(frame.loc[valid, "symbol"].astype(str), times.loc[valid], strict=True):
                key = (symbol, int(timestamp.value))
                if key in seen_keys:
                    duplicate_rows += 1
                seen_keys.add(key)
            dates = work_times.dt.normalize()
            diffs = work_times.diff()
            same_day = dates.eq(dates.shift())
            gaps = diffs[same_day & diffs.gt(ONE_MINUTE)]
            within_day_gap_count += int(len(gaps))
            if not gaps.empty:
                max_within_day_gap_minutes = max(max_within_day_gap_minutes, int(gaps.max().total_seconds() // 60) - 1)
            partition_first = work_times.iloc[0]
            partition_latest = work_times.iloc[-1]
            first = partition_first if first is None or partition_first < first else first
            latest = partition_latest if latest is None or partition_latest > latest else latest
        source_mismatch_rows += int((frame["source"].astype(str) != VNDIRECT_SOURCE).sum())
        symbol_mismatch_rows += int((frame["symbol"].astype(str) != VNDIRECT_SYMBOL).sum())
        del frame, numeric, times
        release_unused_memory()

    expected = pd.Timestamp(expected_latest or last_closed_vn_minute()).floor("min")
    tail_lag_minutes = None if latest is None else max(0, int((expected - latest).total_seconds() // 60))
    integrity_errors = (
        len(file_errors)
        + duplicate_rows
        + invalid_time_rows
        + invalid_numeric_rows
        + ohlc_bad_rows
        + negative_rows
        + source_mismatch_rows
        + symbol_mismatch_rows
        + outside_market_window_rows
    )
    status = "pass" if files and first is not None and latest is not None and integrity_errors == 0 else "requires_repair"
    payload: dict[str, object] = {
        "dataset": "vn30f1m_vndirect_dchart_1m",
        "service": "phase_e_vn30f1m_vndirect_1m",
        "status": status,
        "files": len(files),
        "rows": rows,
        "first": first.isoformat() if first is not None else None,
        "latest": latest.isoformat() if latest is not None else None,
        "expected_latest": expected.isoformat(),
        "tail_lag_minutes": tail_lag_minutes,
        "duplicate_rows": duplicate_rows,
        "invalid_time_rows": invalid_time_rows,
        "invalid_numeric_rows": invalid_numeric_rows,
        "ohlc_bad_rows": ohlc_bad_rows,
        "negative_rows": negative_rows,
        "source_mismatch_rows": source_mismatch_rows,
        "symbol_mismatch_rows": symbol_mismatch_rows,
        "outside_market_window_rows": outside_market_window_rows,
        "within_day_gap_count": within_day_gap_count,
        "max_within_day_gap_minutes": max_within_day_gap_minutes,
        "continuity_note": "within-day gaps are preserved source/session evidence; no synthetic candles are written",
        "file_errors": file_errors,
        "validated_at": utc_now_iso(),
    }
    JsonState(PHASE_E_MINUTE_AUDIT_STATE).write(payload)
    return payload


def sync_vndirect_minute(options: VndirectMinuteOptions | None = None, *, historical: bool = True) -> dict[str, object]:
    opts = options or VndirectMinuteOptions()
    if opts.window_days < opts.min_window_days:
        raise ValueError("window_days must be greater than or equal to min_window_days")
    logger = setup_logging("vn30f1m_vndirect_minute")
    proof: dict[str, object] | None = None
    if opts.require_source_proof:
        proof = run_vndirect_probe(VndirectProbeOptions())
        if proof.get("production_gate") != "PASS":
            raise RuntimeError(f"VNDIRECT 1m source proof did not pass: {proof.get('gate_errors')}")

    store = _minute_store()
    state = _minute_state()
    manifest = state.read()
    manifest.setdefault("provider", VNDIRECT_SOURCE)
    manifest.setdefault("symbol", VNDIRECT_SYMBOL)
    manifest.setdefault("version", opts.version)
    manifest.setdefault("windows", {})

    start = _default_minute_start(opts, historical=historical)
    safe_end = last_closed_vn_minute()
    requested_end = pd.Timestamp(opts.end).floor("min") if opts.end else safe_end
    end = min(requested_end, safe_end)
    if start > end:
        payload = {"status": "ok", "reason": "already_current", "start": start.isoformat(), "end": end.isoformat(), "rows_written": 0, "updated_at": utc_now_iso()}
        manifest.update(payload)
        state.write(manifest)
        return payload

    provider = VndirectDChartProvider()
    queue = _minute_windows_descending(start, end, window_days=opts.window_days)
    total_rows_written = 0
    positive_windows = no_data_windows = split_windows = 0
    source_floor: dict[str, object] | None = None
    positive_seen = False
    windows_payload: list[dict[str, object]] = []

    while queue:
        window_start, window_end, window_days = queue.pop(0)
        key = f"{window_start.isoformat()}__{window_end.isoformat()}"
        logger.info("vndirect_dchart_minute_window_start start=%s end=%s window_days=%s", window_start, window_end, window_days)
        result = provider.fetch(start=window_start, end=window_end, resolution="1m")
        record: dict[str, object] = {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "window_days": window_days,
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
            parts = _split_minute_window(window_start, window_end, window_days=window_days, min_window_days=opts.min_window_days) if positive_seen else []
            if parts:
                record["status"] = "split_for_source_boundary"
                manifest["windows"][key] = record
                state.write(manifest)
                queue = parts + queue
                split_windows += len(parts)
                continue
            record["status"] = "source_unavailable"
            manifest["windows"][key] = record
            state.write(manifest)
            windows_payload.append(record)
            if not positive_seen:
                raise RuntimeError(f"VNDIRECT returned no current 1m data for {window_start} -> {window_end}")
            source_floor = {"start": window_start.isoformat(), "end": window_end.isoformat(), "confirmed_at_window_days": window_days}
            break
        if result.status != "success":
            record["status"] = "error"
            manifest["windows"][key] = record
            manifest["last_error"] = result.error or result.status
            manifest["updated_at"] = utc_now_iso()
            state.write(manifest)
            raise RuntimeError(f"VNDIRECT DChart minute {key} failed: {result.status} {result.error}")

        frame = _prepare_minute_frame(result.data)
        if frame.empty:
            raise RuntimeError(f"VNDIRECT DChart minute {key} returned success without usable rows")
        checksum = _frame_checksum(frame)
        append_result = store.append(
            frame,
            time_col="time",
            dedupe_cols=["symbol", "time"],
            attrs=_minute_attrs(opts.version),
            lock_name=f"vn30f1m_vndirect_dchart_1m/{opts.version}",
        )
        positive_seen = True
        positive_windows += 1
        total_rows_written += int(append_result["rows_written"])
        record.update(
            {
                "status": "completed",
                "checksum": checksum,
                "rows_written": int(append_result["rows_written"]),
                "latest_time": str(append_result["latest_time"]),
                "output_partition": f"storage/vn/futures/continuous/1m/symbol={VNDIRECT_SYMBOL}/source={VNDIRECT_SOURCE}/version={opts.version}/year={window_start.year:04d}/month={window_start.month:02d}/part.parquet",
                "last_error": None,
            }
        )
        manifest["windows"][key] = record
        manifest["latest_time"] = str(append_result["latest_time"])
        manifest["last_success_at"] = utc_now_iso()
        manifest["last_error"] = None
        manifest["updated_at"] = utc_now_iso()
        state.write(manifest)
        windows_payload.append(record)
        del frame
        release_unused_memory()

    payload: dict[str, object] = {
        "status": "ok",
        "provider": VNDIRECT_SOURCE,
        "symbol": VNDIRECT_SYMBOL,
        "version": opts.version,
        "resolution": "1m",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "positive_windows": positive_windows,
        "no_data_windows": no_data_windows,
        "split_windows": split_windows,
        "source_floor": source_floor,
        "rows_written": total_rows_written,
        "source_proof": proof,
        "windows": windows_payload,
        "updated_at": utc_now_iso(),
    }
    manifest.update({key: value for key, value in payload.items() if key != "windows"})
    state.write(manifest)
    if opts.audit_phase_e:
        audit = audit_vndirect_minute(version=opts.version, expected_latest=end)
        payload["audit"] = audit
        if audit["status"] != "pass":
            Heartbeat("vn30f1m_vndirect_1m").beat(status="error", error="phase_e_audit_requires_repair")
            raise RuntimeError(f"VNDIRECT Phase E minute audit failed: {audit}")
    Heartbeat("vn30f1m_vndirect_1m").beat(
        rows_written=total_rows_written,
        latest_time=manifest.get("latest_time"),
        source=VNDIRECT_SOURCE,
    )
    release_unused_memory()
    return payload


def live_vndirect_minute(*, version: str = "v1", overlap_minutes: int = 10, sleep_seconds: int = 60) -> None:
    if sleep_seconds <= 0:
        raise ValueError("sleep_seconds must be positive")
    logger = setup_logging("vn30f1m_vndirect_minute_live")
    heartbeat = Heartbeat("vn30f1m_vndirect_1m")
    while True:
        try:
            result = sync_vndirect_minute(
                VndirectMinuteOptions(version=version, overlap_minutes=overlap_minutes),
                historical=False,
            )
            logger.info("vndirect_dchart_minute_sync_done status=%s rows_written=%s", result.get("status"), result.get("rows_written"))
            heartbeat.beat(status=str(result.get("status", "ok")), rows_written=result.get("rows_written"))
        except Exception as exc:
            logger.exception("vndirect_dchart_minute_sync_failed")
            heartbeat.beat(status="error", error=str(exc))
        sleep_with_heartbeat(
            heartbeat,
            sleep_seconds,
            source=VNDIRECT_SOURCE,
        )
