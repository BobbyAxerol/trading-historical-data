from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from collectors.common.calendar_vn import is_trading_day, vn_now
from collectors.common.config import load_yaml
from collectors.common.env import data_root
from collectors.common.locks import FileLock
from collectors.common.logging import setup_logging
from collectors.common.manifest import Heartbeat, JsonState, utc_now_iso
from collectors.common.storage import PartitionedParquetStore, normalize_datetime, read_partition_file, release_unused_memory
from collectors.vn_daily_matrix import build_matrix
from collectors.vn_derivatives.contracts import BackfillOptions, backfill_contracts, options_from_config
from collectors.vn_derivatives.instruments import instrument_dimension_path
from collectors.vn_derivatives.symbols import VN30FutureContract, contract_for_month, generate_contracts
from collectors.vn_derivatives.validate import validate_storage

ROLL_COLUMNS = [
    "trading_date",
    "series",
    "old_instrument_id",
    "new_instrument_id",
    "roll_reason",
    "decision_date",
    "old_close",
    "new_close",
    "roll_gap",
    "roll_ratio",
]
CONTINUOUS_COLUMNS = [
    "time",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "active_instrument_id",
    "roll_flag",
    "roll_gap",
    "roll_ratio",
    "source",
    "quality_flags",
    "ingested_at",
]


@dataclass(frozen=True)
class ContinuousOptions:
    version: str = "v1"
    start: str = "2017-08-10"
    end: str | None = None
    resolutions: tuple[str, ...] = ("1m", "1d")
    series: tuple[str, ...] = ("VN30F1M", "VN30F1M_TRADE")
    volume_confirmation_days: int = 2
    hard_roll_sessions_before_expiry: int = 1


def rolls_path(version: str = "v1") -> Path:
    return data_root() / "vn" / "futures" / "rolls" / f"version={version}" / "rolls.parquet"


def options_from_config_continuous(
    *,
    version: str = "v1",
    start: str | None = None,
    end: str | None = None,
    resolutions: Iterable[str] | None = None,
    series: Iterable[str] | None = None,
) -> ContinuousOptions:
    config = load_yaml("vn_derivatives.yml")
    continuous = config.get("continuous", {})
    configured_series = [continuous.get("calendar_series", "VN30F1M"), continuous.get("tradable_series", "VN30F1M_TRADE")]
    return ContinuousOptions(
        version=version or config.get("dataset_version", "v1"),
        start=start or config.get("backfill_start", "2017-08-10"),
        end=end,
        resolutions=tuple(resolutions or config.get("resolutions", ["1m", "1d"])),
        series=tuple(series or configured_series),
        volume_confirmation_days=int(continuous.get("volume_confirmation_days", 2)),
        hard_roll_sessions_before_expiry=int(continuous.get("hard_roll_sessions_before_expiry", 1)),
    )


def _trading_dates(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    if end < start:
        return []
    result: list[pd.Timestamp] = []
    current = start.normalize()
    while current <= end.normalize():
        if is_trading_day(current.to_pydatetime()):
            result.append(current)
        current += pd.Timedelta(days=1)
    return result


def _previous_trading_date(day: pd.Timestamp, sessions: int = 1) -> pd.Timestamp:
    remaining = max(int(sessions), 0)
    current = day.normalize()
    while remaining:
        current -= pd.Timedelta(days=1)
        if is_trading_day(current.to_pydatetime()):
            remaining -= 1
    return current


def _contract_store(resolution: str) -> PartitionedParquetStore:
    partition = "month" if resolution == "1m" else "year"
    return PartitionedParquetStore(["vn", "futures", "contracts", resolution], partition=partition)


def _continuous_store(resolution: str) -> PartitionedParquetStore:
    partition = "month" if resolution == "1m" else "year"
    return PartitionedParquetStore(["vn", "futures", "continuous", resolution], partition=partition)


def _read_contract(contract: VN30FutureContract, resolution: str, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None) -> pd.DataFrame:
    store = _contract_store(resolution)
    frames = []
    for path in store.files({"symbol": contract.canonical_symbol}):
        try:
            df = read_partition_file(path)
        except Exception:
            continue
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "time" not in df.columns:
        return pd.DataFrame()
    df["time"] = normalize_datetime(df["time"])
    df = df.dropna(subset=["time"])
    if start is not None:
        df = df[df["time"] >= start]
    if end is not None:
        df = df[df["time"] <= end]
    return df.sort_values("time").reset_index(drop=True)


def _daily_contract(contract: VN30FutureContract) -> pd.DataFrame:
    daily = _read_contract(contract, "1d")
    if daily.empty:
        return daily
    daily["time"] = pd.to_datetime(daily["time"], errors="coerce").dt.normalize()
    return daily.dropna(subset=["time"]).drop_duplicates(subset=["time"], keep="last")


def _daily_close(contract: VN30FutureContract, day: pd.Timestamp) -> float | None:
    df = _daily_contract(contract)
    if df.empty:
        return None
    row = df.loc[df["time"] == day.normalize()]
    if row.empty:
        return None
    value = pd.to_numeric(row["close"], errors="coerce").dropna()
    return None if value.empty else float(value.iloc[-1])


def _daily_volume_map(contracts: Iterable[VN30FutureContract]) -> dict[int, dict[pd.Timestamp, float]]:
    result: dict[int, dict[pd.Timestamp, float]] = {}
    for contract in contracts:
        df = _daily_contract(contract)
        if df.empty:
            result[contract.instrument_id] = {}
            continue
        volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
        result[contract.instrument_id] = dict(zip(pd.to_datetime(df["time"]).dt.normalize(), volume.astype(float)))
        del df
    release_unused_memory()
    return result


def _front_contract_for_day(contracts: list[VN30FutureContract], day: pd.Timestamp) -> VN30FutureContract | None:
    for contract in contracts:
        if pd.Timestamp(contract.expiry_date) >= day.normalize():
            return contract
    return contracts[-1] if contracts else None


def _next_contract(contracts: list[VN30FutureContract], current: VN30FutureContract) -> VN30FutureContract | None:
    for idx, contract in enumerate(contracts):
        if contract.instrument_id == current.instrument_id and idx + 1 < len(contracts):
            return contracts[idx + 1]
    return None


def _event_row(
    *,
    trading_date: pd.Timestamp,
    series: str,
    old_contract: VN30FutureContract | None,
    new_contract: VN30FutureContract,
    roll_reason: str,
    decision_date: pd.Timestamp,
) -> dict[str, object]:
    old_close = _daily_close(old_contract, decision_date) if old_contract is not None else None
    new_close = _daily_close(new_contract, decision_date)
    roll_gap = None
    roll_ratio = None
    if old_close is not None and new_close is not None:
        roll_gap = new_close - old_close
        roll_ratio = new_close / old_close if old_close else None
    return {
        "trading_date": trading_date.normalize(),
        "series": series,
        "old_instrument_id": old_contract.instrument_id if old_contract else pd.NA,
        "new_instrument_id": new_contract.instrument_id,
        "roll_reason": roll_reason,
        "decision_date": decision_date.normalize(),
        "old_close": old_close,
        "new_close": new_close,
        "roll_gap": roll_gap,
        "roll_ratio": roll_ratio,
    }


def _calendar_events(contracts: list[VN30FutureContract], trading_dates: list[pd.Timestamp], series: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    active: VN30FutureContract | None = None
    for day in trading_dates:
        chosen = _front_contract_for_day(contracts, day)
        if chosen is None:
            continue
        if active is None:
            events.append(_event_row(trading_date=day, series=series, old_contract=None, new_contract=chosen, roll_reason="initial", decision_date=day))
            active = chosen
        elif chosen.instrument_id != active.instrument_id:
            events.append(_event_row(trading_date=day, series=series, old_contract=active, new_contract=chosen, roll_reason="calendar_expiry", decision_date=day - pd.Timedelta(days=1)))
            active = chosen
    return events


def _tradable_events(options: ContinuousOptions, contracts: list[VN30FutureContract], trading_dates: list[pd.Timestamp], series: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    active: VN30FutureContract | None = None
    volumes = _daily_volume_map(contracts)
    closed_days: list[pd.Timestamp] = []

    for day in trading_dates:
        if active is None:
            active = _front_contract_for_day(contracts, day)
            if active is not None:
                events.append(_event_row(trading_date=day, series=series, old_contract=None, new_contract=active, roll_reason="initial", decision_date=day))
            closed_days.append(day)
            continue
        next_contract = _next_contract(contracts, active)
        if next_contract is None:
            closed_days.append(day)
            continue

        reason: str | None = None
        hard_roll_day = _previous_trading_date(pd.Timestamp(active.expiry_date), options.hard_roll_sessions_before_expiry)
        if day >= hard_roll_day:
            reason = "hard_roll_before_expiry"
        elif len(closed_days) >= options.volume_confirmation_days:
            prior = closed_days[-options.volume_confirmation_days :]
            if all(volumes.get(next_contract.instrument_id, {}).get(d, 0.0) > volumes.get(active.instrument_id, {}).get(d, 0.0) for d in prior):
                reason = "volume_confirmation"

        if reason:
            decision_date = closed_days[-1] if closed_days else day
            events.append(_event_row(trading_date=day, series=series, old_contract=active, new_contract=next_contract, roll_reason=reason, decision_date=decision_date))
            active = next_contract
        closed_days.append(day)
    return events


def build_roll_table(options: ContinuousOptions) -> dict[str, object]:
    start = pd.Timestamp(options.start).normalize()
    end = pd.Timestamp(options.end).normalize() if options.end else pd.Timestamp.utcnow().tz_localize(None).normalize()
    contract_end = (end + pd.DateOffset(months=3)).date()
    contracts = generate_contracts(start=start.date(), end=contract_end)
    trading_dates = _trading_dates(start, end)

    rows: list[dict[str, object]] = []
    if "VN30F1M" in options.series:
        rows.extend(_calendar_events(contracts, trading_dates, "VN30F1M"))
    if "VN30F1M_TRADE" in options.series:
        rows.extend(_tradable_events(options, contracts, trading_dates, "VN30F1M_TRADE"))

    frame = pd.DataFrame(rows, columns=ROLL_COLUMNS)
    if not frame.empty:
        frame["trading_date"] = pd.to_datetime(frame["trading_date"], errors="coerce").dt.normalize()
        frame["decision_date"] = pd.to_datetime(frame["decision_date"], errors="coerce").dt.normalize()
        frame = frame.sort_values(["series", "trading_date"]).reset_index(drop=True)

    path = rolls_path(options.version)
    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")
        if not existing.empty:
            existing["trading_date"] = pd.to_datetime(existing["trading_date"], errors="coerce").dt.normalize()
            keep = (~existing["series"].isin(options.series)) | (existing["trading_date"] < start) | (existing["trading_date"] > end)
            frame = pd.concat([existing.loc[keep, ROLL_COLUMNS], frame], ignore_index=True)
            frame = frame.sort_values(["series", "trading_date"]).drop_duplicates(subset=["series", "trading_date"], keep="last").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    frame.to_parquet(tmp, index=False, engine="pyarrow", compression="zstd")
    tmp.replace(path)
    JsonState(f"vn_derivatives/continuous_{options.version}.json").write(
        {
            "status": "rolls_built",
            "rolls_path": str(path),
            "rolls": int(len(frame)),
            "series": list(options.series),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "updated_at": utc_now_iso(),
        }
    )
    release_unused_memory()
    return {"status": "ok", "path": str(path), "rolls": int(len(frame)), "series": list(options.series)}


def read_roll_table(version: str = "v1") -> pd.DataFrame:
    path = rolls_path(version)
    if not path.exists():
        return pd.DataFrame(columns=ROLL_COLUMNS)
    df = pd.read_parquet(path, engine="pyarrow")
    if not df.empty:
        df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce").dt.normalize()
        df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce").dt.normalize()
    return df


def active_map_from_rolls(rolls: pd.DataFrame, *, series: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[pd.Timestamp, dict[str, object]]:
    selected = rolls.loc[rolls["series"] == series].copy()
    if selected.empty:
        return {}
    selected = selected.sort_values("trading_date")
    trading_dates = _trading_dates(start.normalize(), end.normalize())
    active: dict[str, object] | None = None
    event_by_day = {pd.Timestamp(row.trading_date).normalize(): row for row in selected.itertuples(index=False)}
    result: dict[pd.Timestamp, dict[str, object]] = {}
    for day in trading_dates:
        event = event_by_day.get(day)
        if event is not None:
            active = {
                "instrument_id": int(event.new_instrument_id),
                "roll_flag": event.roll_reason != "initial",
                "roll_gap": event.roll_gap,
                "roll_ratio": event.roll_ratio,
                "roll_reason": event.roll_reason,
            }
        if active is not None:
            result[day] = dict(active)
            if event is None:
                result[day]["roll_flag"] = False
                result[day]["roll_gap"] = pd.NA
                result[day]["roll_ratio"] = pd.NA
    return result


def _contracts_by_id(start: str, end: str | None) -> dict[int, VN30FutureContract]:
    contract_end = (pd.Timestamp(end) + pd.DateOffset(months=3)).date() if end else None
    return {contract.instrument_id: contract for contract in generate_contracts(start=start, end=contract_end)}


def _roll_lookup(rolls: pd.DataFrame, series: str) -> dict[pd.Timestamp, dict[str, object]]:
    selected = rolls.loc[(rolls["series"] == series) & (rolls["roll_reason"] != "initial")]
    return {
        pd.Timestamp(row.trading_date).normalize(): {
            "roll_flag": True,
            "roll_gap": row.roll_gap,
            "roll_ratio": row.roll_ratio,
            "roll_reason": row.roll_reason,
        }
        for row in selected.itertuples(index=False)
    }


def _validate_continuous_frame(df: pd.DataFrame, *, resolution: str) -> list[str]:
    errors: list[str] = []
    if df.empty:
        return errors
    required = set(CONTINUOUS_COLUMNS)
    missing = sorted(required - set(df.columns))
    if missing:
        errors.append(f"missing columns: {missing}")
        return errors
    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    if work["time"].isna().any():
        errors.append("invalid time values")
    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    if work[["open", "high", "low", "close"]].isna().any(axis=1).any():
        errors.append("null OHLC values")
    if (work["high"] < work[["open", "close", "low"]].max(axis=1)).any():
        errors.append("invalid high bounds")
    if (work["low"] > work[["open", "close", "high"]].min(axis=1)).any():
        errors.append("invalid low bounds")
    if (work["volume"] < 0).any():
        errors.append("negative volume")
    if work.duplicated(subset=["symbol", "time"]).any():
        errors.append("duplicate symbol/time")
    if resolution == "1m":
        ids_per_day = work.assign(_date=work["time"].dt.normalize()).groupby(["symbol", "_date"])["active_instrument_id"].nunique()
        mixed = int((ids_per_day > 1).sum())
        if mixed:
            errors.append(f"{mixed} trading days mix multiple active contracts")
    return errors


def _append_continuous(df: pd.DataFrame, *, series: str, version: str, resolution: str) -> dict[str, object]:
    if df.empty:
        return {"rows_written": 0, "latest_time": None}
    store = _continuous_store(resolution)
    return store.append(
        df,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": series, "version": version},
        lock_name=f"vn_derivatives_continuous/{resolution}/{series}/{version}",
    )


def _build_series_resolution(
    *,
    series: str,
    resolution: str,
    options: ContinuousOptions,
    rolls: pd.DataFrame,
    contracts_by_id: dict[int, VN30FutureContract],
) -> dict[str, object]:
    start = pd.Timestamp(options.start).normalize()
    end = pd.Timestamp(options.end).normalize() if options.end else pd.Timestamp.utcnow().tz_localize(None).normalize()
    active = active_map_from_rolls(rolls, series=series, start=start, end=end)
    if not active:
        return {"series": series, "resolution": resolution, "rows_written": 0, "days": 0, "status": "empty_rolls"}

    frames_written = 0
    rows_written = 0
    validation_errors: list[str] = []
    dates_by_instrument: dict[int, list[pd.Timestamp]] = {}
    for day, meta in active.items():
        dates_by_instrument.setdefault(int(meta["instrument_id"]), []).append(day)
    roll_meta = _roll_lookup(rolls, series)

    for instrument_id, dates in sorted(dates_by_instrument.items()):
        contract = contracts_by_id.get(instrument_id)
        if contract is None:
            continue
        date_set = {day.normalize() for day in dates}
        frame = _read_contract(contract, resolution, min(date_set), max(date_set) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1))
        if frame.empty:
            continue
        frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
        frame["_date"] = frame["time"].dt.normalize()
        frame = frame[frame["_date"].isin(date_set)].copy()
        if frame.empty:
            continue
        frame["symbol"] = series
        frame["active_instrument_id"] = instrument_id
        frame["roll_flag"] = frame["_date"].isin(roll_meta)
        frame["roll_gap"] = frame["_date"].map(lambda d: roll_meta.get(d, {}).get("roll_gap", pd.NA))
        frame["roll_ratio"] = frame["_date"].map(lambda d: roll_meta.get(d, {}).get("roll_ratio", pd.NA))
        source = frame["source"].astype(str) if "source" in frame.columns else pd.Series(["unknown"] * len(frame), index=frame.index)
        frame["source"] = "continuous_rebuilt"
        series_flag = "CALENDAR_FRONT_MONTH" if series == "VN30F1M" else "TRADABLE_LIQUIDITY_ROLL"
        frame["quality_flags"] = series_flag + "|contract_source=" + source.fillna("unknown")
        frame["ingested_at"] = utc_now_iso()
        frame = frame[CONTINUOUS_COLUMNS].sort_values("time").drop_duplicates(subset=["symbol", "time"], keep="last")
        errors = _validate_continuous_frame(frame, resolution=resolution)
        if errors:
            validation_errors.extend([f"{series} {resolution} {contract.canonical_symbol}: {error}" for error in errors])
            continue
        result = _append_continuous(frame, series=series, version=options.version, resolution=resolution)
        rows_written += int(result["rows_written"] or 0)
        frames_written += 1
        del frame
        release_unused_memory()

    if validation_errors:
        raise RuntimeError(f"Continuous validation failed: {validation_errors[:5]}")
    return {"series": series, "resolution": resolution, "rows_written": rows_written, "contract_groups": frames_written, "days": len(active), "status": "ok"}


def build_continuous(options: ContinuousOptions) -> dict[str, object]:
    logger = setup_logging("vn_derivatives_continuous")
    roll_result = build_roll_table(options)
    rolls = read_roll_table(options.version)
    contracts_by_id = _contracts_by_id(options.start, options.end)
    results = []
    rows_written = 0
    for series in options.series:
        for resolution in options.resolutions:
            logger.info("vn_derivatives_continuous_build series=%s resolution=%s start=%s end=%s", series, resolution, options.start, options.end)
            result = _build_series_resolution(series=series, resolution=resolution, options=options, rolls=rolls, contracts_by_id=contracts_by_id)
            results.append(result)
            rows_written += int(result.get("rows_written", 0) or 0)
    state = JsonState(f"vn_derivatives/continuous_{options.version}.json")
    state.write(
        {
            "status": "ok",
            "roll_result": roll_result,
            "results": results,
            "rows_written": rows_written,
            "updated_at": utc_now_iso(),
        }
    )
    release_unused_memory()
    return {"status": "ok", "rolls": roll_result, "results": results, "rows_written": rows_written}


def validate_continuous_storage(*, version: str = "v1", resolutions: Iterable[str] | None = None, series: Iterable[str] | None = None) -> dict[str, object]:
    selected_resolutions = list(resolutions or ["1m", "1d"])
    selected_series = list(series or ["VN30F1M", "VN30F1M_TRADE"])
    issues: list[str] = []
    files = 0
    rows = 0
    for resolution in selected_resolutions:
        store = _continuous_store(resolution)
        for symbol in selected_series:
            for path in store.files({"symbol": symbol, "version": version}):
                files += 1
                df = read_partition_file(path)
                rows += len(df)
                issues.extend(f"{path}: {error}" for error in _validate_continuous_frame(df, resolution=resolution))
                del df
                release_unused_memory()
    payload = {
        "status": "ok" if not issues else "error",
        "dataset": "vn_derivatives_continuous",
        "version": version,
        "files": files,
        "rows": rows,
        "issues": issues,
        "updated_at": utc_now_iso(),
    }
    JsonState(f"vn_derivatives/continuous_validation_{version}.json").write(payload)
    return payload


def compare_provider_alias(*, version: str = "v1", series: str = "VN30F1M") -> dict[str, object]:
    continuous_root = data_root() / "vn" / "futures" / "continuous" / "1d"
    provider_root = data_root() / "vn" / "futures" / "1d"
    cont = _read_symbol_like(continuous_root, series, version=version)
    provider = _read_symbol_like(provider_root, series, version=None)
    if cont.empty or provider.empty:
        payload = {"status": "skipped", "reason": "missing continuous or provider alias daily data", "updated_at": utc_now_iso()}
        JsonState(f"vn_derivatives/provider_parity_{version}.json").write(payload)
        return payload
    merged = cont[["time", "close", "volume", "roll_flag"]].merge(provider[["time", "close", "volume"]], on="time", suffixes=("_continuous", "_provider"))
    if merged.empty:
        payload = {"status": "skipped", "reason": "no overlap", "updated_at": utc_now_iso()}
        JsonState(f"vn_derivatives/provider_parity_{version}.json").write(payload)
        return payload
    merged["ret_continuous"] = pd.to_numeric(merged["close_continuous"], errors="coerce").pct_change()
    merged["ret_provider"] = pd.to_numeric(merged["close_provider"], errors="coerce").pct_change()
    non_roll = merged[~merged["roll_flag"].fillna(False)]
    corr = non_roll[["ret_continuous", "ret_provider"]].corr().iloc[0, 1] if len(non_roll) >= 3 else pd.NA
    close_diff = (pd.to_numeric(non_roll["close_continuous"], errors="coerce") - pd.to_numeric(non_roll["close_provider"], errors="coerce")).abs()
    payload = {
        "status": "ok",
        "overlap_start": merged["time"].min().isoformat(),
        "overlap_end": merged["time"].max().isoformat(),
        "overlap_rows": int(len(merged)),
        "daily_return_correlation_ex_roll": None if pd.isna(corr) else float(corr),
        "median_non_roll_close_difference": None if close_diff.empty else float(close_diff.median()),
        "largest_mismatch_dates": [
            {"time": row.time.isoformat(), "close_diff": float(row.close_diff)}
            for row in non_roll.assign(close_diff=close_diff).sort_values("close_diff", ascending=False).head(10).itertuples(index=False)
        ],
        "updated_at": utc_now_iso(),
    }
    JsonState(f"vn_derivatives/provider_parity_{version}.json").write(payload)
    return payload


def _read_symbol_like(root: Path, symbol: str, *, version: str | None) -> pd.DataFrame:
    symbol_root = root / f"symbol={symbol}"
    if version is not None:
        symbol_root = symbol_root / f"version={version}"
    if not symbol_root.exists():
        return pd.DataFrame()
    frames = []
    for path in sorted(symbol_root.rglob("part.parquet")):
        try:
            frames.append(pd.read_parquet(path, engine="pyarrow"))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "time" not in df.columns:
        return pd.DataFrame()
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.normalize()
    return df.dropna(subset=["time"]).drop_duplicates(subset=["time"], keep="last").sort_values("time")


def update_daily_matrix_from_continuous(*, start_date: str | None = None, end_date: str | None = None) -> dict[str, object]:
    return build_matrix(start_date=start_date, end_date=end_date)


def sync_once(*, version: str = "v1", lookback_days: int | None = None, schedule_matrix: bool = True) -> dict[str, object]:
    config = load_yaml("vn_derivatives.yml")
    lookback = int(lookback_days or config.get("requests", {}).get("daily_sync_lookback_days", 45))
    end = vn_now().date().isoformat()
    start = (vn_now() - timedelta(days=lookback)).date().isoformat()
    backfill_options = options_from_config(
        version=version,
        start=start,
        end=end,
        resolutions=("1m", "1d"),
        skip_provider_errors=True,
        complete_empty_windows=False,
    )
    backfill_result = backfill_contracts(backfill_options)
    contract_validation = validate_storage(version=version, resolutions=["1m", "1d"])
    continuous_options = options_from_config_continuous(version=version, start=start, end=end, resolutions=("1m", "1d"))
    continuous_result = build_continuous(continuous_options)
    continuous_validation = validate_continuous_storage(version=version)
    parity = compare_provider_alias(version=version)
    matrix_result = update_daily_matrix_from_continuous() if schedule_matrix else {"status": "skipped"}
    status = "warning" if backfill_result.get("provider_errors") or backfill_result.get("status") == "error" else "ok"
    return {
        "status": status,
        "version": version,
        "backfill": backfill_result,
        "contract_validation": contract_validation,
        "continuous": continuous_result,
        "continuous_validation": continuous_validation,
        "provider_parity": parity,
        "matrix": matrix_result,
        "updated_at": utc_now_iso(),
    }


def live(*, version: str = "v1", schedule: str = "16:30", lookback_days: int | None = None) -> None:
    logger = setup_logging("vn_derivatives_live")
    heartbeat = Heartbeat("vn_derivatives")
    state = JsonState("vn_derivatives/live_schedule.json")
    schedule_state = state.read()
    last_run_date = schedule_state.get("last_run_date")
    while True:
        now = vn_now()
        hh, mm = [int(item) for item in schedule.split(":")]
        due = last_run_date != now.strftime("%Y-%m-%d") and (now.hour > hh or (now.hour == hh and now.minute >= mm))
        if due:
            try:
                result = sync_once(version=version, lookback_days=lookback_days)
                logger.info("vn_derivatives_sync_once_done status=%s rows_written=%s", result.get("status"), result.get("continuous", {}).get("rows_written"))
                heartbeat.beat(status=str(result.get("status", "ok")))
                last_run_date = now.strftime("%Y-%m-%d")
                state.write({"last_run_date": last_run_date, "updated_at": utc_now_iso(), "version": version})
            except Exception as exc:
                logger.exception("vn_derivatives_sync_once_failed")
                heartbeat.beat(status="error", error=str(exc))
        time.sleep(300)
