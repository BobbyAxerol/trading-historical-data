from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from collectors.common.config import load_yaml
from collectors.common.env import state_root
from collectors.common.locks import FileLock
from collectors.common.logging import setup_logging
from collectors.common.manifest import JsonState, utc_now_iso
from collectors.common.storage import PartitionedParquetStore, normalize_datetime, read_partition_file, release_unused_memory, write_partition_file
from collectors.providers import dnse_derivatives, kbs_derivatives
from collectors.vn_derivatives.instruments import build_initial_instrument_dimension, write_instrument_dimension
from collectors.vn_derivatives.symbols import (
    MARKET_START,
    VN30FutureContract,
    contract_for_month,
    generate_contracts,
    parse_canonical_symbol,
    provider_symbol_candidates,
)
from collectors.vn_derivatives.validate import ValidationIssue, validate_contract_frame

SOURCE_KBS = "kbs"
SOURCE_DNSE = "dnse"
SOURCE_AGGREGATED_1M = "aggregated_1m"
SOURCE_PRIORITY = {SOURCE_KBS: 1, SOURCE_DNSE: 2, SOURCE_AGGREGATED_1M: 3}
CONTRACT_COLUMNS = ["time", "instrument_id", "open", "high", "low", "close", "volume", "source", "quality_flags", "ingested_at"]


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    provider_symbol: str | None
    rows: pd.DataFrame
    request_success: bool
    empty_confirmed: bool
    error: str | None = None


@dataclass(frozen=True)
class BackfillOptions:
    version: str = "v1"
    start: str = "2017-08-10"
    end: str | None = None
    resolutions: tuple[str, ...] = ("1m", "1d")
    symbols: tuple[str, ...] | None = None
    max_contracts: int | None = None
    max_windows: int | None = None
    kbs_1m_window_days: int = 7
    dnse_1m_window_days: int = 5
    daily_window_days: int = 365
    min_1m_bars_for_daily: int = 200
    sleep_seconds: float = 0.0
    skip_provider_errors: bool = False
    complete_empty_windows: bool = True


Fetcher = Callable[[VN30FutureContract, str, str, pd.Timestamp, pd.Timestamp], ProviderResult]


def _manifest_path(resolution: str) -> str:
    return f"vn_derivatives/contracts_{resolution}.json"


def _read_manifest(resolution: str) -> dict[str, object]:
    payload = JsonState(_manifest_path(resolution)).read()
    payload.setdefault("dataset", f"vn_derivatives_contracts_{resolution}")
    payload.setdefault("symbols", {})
    return payload


def _write_manifest(resolution: str, payload: dict[str, object]) -> None:
    JsonState(_manifest_path(resolution)).write(payload)


def _update_manifest(resolution: str, symbol: str, **values: object) -> None:
    payload = _read_manifest(resolution)
    symbols = payload.setdefault("symbols", {})
    current = dict(symbols.get(symbol, {}))  # type: ignore[union-attr]
    current.update(values)
    current["updated_at"] = utc_now_iso()
    symbols[symbol] = current  # type: ignore[index]
    _write_manifest(resolution, payload)


def _window_days(options: BackfillOptions, resolution: str, provider: str) -> int:
    if resolution == "1d":
        return options.daily_window_days
    return options.kbs_1m_window_days if provider == SOURCE_KBS else options.dnse_1m_window_days


def _contract_range(contract: VN30FutureContract, options: BackfillOptions) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = max(pd.Timestamp(options.start), pd.Timestamp(MARKET_START), pd.Timestamp(contract.expiry_date) - pd.Timedelta(days=270))
    end = pd.Timestamp(contract.expiry_date) + pd.Timedelta(days=1)
    if options.end:
        end = min(end, pd.Timestamp(options.end))
    return start.normalize(), end.normalize()


def _windows(start: pd.Timestamp, end: pd.Timestamp, days: int) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if end < start:
        return []
    result = []
    current = start
    while current <= end:
        stop = min(current + pd.Timedelta(days=max(days, 1)) - pd.Timedelta(seconds=1), end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
        result.append((current, stop))
        current = stop + pd.Timedelta(seconds=1)
    return result


def _default_fetcher(contract: VN30FutureContract, provider: str, resolution: str, start: pd.Timestamp, end: pd.Timestamp) -> ProviderResult:
    errors: list[str] = []
    for _, provider_symbol in provider_symbol_candidates(contract):
        try:
            if provider == SOURCE_KBS:
                df = kbs_derivatives.fetch_ohlc(provider_symbol, start, end, "1m" if resolution == "1m" else "1d")
            elif provider == SOURCE_DNSE:
                dnse_resolutions = ["1"] if resolution == "1m" else ["1D", "D", "day"]
                dnse_errors = []
                dnse_empty: pd.DataFrame | None = None
                for dnse_resolution in dnse_resolutions:
                    try:
                        df = dnse_derivatives.fetch_ohlc(provider_symbol, start, end, dnse_resolution, asset_type="derivative")
                        if not df.empty:
                            break
                        dnse_empty = df
                    except Exception as exc:
                        dnse_errors.append(f"{dnse_resolution}: {type(exc).__name__}: {exc}")
                else:
                    if dnse_empty is not None:
                        df = dnse_empty
                    elif resolution == "1d" and dnse_errors and all("400 Client Error" in error for error in dnse_errors):
                        df = pd.DataFrame()
                    else:
                        raise RuntimeError("; ".join(dnse_errors))
            else:
                raise ValueError(f"Unsupported provider: {provider}")
            return ProviderResult(provider=provider, provider_symbol=provider_symbol, rows=df, request_success=True, empty_confirmed=df.empty)
        except Exception as exc:
            errors.append(f"{provider_symbol}: {type(exc).__name__}: {exc}")
    return ProviderResult(provider=provider, provider_symbol=None, rows=pd.DataFrame(), request_success=False, empty_confirmed=False, error="; ".join(errors))


def _normalize_contract_rows(df: pd.DataFrame, *, contract: VN30FutureContract, source: str, fallback: bool = False) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)
    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    try:
        if work["time"].dt.tz is not None:
            work["time"] = work["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except Exception:
        pass
    work = work.dropna(subset=["time"])
    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["open", "high", "low", "close"])
    work = work[work["volume"].fillna(0) >= 0]
    if work.empty:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)
    work["instrument_id"] = contract.instrument_id
    work["source"] = source
    flags = []
    if fallback:
        flags.append("DNSE_FALLBACK")
    if source == SOURCE_KBS:
        flags.append("KBS_PRIMARY")
    work["quality_flags"] = "|".join(flags)
    work["ingested_at"] = utc_now_iso()
    return work[CONTRACT_COLUMNS].sort_values("time").drop_duplicates(subset=["instrument_id", "time"], keep="last").reset_index(drop=True)


def merge_provider_rows(contract: VN30FutureContract, kbs: ProviderResult, dnse: ProviderResult) -> tuple[pd.DataFrame, dict[str, object]]:
    kbs_rows = _normalize_contract_rows(kbs.rows, contract=contract, source=SOURCE_KBS) if kbs.request_success else pd.DataFrame(columns=CONTRACT_COLUMNS)
    dnse_rows = _normalize_contract_rows(dnse.rows, contract=contract, source=SOURCE_DNSE, fallback=True) if dnse.request_success else pd.DataFrame(columns=CONTRACT_COLUMNS)
    stats: dict[str, object] = {
        "kbs_rows": int(len(kbs_rows)),
        "dnse_rows": int(len(dnse_rows)),
        "dnse_fallback_rows": 0,
        "provider_errors": [error for error in [kbs.error, dnse.error] if error],
    }
    if kbs_rows.empty and dnse_rows.empty:
        return pd.DataFrame(columns=CONTRACT_COLUMNS), stats
    if kbs_rows.empty:
        stats["dnse_fallback_rows"] = int(len(dnse_rows))
        return dnse_rows, stats
    if dnse_rows.empty:
        return kbs_rows, stats

    kbs_times = set(pd.to_datetime(kbs_rows["time"]))
    fallback = dnse_rows[~pd.to_datetime(dnse_rows["time"]).isin(kbs_times)].copy()
    stats["dnse_fallback_rows"] = int(len(fallback))
    merged = pd.concat([kbs_rows, fallback], ignore_index=True)
    merged["_priority"] = merged["source"].map(SOURCE_PRIORITY).fillna(99)
    merged = (
        merged.sort_values(["instrument_id", "time", "_priority"])
        .drop_duplicates(subset=["instrument_id", "time"], keep="first")
        .drop(columns=["_priority"])
        .reset_index(drop=True)
    )
    return merged, stats


def _store_for_resolution(resolution: str) -> PartitionedParquetStore:
    partition = "month" if resolution == "1m" else "year"
    return PartitionedParquetStore(["vn", "futures", "contracts", resolution], partition=partition)


def _append_contract_rows(df: pd.DataFrame, *, contract: VN30FutureContract, resolution: str) -> dict[str, object]:
    if df.empty:
        return {"rows_written": 0, "latest_time": None}
    store = _store_for_resolution(resolution)
    work = df.copy()
    work["time"] = normalize_datetime(work["time"])
    work = work.dropna(subset=["time"])
    if work.empty:
        return {"rows_written": 0, "latest_time": None}

    latest = work["time"].max()
    rows_written = 0
    work["_partition_year"] = work["time"].dt.year
    work["_partition_month"] = work["time"].dt.month if resolution == "1m" else 1

    with FileLock(f"vn_derivatives_contracts/{resolution}/{contract.canonical_symbol}"):
        for _, part_df in work.groupby(["_partition_year", "_partition_month"], sort=True):
            part_df = part_df.drop(columns=["_partition_year", "_partition_month"])
            part_when = pd.Timestamp(part_df["time"].iloc[0])
            path = store._partition_path(part_when, {"symbol": contract.canonical_symbol})
            if path.exists():
                existing = read_partition_file(path)
                existing["time"] = normalize_datetime(existing["time"])
                combined = pd.concat([existing, part_df], ignore_index=True)
            else:
                existing = None
                combined = part_df
            combined["time"] = normalize_datetime(combined["time"])
            combined["_priority"] = combined["source"].map(SOURCE_PRIORITY).fillna(99)
            combined = (
                combined.dropna(subset=["time"])
                .sort_values(["instrument_id", "time", "_priority"])
                .drop_duplicates(subset=["instrument_id", "time"], keep="first")
                .drop(columns=["_priority"])
                .reset_index(drop=True)
            )
            write_partition_file(combined, path)
            rows_written += len(part_df)
            del part_df, combined
            if existing is not None:
                del existing
            release_unused_memory()
    return {"rows_written": rows_written, "latest_time": latest.isoformat()}


def _read_contract_storage(contract: VN30FutureContract, resolution: str) -> pd.DataFrame:
    store = _store_for_resolution(resolution)
    frames = []
    for path in store.files({"symbol": contract.canonical_symbol}):
        frames.append(read_partition_file(path))
    if not frames:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def aggregate_1m_to_daily(contract: VN30FutureContract, *, min_bars: int = 200) -> pd.DataFrame:
    intraday = _read_contract_storage(contract, "1m")
    if intraday.empty:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)
    work = intraday.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    work = work.dropna(subset=["time"]).sort_values("time")
    work["_date"] = work["time"].dt.normalize()
    counts = work.groupby("_date").size()
    enough_dates = set(counts[counts >= min_bars].index)
    if not enough_dates:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)
    work = work[work["_date"].isin(enough_dates)]
    daily = (
        work.groupby("_date", sort=True)
        .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
        .reset_index()
        .rename(columns={"_date": "time"})
    )
    daily["instrument_id"] = contract.instrument_id
    daily["source"] = SOURCE_AGGREGATED_1M
    daily["quality_flags"] = "AGGREGATED_1M"
    daily["ingested_at"] = utc_now_iso()
    return daily[CONTRACT_COLUMNS]


def _selected_contracts(options: BackfillOptions) -> list[VN30FutureContract]:
    if options.symbols:
        contracts = [contract_for_month(*parse_canonical_symbol(symbol)) for symbol in options.symbols]
    else:
        contracts = generate_contracts(start=options.start, end=options.end)
    if options.max_contracts is not None:
        contracts = contracts[: options.max_contracts]
    return contracts


def _update_instrument_availability(version: str, contracts: Iterable[VN30FutureContract]) -> None:
    path = state_root()
    del path
    df = build_initial_instrument_dimension(version=version)
    for contract in contracts:
        mask = df["canonical_symbol"] == contract.canonical_symbol
        for resolution in ["1m", "1d"]:
            stored = _read_contract_storage(contract, resolution)
            if stored.empty:
                continue
            stored["time"] = pd.to_datetime(stored["time"], errors="coerce")
            first = stored["time"].min()
            last = stored["time"].max()
            df.loc[mask, f"first_{resolution}"] = first
            df.loc[mask, f"last_{resolution}"] = last
            sources = set(stored["source"].dropna().astype(str))
            if SOURCE_KBS in sources:
                df.loc[mask, f"kbs_available_{resolution}"] = True
                df.loc[mask, "kbs_symbol_resolved"] = contract.legacy_symbol
            if SOURCE_DNSE in sources:
                df.loc[mask, f"dnse_available_{resolution}"] = True
                df.loc[mask, "dnse_symbol_resolved"] = contract.legacy_symbol
            if pd.notna(first):
                current = df.loc[mask, "listing_start"].iloc[0]
                if pd.isna(current) or first < current:
                    df.loc[mask, "listing_start"] = first
            if pd.notna(last):
                current = df.loc[mask, "listing_end"].iloc[0]
                if pd.isna(current) or last > current:
                    df.loc[mask, "listing_end"] = last
    write_instrument_dimension(df, version=version)


def backfill_contracts(options: BackfillOptions, *, fetcher: Fetcher | None = None) -> dict[str, object]:
    logger = setup_logging("vn_derivatives_contracts")
    active_fetcher = fetcher or _default_fetcher
    contracts = _selected_contracts(options)
    build_initial_instrument_dimension(start=options.start, end=options.end, version=options.version)
    totals = {
        "status": "ok",
        "version": options.version,
        "contracts_planned": len(contracts),
        "windows_planned": 0,
        "rows_written": 0,
        "dnse_fallback_rows": 0,
        "validation_errors": 0,
        "provider_errors": 0,
    }
    windows_done = 0

    for contract in contracts:
        start, end = _contract_range(contract, options)
        if end < start:
            continue
        for resolution in options.resolutions:
            kbs_days = _window_days(options, resolution, SOURCE_KBS)
            planned = _windows(start, end, kbs_days)
            totals["windows_planned"] += len(planned)
            manifest = _read_manifest(resolution)
            symbol_state = dict(manifest.get("symbols", {}).get(contract.canonical_symbol, {}))  # type: ignore[union-attr]
            completed = set(symbol_state.get("completed_windows", []))
            for window_start, window_end in planned:
                key = f"{window_start.isoformat()}__{window_end.isoformat()}"
                if key in completed:
                    continue
                logger.info("vn_derivatives_backfill_window symbol=%s resolution=%s start=%s end=%s", contract.canonical_symbol, resolution, window_start, window_end)
                kbs = active_fetcher(contract, SOURCE_KBS, resolution, window_start, window_end)
                dnse = active_fetcher(contract, SOURCE_DNSE, resolution, window_start, window_end)
                rows, stats = merge_provider_rows(contract, kbs, dnse)
                provider_errors = stats.get("provider_errors", []) or []
                if provider_errors and rows.empty and not (kbs.empty_confirmed and dnse.empty_confirmed):
                    totals["status"] = "error"
                    totals["provider_errors"] += len(provider_errors)
                    _update_manifest(
                        resolution,
                        contract.canonical_symbol,
                        last_error=f"provider request errors without usable rows: {provider_errors}",
                        last_failed_at=utc_now_iso(),
                    )
                    if options.skip_provider_errors:
                        logger.warning(
                            "vn_derivatives_backfill_window_skipped symbol=%s resolution=%s provider_errors=%s",
                            contract.canonical_symbol,
                            resolution,
                            provider_errors,
                        )
                        continue
                    raise RuntimeError(f"{contract.canonical_symbol} {resolution} provider request failed without usable rows: {provider_errors}")
                if resolution == "1d" and rows.empty:
                    rows = aggregate_1m_to_daily(contract, min_bars=options.min_1m_bars_for_daily)
                    rows = rows[(pd.to_datetime(rows["time"]) >= window_start.normalize()) & (pd.to_datetime(rows["time"]) <= window_end.normalize())]
                if rows.empty and not options.complete_empty_windows:
                    _update_manifest(
                        resolution,
                        contract.canonical_symbol,
                        last_error="empty_response_without_rows",
                        last_failed_at=utc_now_iso(),
                    )
                    logger.warning(
                        "vn_derivatives_backfill_empty_window_skipped symbol=%s resolution=%s start=%s end=%s",
                        contract.canonical_symbol,
                        resolution,
                        window_start,
                        window_end,
                    )
                    continue
                issues = validate_contract_frame(rows, expiry_date=pd.Timestamp(contract.expiry_date))
                error_count = sum(1 for issue in issues if issue.severity == "error")
                totals["validation_errors"] += error_count
                if error_count:
                    totals["status"] = "error"
                    _write_issue_report(contract, resolution, key, issues)
                    raise RuntimeError(f"{contract.canonical_symbol} {resolution} validation failed: {[issue.code for issue in issues]}")
                result = _append_contract_rows(rows, contract=contract, resolution=resolution)
                totals["rows_written"] += int(result["rows_written"] or 0)
                totals["dnse_fallback_rows"] += int(stats.get("dnse_fallback_rows", 0) or 0)
                totals["provider_errors"] += len(stats.get("provider_errors", []) or [])
                completed.add(key)
                latest_time = result.get("latest_time") or symbol_state.get("latest_time")
                _update_manifest(
                    resolution,
                    contract.canonical_symbol,
                    latest_time=latest_time,
                    completed_windows=sorted(completed),
                    rows_written=result["rows_written"],
                    last_success_at=utc_now_iso(),
                    last_error=None,
                )
                windows_done += 1
                del rows
                release_unused_memory()
                if options.max_windows is not None and windows_done >= options.max_windows:
                    _update_instrument_availability(options.version, contracts)
                    return totals | {"windows_done": windows_done, "stopped_by": "max_windows"}
                if options.sleep_seconds:
                    time.sleep(options.sleep_seconds)

    _update_instrument_availability(options.version, contracts)
    return totals | {"windows_done": windows_done}


def _write_issue_report(contract: VN30FutureContract, resolution: str, window_key: str, issues: list[ValidationIssue]) -> None:
    root = state_root() / "vn_derivatives" / "issues" / resolution
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{contract.canonical_symbol}_{window_key.replace(':', '').replace('/', '_')}.json"
    payload = {"symbol": contract.canonical_symbol, "resolution": resolution, "window": window_key, "issues": [issue.__dict__ for issue in issues], "updated_at": utc_now_iso()}
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    tmp.replace(path)


def options_from_config(
    *,
    version: str = "v1",
    start: str | None = None,
    end: str | None = None,
    resolutions: Iterable[str] | None = None,
    symbols: Iterable[str] | None = None,
    max_contracts: int | None = None,
    max_windows: int | None = None,
    sleep_seconds: float = 0.0,
    skip_provider_errors: bool = False,
    complete_empty_windows: bool = True,
) -> BackfillOptions:
    config = load_yaml("vn_derivatives.yml")
    requests = config.get("requests", {})
    validation = config.get("validation", {})
    return BackfillOptions(
        version=version or config.get("dataset_version", "v1"),
        start=start or config.get("backfill_start", "2017-08-10"),
        end=end,
        resolutions=tuple(resolutions or config.get("resolutions", ["1m", "1d"])),
        symbols=tuple(symbols) if symbols else None,
        max_contracts=max_contracts,
        max_windows=max_windows,
        kbs_1m_window_days=int(requests.get("kbs_1m_window_days", 7)),
        dnse_1m_window_days=int(requests.get("dnse_1m_window_days", 5)),
        daily_window_days=int(requests.get("daily_window_days", 365)),
        min_1m_bars_for_daily=int(validation.get("min_1m_bars_for_daily", 200)),
        sleep_seconds=sleep_seconds,
        skip_provider_errors=skip_provider_errors,
        complete_empty_windows=complete_empty_windows,
    )
