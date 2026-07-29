from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from collectors.common.env import state_root
from collectors.common.manifest import utc_now_iso
from collectors.providers import dnse_derivatives, kbs_derivatives
from collectors.vn_derivatives.instruments import build_initial_instrument_dimension
from collectors.vn_derivatives.symbols import MARKET_START, contract_for_month, parse_canonical_symbol, provider_symbol_candidates

PROBE_CONTRACTS = [
    "VN30F1708",
    "VN30F1709",
    "VN30F1712",
    "VN30F1803",
    "VN30F2003",
    "VN30F2206",
    "VN30F2406",
    "VN30F2504",
    "VN30F2505",
    "VN30F2506",
    "VN30F2508",
]
PROBE_RESOLUTIONS = ["1m", "1d"]
PROBE_COLUMNS = [
    "canonical_symbol",
    "provider",
    "provider_symbol",
    "symbol_kind",
    "resolution",
    "request_success",
    "empty_confirmed",
    "first_bar",
    "last_bar",
    "row_count",
    "columns",
    "timezone",
    "price_scale",
    "volume_scale",
    "max_safe_request_days",
    "error",
]


@dataclass(frozen=True)
class ProbeRequest:
    canonical_symbol: str
    provider: str
    provider_symbol: str
    symbol_kind: str
    resolution: str
    start: pd.Timestamp
    end: pd.Timestamp


ProviderFetcher = Callable[[ProbeRequest], pd.DataFrame]


def _probe_window(symbol: str, *, window_days: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    year, month = parse_canonical_symbol(symbol)
    contract = contract_for_month(year, month)
    end = pd.Timestamp(contract.expiry_date) + pd.Timedelta(days=1)
    start = max(pd.Timestamp(MARKET_START), end - pd.Timedelta(days=window_days))
    return start, end


def _price_scale(df: pd.DataFrame) -> str:
    values = []
    for col in ["open", "high", "low", "close"]:
        if col in df.columns:
            values.append(pd.to_numeric(df[col], errors="coerce").dropna())
    if not values:
        return "unknown"
    combined = pd.concat(values)
    if combined.empty:
        return "unknown"
    if combined.median() > 100:
        return "index_points"
    return "unknown"


def _volume_scale(df: pd.DataFrame) -> str:
    if "volume" not in df.columns:
        return "unknown"
    return "contracts_or_lots"


def _format_exception(exc: Exception) -> str:
    parts = [f"{type(exc).__name__}: {exc}"]
    current = exc.__cause__ or exc.__context__
    seen = {id(exc)}
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"caused_by {type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _result_row(request: ProbeRequest, *, df: pd.DataFrame | None = None, error: str | None = None, max_safe_request_days: int | None = None) -> dict[str, object]:
    success = error is None
    frame = df if df is not None else pd.DataFrame()
    times = pd.to_datetime(frame["time"], errors="coerce") if success and "time" in frame.columns and not frame.empty else pd.Series(dtype="datetime64[ns]")
    times = times.dropna()
    return {
        "canonical_symbol": request.canonical_symbol,
        "provider": request.provider,
        "provider_symbol": request.provider_symbol,
        "symbol_kind": request.symbol_kind,
        "resolution": request.resolution,
        "request_success": success,
        "empty_confirmed": bool(success and frame.empty),
        "first_bar": times.min() if not times.empty else pd.NaT,
        "last_bar": times.max() if not times.empty else pd.NaT,
        "row_count": int(len(frame)) if success else 0,
        "columns": ",".join(map(str, frame.columns)) if success else "",
        "timezone": "Asia/Ho_Chi_Minh",
        "price_scale": _price_scale(frame) if success else "unknown",
        "volume_scale": _volume_scale(frame) if success else "unknown",
        "max_safe_request_days": max_safe_request_days,
        "error": error,
    }


def _default_fetchers() -> dict[str, ProviderFetcher]:
    def kbs(request: ProbeRequest) -> pd.DataFrame:
        return kbs_derivatives.fetch_ohlc(request.provider_symbol, request.start, request.end, request.resolution)  # type: ignore[arg-type]

    def dnse(request: ProbeRequest) -> pd.DataFrame:
        resolution = "1" if request.resolution == "1m" else "1D"
        return dnse_derivatives.fetch_ohlc(request.provider_symbol, request.start, request.end, resolution, asset_type="derivative")

    return {"kbs": kbs, "dnse": dnse}


def _requests(contracts: Iterable[str], *, window_days: int, providers: Iterable[str] | None = None) -> list[ProbeRequest]:
    planned: list[ProbeRequest] = []
    provider_list = [provider.strip().lower() for provider in (providers or ["kbs", "dnse"]) if provider.strip()]
    for symbol in contracts:
        year, month = parse_canonical_symbol(symbol)
        contract = contract_for_month(year, month)
        start, end = _probe_window(symbol, window_days=window_days)
        for provider in provider_list:
            for symbol_kind, provider_symbol in provider_symbol_candidates(contract):
                for resolution in PROBE_RESOLUTIONS:
                    planned.append(ProbeRequest(symbol, provider, provider_symbol, symbol_kind, resolution, start, end))
    return planned


def _summary(rows: pd.DataFrame) -> dict[str, object]:
    summary: dict[str, object] = {
        "updated_at": utc_now_iso(),
        "status": "ok",
        "rows": int(len(rows)),
        "notes": [],
    }
    for provider in ["kbs", "dnse"]:
        for resolution in PROBE_RESOLUTIONS:
            subset = rows[(rows["provider"] == provider) & (rows["resolution"] == resolution) & (rows["request_success"]) & (rows["row_count"] > 0)]
            key = f"earliest_{provider}_{resolution}"
            summary[key] = None if subset.empty else str(pd.to_datetime(subset["first_bar"]).min())
            mapping_key = f"{provider}_{resolution}_symbols_with_data"
            summary[mapping_key] = [] if subset.empty else sorted({f"{row.provider_symbol}:{row.symbol_kind}" for row in subset.itertuples()})
    if rows.empty:
        summary["status"] = "empty"
    if rows["request_success"].sum() == 0:
        summary["status"] = "blocked"
        summary["notes"].append("all provider requests failed; check network/API credentials/provider availability")
    return summary


def probe_output_paths(version: str = "v1") -> tuple[Path, Path]:
    root = state_root() / "vn_derivatives"
    return root / f"provider_probe_{version}.parquet", root / f"provider_probe_{version}.json"


def run_provider_probe(
    *,
    contracts: Iterable[str] | None = None,
    window_days: int = 30,
    providers: Iterable[str] | None = None,
    fetchers: dict[str, ProviderFetcher] | None = None,
    version: str = "v1",
) -> dict[str, object]:
    build_initial_instrument_dimension(version=version)
    active_fetchers = fetchers or _default_fetchers()
    rows = []
    for request in _requests(contracts or PROBE_CONTRACTS, window_days=window_days, providers=providers):
        max_days = 7 if request.provider == "kbs" and request.resolution == "1m" else 5 if request.provider == "dnse" and request.resolution == "1m" else 365
        try:
            fetcher = active_fetchers[request.provider]
            df = fetcher(request)
            rows.append(_result_row(request, df=df, max_safe_request_days=max_days))
        except Exception as exc:
            rows.append(_result_row(request, error=_format_exception(exc), max_safe_request_days=max_days))

    result = pd.DataFrame(rows, columns=PROBE_COLUMNS)
    parquet_path, json_path = probe_output_paths(version)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(parquet_path, index=False, engine="pyarrow", compression="zstd")
    summary = _summary(result)
    summary["parquet_path"] = str(parquet_path)
    summary["json_path"] = str(json_path)
    tmp = json_path.with_suffix(json_path.suffix + ".tmp")
    tmp.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str))
    tmp.replace(json_path)
    return summary
