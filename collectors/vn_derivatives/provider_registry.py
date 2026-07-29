from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Iterable

import pandas as pd

from collectors.common.env import state_root
from collectors.common.manifest import utc_now_iso
from collectors.providers import dnse_derivatives, kbs_derivatives, tradingview_derivatives, vietstock_derivatives
from collectors.vn_derivatives.source_gates import (
    ProviderFetchResult,
    classify_http_status,
    empty_ohlcv_frame,
    provider_quality,
    result_to_row,
)
from collectors.vn_derivatives.symbols import contract_for_month, legacy_to_krx, parse_canonical_symbol

VIETSTOCK_CONTRACTS = ["VN30F1M", "VN30F2506", "VN30F2509", "VN30F2512"]
KBS_DNSE_CONTRACTS = ["VN30F1709", "VN30F2406", "VN30F2508", "VN30F2608"]


@dataclass(frozen=True)
class SourceProbeOptions:
    providers: tuple[str, ...] = ("vietstock", "tradingview", "kbs", "dnse")
    contracts: tuple[str, ...] | None = None
    fail_on_no_positive: bool = True
    version: str = "v2"


ProviderProbe = Callable[[], ProviderFetchResult]


def _status_from_exception(exc: Exception) -> tuple[str, int | None, str]:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        response = getattr(current, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            return classify_http_status(int(status_code)), int(status_code), f"{type(exc).__name__}: {exc}"
        current = current.__cause__ or current.__context__
    text = str(exc).lower()
    if "missing dnse_api" in text:
        return "auth_error", None, f"{type(exc).__name__}: {exc}"
    if "timed out" in text or "connection" in text:
        return "unknown_error", None, f"{type(exc).__name__}: {exc}"
    return "unknown_error", None, f"{type(exc).__name__}: {exc}"


def _wrap_kbs(symbol: str, requested_symbol: str, resolution: str) -> ProviderFetchResult:
    try:
        year, month = parse_canonical_symbol(symbol)
        contract = contract_for_month(year, month)
        end = pd.Timestamp(contract.expiry_date) + pd.Timedelta(days=1)
        start = max(pd.Timestamp("2017-08-10"), end - pd.Timedelta(days=7 if resolution == "1m" else 45))
        rows = kbs_derivatives.fetch_ohlc(requested_symbol, start, end, "1m" if resolution == "1m" else "1d")
    except Exception as exc:
        status, http_status, error = _status_from_exception(exc)
        return ProviderFetchResult("kbs", symbol, requested_symbol, None, resolution, status, empty_ohlcv_frame(), http_status=http_status, error=error, endpoint_type="package")
    status = "empty_confirmed" if rows.empty else "success"
    return ProviderFetchResult("kbs", symbol, requested_symbol, requested_symbol, resolution, status, rows, first_bar=rows["time"].min() if not rows.empty else None, last_bar=rows["time"].max() if not rows.empty else None, endpoint_type="package")


def _wrap_dnse(symbol: str, requested_symbol: str, resolution: str) -> ProviderFetchResult:
    try:
        year, month = parse_canonical_symbol(symbol)
        contract = contract_for_month(year, month)
        end = pd.Timestamp(contract.expiry_date) + pd.Timedelta(days=1)
        start = max(pd.Timestamp("2017-08-10"), end - pd.Timedelta(days=7 if resolution == "1m" else 45))
        rows = dnse_derivatives.fetch_ohlc(requested_symbol, start, end, "1" if resolution == "1m" else "1D", asset_type="derivative")
    except Exception as exc:
        status, http_status, error = _status_from_exception(exc)
        return ProviderFetchResult("dnse", symbol, requested_symbol, None, resolution, status, empty_ohlcv_frame(), http_status=http_status, error=error, endpoint_type="rest")
    status = "empty_confirmed" if rows.empty else "success"
    return ProviderFetchResult("dnse", symbol, requested_symbol, requested_symbol, resolution, status, rows, first_bar=rows["time"].min() if not rows.empty else None, last_bar=rows["time"].max() if not rows.empty else None, endpoint_type="rest")


def _contract_symbols(symbol: str) -> list[str]:
    year, month = parse_canonical_symbol(symbol)
    return [symbol, legacy_to_krx(year, month)]


def build_source_probe_plan(options: SourceProbeOptions) -> list[ProviderProbe]:
    providers = set(options.providers)
    contracts = list(options.contracts or ())
    plan: list[ProviderProbe] = []
    if "vietstock" in providers:
        for symbol in contracts or VIETSTOCK_CONTRACTS:
            plan.append(lambda symbol=symbol: vietstock_derivatives.fetch_daily(symbol))
    if "tradingview" in providers:
        for resolution in ["1D", "1m"]:
            plan.append(lambda resolution=resolution: tradingview_derivatives.probe_public_page(resolution=resolution))
    for provider in ["kbs", "dnse"]:
        if provider not in providers:
            continue
        for symbol in contracts or KBS_DNSE_CONTRACTS:
            for requested_symbol in _contract_symbols(symbol):
                for resolution in ["1m", "1D"]:
                    if provider == "kbs":
                        plan.append(lambda symbol=symbol, requested_symbol=requested_symbol, resolution=resolution: _wrap_kbs(symbol, requested_symbol, resolution))
                    else:
                        plan.append(lambda symbol=symbol, requested_symbol=requested_symbol, resolution=resolution: _wrap_dnse(symbol, requested_symbol, resolution))
    return plan


def source_probe_paths(version: str = "v2") -> tuple[object, object, object]:
    root = state_root() / "vn_derivatives"
    return root / f"source_probe_{version}.parquet", root / f"source_probe_{version}.json", root / "source_status.json"


def run_source_probe(options: SourceProbeOptions) -> dict[str, object]:
    plan = build_source_probe_plan(options)
    rows: list[dict[str, object]] = []
    for probe in plan:
        result = probe()
        rows.append(result_to_row(result))
    frame = pd.DataFrame(rows)
    expected = len(plan)
    actual = len(frame)
    positive = int(((frame["status"] == "success") & (frame["row_count"] > 0)).sum()) if not frame.empty else 0
    blocked = int(frame["status"].isin(["blocked", "auth_error", "rate_limited"]).sum()) if not frame.empty else 0
    errors = int((~frame["status"].isin(["success", "empty_confirmed"])).sum()) if not frame.empty else 0
    quality = provider_quality(frame)

    parquet_path, json_path, status_path = source_probe_paths(options.version)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(parquet_path, index=False, engine="pyarrow", compression="zstd")
    summary = {
        "status": "ok",
        "version": options.version,
        "expected_request_count": expected,
        "actual_request_count": actual,
        "positive_request_count": positive,
        "blocked_request_count": blocked,
        "error_request_count": errors,
        "providers": sorted(set(frame["provider"])) if not frame.empty else [],
        "source_status": quality,
        "parquet_path": str(parquet_path),
        "json_path": str(json_path),
        "source_status_path": str(status_path),
        "updated_at": utc_now_iso(),
    }
    if actual != expected:
        summary["status"] = "error"
        summary["error"] = "actual_request_count does not match expected_request_count"
    elif positive == 0:
        summary["status"] = "blocked"
        summary["error"] = "positive_request_count is zero; Phase 2 publish is blocked"

    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str))
    status_path.write_text(json.dumps({"version": options.version, "providers": quality, "updated_at": utc_now_iso()}, indent=2, sort_keys=True, default=str))
    if summary["status"] == "error" or (summary["status"] == "blocked" and options.fail_on_no_positive):
        raise RuntimeError(str(summary.get("error", "source probe failed")))
    return summary


def options_from_cli(*, providers: Iterable[str] | None, contracts: Iterable[str] | None, version: str, no_fail_on_no_positive: bool) -> SourceProbeOptions:
    provider_tuple = tuple(provider.strip().lower() for provider in providers or [] if provider.strip()) or SourceProbeOptions().providers
    contract_tuple = tuple(contract.strip().upper() for contract in contracts or [] if contract.strip()) or None
    return SourceProbeOptions(providers=provider_tuple, contracts=contract_tuple, version=version, fail_on_no_positive=not no_fail_on_no_positive)
