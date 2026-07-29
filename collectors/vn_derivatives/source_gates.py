from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

import pandas as pd

from collectors.common.calendar_vn import filter_trading_hours
from collectors.common.manifest import utc_now_iso

ProviderStatus = Literal[
    "success",
    "empty_confirmed",
    "unsupported_symbol",
    "invalid_request",
    "auth_error",
    "rate_limited",
    "blocked",
    "schema_error",
    "unknown_error",
]

ProviderQualityStatus = Literal["UNVERIFIED", "POSITIVE_PARTIAL", "VALIDATED", "DISABLED"]


@dataclass(frozen=True)
class ProviderFetchResult:
    provider: str
    canonical_symbol: str
    requested_symbol: str
    resolved_symbol: str | None
    resolution: str
    status: ProviderStatus
    rows: pd.DataFrame
    http_status: int | None = None
    first_bar: object | None = None
    last_bar: object | None = None
    error: str | None = None
    installed_version: str | None = None
    endpoint_type: str | None = None
    source_url: str | None = None

    @property
    def row_count(self) -> int:
        return int(len(self.rows)) if isinstance(self.rows, pd.DataFrame) else 0

    @property
    def positive(self) -> bool:
        return self.status == "success" and self.row_count > 0


def empty_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])


def normalize_ohlcv_frame(
    df: pd.DataFrame | None,
    *,
    resolution: str,
    source: str,
    source_symbol: str,
    derivative_session: bool = True,
) -> tuple[pd.DataFrame, str | None]:
    if df is None or df.empty:
        return empty_ohlcv_frame(), None
    work = df.copy()
    lower_map = {str(col).lower(): col for col in work.columns}

    if "datetime" in lower_map:
        work["time"] = work[lower_map["datetime"]]
    elif "timestamp" in lower_map:
        work["time"] = work[lower_map["timestamp"]]
    elif "date" in lower_map and "time" in lower_map and lower_map["date"] != lower_map["time"]:
        work["time"] = work[lower_map["date"]].astype(str) + " " + work[lower_map["time"]].astype(str)
    elif "date" in lower_map:
        work["time"] = work[lower_map["date"]]
    elif "time" in lower_map:
        work["time"] = work[lower_map["time"]]
    else:
        return empty_ohlcv_frame(), "missing time/date column"

    rename = {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "vol": "volume",
        "oi": "open_interest",
        "openinterest": "open_interest",
        "open_interest": "open_interest",
        "settlement": "settlement_price",
        "settlement_price": "settlement_price",
    }
    for key, out_col in rename.items():
        if key in lower_map and out_col not in work.columns:
            work[out_col] = work[lower_map[key]]

    required = ["open", "high", "low", "close"]
    missing = [col for col in required if col not in work.columns]
    if missing:
        return empty_ohlcv_frame(), f"missing OHLC columns: {missing}"

    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    try:
        if work["time"].dt.tz is not None:
            work["time"] = work["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except Exception:
        pass
    work = work.dropna(subset=["time"])
    if work.empty:
        return empty_ohlcv_frame(), "all time values invalid"

    for col in ["open", "high", "low", "close", "volume", "open_interest", "settlement_price"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    if "volume" not in work.columns:
        work["volume"] = pd.NA

    work = work.dropna(subset=["open", "high", "low", "close"])
    if work.empty:
        return empty_ohlcv_frame(), "all OHLC values invalid"

    if str(resolution).lower() in {"1d", "d", "day"}:
        work["time"] = work["time"].dt.normalize()
    elif derivative_session:
        work = filter_trading_hours(work, derivative=True)

    work["source"] = source
    work["source_symbol"] = source_symbol
    work["ingested_at"] = utc_now_iso()
    columns = ["time", "open", "high", "low", "close", "volume"]
    for optional in ["open_interest", "settlement_price", "source", "source_symbol", "ingested_at"]:
        if optional in work.columns:
            columns.append(optional)
    work = (
        work[columns]
        .drop_duplicates(subset=["time"], keep="last")
        .sort_values("time")
        .reset_index(drop=True)
    )
    return work, None


def validation_metrics(df: pd.DataFrame, *, resolution: str) -> dict[str, object]:
    metrics: dict[str, object] = {
        "row_count": int(len(df)),
        "first_bar": None,
        "last_bar": None,
        "duplicate_rows": 0,
        "invalid_ohlc_rows": 0,
        "session_outside_rows": 0,
        "median_bar_interval": None,
        "volume_min": None,
        "volume_max": None,
        "columns": [],
        "timezone": "Asia/Ho_Chi_Minh",
    }
    if df.empty or "time" not in df.columns:
        return metrics
    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    work = work.dropna(subset=["time"])
    if work.empty:
        return metrics
    metrics["columns"] = list(df.columns)
    metrics["first_bar"] = work["time"].min()
    metrics["last_bar"] = work["time"].max()
    metrics["duplicate_rows"] = int(work.duplicated(subset=["time"]).sum())
    for col in ["open", "high", "low", "close", "volume"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    if {"open", "high", "low", "close"}.issubset(work.columns):
        invalid = (
            (work["high"] < work[["open", "close", "low"]].max(axis=1))
            | (work["low"] > work[["open", "close", "high"]].min(axis=1))
        )
        metrics["invalid_ohlc_rows"] = int(invalid.fillna(False).sum())
    if "volume" in work.columns:
        volume = pd.to_numeric(work["volume"], errors="coerce").dropna()
        if not volume.empty:
            metrics["volume_min"] = float(volume.min())
            metrics["volume_max"] = float(volume.max())
    if len(work) > 1:
        diffs = work["time"].sort_values().diff().dropna()
        if not diffs.empty:
            metrics["median_bar_interval"] = str(diffs.median())
    if str(resolution).lower() not in {"1d", "d", "day"}:
        filtered = filter_trading_hours(work[["time"]].copy(), derivative=True)
        metrics["session_outside_rows"] = int(len(work) - len(filtered))
    return metrics


def classify_http_status(status_code: int | None) -> ProviderStatus:
    if status_code is None:
        return "unknown_error"
    if status_code in {401, 403}:
        return "auth_error" if status_code == 401 else "blocked"
    if status_code == 404:
        return "unsupported_symbol"
    if status_code == 429:
        return "rate_limited"
    if status_code == 400:
        return "invalid_request"
    if status_code >= 500:
        return "unknown_error"
    return "success"


def result_to_row(result: ProviderFetchResult) -> dict[str, object]:
    metrics = validation_metrics(result.rows, resolution=result.resolution)
    columns = metrics.pop("columns", [])
    return {
        "provider": result.provider,
        "canonical_symbol": result.canonical_symbol,
        "requested_symbol": result.requested_symbol,
        "resolved_symbol": result.resolved_symbol,
        "resolution": result.resolution,
        "status": result.status,
        "http_status": result.http_status,
        "row_count": result.row_count,
        "first_bar": result.first_bar if result.first_bar is not None else metrics["first_bar"],
        "last_bar": result.last_bar if result.last_bar is not None else metrics["last_bar"],
        "error": result.error,
        "installed_version": result.installed_version,
        "endpoint_type": result.endpoint_type,
        "source_url": result.source_url,
        "columns": ",".join(map(str, columns)) if isinstance(columns, list) else str(columns or ""),
        **metrics,
    }


def provider_quality(rows: pd.DataFrame) -> dict[str, dict[str, object]]:
    output: dict[str, dict[str, object]] = {}
    if rows.empty:
        return output
    for provider, group in rows.groupby("provider"):
        positive = group[(group["status"] == "success") & (group["row_count"] > 0)]
        blocked = group[group["status"].isin(["blocked", "auth_error", "rate_limited"])]
        errors = group[~group["status"].isin(["success", "empty_confirmed"])]
        if not positive.empty:
            status: ProviderQualityStatus = "VALIDATED" if int(positive["row_count"].sum()) > 1000 else "POSITIVE_PARTIAL"
        elif len(blocked) == len(group) or len(errors) == len(group):
            status = "DISABLED"
        else:
            status = "UNVERIFIED"
        output[str(provider)] = {
            "status": status,
            "positive_request_count": int(len(positive)),
            "positive_row_count": int(positive["row_count"].sum()) if not positive.empty else 0,
            "earliest_bar": None if positive.empty else str(pd.to_datetime(positive["first_bar"]).min()),
            "latest_bar": None if positive.empty else str(pd.to_datetime(positive["last_bar"]).max()),
            "updated_at": utc_now_iso(),
        }
    return output


def json_ready(payload: object) -> object:
    if isinstance(payload, pd.Timestamp):
        return payload.isoformat()
    if pd.isna(payload) if not isinstance(payload, (list, dict, tuple, set)) else False:
        return None
    if isinstance(payload, dict):
        return {str(key): json_ready(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [json_ready(value) for value in payload]
    return payload


def dumps_json(payload: object) -> str:
    return json.dumps(json_ready(payload), indent=2, sort_keys=True, default=str)


def dataclass_dict(result: ProviderFetchResult) -> dict[str, object]:
    payload = asdict(result)
    payload["rows"] = f"<DataFrame rows={result.row_count}>"
    return payload
