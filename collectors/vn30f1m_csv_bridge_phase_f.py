"""Phase F bridge for owner-provided VN30F1M 1m CSV history.

Only the raw file is eligible for canonical storage.  The adjusted file is
read solely to preserve evidence that it is a derived analytical series and
must never overwrite executable raw OHLCV.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from collectors.common.calendar_vn import filter_trading_hours
from collectors.common.env import load_environment
from collectors.common.manifest import JsonState, Manifest, utc_now_iso
from collectors.common.storage import PartitionedParquetStore
from collectors.vn30f1m_dnse_phase_f import AUDIT_STATE as DNSE_AUDIT_STATE


SYMBOL = "VN30F1M"
RAW_SOURCE = "legacy_csv_raw"
BRIDGE_AUDIT_STATE = "audits/vn30f1m_csv_bridge_phase_f.json"
CANONICAL_COLUMNS = ["time", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalise_csv(path: Path, *, source: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"CSV input is unavailable: {path}")
    frame = pd.read_csv(path)
    required = {"datetime", "open", "high", "low", "close", "volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")
    work = frame.rename(columns={"datetime": "time"}).copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    try:
        if work["time"].dt.tz is not None:
            work["time"] = work["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except AttributeError:
        pass
    for column in ("open", "high", "low", "close", "volume"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work["symbol"] = SYMBOL
    work["source"] = source
    work["ingested_at"] = utc_now_iso()
    work = work.loc[(work["time"] >= start) & (work["time"] < end), CANONICAL_COLUMNS]
    return work.sort_values("time").reset_index(drop=True)


def _audit_raw(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {
        "rows": int(len(frame)),
        "duplicate_symbol_time_rows": int(frame.duplicated(["symbol", "time"], keep=False).sum()),
        "null_required_rows": int(frame[["time", "symbol", "open", "high", "low", "close", "volume"]].isna().any(axis=1).sum()),
        "invalid_ohlc_rows": 0,
        "negative_volume_rows": int((frame["volume"] < 0).fillna(False).sum()),
        "wrong_symbol_rows": int((frame["symbol"].astype(str).str.upper() != SYMBOL).sum()),
        "wrong_source_rows": int((frame["source"].astype(str) != RAW_SOURCE).sum()),
        "out_of_derivative_session_rows": 0,
        "first_time": str(frame["time"].min()) if not frame.empty else None,
        "last_time": str(frame["time"].max()) if not frame.empty else None,
    }
    invalid = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
        | (frame["high"] < frame["low"])
    )
    result["invalid_ohlc_rows"] = int(invalid.fillna(True).sum())
    valid_time = frame.dropna(subset=["time"])
    result["out_of_derivative_session_rows"] = int(len(valid_time) - len(filter_trading_hours(valid_time, derivative=True)))
    result["status"] = "pass" if result["rows"] and not any(
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
    ) else "fail"
    return result


def _adjusted_evidence(raw: pd.DataFrame, adjusted: pd.DataFrame) -> dict[str, Any]:
    raw_index = raw.set_index("time")
    adjusted_index = adjusted.set_index("time")
    shared = raw_index.index.intersection(adjusted_index.index)
    raw_only = raw_index.index.difference(adjusted_index.index)
    adjusted_only = adjusted_index.index.difference(raw_index.index)
    shared_raw = raw_index.loc[shared, ["open", "high", "low", "close"]]
    shared_adjusted = adjusted_index.loc[shared, ["open", "high", "low", "close"]]
    ohlc_different = int((shared_raw != shared_adjusted).any(axis=1).sum())
    return {
        "adjusted_role": "derived_research_reference_only",
        "raw_rows": int(len(raw)),
        "adjusted_rows": int(len(adjusted)),
        "shared_rows": int(len(shared)),
        "raw_only_rows": int(len(raw_only)),
        "adjusted_only_rows": int(len(adjusted_only)),
        "shared_rows_with_any_ohlc_difference": ohlc_different,
    }


def _require_dnse_audit() -> None:
    audit = JsonState(DNSE_AUDIT_STATE).read()
    if audit.get("status") != "pass" or str(audit.get("symbol", "")).upper() != SYMBOL:
        raise RuntimeError("CSV bridge requires a passing Phase F DNSE storage audit")


def bridge(
    *,
    raw_path: Path,
    adjusted_path: Path,
    start: str,
    end: str,
    require_dnse_audit: bool,
) -> dict[str, Any]:
    if require_dnse_audit:
        _require_dnse_audit()
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize() + pd.Timedelta(days=1)
    raw = _normalise_csv(raw_path, source=RAW_SOURCE, start=start_ts, end=end_ts)
    adjusted = _normalise_csv(adjusted_path, source="adjusted_reference", start=start_ts, end=end_ts)
    raw_audit = _audit_raw(raw)
    if raw_audit["status"] != "pass":
        raise RuntimeError(f"raw VN30F1M CSV failed structural audit: {raw_audit}")

    store = PartitionedParquetStore(["vn", "futures", "1m"], partition="month")
    result = store.append(
        raw,
        time_col="time",
        dedupe_cols=["symbol", "time"],
        attrs={"symbol": SYMBOL},
        lock_name="vn_futures_dnse_1m/VN30F1M",
    )
    stored = pd.concat(
        [
            pd.read_parquet(path)
            for path in sorted((store.root / f"symbol={SYMBOL}").glob("year=*/month=*/part.parquet"))
            if path.is_file()
        ],
        ignore_index=True,
    )
    stored["time"] = pd.to_datetime(stored["time"], errors="coerce")
    stored_segment = stored[(stored["time"] >= start_ts) & (stored["time"] < end_ts)]
    stored_raw = stored_segment[stored_segment["source"] == RAW_SOURCE]
    payload: dict[str, Any] = {
        "status": "pass" if len(stored_raw) == len(raw) else "blocked",
        "dataset_id": "vn_futures_1m",
        "symbol": SYMBOL,
        "raw_path_name": raw_path.name,
        "raw_sha256": _sha256(raw_path),
        "adjusted_path_name": adjusted_path.name,
        "adjusted_sha256": _sha256(adjusted_path),
        "requested_start": start_ts.isoformat(),
        "requested_end": end_ts.isoformat(),
        "raw_audit": raw_audit,
        "adjusted_evidence": _adjusted_evidence(raw, adjusted),
        "rows_written": int(result["rows_written"]),
        "stored_raw_rows": int(len(stored_raw)),
        "updated_at": utc_now_iso(),
    }
    JsonState(BRIDGE_AUDIT_STATE).write(payload)
    Manifest("vn_futures_dnse_1m").update_symbol(
        SYMBOL,
        latest_time=str(result.get("latest_time")),
        last_success_at=utc_now_iso(),
        source="dnse_and_legacy_csv_raw",
        csv_bridge_status=payload["status"],
        last_error=None if payload["status"] == "pass" else "stored raw row count does not equal audited input row count",
    )
    return payload


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Phase F raw VN30F1M CSV bridge")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--adjusted-path", required=True)
    parser.add_argument("--start", default="2018-01-02")
    parser.add_argument("--end", default="2024-12-31")
    parser.add_argument("--require-dnse-audit", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    payload = bridge(
        raw_path=Path(args.raw_path),
        adjusted_path=Path(args.adjusted_path),
        start=args.start,
        end=args.end,
        require_dnse_audit=args.require_dnse_audit,
    )
    if args.json:
        print(json.dumps(payload, sort_keys=True, default=str))
    if payload["status"] != "pass":
        raise RuntimeError("raw CSV bridge verification did not pass")


if __name__ == "__main__":
    main()
