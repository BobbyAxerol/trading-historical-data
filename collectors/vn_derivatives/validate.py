from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from collectors.common.env import data_root, state_root
from collectors.common.manifest import utc_now_iso
from collectors.common.storage import read_partition_file
from collectors.vn_derivatives.instruments import instrument_dimension_path


@dataclass
class ValidationIssue:
    severity: str
    code: str
    message: str
    rows: int = 0


@dataclass
class ValidationReport:
    status: str = "ok"
    dataset: str = "vn_derivatives_contracts"
    version: str = "v1"
    files: int = 0
    rows: int = 0
    duplicate_keys: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)

    def add(self, severity: str, code: str, message: str, rows: int = 0) -> None:
        self.issues.append(ValidationIssue(severity, code, message, rows))
        if severity == "error":
            self.status = "error"
        elif severity == "warning" and self.status == "ok":
            self.status = "warning"

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "dataset": self.dataset,
            "version": self.version,
            "files": self.files,
            "rows": self.rows,
            "duplicate_keys": self.duplicate_keys,
            "issues": [issue.__dict__ for issue in self.issues],
            "updated_at": utc_now_iso(),
        }


def validate_contract_frame(df: pd.DataFrame, *, expiry_date: pd.Timestamp | None = None) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if df.empty:
        return issues
    required = {"time", "instrument_id", "open", "high", "low", "close", "volume", "source", "quality_flags", "ingested_at"}
    missing = sorted(required - set(df.columns))
    if missing:
        issues.append(ValidationIssue("error", "missing_columns", f"Missing columns: {missing}", len(df)))
        return issues

    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    invalid_time = int(work["time"].isna().sum())
    if invalid_time:
        issues.append(ValidationIssue("error", "invalid_time", "Rows with invalid time", invalid_time))

    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    null_prices = int(work[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if null_prices:
        issues.append(ValidationIssue("error", "null_price", "Rows with null OHLC", null_prices))

    checks = [
        ("high_lt_open", work["high"] < work["open"]),
        ("high_lt_close", work["high"] < work["close"]),
        ("high_lt_low", work["high"] < work["low"]),
        ("low_gt_open", work["low"] > work["open"]),
        ("low_gt_close", work["low"] > work["close"]),
        ("negative_volume", work["volume"] < 0),
    ]
    for code, mask in checks:
        rows = int(mask.fillna(False).sum())
        if rows:
            issues.append(ValidationIssue("error", code, code.replace("_", " "), rows))

    duplicate_keys = int(work.duplicated(subset=["instrument_id", "time"]).sum())
    if duplicate_keys:
        issues.append(ValidationIssue("error", "duplicate_keys", "Duplicate (instrument_id, time)", duplicate_keys))

    if expiry_date is not None:
        expiry_end = pd.Timestamp(expiry_date).normalize() + pd.Timedelta(days=1)
        after_expiry = int((work["time"] >= expiry_end).fillna(False).sum())
        if after_expiry:
            issues.append(ValidationIssue("error", "after_expiry", "Rows after expiry session", after_expiry))

    price_values = work[["open", "high", "low", "close"]].stack().dropna()
    if not price_values.empty:
        off_grid = ((price_values * 10).round() - price_values * 10).abs() > 1e-6
        rows = int(off_grid.sum())
        if rows:
            issues.append(ValidationIssue("warning", "tick_size_off_grid", "Prices not aligned to 0.1 index-point tick", rows))
    return issues


def contract_files(version: str, resolution: str, symbol: str | None = None) -> list[Path]:
    root = data_root() / "vn" / "futures" / "contracts" / resolution
    if symbol:
        root = root / f"symbol={symbol}"
    if not root.exists():
        return []
    return sorted(root.rglob("part.parquet"))


def _instrument_expiry_map(version: str) -> dict[int, pd.Timestamp]:
    path = instrument_dimension_path(version)
    if not path.exists():
        return {}
    df = pd.read_parquet(path, columns=["instrument_id", "expiry_date"], engine="pyarrow")
    df["expiry_date"] = pd.to_datetime(df["expiry_date"], errors="coerce")
    return {int(row.instrument_id): row.expiry_date for row in df.itertuples() if pd.notna(row.expiry_date)}


def validate_storage(*, version: str = "v1", resolutions: list[str] | None = None, symbols: list[str] | None = None) -> dict[str, object]:
    report = ValidationReport(version=version)
    expiry_map = _instrument_expiry_map(version)
    selected_resolutions = resolutions or ["1m", "1d"]
    selected_symbols = symbols or [None]
    duplicate_frames = []

    for resolution in selected_resolutions:
        for symbol in selected_symbols:
            for path in contract_files(version, resolution, symbol=symbol):
                report.files += 1
                try:
                    df = read_partition_file(path)
                except Exception as exc:
                    report.add("error", "read_error", f"{path}: {exc}")
                    continue
                report.rows += int(len(df))
                expiry = None
                if not df.empty and "instrument_id" in df.columns:
                    instrument_ids = pd.to_numeric(df["instrument_id"], errors="coerce").dropna().unique()
                    if len(instrument_ids) == 1:
                        expiry = expiry_map.get(int(instrument_ids[0]))
                for issue in validate_contract_frame(df, expiry_date=expiry):
                    report.add(issue.severity, issue.code, f"{path}: {issue.message}", issue.rows)
                if "instrument_id" in df.columns and "time" in df.columns:
                    duplicate_frames.append(df[["instrument_id", "time"]].copy())

    if duplicate_frames:
        keys = pd.concat(duplicate_frames, ignore_index=True)
        keys["time"] = pd.to_datetime(keys["time"], errors="coerce")
        report.duplicate_keys = int(keys.duplicated(subset=["instrument_id", "time"]).sum())
        if report.duplicate_keys:
            report.add("error", "duplicate_keys_global", "Duplicate keys across partitions", report.duplicate_keys)

    out = state_root() / "vn_derivatives" / f"contracts_validation_{version}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True, default=str))
    tmp.replace(out)
    return report.to_dict() | {"path": str(out)}

