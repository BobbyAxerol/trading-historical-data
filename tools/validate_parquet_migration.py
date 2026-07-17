from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from collectors.common.env import data_root, load_environment, state_root
from collectors.common.manifest import utc_now_iso
from collectors.common.storage import DATETIME_COLUMNS, normalize_common_datetime_columns

VOLATILE_COMPARE_COLUMNS = {"source", "ingested_at", "close_time"}


def _relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def discover_csv_parts(root: Path, dataset_prefix: str | None = None) -> list[Path]:
    search_root = root / dataset_prefix.strip("/") if dataset_prefix else root
    if not search_root.exists():
        return []
    return sorted(search_root.glob("**/part.csv.gz"))


def _time_col(columns: list[str]) -> str | None:
    for col in ("time", "snapshot_time", "sample_time"):
        if col in columns:
            return col
    return None


def _key_cols(columns: list[str], time_col: str | None) -> list[str]:
    if time_col is None:
        return []
    for entity_col in ("symbol", "underlying"):
        if entity_col in columns:
            return [entity_col, time_col]
    return [time_col]


def _csv_header_and_rows(path: Path) -> tuple[list[str], int]:
    with gzip.open(path, "rt") as handle:
        header = handle.readline().rstrip("\n\r").split(",")
        rows = sum(1 for _ in handle)
    return header, rows


def _parquet_header_and_rows(path: Path) -> tuple[list[str], int]:
    parquet_file = pq.ParquetFile(path)
    return parquet_file.schema.names, parquet_file.metadata.num_rows


def _read_csv(path: Path, *, usecols: list[str] | None = None) -> pd.DataFrame:
    return normalize_common_datetime_columns(pd.read_csv(path, compression="gzip", usecols=usecols))


def _read_parquet(path: Path, *, columns: list[str] | None = None) -> pd.DataFrame:
    return normalize_common_datetime_columns(pd.read_parquet(path, columns=columns, engine="pyarrow"))


def _read_csv_sample(path: Path, *, indices: list[int], usecols: list[str]) -> pd.DataFrame:
    if not indices:
        return pd.DataFrame(columns=usecols)
    wanted = set(indices)
    return normalize_common_datetime_columns(
        pd.read_csv(
            path,
            compression="gzip",
            usecols=usecols,
            skiprows=lambda row_number: row_number > 0 and (row_number - 1) not in wanted,
        )
    )


def _time_bounds(df: pd.DataFrame, time_col: str | None) -> tuple[str | None, str | None, int]:
    if time_col is None or time_col not in df.columns:
        return None, None, 0
    parsed = pd.to_datetime(df[time_col], errors="coerce")
    null_count = int(parsed.isna().sum())
    parsed = parsed.dropna()
    if parsed.empty:
        return None, None, null_count
    return str(parsed.min()), str(parsed.max()), null_count


def _duplicate_count(df: pd.DataFrame, key_cols: list[str]) -> int:
    if not key_cols or not set(key_cols).issubset(df.columns):
        return 0
    return int(df.duplicated(subset=key_cols).sum())


def _missing_keys(csv_df: pd.DataFrame, parquet_df: pd.DataFrame, key_cols: list[str]) -> int:
    if not key_cols or not set(key_cols).issubset(csv_df.columns) or not set(key_cols).issubset(parquet_df.columns):
        return 0
    csv_keys = csv_df[key_cols].drop_duplicates()
    parquet_keys = parquet_df[key_cols].drop_duplicates()
    merged = csv_keys.merge(parquet_keys, on=key_cols, how="left", indicator=True)
    return int((merged["_merge"] == "left_only").sum())


def _sample_indices(length: int, sample_rows: int) -> list[int]:
    if length <= 0 or sample_rows <= 0:
        return []
    if length <= sample_rows:
        return list(range(length))
    return sorted(set(np.linspace(0, length - 1, sample_rows, dtype=int).tolist()))


def _numeric_equal(left: pd.Series, right: pd.Series) -> bool:
    left_num = pd.to_numeric(left, errors="coerce")
    right_num = pd.to_numeric(right, errors="coerce")
    mask = left_num.notna() | right_num.notna()
    if not mask.any():
        return False
    return bool(np.allclose(left_num[mask].to_numpy(dtype=float), right_num[mask].to_numpy(dtype=float), equal_nan=True))


def _series_equal(left: pd.Series, right: pd.Series, column: str) -> bool:
    if column in DATETIME_COLUMNS:
        left_dt = pd.to_datetime(left, errors="coerce")
        right_dt = pd.to_datetime(right, errors="coerce")
        return bool(left_dt.equals(right_dt))
    if _numeric_equal(left, right):
        return True
    left_obj = left.astype("string").fillna("<NA>")
    right_obj = right.astype("string").fillna("<NA>")
    return bool(left_obj.equals(right_obj))


def _sample_mismatches(
    csv_path: Path,
    parquet_path: Path,
    *,
    csv_rows: int,
    columns: list[str],
    key_cols: list[str],
    sample_rows: int,
) -> list[str]:
    if not key_cols:
        return []
    indices = _sample_indices(csv_rows, sample_rows)
    if not indices:
        return []
    compare_cols = [col for col in columns if col not in VOLATILE_COMPARE_COLUMNS]
    read_cols = list(dict.fromkeys(key_cols + compare_cols))
    csv_unique = _read_csv_sample(csv_path, indices=indices, usecols=read_cols)
    parquet_unique = _read_parquet(parquet_path, columns=[col for col in read_cols if col in columns])
    if not key_cols or not set(key_cols).issubset(csv_unique.columns) or not set(key_cols).issubset(parquet_unique.columns):
        return []
    csv_unique = csv_unique.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
    parquet_unique = parquet_unique.drop_duplicates(subset=key_cols, keep="last")
    merged = csv_unique.merge(parquet_unique, on=key_cols, how="left", suffixes=("_csv", "_parquet"), indicator=True)
    mismatches: list[str] = []
    if (merged["_merge"] == "left_only").any():
        mismatches.append("sample_missing_keys")
        return mismatches
    compare_cols = [
        col
        for col in compare_cols
        if col in csv_unique.columns and col in parquet_unique.columns and col not in key_cols
    ]
    for col in compare_cols:
        left = merged[f"{col}_csv"]
        right = merged[f"{col}_parquet"]
        if not _series_equal(left, right, col):
            mismatches.append(col)
    return mismatches


def validate_one(csv_path: Path, *, root: Path, sample_rows: int) -> dict[str, Any]:
    parquet_path = csv_path.with_name("part.parquet")
    item: dict[str, Any] = {
        "csv_path": _relative(csv_path, root),
        "parquet_path": _relative(parquet_path, root),
        "status": "pending",
        "errors": [],
        "warnings": [],
        "csv_rows": 0,
        "parquet_rows": 0,
        "row_delta": 0,
        "columns": [],
        "time_col": None,
        "csv_min_time": None,
        "csv_max_time": None,
        "parquet_min_time": None,
        "parquet_max_time": None,
        "csv_null_time": 0,
        "parquet_null_time": 0,
        "parquet_duplicate_keys": 0,
        "csv_keys_missing_in_parquet": 0,
        "sample_mismatch_columns": [],
        "parquet_newer_than_csv": False,
    }
    started = time.time()
    if not parquet_path.exists():
        item["status"] = "error"
        item["errors"].append("missing_parquet")
        return item

    try:
        csv_cols, csv_rows = _csv_header_and_rows(csv_path)
        parquet_cols, parquet_rows = _parquet_header_and_rows(parquet_path)
        parquet_newer_than_csv = parquet_path.stat().st_mtime > csv_path.stat().st_mtime
        time_col = _time_col(csv_cols)
        key_cols = _key_cols(csv_cols, time_col)
        key_read_cols = [col for col in key_cols if col in csv_cols and col in parquet_cols]
        if key_read_cols:
            csv_key_df = _read_csv(csv_path, usecols=key_read_cols)
            parquet_key_df = _read_parquet(parquet_path, columns=key_read_cols)
        else:
            csv_key_df = pd.DataFrame()
            parquet_key_df = pd.DataFrame()
        csv_min, csv_max, csv_null_time = _time_bounds(csv_key_df, time_col)
        parquet_min, parquet_max, parquet_null_time = _time_bounds(parquet_key_df, time_col)
        duplicate_keys = _duplicate_count(parquet_key_df, key_cols)
        missing_keys = _missing_keys(csv_key_df, parquet_key_df, key_cols)
        sample_mismatches = _sample_mismatches(
            csv_path,
            parquet_path,
            csv_rows=csv_rows,
            columns=[col for col in csv_cols if col in parquet_cols],
            key_cols=key_cols,
            sample_rows=sample_rows,
        )

        errors: list[str] = []
        warnings: list[str] = []
        if csv_cols != parquet_cols:
            errors.append("column_order_mismatch")
        if parquet_rows < csv_rows and missing_keys:
            errors.append("parquet_has_fewer_rows_and_missing_keys")
        if time_col and parquet_null_time:
            errors.append("parquet_null_time")
        if duplicate_keys:
            errors.append("parquet_duplicate_keys")
        if missing_keys:
            errors.append("csv_keys_missing_in_parquet")
        if csv_min and parquet_min and pd.Timestamp(parquet_min) > pd.Timestamp(csv_min):
            errors.append("parquet_min_time_after_csv")
        if csv_max and parquet_max and pd.Timestamp(parquet_max) < pd.Timestamp(csv_max):
            errors.append("parquet_max_time_before_csv")
        if sample_mismatches and parquet_newer_than_csv and not missing_keys:
            warnings.append("sample_value_mismatch_parquet_newer_than_csv")
        elif sample_mismatches:
            errors.append("sample_value_mismatch")

        item.update(
            {
                "status": "error" if errors else "ok",
                "errors": errors,
                "warnings": warnings,
                "csv_rows": int(csv_rows),
                "parquet_rows": int(parquet_rows),
                "row_delta": int(parquet_rows - csv_rows),
                "columns": csv_cols,
                "time_col": time_col,
                "csv_min_time": csv_min,
                "csv_max_time": csv_max,
                "parquet_min_time": parquet_min,
                "parquet_max_time": parquet_max,
                "csv_null_time": csv_null_time,
                "parquet_null_time": parquet_null_time,
                "parquet_duplicate_keys": duplicate_keys,
                "csv_keys_missing_in_parquet": missing_keys,
                "sample_mismatch_columns": sample_mismatches,
                "parquet_newer_than_csv": parquet_newer_than_csv,
                "duration_seconds": round(time.time() - started, 3),
            }
        )
    except Exception as exc:
        item["status"] = "error"
        item["errors"].append(str(exc))
    return item


def summarize(items: list[dict[str, Any]], *, root: Path, dataset_prefix: str | None, workers: int, sample_rows: int) -> dict[str, Any]:
    errors = [item for item in items if item["status"] != "ok"]
    warnings = [item for item in items if item.get("warnings")]
    return {
        "tool": "validate_parquet_migration",
        "updated_at": utc_now_iso(),
        "data_root": str(root),
        "dataset_prefix": dataset_prefix,
        "workers": workers,
        "sample_rows": sample_rows,
        "total_files": len(items),
        "ok_files": len(items) - len(errors),
        "error_files": len(errors),
        "warning_files": len(warnings),
        "csv_rows": int(sum(item.get("csv_rows") or 0 for item in items)),
        "parquet_rows": int(sum(item.get("parquet_rows") or 0 for item in items)),
        "row_delta": int(sum(item.get("row_delta") or 0 for item in items)),
        "parquet_duplicate_keys": int(sum(item.get("parquet_duplicate_keys") or 0 for item in items)),
        "csv_keys_missing_in_parquet": int(sum(item.get("csv_keys_missing_in_parquet") or 0 for item in items)),
        "errors": errors[:100],
        "warnings": warnings[:100],
        "items": items,
    }


def write_report(report: dict[str, Any], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True))
    os.replace(tmp, report_path)


def run_validation(
    *,
    dataset_prefix: str | None,
    workers: int,
    sample_rows: int,
    report_path: Path | None = None,
) -> dict[str, Any]:
    root = data_root()
    csv_paths = discover_csv_parts(root, dataset_prefix)
    max_workers = max(1, int(workers))
    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(validate_one, path, root=root, sample_rows=sample_rows) for path in csv_paths]
        for future in as_completed(futures):
            items.append(future.result())
    items = sorted(items, key=lambda item: item["csv_path"])
    report = summarize(items, root=root, dataset_prefix=dataset_prefix, workers=max_workers, sample_rows=sample_rows)
    write_report(report, report_path or (state_root() / "parquet_validation_report.json"))
    return report


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Validate _get_data part.csv.gz partitions against part.parquet files.")
    parser.add_argument("--dataset", default=None, help="Optional storage-relative prefix, e.g. crypto/binance_futures/1m")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--sample-rows", type=int, default=25)
    parser.add_argument("--report", default=None)
    args = parser.parse_args()
    report = run_validation(
        dataset_prefix=args.dataset,
        workers=args.workers,
        sample_rows=args.sample_rows,
        report_path=Path(args.report).resolve() if args.report else None,
    )
    print(
        "parquet validation report: "
        f"total={report['total_files']} ok={report['ok_files']} errors={report['error_files']} "
        f"warnings={report['warning_files']} "
        f"row_delta={report['row_delta']} missing_keys={report['csv_keys_missing_in_parquet']} "
        f"duplicate_keys={report['parquet_duplicate_keys']}"
    )
    if report["error_files"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
