from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from collectors.common.env import data_root, load_environment, state_root
from collectors.common.manifest import utc_now_iso

FEATURES = ("open", "high", "low", "close", "volume")
DATASETS = {
    "binance_daily_matrix": ("crypto", "binance_daily_matrix"),
    "vn_daily_matrix": ("vn", "equity", "daily_matrix"),
}


def _matrix_dir(dataset: str) -> Path:
    try:
        parts = DATASETS[dataset]
    except KeyError as exc:
        raise ValueError(f"Unsupported matrix dataset: {dataset}") from exc
    return data_root().joinpath(*parts)


def _read_csv_matrix(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, compression="gzip", index_col=0)
    return _normalize_matrix(df)


def _read_parquet_matrix(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path, engine="pyarrow")
    return _normalize_matrix(df)


def _normalize_matrix(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work.index = pd.to_datetime(work.index, errors="coerce")
    work = work[~work.index.isna()]
    work = work[~work.index.duplicated(keep="last")].sort_index()
    work.index = work.index.normalize()
    work.index.name = "time"
    work.columns = [str(col).upper() for col in work.columns]
    return work


def _atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, engine="pyarrow", compression="zstd")
    os.replace(tmp, path)


def _values_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    left_num = left.apply(pd.to_numeric, errors="coerce")
    right_num = right.apply(pd.to_numeric, errors="coerce")
    return bool(np.allclose(left_num.to_numpy(dtype=float), right_num.to_numpy(dtype=float), equal_nan=True))


def validate_pair(csv_path: Path, parquet_path: Path) -> dict[str, Any]:
    item: dict[str, Any] = {
        "csv_path": str(csv_path),
        "parquet_path": str(parquet_path),
        "status": "pending",
        "errors": [],
        "csv_shape": None,
        "parquet_shape": None,
        "csv_min_time": None,
        "csv_max_time": None,
        "parquet_min_time": None,
        "parquet_max_time": None,
    }
    if not csv_path.exists():
        item["status"] = "missing_csv"
        return item
    if not parquet_path.exists():
        item["status"] = "error"
        item["errors"].append("missing_parquet")
        return item

    csv_df = _read_csv_matrix(csv_path)
    parquet_df = _read_parquet_matrix(parquet_path)
    errors: list[str] = []
    if csv_df.shape != parquet_df.shape:
        errors.append("shape_mismatch")
    if list(csv_df.columns) != list(parquet_df.columns):
        errors.append("columns_mismatch")
    if not csv_df.index.equals(parquet_df.index):
        errors.append("index_mismatch")
    if not errors and not _values_equal(csv_df, parquet_df):
        errors.append("value_mismatch")

    item.update(
        {
            "status": "error" if errors else "ok",
            "errors": errors,
            "csv_shape": list(csv_df.shape),
            "parquet_shape": list(parquet_df.shape),
            "csv_min_time": str(csv_df.index.min()) if not csv_df.empty else None,
            "csv_max_time": str(csv_df.index.max()) if not csv_df.empty else None,
            "parquet_min_time": str(parquet_df.index.min()) if not parquet_df.empty else None,
            "parquet_max_time": str(parquet_df.index.max()) if not parquet_df.empty else None,
        }
    )
    return item


def migrate_feature(
    feature: str,
    *,
    dry_run: bool,
    overwrite: bool,
    cleanup_csv: bool,
    confirm: bool,
    matrix_dir: Path,
) -> dict[str, Any]:
    csv_path = matrix_dir / f"{feature}.csv.gz"
    parquet_path = matrix_dir / f"{feature}.parquet"
    item: dict[str, Any] = {
        "feature": feature,
        "csv_path": str(csv_path),
        "parquet_path": str(parquet_path),
        "status": "pending",
        "converted": False,
        "deleted_csv": False,
        "csv_size_bytes": csv_path.stat().st_size if csv_path.exists() else 0,
        "parquet_size_bytes": parquet_path.stat().st_size if parquet_path.exists() else 0,
        "validation": None,
        "errors": [],
    }

    if not csv_path.exists() and not parquet_path.exists():
        item["status"] = "missing_source"
        item["errors"].append("missing_csv_and_parquet")
        return item

    should_convert = csv_path.exists() and (overwrite or not parquet_path.exists() or parquet_path.stat().st_mtime < csv_path.stat().st_mtime)
    if should_convert:
        if dry_run:
            item["status"] = "dry_run_convert"
            return item
        df = _read_csv_matrix(csv_path)
        _atomic_write_parquet(df, parquet_path)
        csv_mtime = csv_path.stat().st_mtime
        os.utime(parquet_path, (csv_mtime, csv_mtime))
        item["converted"] = True
        item["parquet_size_bytes"] = parquet_path.stat().st_size

    if csv_path.exists():
        validation = validate_pair(csv_path, parquet_path)
        item["validation"] = validation
        if validation["status"] != "ok":
            item["status"] = "blocked_validation_error"
            item["errors"].extend(validation["errors"])
            return item
        if parquet_path.stat().st_mtime < csv_path.stat().st_mtime:
            item["status"] = "blocked_parquet_older_than_csv"
            item["errors"].append("parquet_older_than_csv")
            return item
        if cleanup_csv:
            if dry_run or not confirm:
                item["status"] = "dry_run_delete_csv"
                return item
            csv_path.unlink()
            item["deleted_csv"] = True
    elif parquet_path.exists():
        item["validation"] = {"status": "parquet_only"}

    item["status"] = "ok"
    return item


def write_report(report: dict[str, Any], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True))
    os.replace(tmp, report_path)


def run_migration(
    *,
    dry_run: bool,
    overwrite: bool,
    cleanup_csv: bool,
    confirm: bool,
    dataset: str = "binance_daily_matrix",
    report_path: Path | None = None,
) -> dict[str, Any]:
    matrix_dir = _matrix_dir(dataset)
    items = [
        migrate_feature(
            feature,
            dry_run=dry_run,
            overwrite=overwrite,
            cleanup_csv=cleanup_csv,
            confirm=confirm,
            matrix_dir=matrix_dir,
        )
        for feature in FEATURES
    ]
    errors = [item for item in items if item.get("errors")]
    report = {
        "tool": "migrate_binance_daily_matrix_parquet",
        "dataset": dataset,
        "updated_at": utc_now_iso(),
        "matrix_dir": str(matrix_dir),
        "dry_run": dry_run,
        "overwrite": overwrite,
        "cleanup_csv": cleanup_csv,
        "confirm": confirm,
        "total_features": len(items),
        "ok_features": len(items) - len(errors),
        "error_features": len(errors),
        "converted_features": int(sum(1 for item in items if item.get("converted"))),
        "deleted_csv_features": int(sum(1 for item in items if item.get("deleted_csv"))),
        "csv_size_bytes": int(sum(item.get("csv_size_bytes") or 0 for item in items)),
        "parquet_size_bytes": int(sum(item.get("parquet_size_bytes") or 0 for item in items)),
        "errors": errors,
        "items": items,
    }
    default_report = (
        "binance_daily_matrix_parquet_migration_report.json"
        if dataset == "binance_daily_matrix"
        else f"{dataset}_parquet_migration_report.json"
    )
    write_report(report, report_path or (state_root() / default_report))
    return report


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Migrate daily matrix CSV.GZ files to Parquet.")
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="binance_daily_matrix")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--cleanup-csv", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    report = run_migration(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        cleanup_csv=args.cleanup_csv,
        confirm=args.confirm,
        dataset=args.dataset,
        report_path=Path(args.report).resolve() if args.report else None,
    )
    print(
        f"{args.dataset} parquet migration: "
        f"total={report['total_features']} ok={report['ok_features']} errors={report['error_features']} "
        f"converted={report['converted_features']} deleted_csv={report['deleted_csv_features']} "
        f"csv_size_bytes={report['csv_size_bytes']} parquet_size_bytes={report['parquet_size_bytes']}"
    )
    if report["error_features"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
