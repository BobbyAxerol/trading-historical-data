from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

from collectors.common.env import data_root, load_environment, state_root
from collectors.common.manifest import utc_now_iso

DEFAULT_DATETIME_COLUMNS = ("time", "close_time", "sample_time", "ingested_at")


def _atomic_to_parquet(df: pd.DataFrame, path: Path, *, compression: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False, engine="pyarrow", compression=compression)
    os.replace(tmp, path)


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


def should_convert(csv_path: Path, parquet_path: Path, *, overwrite: bool) -> bool:
    if overwrite:
        return True
    if not parquet_path.exists():
        return True
    return parquet_path.stat().st_mtime < csv_path.stat().st_mtime


def parse_datetime_columns(df: pd.DataFrame, columns: tuple[str, ...] = DEFAULT_DATETIME_COLUMNS) -> tuple[pd.DataFrame, list[str]]:
    converted: list[str] = []
    work = df.copy()
    for col in columns:
        if col not in work.columns:
            continue
        raw = work[col]
        non_null = raw.dropna()
        if non_null.empty:
            continue
        parsed = pd.to_datetime(raw, errors="coerce")
        # Only convert columns that are overwhelmingly datetime-like; this keeps
        # accidental object columns stable.
        if parsed.notna().sum() >= max(1, int(non_null.shape[0] * 0.99)):
            try:
                if getattr(parsed.dt, "tz", None) is not None:
                    parsed = parsed.dt.tz_convert(None)
            except Exception:
                pass
            work[col] = parsed
            converted.append(col)
    return work, converted


def _min_max_time(df: pd.DataFrame) -> tuple[str | None, str | None]:
    if "time" not in df.columns or df.empty:
        return None, None
    parsed = pd.to_datetime(df["time"], errors="coerce")
    parsed = parsed.dropna()
    if parsed.empty:
        return None, None
    return str(parsed.min()), str(parsed.max())


def convert_one(
    csv_path: Path,
    *,
    root: Path,
    overwrite: bool,
    dry_run: bool,
    compression: str,
) -> dict[str, Any]:
    parquet_path = csv_path.with_name("part.parquet")
    item: dict[str, Any] = {
        "csv_path": _relative(csv_path, root),
        "parquet_path": _relative(parquet_path, root),
        "status": "pending",
        "rows": 0,
        "columns": [],
        "datetime_columns": [],
        "csv_size_bytes": csv_path.stat().st_size if csv_path.exists() else 0,
        "parquet_size_bytes": parquet_path.stat().st_size if parquet_path.exists() else 0,
        "min_time": None,
        "max_time": None,
        "error": None,
    }

    if not should_convert(csv_path, parquet_path, overwrite=overwrite):
        item["status"] = "skipped_up_to_date"
        return item

    if dry_run:
        item["status"] = "dry_run_convert"
        return item

    started = time.time()
    try:
        raw = pd.read_csv(csv_path, compression="gzip")
        df, converted_cols = parse_datetime_columns(raw)
        _atomic_to_parquet(df, parquet_path, compression=compression)
        loaded_meta = pd.read_parquet(parquet_path, engine="pyarrow")
        if len(loaded_meta) != len(raw):
            raise RuntimeError(f"row_count_mismatch csv={len(raw)} parquet={len(loaded_meta)}")
        if list(loaded_meta.columns) != list(raw.columns):
            raise RuntimeError("column_order_mismatch")
        min_time, max_time = _min_max_time(loaded_meta)
        item.update(
            {
                "status": "converted",
                "rows": int(len(raw)),
                "columns": list(raw.columns),
                "datetime_columns": converted_cols,
                "parquet_size_bytes": parquet_path.stat().st_size,
                "min_time": min_time,
                "max_time": max_time,
                "duration_seconds": round(time.time() - started, 3),
            }
        )
    except Exception as exc:
        item["status"] = "error"
        item["error"] = str(exc)
    return item


def summarize(items: list[dict[str, Any]], *, root: Path, dataset_prefix: str | None, dry_run: bool, workers: int, compression: str) -> dict[str, Any]:
    converted = [item for item in items if item["status"] == "converted"]
    skipped = [item for item in items if item["status"] == "skipped_up_to_date"]
    dry = [item for item in items if item["status"] == "dry_run_convert"]
    errors = [item for item in items if item["status"] == "error"]
    return {
        "tool": "convert_csv_gz_to_parquet",
        "updated_at": utc_now_iso(),
        "data_root": str(root),
        "dataset_prefix": dataset_prefix,
        "dry_run": dry_run,
        "workers": workers,
        "compression": compression,
        "total_files": len(items),
        "converted_files": len(converted),
        "skipped_files": len(skipped),
        "dry_run_files": len(dry),
        "error_files": len(errors),
        "csv_size_bytes": int(sum(item.get("csv_size_bytes") or 0 for item in items)),
        "parquet_size_bytes": int(sum(item.get("parquet_size_bytes") or 0 for item in items)),
        "rows_converted": int(sum(item.get("rows") or 0 for item in converted)),
        "errors": errors[:100],
        "items": items,
    }


def write_report(report: dict[str, Any], report_path: Path) -> None:
    import json

    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True))
    os.replace(tmp, report_path)


def run_conversion(
    *,
    dataset_prefix: str | None,
    workers: int,
    overwrite: bool,
    dry_run: bool,
    compression: str,
    report_path: Path | None = None,
) -> dict[str, Any]:
    root = data_root()
    csv_paths = discover_csv_parts(root, dataset_prefix)
    max_workers = max(1, int(workers))

    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                convert_one,
                path,
                root=root,
                overwrite=overwrite,
                dry_run=dry_run,
                compression=compression,
            )
            for path in csv_paths
        ]
        for future in as_completed(futures):
            items.append(future.result())

    items = sorted(items, key=lambda item: item["csv_path"])
    report = summarize(items, root=root, dataset_prefix=dataset_prefix, dry_run=dry_run, workers=max_workers, compression=compression)
    write_report(report, report_path or (state_root() / "parquet_migration_report.json"))
    return report


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Convert _get_data part.csv.gz partitions to part.parquet files.")
    parser.add_argument("--dataset", default=None, help="Optional storage-relative prefix, e.g. crypto/binance_futures_metrics/5m")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    report = run_conversion(
        dataset_prefix=args.dataset,
        workers=args.workers,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        compression=args.compression,
        report_path=Path(args.report).resolve() if args.report else None,
    )
    print(
        "csv.gz -> parquet report: "
        f"total={report['total_files']} converted={report['converted_files']} "
        f"dry_run={report['dry_run_files']} skipped={report['skipped_files']} errors={report['error_files']}"
    )


if __name__ == "__main__":
    main()

