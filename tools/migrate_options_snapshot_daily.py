from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from collectors.common.env import data_root, load_environment, state_root
from collectors.common.manifest import utc_now_iso
from collectors.common.storage import normalize_common_datetime_columns, release_unused_memory

DATASET_PATH = ("options", "binance", "snapshot_5m")
TIME_COL = "snapshot_time"
KEY_COLS = ["snapshot_time", "symbol"]


def _root() -> Path:
    return data_root().joinpath(*DATASET_PATH)


def _monthly_files(root: Path) -> list[Path]:
    return sorted(path for path in root.glob("underlying=*/year=*/month=*/part.parquet") if "day=" not in path.as_posix())


def _read(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path, engine="pyarrow")
    df = normalize_common_datetime_columns(df)
    if TIME_COL not in df.columns and "time" in df.columns:
        df = df.rename(columns={"time": TIME_COL})
    if TIME_COL in df.columns:
        df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
    return df


def _write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    save = normalize_common_datetime_columns(df)
    save.to_parquet(tmp, index=False, engine="pyarrow", compression="zstd")
    os.replace(tmp, path)


def _daily_path(monthly_path: Path, snapshot_time: pd.Timestamp) -> Path:
    return monthly_path.parent / f"day={snapshot_time.day:02d}" / "part.parquet"


def _key_frame(df: pd.DataFrame) -> pd.DataFrame:
    cols = [col for col in KEY_COLS if col in df.columns]
    if set(cols) != set(KEY_COLS):
        return pd.DataFrame(columns=KEY_COLS)
    result = df[KEY_COLS].copy()
    result[TIME_COL] = pd.to_datetime(result[TIME_COL], errors="coerce")
    return result.dropna(subset=[TIME_COL]).drop_duplicates()


def _missing_keys(source: pd.DataFrame, target: pd.DataFrame) -> int:
    source_keys = _key_frame(source)
    target_keys = _key_frame(target)
    if source_keys.empty:
        return 0
    if target_keys.empty:
        return int(len(source_keys))
    merged = source_keys.merge(target_keys, on=KEY_COLS, how="left", indicator=True)
    return int((merged["_merge"] == "left_only").sum())


def migrate_file(path: Path, *, dry_run: bool, cleanup_monthly: bool, confirm: bool) -> dict[str, Any]:
    item: dict[str, Any] = {
        "monthly_path": str(path),
        "status": "pending",
        "rows": 0,
        "daily_files": [],
        "converted": False,
        "deleted_monthly": False,
        "missing_keys": 0,
        "errors": [],
    }
    try:
        df = _read(path)
        if TIME_COL not in df.columns:
            item["status"] = "error"
            item["errors"].append("missing_snapshot_time")
            return item
        df = df.dropna(subset=[TIME_COL])
        item["rows"] = int(len(df))
        if df.empty:
            item["status"] = "empty"
            return item

        daily_paths: list[Path] = []
        for _, day_df in df.groupby(df[TIME_COL].dt.normalize(), sort=True):
            day_ts = pd.Timestamp(day_df[TIME_COL].iloc[0])
            out = _daily_path(path, day_ts)
            daily_paths.append(out)
            if dry_run:
                continue
            if out.exists():
                existing = _read(out)
                combined = pd.concat([existing, day_df], ignore_index=True)
            else:
                combined = day_df.copy()
            combined[TIME_COL] = pd.to_datetime(combined[TIME_COL], errors="coerce")
            combined = (
                combined.dropna(subset=[TIME_COL])
                .drop_duplicates(subset=KEY_COLS, keep="last")
                .sort_values(KEY_COLS)
                .reset_index(drop=True)
            )
            _write(combined, out)
            del combined
            if "existing" in locals():
                del existing
            release_unused_memory()

        item["daily_files"] = [str(out) for out in sorted(set(daily_paths))]
        if dry_run:
            item["status"] = "dry_run"
            return item
        item["converted"] = True

        target_frames = [_read(out) for out in sorted(set(daily_paths)) if out.exists()]
        target = pd.concat(target_frames, ignore_index=True) if target_frames else pd.DataFrame()
        missing = _missing_keys(df, target)
        item["missing_keys"] = missing
        if missing:
            item["status"] = "blocked_validation_error"
            item["errors"].append("daily_missing_monthly_keys")
            return item

        if cleanup_monthly:
            if not confirm:
                item["status"] = "dry_run_delete_monthly"
                return item
            path.unlink()
            item["deleted_monthly"] = True
        item["status"] = "ok"
        return item
    except Exception as exc:
        item["status"] = "error"
        item["errors"].append(str(exc))
        return item
    finally:
        release_unused_memory()


def write_report(report: dict[str, Any], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True))
    os.replace(tmp, report_path)


def run_migration(
    *,
    dry_run: bool,
    cleanup_monthly: bool,
    confirm: bool,
    report_path: Path | None = None,
) -> dict[str, Any]:
    root = _root()
    items = [migrate_file(path, dry_run=dry_run, cleanup_monthly=cleanup_monthly, confirm=confirm) for path in _monthly_files(root)]
    errors = [item for item in items if item.get("errors")]
    report = {
        "tool": "migrate_options_snapshot_daily",
        "updated_at": utc_now_iso(),
        "root": str(root),
        "dry_run": dry_run,
        "cleanup_monthly": cleanup_monthly,
        "confirm": confirm,
        "total_monthly_files": len(items),
        "ok_files": len(items) - len(errors),
        "error_files": len(errors),
        "converted_files": int(sum(1 for item in items if item.get("converted"))),
        "deleted_monthly_files": int(sum(1 for item in items if item.get("deleted_monthly"))),
        "rows": int(sum(item.get("rows") or 0 for item in items)),
        "missing_keys": int(sum(item.get("missing_keys") or 0 for item in items)),
        "errors": errors,
        "items": items,
    }
    write_report(report, report_path or (state_root() / "options_snapshot_daily_migration_report.json"))
    return report


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Migrate Binance options snapshot monthly Parquet files to daily partitions.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cleanup-monthly", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--report", default=None)
    args = parser.parse_args()
    report = run_migration(
        dry_run=args.dry_run,
        cleanup_monthly=args.cleanup_monthly,
        confirm=args.confirm,
        report_path=Path(args.report).resolve() if args.report else None,
    )
    print(
        "options snapshot daily migration: "
        f"monthly={report['total_monthly_files']} ok={report['ok_files']} errors={report['error_files']} "
        f"converted={report['converted_files']} deleted_monthly={report['deleted_monthly_files']} "
        f"rows={report['rows']} missing_keys={report['missing_keys']}"
    )


if __name__ == "__main__":
    main()
