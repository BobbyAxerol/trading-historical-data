from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from collectors.common.env import data_root, load_environment, state_root
from collectors.common.manifest import utc_now_iso
from tools.validate_parquet_migration import discover_csv_parts, validate_one


def _relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def cleanup_one(
    csv_path: Path,
    *,
    root: Path,
    dry_run: bool,
    sample_rows: int,
    allow_warnings: bool,
) -> dict[str, Any]:
    parquet_path = csv_path.with_name("part.parquet")
    validation = validate_one(csv_path, root=root, sample_rows=sample_rows)
    item: dict[str, Any] = {
        "csv_path": _relative(csv_path, root),
        "parquet_path": _relative(parquet_path, root),
        "status": "pending",
        "dry_run": dry_run,
        "csv_size_bytes": csv_path.stat().st_size if csv_path.exists() else 0,
        "errors": list(validation.get("errors") or []),
        "warnings": list(validation.get("warnings") or []),
        "validation": {
            "status": validation.get("status"),
            "csv_rows": validation.get("csv_rows"),
            "parquet_rows": validation.get("parquet_rows"),
            "row_delta": validation.get("row_delta"),
            "csv_keys_missing_in_parquet": validation.get("csv_keys_missing_in_parquet"),
            "parquet_duplicate_keys": validation.get("parquet_duplicate_keys"),
            "parquet_newer_than_csv": validation.get("parquet_newer_than_csv"),
        },
    }

    if validation.get("status") != "ok":
        item["status"] = "blocked_validation_error"
        return item
    if validation.get("warnings") and not allow_warnings:
        item["status"] = "blocked_validation_warning"
        return item
    if not parquet_path.exists():
        item["status"] = "blocked_missing_parquet"
        item["errors"].append("missing_parquet")
        return item
    if parquet_path.stat().st_mtime < csv_path.stat().st_mtime:
        item["status"] = "blocked_parquet_older_than_csv"
        item["errors"].append("parquet_older_than_csv")
        return item
    if dry_run:
        item["status"] = "dry_run_delete"
        return item

    try:
        csv_path.unlink()
        item["status"] = "deleted"
    except Exception as exc:
        item["status"] = "delete_error"
        item["errors"].append(str(exc))
    return item


def summarize(
    items: list[dict[str, Any]],
    *,
    root: Path,
    dataset_prefix: str | None,
    workers: int,
    dry_run: bool,
    sample_rows: int,
    allow_warnings: bool,
) -> dict[str, Any]:
    deleted = [item for item in items if item["status"] == "deleted"]
    dry = [item for item in items if item["status"] == "dry_run_delete"]
    blocked = [item for item in items if item["status"].startswith("blocked_")]
    errors = [item for item in items if item["status"] == "delete_error"]
    return {
        "tool": "cleanup_csv_gz_after_parquet",
        "updated_at": utc_now_iso(),
        "data_root": str(root),
        "dataset_prefix": dataset_prefix,
        "workers": workers,
        "dry_run": dry_run,
        "sample_rows": sample_rows,
        "allow_warnings": allow_warnings,
        "total_files": len(items),
        "deleted_files": len(deleted),
        "dry_run_delete_files": len(dry),
        "blocked_files": len(blocked),
        "error_files": len(errors),
        "reclaimable_bytes": int(sum(item.get("csv_size_bytes") or 0 for item in dry)),
        "deleted_bytes": int(sum(item.get("csv_size_bytes") or 0 for item in deleted)),
        "blocked": blocked[:100],
        "errors": errors[:100],
        "items": items,
    }


def write_report(report: dict[str, Any], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True))
    os.replace(tmp, report_path)


def run_cleanup(
    *,
    dataset_prefix: str | None,
    workers: int,
    dry_run: bool,
    sample_rows: int,
    allow_warnings: bool,
    report_path: Path | None = None,
) -> dict[str, Any]:
    root = data_root()
    csv_paths = discover_csv_parts(root, dataset_prefix)
    max_workers = max(1, int(workers))
    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                cleanup_one,
                path,
                root=root,
                dry_run=dry_run,
                sample_rows=sample_rows,
                allow_warnings=allow_warnings,
            )
            for path in csv_paths
        ]
        for future in as_completed(futures):
            items.append(future.result())

    items = sorted(items, key=lambda item: item["csv_path"])
    report = summarize(
        items,
        root=root,
        dataset_prefix=dataset_prefix,
        workers=max_workers,
        dry_run=dry_run,
        sample_rows=sample_rows,
        allow_warnings=allow_warnings,
    )
    write_report(report, report_path or (state_root() / "parquet_cleanup_report.json"))
    return report


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description="Delete part.csv.gz only after guarded Parquet validation passes.")
    parser.add_argument("--dataset", default=None, help="Optional storage-relative prefix, e.g. crypto/binance_spot/1m")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--sample-rows", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--strict-no-warnings", action="store_true")
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    dry_run = args.dry_run or not args.confirm
    report = run_cleanup(
        dataset_prefix=args.dataset,
        workers=args.workers,
        dry_run=dry_run,
        sample_rows=args.sample_rows,
        allow_warnings=not args.strict_no_warnings,
        report_path=Path(args.report).resolve() if args.report else None,
    )
    print(
        "csv.gz cleanup report: "
        f"total={report['total_files']} deleted={report['deleted_files']} "
        f"dry_run_delete={report['dry_run_delete_files']} blocked={report['blocked_files']} "
        f"errors={report['error_files']} reclaimable_bytes={report['reclaimable_bytes']} "
        f"deleted_bytes={report['deleted_bytes']}"
    )
    if report["blocked_files"] or report["error_files"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
