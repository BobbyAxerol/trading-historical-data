from __future__ import annotations

import gc
import os
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa

from .discovery import latest_time_from_files
from .env import data_root
from .locks import FileLock

DATETIME_COLUMNS = ("time", "close_time", "sample_time", "snapshot_time", "ingested_at")


def release_unused_memory() -> None:
    """Best-effort release after large partition rewrites."""
    gc.collect()
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def normalize_datetime(series: pd.Series, *, utc: bool = False) -> pd.Series:
    values = pd.to_datetime(series, errors="coerce", utc=utc)
    if utc:
        return values
    try:
        values = values.dt.tz_localize(None)
    except TypeError:
        try:
            values = values.dt.tz_convert(None)
        except Exception:
            pass
    return values


def _parse_mixed_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_values = numeric.dropna()
    if not numeric_values.empty:
        max_abs = numeric_values.abs().max()
        unit = "ns"
        if max_abs < 10**12:
            unit = "s"
        elif max_abs < 10**14:
            unit = "ms"
        elif max_abs < 10**17:
            unit = "us"
        numeric_parsed = pd.to_datetime(numeric, unit=unit, errors="coerce", utc=True)
        parsed.loc[numeric.notna()] = numeric_parsed.loc[numeric.notna()]
    return parsed


def normalize_common_datetime_columns(df: pd.DataFrame, columns: Iterable[str] = DATETIME_COLUMNS) -> pd.DataFrame:
    work = df.copy()
    for col in columns:
        if col not in work.columns:
            continue
        non_null = work[col].dropna()
        if non_null.empty:
            continue
        parsed = _parse_mixed_datetime(work[col])
        if parsed.notna().sum() < max(1, int(non_null.shape[0] * 0.99)):
            continue
        try:
            parsed = parsed.dt.tz_convert(None)
        except Exception:
            try:
                parsed = parsed.dt.tz_localize(None)
            except Exception:
                pass
        work[col] = parsed
    return work


def _atomic_to_csv_gz(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, index=False, compression="gzip")
    os.replace(tmp, path)


def _atomic_to_parquet(df: pd.DataFrame, path: Path, *, compression: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    save_df = normalize_common_datetime_columns(df)
    save_df.to_parquet(tmp, index=False, engine="pyarrow", compression=compression)
    os.replace(tmp, path)


def read_partition_file(path: Path, *, usecols: list[str] | None = None, nrows: int | None = None) -> pd.DataFrame:
    if path.suffix == ".parquet":
        df = pd.read_parquet(path, columns=usecols, engine="pyarrow")
        return df.head(nrows) if nrows is not None else df
    return pd.read_csv(path, compression="gzip", usecols=usecols, nrows=nrows)


def write_partition_file(df: pd.DataFrame, path: Path, *, compression: str = "zstd") -> None:
    if path.suffix == ".parquet":
        _atomic_to_parquet(df, path, compression=compression)
    else:
        _atomic_to_csv_gz(df, path)


def _select_existing_partition_file(path: Path) -> Path | None:
    if path.exists():
        return path
    if path.suffix == ".parquet":
        fallback = path.with_name("part.csv.gz")
        if fallback.exists():
            return fallback
    return None


class PartitionedCsvGzStore:
    def __init__(
        self,
        dataset_parts: Iterable[str],
        *,
        partition: str = "month",
        filename: str = "part.csv.gz",
    ):
        self.root = data_root().joinpath(*dataset_parts)
        self.partition = partition
        self.filename = filename

    def _partition_path(self, when: pd.Timestamp, attrs: dict[str, str]) -> Path:
        path = self.root
        for key, value in attrs.items():
            path = path / f"{key}={value}"
        path = path / f"year={when.year:04d}"
        if self.partition in {"month", "day"}:
            path = path / f"month={when.month:02d}"
        if self.partition == "day":
            path = path / f"day={when.day:02d}"
        return path / self.filename

    def files(self, attrs: dict[str, str]) -> list[Path]:
        path = self.root
        for key, value in attrs.items():
            path = path / f"{key}={value}"
        return list(path.glob("year=*/part.csv.gz")) + list(path.glob("year=*/month=*/part.csv.gz"))

    def latest_time(self, *, attrs: dict[str, str], time_col: str) -> pd.Timestamp | None:
        return latest_time_from_files(self.files(attrs), [time_col])

    def append(
        self,
        df: pd.DataFrame,
        *,
        time_col: str,
        dedupe_cols: list[str],
        attrs: dict[str, str],
        lock_name: str,
    ) -> dict[str, object]:
        if df.empty:
            return {"rows_written": 0, "latest_time": None}

        work = df.copy()
        work[time_col] = normalize_datetime(work[time_col])
        work = work.dropna(subset=[time_col])
        if work.empty:
            return {"rows_written": 0, "latest_time": None}

        latest = work[time_col].max()
        existing_latest = self.latest_time(attrs=attrs, time_col=time_col)
        if existing_latest is not None and existing_latest > latest:
            latest = existing_latest
        rows_written = 0
        work["_partition_year"] = work[time_col].dt.year
        work["_partition_month"] = work[time_col].dt.month if self.partition == "month" else 1

        with FileLock(lock_name):
            for _, part_df in work.groupby(["_partition_year", "_partition_month"], sort=True):
                part_df = part_df.drop(columns=["_partition_year", "_partition_month"])
                part_when = pd.Timestamp(part_df[time_col].iloc[0])
                path = self._partition_path(part_when, attrs)

                if path.exists():
                    existing = pd.read_csv(path, compression="gzip")
                    existing[time_col] = normalize_datetime(existing[time_col])
                    combined = pd.concat([existing, part_df], ignore_index=True)
                else:
                    combined = part_df

                combined[time_col] = normalize_datetime(combined[time_col])
                combined_latest = combined[time_col].max()
                if pd.notna(combined_latest) and combined_latest > latest:
                    latest = combined_latest
                combined = (
                    combined.dropna(subset=[time_col])
                    .drop_duplicates(subset=dedupe_cols, keep="last")
                    .sort_values(dedupe_cols)
                    .reset_index(drop=True)
                )
                save_df = combined.copy()
                save_df[time_col] = save_df[time_col].dt.strftime("%Y-%m-%d %H:%M:%S")
                _atomic_to_csv_gz(save_df, path)
                rows_written += len(part_df)

        return {"rows_written": rows_written, "latest_time": latest.isoformat()}


class PartitionedParquetStore:
    def __init__(
        self,
        dataset_parts: Iterable[str],
        *,
        partition: str = "month",
        filename: str = "part.parquet",
        compression: str = "zstd",
    ):
        self.root = data_root().joinpath(*dataset_parts)
        self.partition = partition
        self.filename = filename
        self.compression = compression

    def _partition_path(self, when: pd.Timestamp, attrs: dict[str, str]) -> Path:
        path = self.root
        for key, value in attrs.items():
            path = path / f"{key}={value}"
        path = path / f"year={when.year:04d}"
        if self.partition in {"month", "day"}:
            path = path / f"month={when.month:02d}"
        if self.partition == "day":
            path = path / f"day={when.day:02d}"
        return path / self.filename

    def files(self, attrs: dict[str, str]) -> list[Path]:
        path = self.root
        for key, value in attrs.items():
            path = path / f"{key}={value}"
        files: list[Path] = []
        for partition_dir in list(path.glob("year=*")) + list(path.glob("year=*/month=*")) + list(path.glob("year=*/month=*/day=*")):
            if not partition_dir.is_dir():
                continue
            parquet = partition_dir / self.filename
            csv = partition_dir / "part.csv.gz"
            if parquet.exists():
                files.append(parquet)
            elif csv.exists():
                files.append(csv)
        return sorted(files)

    def latest_time(self, *, attrs: dict[str, str], time_col: str) -> pd.Timestamp | None:
        return latest_time_from_files(self.files(attrs), [time_col])

    def append(
        self,
        df: pd.DataFrame,
        *,
        time_col: str,
        dedupe_cols: list[str],
        attrs: dict[str, str],
        lock_name: str,
    ) -> dict[str, object]:
        if df.empty:
            return {"rows_written": 0, "latest_time": None}

        work = df.copy()
        work[time_col] = normalize_datetime(work[time_col])
        work = work.dropna(subset=[time_col])
        if work.empty:
            return {"rows_written": 0, "latest_time": None}

        latest = work[time_col].max()
        existing_latest = self.latest_time(attrs=attrs, time_col=time_col)
        if existing_latest is not None and existing_latest > latest:
            latest = existing_latest
        rows_written = 0
        work["_partition_year"] = work[time_col].dt.year
        work["_partition_month"] = work[time_col].dt.month if self.partition in {"month", "day"} else 1
        work["_partition_day"] = work[time_col].dt.day if self.partition == "day" else 1

        with FileLock(lock_name):
            for _, part_df in work.groupby(["_partition_year", "_partition_month", "_partition_day"], sort=True):
                part_df = part_df.drop(columns=["_partition_year", "_partition_month", "_partition_day"])
                part_when = pd.Timestamp(part_df[time_col].iloc[0])
                path = self._partition_path(part_when, attrs)
                existing = None

                existing_path = _select_existing_partition_file(path)
                if existing_path is not None:
                    existing = read_partition_file(existing_path)
                    existing[time_col] = normalize_datetime(existing[time_col])
                    combined = pd.concat([existing, part_df], ignore_index=True)
                else:
                    combined = part_df

                combined[time_col] = normalize_datetime(combined[time_col])
                combined_latest = combined[time_col].max()
                if pd.notna(combined_latest) and combined_latest > latest:
                    latest = combined_latest
                combined = (
                    combined.dropna(subset=[time_col])
                    .drop_duplicates(subset=dedupe_cols, keep="last")
                    .sort_values(dedupe_cols)
                    .reset_index(drop=True)
                )
                _atomic_to_parquet(combined, path, compression=self.compression)
                rows_written += len(part_df)
                del part_df, combined
                if existing is not None:
                    del existing
                release_unused_memory()

        del work
        release_unused_memory()

        return {"rows_written": rows_written, "latest_time": latest.isoformat()}
