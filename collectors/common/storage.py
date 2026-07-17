from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import pandas as pd

from .discovery import latest_time_from_files
from .env import data_root
from .locks import FileLock


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


def _atomic_to_csv_gz(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, index=False, compression="gzip")
    os.replace(tmp, path)


def _atomic_to_parquet(df: pd.DataFrame, path: Path, *, compression: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False, engine="pyarrow", compression=compression)
    os.replace(tmp, path)


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
        if self.partition == "month":
            path = path / f"month={when.month:02d}"
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
        if self.partition == "month":
            path = path / f"month={when.month:02d}"
        return path / self.filename

    def files(self, attrs: dict[str, str]) -> list[Path]:
        path = self.root
        for key, value in attrs.items():
            path = path / f"{key}={value}"
        return list(path.glob(f"year=*/{self.filename}")) + list(path.glob(f"year=*/month=*/{self.filename}"))

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
                    existing = pd.read_parquet(path, engine="pyarrow")
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

        return {"rows_written": rows_written, "latest_time": latest.isoformat()}
