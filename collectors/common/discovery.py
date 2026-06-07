from __future__ import annotations

import csv
import gzip
from pathlib import Path
from typing import Iterable

import pandas as pd


def parse_time(value: object) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert(None) if hasattr(ts, "tz_convert") else ts.tz_localize(None)
    return pd.Timestamp(ts)


def tail_csv_row(path: Path) -> dict[str, str] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None

    opener = gzip.open if path.suffix == ".gz" else open
    try:
        with opener(path, "rt", newline="") as fh:
            header_line = fh.readline()
            if not header_line:
                return None
            headers = next(csv.reader([header_line]))
            last_line = None
            for line in fh:
                if line.strip():
                    last_line = line
            if not last_line:
                return None
            values = next(csv.reader([last_line]))
            return dict(zip(headers, values, strict=False))
    except Exception:
        return None


def latest_time_from_csv(path: Path, time_cols: Iterable[str]) -> pd.Timestamp | None:
    row = tail_csv_row(path)
    if not row:
        return None
    for col in time_cols:
        if col in row:
            ts = parse_time(row[col])
            if ts is not None:
                return ts
    return None


def latest_time_from_parquet(path: Path, time_cols: Iterable[str]) -> pd.Timestamp | None:
    if not path.exists():
        return None
    for col in time_cols:
        try:
            df = pd.read_parquet(path, columns=[col])
        except Exception:
            continue
        if not df.empty:
            return parse_time(df[col].max())
    return None


def latest_time_from_files(paths: Iterable[Path], time_cols: Iterable[str]) -> pd.Timestamp | None:
    latest: pd.Timestamp | None = None
    for path in paths:
        if path.suffix == ".parquet":
            ts = latest_time_from_parquet(path, time_cols)
        else:
            ts = latest_time_from_csv(path, time_cols)
        if ts is not None and (latest is None or ts > latest):
            latest = ts
    return latest


def latest_time_from_globs(patterns: Iterable[Path], time_cols: Iterable[str]) -> pd.Timestamp | None:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(pattern.parent.glob(pattern.name))
    return latest_time_from_files(paths, time_cols)


def max_timestamp(*values: object) -> pd.Timestamp | None:
    latest: pd.Timestamp | None = None
    for value in values:
        ts = parse_time(value)
        if ts is not None and (latest is None or ts > latest):
            latest = ts
    return latest

