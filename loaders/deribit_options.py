from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

from collectors.deribit.schema import CANONICAL_TRADE_COLUMNS, SNAPSHOT_5M_COLUMNS

BASE_DIR = Path(__file__).resolve().parents[1]


def _storage_root() -> Path:
    return Path(os.getenv("DATA_ROOT", str(BASE_DIR / "storage"))).resolve()


def _path_list(paths: list[Path]) -> str:
    quoted = [f"'{str(path).replace(chr(39), chr(39) + chr(39))}'" for path in paths]
    return "[" + ", ".join(quoted) + "]"


def _resolve_columns(columns: str | list[str] | tuple[str, ...] | None, default: list[str]) -> list[str] | None:
    if columns == "full":
        return None
    if columns is None:
        return list(default)
    if isinstance(columns, str):
        return [columns]
    seen: set[str] = set()
    resolved: list[str] = []
    for col in columns:
        value = str(col)
        if value not in seen:
            seen.add(value)
            resolved.append(value)
    return resolved


class _DeribitParquetLoaderBase:
    dataset_name: str = ""
    path_parts: tuple[str, ...] = ()
    default_columns: list[str] = []
    timestamp_col: str = "timestamp_ms"

    def _root(self, version: str, currency: str) -> Path:
        return _storage_root().joinpath(*self.path_parts, f"version={version}", f"currency={currency.upper()}")

    def _files(self, *, version: str, currency: str, start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> list[Path]:
        root = self._root(version, currency)
        if not root.exists():
            return []
        paths: list[Path] = []
        for year_dir in root.glob("year=*"):
            try:
                year = int(year_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            if start_ts is not None and year < start_ts.year:
                continue
            if end_ts is not None and year > end_ts.year:
                continue
            for month_dir in year_dir.glob("month=*"):
                try:
                    month = int(month_dir.name.split("=", 1)[1])
                except (IndexError, ValueError):
                    continue
                if start_ts is not None and (year, month) < (start_ts.year, start_ts.month):
                    continue
                if end_ts is not None and (year, month) > (end_ts.year, end_ts.month):
                    continue
                for day_dir in month_dir.glob("day=*"):
                    try:
                        day = int(day_dir.name.split("=", 1)[1])
                    except (IndexError, ValueError):
                        continue
                    day_ts = pd.Timestamp(year=year, month=month, day=day)
                    if start_ts is not None and day_ts.normalize() < start_ts.normalize():
                        continue
                    if end_ts is not None and day_ts.normalize() > end_ts.normalize():
                        continue
                    paths.extend(sorted(day_dir.glob("*.parquet")))
        return sorted(path for path in paths if path.is_file())

    def _empty(self, columns: list[str] | None) -> pd.DataFrame:
        return pd.DataFrame(columns=columns or self.default_columns)

    def _query(
        self,
        *,
        version: str,
        currency: str,
        start_date: str | None,
        end_date: str | None,
        columns: str | list[str] | tuple[str, ...] | None,
        where: list[str] | None = None,
        params: list[Any] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None
        read_columns = _resolve_columns(columns, self.default_columns)
        files = self._files(version=version, currency=currency, start_ts=start_ts, end_ts=end_ts)
        if not files:
            return self._empty(read_columns)

        import duckdb

        selected = "*" if read_columns is None else ", ".join(read_columns)
        filters = [f"{self.timestamp_col} IS NOT NULL"]
        query_params: list[Any] = []
        if start_ts is not None:
            filters.append(f"{self.timestamp_col} >= ?")
            query_params.append(int(start_ts.timestamp() * 1000))
        if end_ts is not None:
            filters.append(f"{self.timestamp_col} <= ?")
            query_params.append(int(end_ts.timestamp() * 1000))
        if where:
            filters.extend(where)
            query_params.extend(params or [])

        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        query = f"""
            SELECT {selected}
            FROM read_parquet({_path_list(files)}, hive_partitioning=true, union_by_name=true)
            WHERE {" AND ".join(filters)}
            ORDER BY {self.timestamp_col}, instrument_id
            {limit_sql}
        """
        con = duckdb.connect(database=":memory:")
        try:
            return con.execute(query, query_params).fetchdf()
        finally:
            con.close()


class DeribitOptionTradesLoader(_DeribitParquetLoaderBase):
    dataset_name = "deribit_option_trades"
    path_parts = ("options", "deribit", "trades")
    default_columns = list(CANONICAL_TRADE_COLUMNS)

    def load(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        currency: str = "BTC",
        instruments: list[int] | int | None = None,
        option_type: str | None = None,
        dte_min: int | None = None,
        dte_max: int | None = None,
        columns: str | list[str] | tuple[str, ...] | None = None,
        version: str = "v1",
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        del option_type, dte_min, dte_max, check_val
        where: list[str] = []
        params: list[Any] = []
        if instruments is not None:
            values = [instruments] if isinstance(instruments, int) else list(instruments)
            placeholders = ", ".join("?" for _ in values)
            where.append(f"instrument_id IN ({placeholders})")
            params.extend(int(value) for value in values)
        return self._query(
            version=version,
            currency=currency,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            where=where,
            params=params,
            limit=limit,
        )


class DeribitOptionSnapshots5mLoader(_DeribitParquetLoaderBase):
    dataset_name = "deribit_option_snapshots_5m"
    path_parts = ("options", "deribit", "snapshot_5m")
    default_columns = list(SNAPSHOT_5M_COLUMNS)

    def load(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        currency: str = "BTC",
        entry_eligible_only: bool = False,
        columns: str | list[str] | tuple[str, ...] | None = None,
        version: str = "v1",
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        del check_val
        where = ["entry_eligible = true"] if entry_eligible_only else []
        return self._query(
            version=version,
            currency=currency,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            where=where,
            limit=limit,
        )


class DeribitOptionOverlayLoader:
    dataset_name = "deribit_option_overlay"

    def load(
        self,
        *,
        instrument_ids: list[int],
        start_date: str | None = None,
        end_date: str | None = None,
        currency: str = "BTC",
        resolution: str = "5m",
        version: str = "v1",
    ) -> pd.DataFrame:
        del instrument_ids, start_date, end_date, currency, resolution, version
        return pd.DataFrame(columns=SNAPSHOT_5M_COLUMNS)
