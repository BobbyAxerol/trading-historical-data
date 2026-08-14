from __future__ import annotations

import gc
import logging
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loaders.deribit_options import DeribitOptionOverlayLoader, DeribitOptionSnapshots5mLoader, DeribitOptionTradesLoader
from storage_manifest import assert_loader_compatible

# Setup Logger
logger = logging.getLogger("data_loader")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Base directories resolved relative to this module file location
BASE_DIR = Path(__file__).parent.resolve()
STORAGE_DIR = Path(
    os.getenv(
        "HISTORICAL_MARKET_DATA_ROOT",
        os.getenv("DATA_ROOT", str(BASE_DIR / "storage")),
    )
).resolve()
OHLCV_COLUMNS = ("time", "symbol", "open", "high", "low", "close", "volume")
LOADER_CONTRACT_VERSION = "hmd-loader-v1"
_TIMEFRAME_RE = re.compile(r"^(?P<count>\d+)\s*(?P<unit>s|sec|secs|second|seconds|min|minute|minutes|h|hour|hours|d|day|days)$")
_BINANCE_USDM_QUARTERLY_SYMBOL_RE = re.compile(r"^[A-Z0-9]+_\d{6}$")


def _is_binance_usdm_quarterly_symbol(symbol: str) -> bool:
    """Return whether *symbol* is a concrete USD-M quarterly contract.

    Binance names these contracts as ``PAIR_YYMMDD`` (for example
    ``BTCUSDT_260925``).  Perpetual and quarterly candles deliberately share
    one physical storage tree, so readers use this only for their default
    discovery behavior.  Explicit ``symbols=`` queries retain their existing
    pass-through semantics for compatibility.
    """

    return bool(_BINANCE_USDM_QUARTERLY_SYMBOL_RE.fullmatch(str(symbol).strip().upper()))


def _release_manifest_enforced() -> bool:
    """Enable fail-closed compatibility checks for installed/runtime readers.

    Source-tree development and writer-side utilities remain convenient when
    only ``DATA_ROOT`` is declared.  A wheel consumer must set the dedicated
    ``HISTORICAL_MARKET_DATA_ROOT`` for its read-only mount, which makes this
    guard mandatory. Tests can also opt in explicitly with the boolean
    environment variable.
    """

    explicit = os.getenv("HISTORICAL_MARKET_DATA_ROOT")
    forced = os.getenv("HISTORICAL_MARKET_DATA_REQUIRE_RELEASE_MANIFEST", "").strip().lower()
    return bool(explicit) or forced in {"1", "true", "yes", "on"}


def _assert_reader_compatible(dataset_id: str | None) -> None:
    if dataset_id and _release_manifest_enforced():
        assert_loader_compatible(
            STORAGE_DIR,
            dataset_id=dataset_id,
            loader_contract_version=LOADER_CONTRACT_VERSION,
        )


def _release_unused_memory() -> None:
    """Best-effort release of Python and Arrow memory after large materializations."""
    gc.collect()
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def _ordered_unique(values: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _resolve_columns_arg(columns: str | list[str] | tuple[str, ...] | None, default_columns: tuple[str, ...] | None) -> list[str] | None:
    if columns == "full":
        return None
    if columns is None:
        return list(default_columns) if default_columns else None
    if isinstance(columns, str):
        return [columns]
    return _ordered_unique([str(col) for col in columns])


def _file_columns(path: Path) -> list[str]:
    if path.name.endswith(".parquet"):
        return list(pq.ParquetFile(path).schema.names)
    return list(pd.read_csv(path, compression="gzip", nrows=0).columns)


def _existing_columns(path: Path, columns: list[str] | None) -> list[str] | None:
    if columns is None:
        return None
    available = set(_file_columns(path))
    return [col for col in columns if col in available]


def _select_partition_file(partition_dir: Path) -> Path | None:
    """Prefer fresh Parquet partitions, fallback to CSV during migration."""
    parquet_file = partition_dir / "part.parquet"
    csv_file = partition_dir / "part.csv.gz"
    if parquet_file.exists():
        if not csv_file.exists() or parquet_file.stat().st_mtime >= csv_file.stat().st_mtime:
            return parquet_file
    if csv_file.exists():
        return csv_file
    return None


def _read_partition_file(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    read_columns = _existing_columns(path, columns)
    if path.name.endswith(".parquet"):
        return pd.read_parquet(path, columns=read_columns, engine="pyarrow")
    return pd.read_csv(path, compression="gzip", usecols=read_columns)


def _read_partition_with_fallback(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    try:
        return _read_partition_file(path, columns=columns)
    except Exception:
        if path.name.endswith(".parquet"):
            csv_fallback = path.with_name("part.csv.gz")
            if csv_fallback.exists():
                return _read_partition_file(csv_fallback, columns=columns)
        raise


def _get_new_symbol_files(
    dataset_path: Path,
    is_option: bool,
    symbol: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> list[Path]:
    """Resolve partition paths overlapping with the query date/time window."""
    partition_name = "underlying" if is_option else "symbol"
    symbol_dir = dataset_path / f"{partition_name}={symbol.upper()}"
    if not symbol_dir.exists():
        symbol_dir = dataset_path / f"{partition_name}={symbol}"
        if not symbol_dir.exists():
            symbol_dir = dataset_path / f"{partition_name}={symbol.lower()}"
            if not symbol_dir.exists():
                return []

    paths = []
    # symbol_dir contains year=XXXX
    for year_dir in symbol_dir.glob("year=*"):
        try:
            year = int(year_dir.name.split("=")[1])
        except (IndexError, ValueError):
            continue
        if start_ts is not None and year < start_ts.year:
            continue
        if end_ts is not None and year > end_ts.year:
            continue

        # Check for monthly partitioning (year=XXXX/month=XX/part.csv.gz)
        month_dirs = list(year_dir.glob("month=*"))
        if not month_dirs:
            # Partitioned by year only (e.g. 1d datasets)
            part_file = _select_partition_file(year_dir)
            if part_file is not None:
                paths.append(part_file)
            continue

        # Monthly filter
        for month_dir in month_dirs:
            try:
                month = int(month_dir.name.split("=")[1])
            except (IndexError, ValueError):
                continue
            if start_ts is not None:
                if year < start_ts.year or (year == start_ts.year and month < start_ts.month):
                    continue
            if end_ts is not None:
                if year > end_ts.year or (year == end_ts.year and month > end_ts.month):
                    continue
            day_dirs = list(month_dir.glob("day=*"))
            if day_dirs:
                for day_dir in day_dirs:
                    try:
                        day = int(day_dir.name.split("=")[1])
                    except (IndexError, ValueError):
                        continue
                    day_start = pd.Timestamp(year=year, month=month, day=day)
                    if start_ts is not None and day_start.normalize() < start_ts.normalize():
                        continue
                    if end_ts is not None and day_start.normalize() > end_ts.normalize():
                        continue
                    part_file = _select_partition_file(day_dir)
                    if part_file is not None:
                        paths.append(part_file)
            else:
                part_file = _select_partition_file(month_dir)
                if part_file is not None:
                    paths.append(part_file)
    return sorted(paths)


def _get_versioned_symbol_files(
    dataset_path: Path,
    symbol: str,
    version: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> list[Path]:
    symbol_root = dataset_path / f"symbol={symbol.upper()}"
    source_version_dirs = sorted(path for path in symbol_root.glob(f"source=*/version={version}") if path.is_dir())
    version_dirs = source_version_dirs or [symbol_root / f"version={version}"]
    version_dirs = [path for path in version_dirs if path.exists()]
    if not version_dirs:
        return []
    paths = []
    for symbol_dir in version_dirs:
        for year_dir in symbol_dir.glob("year=*"):
            try:
                year = int(year_dir.name.split("=")[1])
            except (IndexError, ValueError):
                continue
            if start_ts is not None and year < start_ts.year:
                continue
            if end_ts is not None and year > end_ts.year:
                continue
            month_dirs = list(year_dir.glob("month=*"))
            if not month_dirs:
                part_file = _select_partition_file(year_dir)
                if part_file is not None:
                    paths.append(part_file)
                continue
            for month_dir in month_dirs:
                try:
                    month = int(month_dir.name.split("=")[1])
                except (IndexError, ValueError):
                    continue
                if start_ts is not None and (year, month) < (start_ts.year, start_ts.month):
                    continue
                if end_ts is not None and (year, month) > (end_ts.year, end_ts.month):
                    continue
                part_file = _select_partition_file(month_dir)
                if part_file is not None:
                    paths.append(part_file)
    return sorted(paths)


def _normalize_timeframe(timeframe: str) -> tuple[str, timedelta]:
    value = str(timeframe).strip().lower()
    if value.endswith("t"):
        value = f"{value[:-1]}min"
    elif value.endswith("m") and not value.endswith("min"):
        value = f"{value[:-1]}min"
    match = _TIMEFRAME_RE.match(value)
    if not match:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    count = int(match.group("count"))
    unit = match.group("unit")
    if count <= 0:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    if unit in {"s", "sec", "secs", "second", "seconds"}:
        return f"{count}s", timedelta(seconds=count)
    if unit in {"min", "minute", "minutes"}:
        return f"{count}min", timedelta(minutes=count)
    if unit in {"h", "hour", "hours"}:
        return f"{count}h", timedelta(hours=count)
    return f"{count}D", timedelta(days=count)


def _duckdb_interval_literal(step: timedelta) -> str:
    seconds = int(step.total_seconds())
    if seconds % 86400 == 0:
        return f"INTERVAL '{seconds // 86400} days'"
    if seconds % 3600 == 0:
        return f"INTERVAL '{seconds // 3600} hours'"
    if seconds % 60 == 0:
        return f"INTERVAL '{seconds // 60} minutes'"
    return f"INTERVAL '{seconds} seconds'"


def _duckdb_path_list(paths: list[Path]) -> str:
    quoted = [f"'{str(path).replace(chr(39), chr(39) + chr(39))}'" for path in paths]
    return "[" + ", ".join(quoted) + "]"


class MarketDataLoaderBase:
    """Base reader class handling path routing, loading, normalization, and validation."""

    DATASET_NAME: str = ""
    NEW_PATH_PARTS: tuple[str, ...] = ()
    IS_OPTION: bool = False
    TZ_INFO: str = "UTC"  # Either "Asia/Ho_Chi_Minh" (naive) or "UTC" (naive)
    DEFAULT_COLUMNS: tuple[str, ...] | None = None
    RESAMPLE_SUPPORTED: bool = False
    TIME_COLUMN: str = "time"
    RESAMPLE_VOLUME_DTYPE: str = "int64"
    RELEASE_DATASET_ID: str | None = None

    def _assert_reader_compatible(self) -> None:
        _assert_reader_compatible(self.RELEASE_DATASET_ID)

    def _get_new_path(self) -> Path:
        return STORAGE_DIR.joinpath(*self.NEW_PATH_PARTS)

    def _discover_symbols(self) -> list[str]:
        """Discover unique symbols in filesystem."""
        symbols = set()
        new_path = self._get_new_path()

        if new_path.exists():
            partition_prefix = "underlying=" if self.IS_OPTION else "symbol="
            for d in new_path.iterdir():
                if d.is_dir() and d.name.startswith(partition_prefix):
                    sym = d.name.split("=")[1].upper()
                    symbols.add(sym)

        return sorted(list(symbols))

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names, datatypes, timezones, sorting, and indexing."""
        if "time" in df.columns:
            # Parse datetime and drop NaNs
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df = df.dropna(subset=["time"])

            # Clean and align timezone to naive representations of local time/UTC
            try:
                if df["time"].dt.tz is not None:
                    df["time"] = df["time"].dt.tz_convert(self.TZ_INFO).dt.tz_localize(None)
            except Exception:
                pass

            # Standard type casts
            for col in ["open", "high", "low", "close"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            if "volume" in df.columns:
                df["volume"] = (
                    pd.to_numeric(df["volume"], errors="coerce")
                    .fillna(0)
                    .astype(self.RESAMPLE_VOLUME_DTYPE)
                )

            # Sort values
            sort_cols = ["symbol", "time"] if "symbol" in df.columns else ["time"]
            df = df.sort_values(sort_cols).reset_index(drop=True)

        return df

    def _filter_time_column(self, df: pd.DataFrame) -> str | None:
        if self.TIME_COLUMN in df.columns:
            return self.TIME_COLUMN
        if "time" in df.columns:
            return "time"
        return None

    def load(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
        columns: str | list[str] | tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        """
        Load, normalize, and validate dataset.

        Parameters
        ----------
        symbols : str, list of str, or None, default None
            Symbol(s) to query. E.g. "FPT", ["FPT", "ACB"]. If None, load all.
        start_date : str or None
            Filter starting from date/time (inclusive).
        end_date : str or None
            Filter up to date/time (inclusive).
        limit : int or None
            Limit row count of resulting DataFrame.
        check_val : bool, default True
            If True, run logical data validation and log warnings for anomalies.
        columns : {"full"}, list of str, str, or None
            Columns to read. None uses the loader default projection. For OHLCV
            loaders this is time/symbol/open/high/low/close/volume. Pass
            "full" to read every stored column.

        Returns
        -------
        pd.DataFrame
            Normalized DataFrame.
        """
        self._assert_reader_compatible()
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None

        read_columns = _resolve_columns_arg(columns, self.DEFAULT_COLUMNS)

        # Resolve symbol list
        if symbols is not None:
            symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)
            symbol_list = [s.strip().upper() for s in symbol_list]
        else:
            symbol_list = self._discover_symbols()

        if not symbol_list:
            logger.warning("No symbols found or specified for dataset %s", self.DATASET_NAME)
            return pd.DataFrame()

        dfs = []
        new_path = self._get_new_path()

        for sym in symbol_list:
            df_sym = pd.DataFrame()

            # Try "new" format
            if new_path.exists():
                files = _get_new_symbol_files(new_path, self.IS_OPTION, sym, start_ts, end_ts)
                if files:
                    sym_dfs = []
                    for f in files:
                        try:
                            sym_dfs.append(_read_partition_with_fallback(f, columns=read_columns))
                        except Exception as exc:
                            logger.error("Failed to read partition %s: %s", f, exc)
                    if sym_dfs:
                        df_sym = pd.concat(sym_dfs, ignore_index=True)
                        filter_col = self._filter_time_column(df_sym)
                        if filter_col is not None:
                            df_sym[filter_col] = pd.to_datetime(df_sym[filter_col], errors="coerce")
                            df_sym = df_sym.dropna(subset=[filter_col])
                            if start_ts is not None:
                                df_sym = df_sym[df_sym[filter_col] >= start_ts]
                            if end_ts is not None:
                                df_sym = df_sym[df_sym[filter_col] <= end_ts]

            if not df_sym.empty:
                if "symbol" not in df_sym.columns:
                    df_sym["symbol"] = sym
                dfs.append(df_sym)

        if not dfs:
            return pd.DataFrame()

        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = self._normalize(combined_df)

        if check_val:
            report = validate_data(combined_df, self.DATASET_NAME)
            if not report["valid"]:
                logger.warning("Validation warnings for dataset %s: %s", self.DATASET_NAME, report["errors"])

        if limit is not None:
            combined_df = combined_df.head(limit)

        _release_unused_memory()
        return combined_df

    def _resolve_symbol_list(self, symbols: str | list[str] | None) -> list[str]:
        if symbols is not None:
            symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)
            return [s.strip().upper() for s in symbol_list]
        return self._discover_symbols()

    def _resampled_files(self, symbols: str | list[str] | None, start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> tuple[list[str], dict[str, list[Path]]]:
        symbol_list = self._resolve_symbol_list(symbols)
        new_path = self._get_new_path()
        files_by_symbol: dict[str, list[Path]] = {}
        if not new_path.exists():
            return symbol_list, files_by_symbol
        for sym in symbol_list:
            files = _get_new_symbol_files(new_path, self.IS_OPTION, sym, start_ts, end_ts)
            if files:
                files_by_symbol[sym] = files
        return symbol_list, files_by_symbol

    def _load_resampled_duckdb(
        self,
        *,
        symbols: str | list[str] | None,
        timeframe: str,
        start_ts: pd.Timestamp | None,
        end_ts: pd.Timestamp | None,
        limit: int | None,
    ) -> pd.DataFrame:
        import duckdb

        _, step = _normalize_timeframe(timeframe)
        _, files_by_symbol = self._resampled_files(symbols, start_ts, end_ts)
        all_paths = [path for files in files_by_symbol.values() for path in files]
        if not all_paths:
            return pd.DataFrame(columns=list(OHLCV_COLUMNS))
        if any(not path.name.endswith(".parquet") for path in all_paths):
            raise RuntimeError("DuckDB resample requires Parquet partitions; use pandas fallback for legacy CSV partitions.")
        paths = all_paths

        interval = _duckdb_interval_literal(step)
        path_list = _duckdb_path_list(paths)
        where = ["time IS NOT NULL"]
        params: list[Any] = []
        if start_ts is not None:
            where.append("time >= ?")
            params.append(start_ts.to_pydatetime())
        if end_ts is not None:
            where.append("time <= ?")
            params.append(end_ts.to_pydatetime())
        where_sql = " AND ".join(where)
        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        volume_expr = (
            "COALESCE(CAST(floor(CAST(volume AS DOUBLE)) AS BIGINT), 0) AS volume"
            if self.RESAMPLE_VOLUME_DTYPE == "int64"
            else "CAST(volume AS DOUBLE) AS volume"
        )
        query = f"""
            WITH src AS (
                SELECT
                    CAST(time AS TIMESTAMP) AS time,
                    upper(CAST(symbol AS VARCHAR)) AS symbol,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    {volume_expr}
                FROM read_parquet({path_list})
            ),
            filtered AS (
                SELECT * FROM src WHERE {where_sql}
            )
            SELECT
                time_bucket({interval}, time) AS time,
                symbol,
                arg_min(open, time) AS open,
                max(high) AS high,
                min(low) AS low,
                arg_max(close, time) AS close,
                sum(volume) AS volume
            FROM filtered
            GROUP BY symbol, time_bucket({interval}, time)
            HAVING open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
            ORDER BY symbol, time
            {limit_sql}
        """
        con = duckdb.connect(database=":memory:")
        try:
            try:
                con.execute("SET memory_limit='1024MB'")
            except Exception:
                pass
            return con.execute(query, params).fetchdf()
        finally:
            con.close()

    def _load_resampled_pandas(
        self,
        *,
        symbols: str | list[str] | None,
        timeframe: str,
        start_ts: pd.Timestamp | None,
        end_ts: pd.Timestamp | None,
        limit: int | None,
    ) -> pd.DataFrame:
        pandas_rule, _ = _normalize_timeframe(timeframe)
        _, files_by_symbol = self._resampled_files(symbols, start_ts, end_ts)
        pieces: list[pd.DataFrame] = []
        read_columns = list(OHLCV_COLUMNS)

        for sym, files in files_by_symbol.items():
            symbol_pieces: list[pd.DataFrame] = []
            for path in files:
                try:
                    chunk = _read_partition_with_fallback(path, columns=read_columns)
                except Exception as exc:
                    logger.error("Failed to read partition %s for resample: %s", path, exc)
                    continue
                if chunk.empty or "time" not in chunk.columns:
                    continue
                if "symbol" not in chunk.columns:
                    chunk["symbol"] = sym
                chunk["time"] = pd.to_datetime(chunk["time"], errors="coerce")
                chunk = chunk.dropna(subset=["time"])
                if start_ts is not None:
                    chunk = chunk[chunk["time"] >= start_ts]
                if end_ts is not None:
                    chunk = chunk[chunk["time"] <= end_ts]
                if chunk.empty:
                    continue
                for col in ["open", "high", "low", "close", "volume"]:
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
                if self.RESAMPLE_VOLUME_DTYPE == "int64":
                    chunk["volume"] = chunk["volume"].fillna(0).astype("int64")
                chunk = chunk.dropna(subset=["open", "high", "low", "close"])
                if chunk.empty:
                    continue
                chunk = chunk.sort_values("time").set_index("time")
                resampled = chunk.resample(pandas_rule).agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                    }
                )
                resampled = resampled.dropna(subset=["open", "high", "low", "close"]).reset_index()
                resampled["symbol"] = sym
                symbol_pieces.append(resampled[["time", "symbol", "open", "high", "low", "close", "volume"]])
                del chunk, resampled
                _release_unused_memory()

            if symbol_pieces:
                symbol_df = pd.concat(symbol_pieces, ignore_index=True)
                symbol_df = (
                    symbol_df.sort_values(["symbol", "time"])
                    .groupby(["symbol", "time"], as_index=False, sort=True)
                    .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
                )
                pieces.append(symbol_df[["time", "symbol", "open", "high", "low", "close", "volume"]])
                del symbol_df, symbol_pieces
                _release_unused_memory()

        if not pieces:
            return pd.DataFrame(columns=list(OHLCV_COLUMNS))

        result = pd.concat(pieces, ignore_index=True)
        result = result.sort_values(["symbol", "time"]).reset_index(drop=True)
        if limit is not None:
            result = result.head(limit)
        return result

    def load_resampled(
        self,
        symbols: str | list[str] | None = None,
        timeframe: str = "5min",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
        engine: str = "duckdb",
    ) -> pd.DataFrame:
        """Load OHLCV resampled from canonical 1m Parquet without materializing full 1m history first."""
        self._assert_reader_compatible()
        if not self.RESAMPLE_SUPPORTED or self.DEFAULT_COLUMNS != OHLCV_COLUMNS:
            raise NotImplementedError(f"load_resampled is only supported for OHLCV loaders, got {self.DATASET_NAME}")

        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None

        if engine == "duckdb":
            try:
                result = self._load_resampled_duckdb(
                    symbols=symbols,
                    timeframe=timeframe,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    limit=limit,
                )
            except Exception as exc:
                logger.warning("DuckDB resample failed for %s; falling back to pandas chunks: %s", self.DATASET_NAME, exc)
                result = self._load_resampled_pandas(
                    symbols=symbols,
                    timeframe=timeframe,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    limit=limit,
                )
        elif engine == "pandas":
            result = self._load_resampled_pandas(
                symbols=symbols,
                timeframe=timeframe,
                start_ts=start_ts,
                end_ts=end_ts,
                limit=limit,
            )
        else:
            raise ValueError(f"Unsupported resample engine: {engine}")

        result = self._normalize(result)
        if "time" in result.columns:
            result["time"] = pd.to_datetime(result["time"], errors="coerce").astype("datetime64[ns]")
        result = result.dropna(subset=["open", "high", "low", "close"])
        result = result[["time", "symbol", "open", "high", "low", "close", "volume"]]
        if check_val:
            report = validate_data(result, "resampled_ohlcv")
            if not report["valid"]:
                logger.warning("Validation warnings for resampled dataset %s: %s", self.DATASET_NAME, report["errors"])
        _release_unused_memory()
        return result


# =====================================================================
# Specialized Subclasses
# =====================================================================


class VnStock1m(MarketDataLoaderBase):
    """Loads 1m stock candles for Vietnam equities (naive time represented in Asia/Ho_Chi_Minh)."""

    DATASET_NAME = "vn_stock_1m"
    NEW_PATH_PARTS = ("vn", "equity", "1m")
    TZ_INFO = "Asia/Ho_Chi_Minh"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RESAMPLE_SUPPORTED = True
    RELEASE_DATASET_ID = "vn_stock_1m"


class VnStockDaily(MarketDataLoaderBase):
    """Loads daily stock candles for Vietnam equities (naive time represented in Asia/Ho_Chi_Minh)."""

    DATASET_NAME = "vn_stock_daily"
    NEW_PATH_PARTS = ("vn", "equity", "1d")
    TZ_INFO = "Asia/Ho_Chi_Minh"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RELEASE_DATASET_ID = "vn_stock_daily"


class VnFutures1m(MarketDataLoaderBase):
    """Loads 1m derivative futures for Vietnam contracts (naive time represented in Asia/Ho_Chi_Minh)."""

    DATASET_NAME = "vn_futures_1m"
    NEW_PATH_PARTS = ("vn", "futures", "1m")
    TZ_INFO = "Asia/Ho_Chi_Minh"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RESAMPLE_SUPPORTED = True
    RELEASE_DATASET_ID = "vn_futures_1m"


class VnDerivativesContracts1m(MarketDataLoaderBase):
    """Loads concrete VN30 futures contracts, e.g. VN30F2508, from contract-level 1m storage."""

    DATASET_NAME = "vn_derivatives_contracts_1m"
    NEW_PATH_PARTS = ("vn", "futures", "contracts", "1m")
    TZ_INFO = "Asia/Ho_Chi_Minh"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RESAMPLE_SUPPORTED = True
    RELEASE_DATASET_ID = "vn_derivatives_contracts_1m"


class VnDerivativesContractsDaily(MarketDataLoaderBase):
    """Loads concrete VN30 futures contracts, e.g. VN30F2508, from contract-level daily storage."""

    DATASET_NAME = "vn_derivatives_contracts_1d"
    NEW_PATH_PARTS = ("vn", "futures", "contracts", "1d")
    TZ_INFO = "Asia/Ho_Chi_Minh"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RELEASE_DATASET_ID = "vn_derivatives_contracts_1d"


class VnDerivativesContinuousBase(MarketDataLoaderBase):
    """Base loader for rebuilt VN30 futures continuous series under symbol/version partitions."""

    VERSION = "v1"
    TZ_INFO = "Asia/Ho_Chi_Minh"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RELEASE_DATASET_ID = "vn_derivatives_continuous"

    def _discover_symbols(self) -> list[str]:
        root = self._get_new_path()
        if not root.exists():
            return []
        return sorted(
            path.name.split("=", 1)[1].upper()
            for path in root.glob("symbol=*")
            if (path / f"version={self.VERSION}").exists() or any(path.glob(f"source=*/version={self.VERSION}"))
        )

    def _files_for_symbol(self, symbol: str, start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> list[Path]:
        return _get_versioned_symbol_files(self._get_new_path(), symbol, self.VERSION, start_ts, end_ts)

    def load(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
        columns: str | list[str] | tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        self._assert_reader_compatible()
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None
        read_columns = _resolve_columns_arg(columns, self.DEFAULT_COLUMNS)
        symbol_list = [symbols] if isinstance(symbols, str) else list(symbols) if symbols is not None else self._discover_symbols()
        symbol_list = [str(symbol).strip().upper() for symbol in symbol_list if str(symbol).strip()]
        frames = []
        for symbol in symbol_list:
            parts = []
            for path in self._files_for_symbol(symbol, start_ts, end_ts):
                try:
                    parts.append(_read_partition_with_fallback(path, columns=read_columns))
                except Exception as exc:
                    logger.error("Failed to read partition %s: %s", path, exc)
            if not parts:
                continue
            df = pd.concat(parts, ignore_index=True)
            filter_col = self._filter_time_column(df)
            if filter_col is not None:
                df[filter_col] = pd.to_datetime(df[filter_col], errors="coerce")
                df = df.dropna(subset=[filter_col])
                if start_ts is not None:
                    df = df[df[filter_col] >= start_ts]
                if end_ts is not None:
                    df = df[df[filter_col] <= end_ts]
            if "symbol" not in df.columns:
                df["symbol"] = symbol
            frames.append(df)
            del parts
        if not frames:
            return pd.DataFrame()
        combined_df = pd.concat(frames, ignore_index=True)
        combined_df = self._normalize(combined_df)
        if check_val:
            report = validate_data(combined_df, self.DATASET_NAME)
            if not report["valid"]:
                logger.warning("Validation warnings for dataset %s: %s", self.DATASET_NAME, report["errors"])
        if limit is not None:
            combined_df = combined_df.head(limit)
        _release_unused_memory()
        return combined_df

    def _resampled_files(self, symbols: str | list[str] | None, start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> tuple[list[str], dict[str, list[Path]]]:
        symbol_list = self._resolve_symbol_list(symbols)
        files_by_symbol: dict[str, list[Path]] = {}
        for sym in symbol_list:
            files = self._files_for_symbol(sym, start_ts, end_ts)
            if files:
                files_by_symbol[sym] = files
        return symbol_list, files_by_symbol


class VnDerivativesContinuous1m(VnDerivativesContinuousBase):
    """Loads rebuilt continuous VN30 futures 1m series, e.g. VN30F1M and VN30F1M_TRADE."""

    DATASET_NAME = "vn_derivatives_continuous_1m"
    NEW_PATH_PARTS = ("vn", "futures", "continuous", "1m")
    RESAMPLE_SUPPORTED = True


class VnDerivativesContinuousDaily(VnDerivativesContinuousBase):
    """Loads rebuilt continuous VN30 futures daily series, e.g. VN30F1M and VN30F1M_TRADE."""

    DATASET_NAME = "vn_derivatives_continuous_1d"
    NEW_PATH_PARTS = ("vn", "futures", "continuous", "1d")


class CryptoBinance1m(MarketDataLoaderBase):
    """Loads 1m crypto futures candles from Binance (naive time represented in UTC)."""

    DATASET_NAME = "crypto_1m"
    NEW_PATH_PARTS = ("crypto", "binance_futures", "1m")
    TZ_INFO = "UTC"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RESAMPLE_SUPPORTED = True
    # USD-M futures volume is denominated in the base asset and is fractional.
    # Keep it lossless for both direct reads and resampled queries.
    RESAMPLE_VOLUME_DTYPE = "float64"
    RELEASE_DATASET_ID = "binance_perpetual_spot_quarterly"

    def _discover_futures_symbols(self) -> list[str]:
        """Discover all symbols in the shared USD-M futures storage tree."""

        return super()._discover_symbols()

    def _discover_symbols(self) -> list[str]:
        """Discover perpetual symbols only when the consumer omits symbols."""

        return [
            symbol
            for symbol in self._discover_futures_symbols()
            if not _is_binance_usdm_quarterly_symbol(symbol)
        ]


class CryptoBinanceQuarterly1m(CryptoBinance1m):
    """Loads concrete Binance USD-M quarterly contracts from the shared futures 1m storage."""

    DATASET_NAME = "crypto_binance_quarterly_1m"

    def _discover_symbols(self) -> list[str]:
        """Discover concrete quarterly contracts only in the shared tree."""

        return [
            symbol
            for symbol in self._discover_futures_symbols()
            if _is_binance_usdm_quarterly_symbol(symbol)
        ]


class CryptoBinanceSpot1m(MarketDataLoaderBase):
    """Loads 1m Binance Spot candles (naive time represented in UTC)."""

    DATASET_NAME = "crypto_binance_spot_1m"
    NEW_PATH_PARTS = ("crypto", "binance_spot", "1m")
    TZ_INFO = "UTC"
    DEFAULT_COLUMNS = OHLCV_COLUMNS
    RESAMPLE_SUPPORTED = True
    RESAMPLE_VOLUME_DTYPE = "float64"
    RELEASE_DATASET_ID = "binance_perpetual_spot_quarterly"

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        if "time" not in result.columns:
            return result

        result["time"] = pd.to_datetime(result["time"], errors="coerce")
        result = result.dropna(subset=["time"])
        try:
            if result["time"].dt.tz is not None:
                result["time"] = result["time"].dt.tz_convert(self.TZ_INFO).dt.tz_localize(None)
        except Exception:
            pass

        for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce").astype("float64")
        if "number_of_trades" in result.columns:
            result["number_of_trades"] = pd.to_numeric(result["number_of_trades"], errors="coerce").astype("Int64")

        sort_cols = ["symbol", "time"] if "symbol" in result.columns else ["time"]
        return result.sort_values(sort_cols).reset_index(drop=True)


class BinanceOptions5m(MarketDataLoaderBase):
    """Loads 5m options snap shots from Binance (naive time represented in UTC)."""

    DATASET_NAME = "options_5m"
    NEW_PATH_PARTS = ("options", "binance", "snapshot_5m")
    IS_OPTION = True
    TZ_INFO = "UTC"
    TIME_COLUMN = "snapshot_time"
    RELEASE_DATASET_ID = "binance_options_5m"

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        if "snapshot_time" not in result.columns:
            return result
        result["snapshot_time"] = pd.to_datetime(result["snapshot_time"], errors="coerce")
        result = result.dropna(subset=["snapshot_time"])
        try:
            if result["snapshot_time"].dt.tz is not None:
                result["snapshot_time"] = result["snapshot_time"].dt.tz_convert(self.TZ_INFO).dt.tz_localize(None)
        except Exception:
            pass
        if "time" in result.columns:
            result["time"] = pd.to_datetime(result["time"], errors="coerce")

        for col in result.columns:
            if col in {"snapshot_time", "time", "underlying", "symbol", "source", "ingested_at"}:
                continue
            result[col] = pd.to_numeric(result[col], errors="coerce")

        sort_cols = ["underlying", "snapshot_time", "symbol"] if "underlying" in result.columns else ["snapshot_time", "symbol"]
        return result.sort_values([col for col in sort_cols if col in result.columns]).reset_index(drop=True)


class BinanceOrderBookSnapshot1h(MarketDataLoaderBase):
    """Loads 1h Binance USD-M order book depth features (naive time represented in UTC)."""

    DATASET_NAME = "crypto_binance_orderbook_snapshot_1h"
    NEW_PATH_PARTS = ("crypto", "binance_orderbook_snapshot", "1h")
    TZ_INFO = "UTC"
    RELEASE_DATASET_ID = "binance_metrics_orderbook"

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        if "time" not in result.columns:
            return result
        result["time"] = pd.to_datetime(result["time"], errors="coerce")
        result = result.dropna(subset=["time"])
        if "sample_time" in result.columns:
            result["sample_time"] = pd.to_datetime(result["sample_time"], errors="coerce")
        for col in result.columns:
            if col in {"time", "sample_time", "market", "symbol", "contract_type", "source", "ingested_at"}:
                continue
            result[col] = pd.to_numeric(result[col], errors="coerce")
        sort_cols = ["symbol", "time"] if "symbol" in result.columns else ["time"]
        return result.sort_values(sort_cols).reset_index(drop=True)

    def load_features(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        """Alias for load(); returns feature rows such as bid_depth_1pct and q_bid_depth_1pct."""
        return self.load(symbols=symbols, start_date=start_date, end_date=end_date, limit=limit, check_val=check_val)


class BinanceFuturesMetrics5m(MarketDataLoaderBase):
    """Loads Binance USD-M futures metrics 5m from Vision (OI and long/short ratios)."""

    DATASET_NAME = "crypto_binance_futures_metrics_5m"
    NEW_PATH_PARTS = ("crypto", "binance_futures_metrics", "5m")
    TZ_INFO = "UTC"
    RELEASE_DATASET_ID = "binance_metrics_orderbook"

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        if "time" not in result.columns:
            return result
        result["time"] = pd.to_datetime(result["time"], errors="coerce")
        result = result.dropna(subset=["time"])
        for col in result.columns:
            if col in {"time", "market", "symbol", "contract_type", "source", "ingested_at"}:
                continue
            result[col] = pd.to_numeric(result[col], errors="coerce")
        sort_cols = ["symbol", "time"] if "symbol" in result.columns else ["time"]
        return result.sort_values(sort_cols).reset_index(drop=True)

    def load_open_interest(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        cols = ["time", "market", "symbol", "contract_type", "sum_open_interest", "sum_open_interest_value", "source", "ingested_at"]
        df = self.load(symbols=symbols, start_date=start_date, end_date=end_date, limit=limit, check_val=check_val, columns=cols)
        return df[[col for col in cols if col in df.columns]] if not df.empty else df

    def load_long_short_ratios(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        cols = [
            "time",
            "market",
            "symbol",
            "contract_type",
            "count_toptrader_long_short_ratio",
            "sum_toptrader_long_short_ratio",
            "count_long_short_ratio",
            "sum_taker_long_short_vol_ratio",
            "source",
            "ingested_at",
        ]
        df = self.load(symbols=symbols, start_date=start_date, end_date=end_date, limit=limit, check_val=check_val, columns=cols)
        return df[[col for col in cols if col in df.columns]] if not df.empty else df


class DeribitOptionTrades:
    """Loads Deribit BTC option canonical trade events (V1, compact-liquid trades-only)."""

    DATASET_NAME = "deribit_option_trades"
    TZ_INFO = "UTC"

    def __init__(self):
        self._loader = DeribitOptionTradesLoader()

    def load(
        self,
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
        return self._loader.load(
            start_date=start_date,
            end_date=end_date,
            currency=currency,
            instruments=instruments,
            option_type=option_type,
            dte_min=dte_min,
            dte_max=dte_max,
            columns=columns,
            version=version,
            limit=limit,
            check_val=check_val,
        )


class DeribitOptionSnapshots5m:
    """Loads Deribit BTC option compact-liquid 5m candidate snapshots (V1)."""

    DATASET_NAME = "deribit_option_snapshots_5m"
    TZ_INFO = "UTC"

    def __init__(self):
        self._loader = DeribitOptionSnapshots5mLoader()

    def load(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        currency: str = "BTC",
        entry_eligible_only: bool = False,
        columns: str | list[str] | tuple[str, ...] | None = None,
        version: str = "v1",
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        return self._loader.load(
            start_date=start_date,
            end_date=end_date,
            currency=currency,
            entry_eligible_only=entry_eligible_only,
            columns=columns,
            version=version,
            limit=limit,
            check_val=check_val,
        )


class DeribitOptionOverlay:
    """Builds held-position overlay rows on demand. Phase 0 returns an empty schema-compatible frame."""

    DATASET_NAME = "deribit_option_overlay"
    TZ_INFO = "UTC"

    def __init__(self):
        self._loader = DeribitOptionOverlayLoader()

    def load(
        self,
        instrument_ids: list[int],
        start_date: str | None = None,
        end_date: str | None = None,
        currency: str = "BTC",
        resolution: str = "5m",
        version: str = "v1",
    ) -> pd.DataFrame:
        return self._loader.load(
            instrument_ids=instrument_ids,
            start_date=start_date,
            end_date=end_date,
            currency=currency,
            resolution=resolution,
            version=version,
        )


class CryptoDailyMatrix:
    """Loads pivoted daily matrices (open, high, low, close, volume) for top 400 Binance futures."""

    DATASET_NAME = "binance_daily_matrix"
    OHLCV_DATASET_NAME = "crypto_daily_ohlcv"
    TZ_INFO = "UTC"
    FEATURES = ("open", "high", "low", "close", "volume")
    RELEASE_DATASET_ID = "binance_daily_matrix"

    def _assert_reader_compatible(self) -> None:
        _assert_reader_compatible(self.RELEASE_DATASET_ID)

    def _get_path(self, feature: str) -> Path:
        matrix_dir = STORAGE_DIR / "crypto" / "binance_daily_matrix"
        parquet_path = matrix_dir / f"{feature.lower()}.parquet"
        csv_path = matrix_dir / f"{feature.lower()}.csv.gz"
        if parquet_path.exists():
            if not csv_path.exists() or parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
                return parquet_path
        if csv_path.exists():
            return csv_path
        return parquet_path

    def _read_matrix(self, path: Path) -> pd.DataFrame:
        if path.suffix == ".parquet":
            return pd.read_parquet(path, engine="pyarrow")
        return pd.read_csv(path, compression="gzip", index_col=0)

    def _normalize_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def load(
        self,
        feature: str,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
    ) -> pd.DataFrame:
        """
        Load a specific feature matrix.

        Parameters
        ----------
        feature : {"open", "high", "low", "close", "volume"}
            Required. Feature matrix to load.
        symbols : str, list of str, or None
            Keep only these columns (symbols). If None, keep all.
        start_date : str or None
            Filter index from date/time (inclusive).
        end_date : str or None
            Filter index to date/time (inclusive).
        limit : int or None
            Limit row count of resulting matrix.
        check_val : bool, default True
            If True, perform matrix checks.
        """
        self._assert_reader_compatible()
        feature = feature.lower()
        valid_features = set(self.FEATURES)
        if feature not in valid_features:
            raise ValueError(f"Invalid feature '{feature}'. Supported features: {valid_features}")

        path = self._get_path(feature)
        if not path.exists():
            logger.warning("Matrix file not found: %s", path)
            return pd.DataFrame()

        df = self._read_matrix(path)

        # Normalize index to DatetimeIndex (naive UTC)
        df.index = pd.to_datetime(df.index)
        if df.index.tz is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)

        # Slicing dates
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None

        if start_ts is not None:
            df = df[df.index >= start_ts]
        if end_ts is not None:
            df = df[df.index <= end_ts]

        # Slicing columns
        if symbols is not None:
            symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)
            symbol_list = [s.strip().upper() for s in symbol_list]
            existing_cols = [c for c in symbol_list if c in df.columns]
            df = df[existing_cols]

        # Ensure all columns are numeric
        for col in df.columns:
            if feature == "volume":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        # Run validation
        if check_val:
            report = validate_data(df, self.DATASET_NAME)
            if not report["valid"]:
                logger.warning("Validation warnings for dataset %s: %s", self.DATASET_NAME, report["errors"])

        # Slice limit
        if limit is not None:
            df = df.head(limit)

        return df

    def load_features(
        self,
        features: list[str] | tuple[str, ...] | None = None,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Load multiple feature matrices keyed by feature name."""
        selected = list(features or self.FEATURES)
        matrices: dict[str, pd.DataFrame] = {}
        for feature in selected:
            matrices[feature.lower()] = self.load(
                feature=feature,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                check_val=check_val,
            )
        return matrices

    def load_ohlcv(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
        dropna: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Load OHLCV as the strategy-friendly ``dict[symbol, DataFrame]`` format.

        Each returned DataFrame is indexed by daily UTC-naive datetime and has
        columns: ``open``, ``high``, ``low``, ``close``, ``volume``.
        """
        matrices = self.load_features(
            features=list(self.FEATURES),
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            check_val=False,
        )
        if any(df.empty for df in matrices.values()):
            return {}

        common_symbols = set(matrices["close"].columns)
        for feature in self.FEATURES:
            common_symbols &= set(matrices[feature].columns)

        if symbols is not None:
            requested = [symbols] if isinstance(symbols, str) else list(symbols)
            requested = [symbol.strip().upper() for symbol in requested]
            ordered_symbols = [symbol for symbol in requested if symbol in common_symbols]
        else:
            ordered_symbols = [symbol for symbol in matrices["close"].columns if symbol in common_symbols]

        data_dict: dict[str, pd.DataFrame] = {}
        for symbol in ordered_symbols:
            temp_df = pd.DataFrame({feature: matrices[feature][symbol] for feature in self.FEATURES})
            temp_df.index = pd.to_datetime(temp_df.index)
            temp_df = temp_df.sort_index()
            if dropna:
                temp_df = temp_df.dropna()
            if temp_df.empty:
                continue
            if "volume" in temp_df.columns:
                temp_df["volume"] = pd.to_numeric(temp_df["volume"], errors="coerce").fillna(0).astype("int64")
            for feature in ("open", "high", "low", "close"):
                temp_df[feature] = pd.to_numeric(temp_df[feature], errors="coerce").astype("float64")
            temp_df = self._normalize_ohlcv(temp_df)
            data_dict[symbol] = temp_df

        if check_val and data_dict:
            frame = self.load_ohlcv_frame(
                symbols=list(data_dict.keys()),
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                check_val=False,
                dropna=dropna,
                _data_dict=data_dict,
            )
            report = validate_data(frame, self.OHLCV_DATASET_NAME)
            if not report["valid"]:
                logger.warning("Validation warnings for dataset %s: %s", self.DATASET_NAME, report["errors"])

        return data_dict

    def load_ohlcv_frame(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
        dropna: bool = True,
        _data_dict: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        """Load OHLCV as a long DataFrame with columns time, symbol, OHLCV."""
        data_dict = _data_dict or self.load_ohlcv(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            check_val=False,
            dropna=dropna,
        )
        frames = []
        for symbol, df in data_dict.items():
            part = df.reset_index().rename(columns={"index": "time"})
            if part.columns[0] != "time":
                part = part.rename(columns={part.columns[0]: "time"})
            part["symbol"] = symbol
            frames.append(part[["time", "symbol", "open", "high", "low", "close", "volume"]])
        if not frames:
            return pd.DataFrame(columns=["time", "symbol", "open", "high", "low", "close", "volume"])

        result = pd.concat(frames, ignore_index=True)
        result["time"] = pd.to_datetime(result["time"], errors="coerce")
        result = result.dropna(subset=["time"]).sort_values(["symbol", "time"]).reset_index(drop=True)
        if check_val:
            report = validate_data(result, self.OHLCV_DATASET_NAME)
            if not report["valid"]:
                logger.warning("Validation warnings for dataset %s: %s", self.DATASET_NAME, report["errors"])
        return result


class VNDailyMatrix(CryptoDailyMatrix):
    """Loads pivoted daily matrices for the VN equity universe."""

    DATASET_NAME = "vn_daily_matrix"
    OHLCV_DATASET_NAME = "vn_daily_ohlcv"
    TZ_INFO = "Asia/Ho_Chi_Minh"
    RELEASE_DATASET_ID = "vn_daily_matrix"

    def _get_path(self, feature: str) -> Path:
        matrix_dir = STORAGE_DIR / "vn" / "equity" / "daily_matrix"
        parquet_path = matrix_dir / f"{feature.lower()}.parquet"
        csv_path = matrix_dir / f"{feature.lower()}.csv.gz"
        if parquet_path.exists():
            if not csv_path.exists() or parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
                return parquet_path
        if csv_path.exists():
            return csv_path
        return parquet_path

    def _normalize_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Repair vendor OHLC bounds while preserving every observed price."""
        result = df.copy()
        prices = result[["open", "high", "low", "close"]]
        result["high"] = prices.max(axis=1, skipna=False)
        result["low"] = prices.min(axis=1, skipna=False)
        return result


# =====================================================================
# Validation Function & Master Router
# =====================================================================


def validate_data(df: pd.DataFrame, dataset: str) -> dict[str, Any]:
    """
    Validate dataset schema, types, logical constraints, and continuity.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame loaded via loader classes.
    dataset : str
        The dataset type.

    Returns
    -------
    dict
        Validation report.
    """
    report = {"valid": True, "row_count": len(df), "errors": [], "info": {}}

    if df.empty:
        report["valid"] = False
        report["errors"].append("DataFrame is empty.")
        return report

    if dataset in ("binance_daily_matrix", "vn_daily_matrix"):
        # Check index
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                pd.to_datetime(df.index)
            except Exception as exc:
                report["valid"] = False
                report["errors"].append(f"Matrix index is not parseable to DatetimeIndex: {exc}")

        # Check columns are strings
        non_string_cols = [c for c in df.columns if not isinstance(c, str)]
        if non_string_cols:
            report["valid"] = False
            report["errors"].append(f"Found non-string columns in matrix: {non_string_cols}")

        # Check numeric data types
        for col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                report["valid"] = False
                report["errors"].append(f"Column {col} is not numeric.")

        nan_count = df.isna().sum().sum()
        report["info"]["nan_count"] = int(nan_count)
        report["info"]["shape"] = df.shape
        report["info"]["min_date"] = str(df.index.min()) if not df.empty else None
        report["info"]["max_date"] = str(df.index.max()) if not df.empty else None
        return report

    if dataset in ("crypto_binance_orderbook_snapshot_1h", "binance_orderbook_snapshot_1h", "orderbook_snapshot_1h"):
        required_cols = ["time", "symbol", "bid_depth_1pct", "ask_depth_1pct", "q_bid_depth_1pct", "q_ask_depth_1pct"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            report["valid"] = False
            report["errors"].append(f"Missing required columns: {missing_cols}")
            return report
        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            try:
                pd.to_datetime(df["time"])
            except Exception as exc:
                report["valid"] = False
                report["errors"].append(f"Column 'time' is not parseable as datetime: {exc}")
        for col in [c for c in df.columns if c.endswith("depth") or "_depth_" in c or c.endswith("imbalance") or "_imbalance_" in c or c in {"mid_price", "best_bid", "best_ask", "spread", "spread_bps"}]:
            if not pd.api.types.is_numeric_dtype(df[col]):
                report["valid"] = False
                report["errors"].append(f"Column '{col}' is not numeric.")
        for col in [c for c in df.columns if "depth" in c]:
            neg_count = (df[col].dropna() < 0).sum()
            if neg_count > 0:
                report["valid"] = False
                report["errors"].append(f"Column '{col}' contains {neg_count} negative values.")
        dup_count = df.duplicated(subset=["symbol", "time"]).sum()
        report["info"]["duplicate_count"] = int(dup_count)
        if dup_count > 0:
            report["valid"] = False
            report["errors"].append(f"Found {dup_count} duplicate rows by symbol and time.")
        report["info"]["symbols"] = list(df["symbol"].unique())
        report["info"]["min_time"] = str(df["time"].min())
        report["info"]["max_time"] = str(df["time"].max())
        return report

    if dataset in ("crypto_binance_futures_metrics_5m", "binance_futures_metrics_5m", "futures_metrics_5m"):
        required_cols = ["time", "symbol", "sum_open_interest", "sum_open_interest_value", "count_long_short_ratio"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            report["valid"] = False
            report["errors"].append(f"Missing required columns: {missing_cols}")
            return report
        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            try:
                pd.to_datetime(df["time"])
            except Exception as exc:
                report["valid"] = False
                report["errors"].append(f"Column 'time' is not parseable as datetime: {exc}")
        numeric_cols = [
            "sum_open_interest",
            "sum_open_interest_value",
            "count_toptrader_long_short_ratio",
            "sum_toptrader_long_short_ratio",
            "count_long_short_ratio",
            "sum_taker_long_short_vol_ratio",
        ]
        for col in numeric_cols:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                report["valid"] = False
                report["errors"].append(f"Column '{col}' is not numeric.")
        for col in ("sum_open_interest", "sum_open_interest_value"):
            if col in df.columns:
                neg_count = (df[col].dropna() < 0).sum()
                if neg_count > 0:
                    report["valid"] = False
                    report["errors"].append(f"Column '{col}' contains {neg_count} negative values.")
        dup_count = df.duplicated(subset=["symbol", "time"]).sum()
        report["info"]["duplicate_count"] = int(dup_count)
        if dup_count > 0:
            report["valid"] = False
            report["errors"].append(f"Found {dup_count} duplicate rows by symbol and time.")
        report["info"]["symbols"] = list(df["symbol"].unique())
        report["info"]["min_time"] = str(df["time"].min())
        report["info"]["max_time"] = str(df["time"].max())
        return report

    if dataset in ("options_5m", "binance_options_5m", "options_binance_snapshot_5m"):
        required_cols = ["snapshot_time", "underlying", "symbol"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            report["valid"] = False
            report["errors"].append(f"Missing required columns: {missing_cols}")
            return report
        if not pd.api.types.is_datetime64_any_dtype(df["snapshot_time"]):
            try:
                pd.to_datetime(df["snapshot_time"])
            except Exception as exc:
                report["valid"] = False
                report["errors"].append(f"Column 'snapshot_time' is not parseable as datetime: {exc}")
        for col in df.columns:
            if col in {"snapshot_time", "time", "underlying", "symbol", "expiry", "type", "source", "ingested_at"}:
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                report["valid"] = False
                report["errors"].append(f"Column '{col}' is not numeric.")
        dup_count = df.duplicated(subset=["snapshot_time", "symbol"]).sum()
        report["info"]["duplicate_count"] = int(dup_count)
        if dup_count > 0:
            report["valid"] = False
            report["errors"].append(f"Found {dup_count} duplicate rows by symbol and snapshot_time.")
        report["info"]["underlyings"] = list(df["underlying"].unique())
        report["info"]["min_time"] = str(df["snapshot_time"].min())
        report["info"]["max_time"] = str(df["snapshot_time"].max())
        return report

    # Standard OHLCV validation
    required_cols = ["time", "symbol", "open", "high", "low", "close", "volume"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        report["valid"] = False
        report["errors"].append(f"Missing required columns: {missing_cols}")
        return report

    # Type validation
    if not pd.api.types.is_datetime64_any_dtype(df["time"]):
        try:
            pd.to_datetime(df["time"])
        except Exception as exc:
            report["valid"] = False
            report["errors"].append(f"Column 'time' is not parseable as datetime: {exc}")

    for col in ["open", "high", "low", "close"]:
        if not pd.api.types.is_numeric_dtype(df[col]):
            report["valid"] = False
            report["errors"].append(f"Column '{col}' is not numeric.")

    if not pd.api.types.is_numeric_dtype(df["volume"]):
        report["valid"] = False
        report["errors"].append("Column 'volume' is not numeric.")

    if not report["valid"]:
        return report

    # Logical constraints check
    for col in ["open", "high", "low", "close"]:
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            report["valid"] = False
            report["errors"].append(f"Column '{col}' contains {neg_count} negative values.")

    bad_high = (df["high"] < df["low"]).sum()
    if bad_high > 0:
        report["valid"] = False
        report["errors"].append(f"Logical error: 'high' < 'low' in {bad_high} rows.")

    bad_open_high = (df["high"] < df["open"]).sum()
    if bad_open_high > 0:
        report["valid"] = False
        report["errors"].append(f"Logical error: 'high' < 'open' in {bad_open_high} rows.")

    bad_close_high = (df["high"] < df["close"]).sum()
    if bad_close_high > 0:
        report["valid"] = False
        report["errors"].append(f"Logical error: 'high' < 'close' in {bad_close_high} rows.")

    bad_open_low = (df["low"] > df["open"]).sum()
    if bad_open_low > 0:
        report["valid"] = False
        report["errors"].append(f"Logical error: 'low' > 'open' in {bad_open_low} rows.")

    bad_close_low = (df["low"] > df["close"]).sum()
    if bad_close_low > 0:
        report["valid"] = False
        report["errors"].append(f"Logical error: 'low' > 'close' in {bad_close_low} rows.")

    neg_vol = (df["volume"] < 0).sum()
    if neg_vol > 0:
        report["valid"] = False
        report["errors"].append(f"Column 'volume' contains {neg_vol} negative values.")

    nan_count = df[required_cols].isna().sum().sum()
    report["info"]["nan_count"] = int(nan_count)
    if nan_count > 0:
        report["valid"] = False
        report["errors"].append(f"Found {nan_count} NaN values in required columns.")

    dup_count = df.duplicated(subset=["symbol", "time"]).sum()
    report["info"]["duplicate_count"] = int(dup_count)
    if dup_count > 0:
        report["valid"] = False
        report["errors"].append(f"Found {dup_count} duplicate rows by symbol and time.")

    unsorted_symbols = []
    for sym, group in df.groupby("symbol"):
        if not group["time"].is_monotonic_increasing:
            unsorted_symbols.append(sym)
    if unsorted_symbols:
        report["valid"] = False
        report["errors"].append(f"Time is not monotonically increasing for symbols: {unsorted_symbols}")

    report["info"]["symbols"] = list(df["symbol"].unique())
    report["info"]["min_time"] = str(df["time"].min())
    report["info"]["max_time"] = str(df["time"].max())

    expected_gap = None
    if dataset in ("crypto_1m", "crypto_binance_futures_1m", "crypto_binance_quarterly_1m", "binance_usdm_quarterly_1m", "crypto_binance_spot_1m", "binance_spot_1m", "crypto_spot_1m"):
        expected_gap = timedelta(minutes=1)
    elif dataset == "crypto_daily_ohlcv":
        expected_gap = timedelta(days=1)

    if expected_gap is not None:
        continuity_errors = []
        total_gap_count = 0
        max_gap = None
        for sym, group in df.groupby("symbol"):
            times = group["time"].dropna().sort_values().drop_duplicates().reset_index(drop=True)
            diffs = times.diff().dropna()
            gaps = diffs[diffs > expected_gap]
            if gaps.empty:
                continue
            total_gap_count += len(gaps)
            local_max = gaps.max()
            max_gap = local_max if max_gap is None or local_max > max_gap else max_gap
            first_idx = gaps.index[0]
            continuity_errors.append(
                f"{sym}: {len(gaps)} gaps, first {times.iloc[first_idx - 1]} -> {times.iloc[first_idx]} ({times.iloc[first_idx] - times.iloc[first_idx - 1]})"
            )
        report["info"]["continuity_gap_count"] = int(total_gap_count)
        report["info"]["max_continuity_gap"] = str(max_gap) if max_gap is not None else None
        if continuity_errors:
            report["valid"] = False
            report["errors"].append(f"Continuity gaps detected for expected step {expected_gap}: {continuity_errors[:5]}")

    return report


def load_data(
    dataset: str,
    symbols: str | list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
    feature: str | None = None,
    check_val: bool = True,
    columns: str | list[str] | tuple[str, ...] | None = None,
    timeframe: str | None = None,
    engine: str = "duckdb",
    currency: str = "BTC",
    instruments: list[int] | int | None = None,
    option_type: str | None = None,
    dte_min: int | None = None,
    dte_max: int | None = None,
    version: str = "v1",
    entry_eligible_only: bool = False,
) -> pd.DataFrame:
    """Master routing wrapper to invoke the correct reader subclass."""
    dataset_lower = dataset.lower()
    if timeframe is not None:
        if dataset_lower in ("vn_stock_1m", "vn_equity_1m"):
            return VnStock1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        elif dataset_lower in ("vn_futures_1m",):
            return VnFutures1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        elif dataset_lower in ("vn_derivatives_contracts_1m", "vn30_contracts_1m"):
            return VnDerivativesContracts1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        elif dataset_lower in ("vn_derivatives_continuous_1m", "vn30_continuous_1m", "vn30f1m_continuous_1m"):
            return VnDerivativesContinuous1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        elif dataset_lower in ("crypto_1m",):
            return CryptoBinance1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        elif dataset_lower in ("crypto_binance_quarterly_1m", "binance_usdm_quarterly_1m"):
            return CryptoBinanceQuarterly1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        elif dataset_lower in ("crypto_binance_spot_1m", "binance_spot_1m", "crypto_spot_1m"):
            return CryptoBinanceSpot1m().load_resampled(symbols, timeframe, start_date, end_date, limit, check_val, engine)
        raise ValueError(f"timeframe resampling is not supported for dataset: {dataset}")

    if dataset_lower in ("vn_stock_1m", "vn_equity_1m"):
        return VnStock1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("vn_stock_daily", "vn_equity_1d", "vn_stock_1d"):
        return VnStockDaily().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("vn_futures_1m",):
        return VnFutures1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("vn_derivatives_contracts_1m", "vn30_contracts_1m"):
        return VnDerivativesContracts1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("vn_derivatives_contracts_1d", "vn30_contracts_1d"):
        return VnDerivativesContractsDaily().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("vn_derivatives_continuous_1m", "vn30_continuous_1m", "vn30f1m_continuous_1m"):
        return VnDerivativesContinuous1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("vn_derivatives_continuous_1d", "vn30_continuous_1d", "vn30f1m_continuous_1d"):
        return VnDerivativesContinuousDaily().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("crypto_1m",):
        return CryptoBinance1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("crypto_binance_quarterly_1m", "binance_usdm_quarterly_1m"):
        return CryptoBinanceQuarterly1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("crypto_binance_spot_1m", "binance_spot_1m", "crypto_spot_1m"):
        return CryptoBinanceSpot1m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("crypto_binance_orderbook_snapshot_1h", "binance_orderbook_snapshot_1h", "orderbook_snapshot_1h"):
        return BinanceOrderBookSnapshot1h().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("crypto_binance_futures_metrics_5m", "binance_futures_metrics_5m", "futures_metrics_5m"):
        return BinanceFuturesMetrics5m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("options_5m",):
        return BinanceOptions5m().load(symbols, start_date, end_date, limit, check_val, columns=columns)
    elif dataset_lower in ("deribit_option_trades", "deribit_btc_option_trades", "deribit_options_trades_v1"):
        return DeribitOptionTrades().load(
            start_date=start_date,
            end_date=end_date,
            currency=currency,
            instruments=instruments,
            option_type=option_type,
            dte_min=dte_min,
            dte_max=dte_max,
            columns=columns,
            version=version,
            limit=limit,
            check_val=check_val,
        )
    elif dataset_lower in ("deribit_option_snapshots_5m", "deribit_btc_option_snapshots_5m", "deribit_options_5m"):
        return DeribitOptionSnapshots5m().load(
            start_date=start_date,
            end_date=end_date,
            currency=currency,
            entry_eligible_only=entry_eligible_only,
            columns=columns,
            version=version,
            limit=limit,
            check_val=check_val,
        )
    elif dataset_lower in ("binance_daily_matrix",):
        if not feature:
            raise ValueError("Parameter 'feature' is required for binance_daily_matrix.")
        return CryptoDailyMatrix().load(feature, symbols, start_date, end_date, limit, check_val)
    elif dataset_lower in ("vn_daily_matrix",):
        if not feature:
            raise ValueError("Parameter 'feature' is required for vn_daily_matrix.")
        return VNDailyMatrix().load(feature, symbols, start_date, end_date, limit, check_val)
    else:
        logger.warning("Unknown dataset: %s", dataset)
        return pd.DataFrame()
