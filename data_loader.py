from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

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
STORAGE_DIR = BASE_DIR / "storage"


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
            part_file = year_dir / "part.csv.gz"
            if part_file.exists():
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
            part_file = month_dir / "part.csv.gz"
            if part_file.exists():
                paths.append(part_file)
    return sorted(paths)


class MarketDataLoaderBase:
    """Base reader class handling path routing, loading, normalization, and validation."""

    DATASET_NAME: str = ""
    NEW_PATH_PARTS: tuple[str, ...] = ()
    IS_OPTION: bool = False
    TZ_INFO: str = "UTC"  # Either "Asia/Ho_Chi_Minh" (naive) or "UTC" (naive)

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
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

            # Sort values
            sort_cols = ["symbol", "time"] if "symbol" in df.columns else ["time"]
            df = df.sort_values(sort_cols).reset_index(drop=True)

        return df

    def load(
        self,
        symbols: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        check_val: bool = True,
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

        Returns
        -------
        pd.DataFrame
            Normalized DataFrame.
        """
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None

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
                            sym_dfs.append(pd.read_csv(f, compression="gzip"))
                        except Exception as exc:
                            logger.error("Failed to read partition %s: %s", f, exc)
                    if sym_dfs:
                        df_sym = pd.concat(sym_dfs, ignore_index=True)
                        if "time" in df_sym.columns:
                            df_sym["time"] = pd.to_datetime(df_sym["time"], errors="coerce")
                            df_sym = df_sym.dropna(subset=["time"])
                            if start_ts is not None:
                                df_sym = df_sym[df_sym["time"] >= start_ts]
                            if end_ts is not None:
                                df_sym = df_sym[df_sym["time"] <= end_ts]

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

        return combined_df


# =====================================================================
# Specialized Subclasses
# =====================================================================


class VnStock1m(MarketDataLoaderBase):
    """Loads 1m stock candles for Vietnam equities (naive time represented in Asia/Ho_Chi_Minh)."""

    DATASET_NAME = "vn_stock_1m"
    NEW_PATH_PARTS = ("vn", "equity", "1m")
    TZ_INFO = "Asia/Ho_Chi_Minh"


class VnStockDaily(MarketDataLoaderBase):
    """Loads daily stock candles for Vietnam equities (naive time represented in Asia/Ho_Chi_Minh)."""

    DATASET_NAME = "vn_stock_daily"
    NEW_PATH_PARTS = ("vn", "equity", "1d")
    TZ_INFO = "Asia/Ho_Chi_Minh"


class VnFutures1m(MarketDataLoaderBase):
    """Loads 1m derivative futures for Vietnam contracts (naive time represented in Asia/Ho_Chi_Minh)."""

    DATASET_NAME = "vn_futures_1m"
    NEW_PATH_PARTS = ("vn", "futures", "1m")
    TZ_INFO = "Asia/Ho_Chi_Minh"


class CryptoBinance1m(MarketDataLoaderBase):
    """Loads 1m crypto futures candles from Binance (naive time represented in UTC)."""

    DATASET_NAME = "crypto_1m"
    NEW_PATH_PARTS = ("crypto", "binance_futures", "1m")
    TZ_INFO = "UTC"


class BinanceOptions5m(MarketDataLoaderBase):
    """Loads 5m options snap shots from Binance (naive time represented in UTC)."""

    DATASET_NAME = "options_5m"
    NEW_PATH_PARTS = ("options", "binance", "snapshot_5m")
    IS_OPTION = True
    TZ_INFO = "UTC"


class CryptoDailyMatrix:
    """Loads pivoted daily matrices (open, high, low, close, volume) for top 400 Binance futures."""

    DATASET_NAME = "binance_daily_matrix"
    OHLCV_DATASET_NAME = "crypto_daily_ohlcv"
    TZ_INFO = "UTC"
    FEATURES = ("open", "high", "low", "close", "volume")

    def _get_path(self, feature: str) -> Path:
        return STORAGE_DIR / "crypto" / "binance_daily_matrix" / f"{feature.lower()}.csv.gz"

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
        feature = feature.lower()
        valid_features = set(self.FEATURES)
        if feature not in valid_features:
            raise ValueError(f"Invalid feature '{feature}'. Supported features: {valid_features}")

        path = self._get_path(feature)
        if not path.exists():
            logger.warning("Matrix file not found: %s", path)
            return pd.DataFrame()

        df = pd.read_csv(path, compression="gzip", index_col=0)

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

    def _get_path(self, feature: str) -> Path:
        return STORAGE_DIR / "vn" / "equity" / "daily_matrix" / f"{feature.lower()}.csv.gz"

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
    if dataset == "crypto_1m":
        expected_gap = pd.Timedelta(minutes=1)
    elif dataset == "crypto_daily_ohlcv":
        expected_gap = pd.Timedelta(days=1)

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
) -> pd.DataFrame:
    """Master routing wrapper to invoke the correct reader subclass."""
    dataset_lower = dataset.lower()
    if dataset_lower in ("vn_stock_1m", "vn_equity_1m"):
        return VnStock1m().load(symbols, start_date, end_date, limit, check_val)
    elif dataset_lower in ("vn_stock_daily", "vn_equity_1d", "vn_stock_1d"):
        return VnStockDaily().load(symbols, start_date, end_date, limit, check_val)
    elif dataset_lower in ("vn_futures_1m",):
        return VnFutures1m().load(symbols, start_date, end_date, limit, check_val)
    elif dataset_lower in ("crypto_1m",):
        return CryptoBinance1m().load(symbols, start_date, end_date, limit, check_val)
    elif dataset_lower in ("options_5m",):
        return BinanceOptions5m().load(symbols, start_date, end_date, limit, check_val)
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
