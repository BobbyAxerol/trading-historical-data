from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from collectors.common.env import data_root
from collectors.vn_derivatives.symbols import VN30FutureContract, generate_contracts

INSTRUMENT_COLUMNS = [
    "instrument_id",
    "canonical_symbol",
    "legacy_symbol",
    "krx_symbol",
    "expiry_date",
    "listing_start",
    "listing_end",
    "exchange_symbol_at_listing",
    "kbs_symbol_resolved",
    "dnse_symbol_resolved",
    "kbs_available_1m",
    "kbs_available_1d",
    "dnse_available_1m",
    "dnse_available_1d",
    "first_1m",
    "last_1m",
    "first_1d",
    "last_1d",
]


def instrument_dimension_path(version: str = "v1") -> Path:
    return data_root() / "vn" / "futures" / "instruments" / f"version={version}" / "instruments.parquet"


def contracts_to_frame(contracts: Iterable[VN30FutureContract]) -> pd.DataFrame:
    rows = []
    for contract in contracts:
        rows.append(
            {
                "instrument_id": contract.instrument_id,
                "canonical_symbol": contract.canonical_symbol,
                "legacy_symbol": contract.legacy_symbol,
                "krx_symbol": contract.krx_symbol,
                "expiry_date": pd.Timestamp(contract.expiry_date),
                "listing_start": pd.NaT,
                "listing_end": pd.NaT,
                "exchange_symbol_at_listing": pd.NA,
                "kbs_symbol_resolved": pd.NA,
                "dnse_symbol_resolved": pd.NA,
                "kbs_available_1m": False,
                "kbs_available_1d": False,
                "dnse_available_1m": False,
                "dnse_available_1d": False,
                "first_1m": pd.NaT,
                "last_1m": pd.NaT,
                "first_1d": pd.NaT,
                "last_1d": pd.NaT,
            }
        )
    return pd.DataFrame(rows, columns=INSTRUMENT_COLUMNS)


def write_instrument_dimension(df: pd.DataFrame, *, version: str = "v1") -> Path:
    path = instrument_dimension_path(version)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    out = df.reindex(columns=INSTRUMENT_COLUMNS).copy()
    out.to_parquet(tmp, index=False, engine="pyarrow", compression="zstd")
    tmp.replace(path)
    return path


def build_initial_instrument_dimension(*, start: str = "2017-08-10", end: str | None = None, horizon_months: int = 6, version: str = "v1") -> pd.DataFrame:
    df = contracts_to_frame(generate_contracts(start=start, end=end, horizon_months=horizon_months))
    write_instrument_dimension(df, version=version)
    return df

