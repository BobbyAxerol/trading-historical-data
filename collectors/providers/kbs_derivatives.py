from __future__ import annotations

from typing import Literal

import pandas as pd

from collectors.common.manifest import utc_now_iso

Resolution = Literal["1m", "1d"]


def _normalize_ohlc(df: pd.DataFrame, *, provider_symbol: str, resolution: Resolution) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"])

    work = df.copy()
    rename = {
        "date": "time",
        "datetime": "time",
        "tradingDate": "time",
        "trading_date": "time",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
    }
    work = work.rename(columns={key: value for key, value in rename.items() if key in work.columns})
    if "time" not in work.columns:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"])

    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    try:
        if work["time"].dt.tz is not None:
            work["time"] = work["time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    except Exception:
        pass
    work = work.dropna(subset=["time"])
    if work.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"])

    for col in ["open", "high", "low", "close"]:
        if col not in work.columns:
            work[col] = pd.NA
        work[col] = pd.to_numeric(work[col], errors="coerce").astype("float64")
    if "volume" not in work.columns:
        work["volume"] = 0
    work["volume"] = pd.to_numeric(work["volume"], errors="coerce").fillna(0).astype("float64")

    work = work.dropna(subset=["open", "high", "low", "close"])
    work = work[work["volume"] >= 0]
    if resolution == "1d":
        work["time"] = work["time"].dt.normalize()

    work["provider_symbol"] = provider_symbol
    work["source"] = "kbs"
    work["ingested_at"] = utc_now_iso()
    return work[["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"]].sort_values("time").reset_index(drop=True)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "provider_symbol", "source", "ingested_at"])


def _is_empty_provider_error(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        message = str(current).lower()
        if "dữ liệu trống" in message or "du lieu trong" in message or "empty" in message or "no data" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def fetch_ohlc(
    provider_symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    resolution: Resolution,
    *,
    auto_convert: bool = False,
) -> pd.DataFrame:
    """Fetch KBS/vnstock OHLC for a VN derivative provider symbol.

    ``auto_convert`` is intentionally explicit for probe accounting. The caller
    should still try legacy and KRX symbols separately so source coverage is not
    hidden by vnstock's convenience conversion.
    """
    try:
        from vnstock import Quote, register_user
    except Exception as exc:  # pragma: no cover - depends on optional runtime package
        raise RuntimeError(f"vnstock unavailable: {exc}") from exc

    try:
        register_user(skip=True)
    except Exception:
        pass

    interval = "1m" if resolution == "1m" else "1D"
    symbol = provider_symbol if not auto_convert else provider_symbol
    quote = Quote(symbol=symbol, source="KBS", show_log=False)
    try:
        raw = quote.history(
            start=pd.Timestamp(start).strftime("%Y-%m-%d"),
            end=pd.Timestamp(end).strftime("%Y-%m-%d"),
            interval=interval,
            show_log=False,
        )
    except Exception as exc:
        if _is_empty_provider_error(exc):
            return _empty_frame()
        raise
    return _normalize_ohlc(raw, provider_symbol=provider_symbol, resolution=resolution)
