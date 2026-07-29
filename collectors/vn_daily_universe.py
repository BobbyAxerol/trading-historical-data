from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from collectors.common.env import data_root, state_root
from collectors.common.storage import read_partition_file, release_unused_memory

REPORT_COLUMNS = [
    "symbol",
    "asset_type",
    "first_valid_date",
    "last_valid_date",
    "row_count",
    "coverage_ratio",
    "max_internal_gap",
    "median_turnover_60d",
    "median_turnover_252d",
    "score",
    "tier",
    "reasons",
]


@dataclass(frozen=True)
class SymbolMetrics:
    symbol: str
    asset_type: str
    first_valid_date: str | None
    last_valid_date: str | None
    row_count: int
    coverage_ratio: float
    max_internal_gap: int
    median_turnover_60d: float
    median_turnover_252d: float
    history_days: int
    stale_days: int | None
    ohlc_invalid_rows: int
    volume_invalid_rows: int
    liquidity_value: float
    reasons: list[str]


def configured_equity_symbols(config: dict) -> list[str]:
    return _ordered_unique([*(config.get("symbols") or []), *(config.get("candidate_symbols") or [])])


def configured_external_symbols(config: dict) -> list[str]:
    return _ordered_unique(config.get("external_symbols") or [])


def build_universe_report(
    *,
    equity_symbols: Iterable[str],
    external_symbols: Iterable[str] | None = None,
    as_of_date: str | None = None,
    write: bool = True,
) -> pd.DataFrame:
    as_of = pd.to_datetime(as_of_date).normalize() if as_of_date else pd.Timestamp.utcnow().tz_localize(None).normalize()
    rows: list[SymbolMetrics] = []
    equity_root = data_root() / "vn" / "equity" / "1d"
    futures_root = data_root() / "vn" / "futures" / "1d"

    for symbol in _ordered_unique(equity_symbols):
        rows.append(_metrics_for_symbol(equity_root, symbol, asset_type="equity", as_of=as_of))
    for symbol in _ordered_unique(external_symbols or []):
        rows.append(_metrics_for_symbol(futures_root, symbol, asset_type="future", as_of=as_of))

    report = _score_rows(rows)
    if write:
        path = state_root() / "vn_daily_universe_report.csv.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        report.to_csv(tmp, index=False, compression="gzip")
        tmp.replace(path)
    release_unused_memory()
    return report


def _metrics_for_symbol(root: Path, symbol: str, *, asset_type: str, as_of: pd.Timestamp) -> SymbolMetrics:
    df = _read_symbol(root, symbol)
    if df.empty:
        return SymbolMetrics(
            symbol=symbol,
            asset_type=asset_type,
            first_valid_date=None,
            last_valid_date=None,
            row_count=0,
            coverage_ratio=0.0,
            max_internal_gap=0,
            median_turnover_60d=0.0,
            median_turnover_252d=0.0,
            history_days=0,
            stale_days=None,
            ohlc_invalid_rows=0,
            volume_invalid_rows=0,
            liquidity_value=0.0,
            reasons=["no_data"],
        )

    df = _normalize_ohlcv(df, symbol)
    if df.empty:
        return SymbolMetrics(
            symbol=symbol,
            asset_type=asset_type,
            first_valid_date=None,
            last_valid_date=None,
            row_count=0,
            coverage_ratio=0.0,
            max_internal_gap=0,
            median_turnover_60d=0.0,
            median_turnover_252d=0.0,
            history_days=0,
            stale_days=None,
            ohlc_invalid_rows=0,
            volume_invalid_rows=0,
            liquidity_value=0.0,
            reasons=["invalid_schema"],
        )

    first = df["time"].min()
    last = df["time"].max()
    row_count = int(df.shape[0])
    expected_sessions = max(1, len(pd.bdate_range(first, last)))
    coverage_ratio = min(1.0, row_count / expected_sessions)
    diffs = df["time"].sort_values().diff().dt.days.dropna()
    max_internal_gap = int(diffs.max()) if not diffs.empty else 0
    history_days = max(0, int((last - first).days))
    stale_days = max(0, int((as_of - last).days))

    max_price = df[["open", "high", "low", "close"]].max(axis=1, skipna=False)
    min_price = df[["open", "high", "low", "close"]].min(axis=1, skipna=False)
    ohlc_invalid_rows = int(((df["high"] < max_price) | (df["low"] > min_price)).sum())
    volume_invalid_rows = int((df["volume"] < 0).sum())
    turnover = (df["close"] * df["volume"]).replace([pd.NA, pd.NaT], pd.NA)
    median_60 = float(turnover.tail(60).median()) if not turnover.tail(60).dropna().empty else 0.0
    median_252 = float(turnover.tail(252).median()) if not turnover.tail(252).dropna().empty else 0.0
    liquidity_value = max(median_60, median_252)

    reasons: list[str] = []
    if ohlc_invalid_rows:
        reasons.append(f"ohlc_invalid_rows={ohlc_invalid_rows}")
    if volume_invalid_rows:
        reasons.append(f"volume_invalid_rows={volume_invalid_rows}")
    if max_internal_gap > 30:
        reasons.append(f"large_internal_gap={max_internal_gap}")
    if stale_days > 14:
        reasons.append(f"stale_days={stale_days}")
    if coverage_ratio < 0.6:
        reasons.append(f"low_coverage={coverage_ratio:.2f}")

    return SymbolMetrics(
        symbol=symbol,
        asset_type=asset_type,
        first_valid_date=first.strftime("%Y-%m-%d"),
        last_valid_date=last.strftime("%Y-%m-%d"),
        row_count=row_count,
        coverage_ratio=round(float(coverage_ratio), 6),
        max_internal_gap=max_internal_gap,
        median_turnover_60d=median_60,
        median_turnover_252d=median_252,
        history_days=history_days,
        stale_days=stale_days,
        ohlc_invalid_rows=ohlc_invalid_rows,
        volume_invalid_rows=volume_invalid_rows,
        liquidity_value=liquidity_value,
        reasons=reasons,
    )


def _score_rows(rows: list[SymbolMetrics]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=REPORT_COLUMNS)
    equity_liquidity = sorted({row.liquidity_value for row in rows if row.asset_type == "equity" and row.liquidity_value > 0})
    q75 = _quantile(equity_liquidity, 0.75)
    max_liquidity = max(equity_liquidity) if equity_liquidity else 0.0

    payload: list[dict[str, object]] = []
    for row in rows:
        if row.asset_type != "equity":
            payload.append(_payload(row, score=0.0, tier="auxiliary"))
            continue
        if row.row_count <= 0:
            payload.append(_payload(row, score=0.0, tier="review"))
            continue

        liquidity_score = 0.0 if max_liquidity <= 0 else min(100.0, 100.0 * row.liquidity_value / max_liquidity)
        continuity_score = max(0.0, min(100.0, row.coverage_ratio * 100.0 - max(0, row.max_internal_gap - 14)))
        history_score = min(100.0, 100.0 * row.history_days / (365.25 * 5))
        recent_score = _recent_score(row.stale_days)
        score = round(0.4 * liquidity_score + 0.3 * continuity_score + 0.2 * history_score + 0.1 * recent_score, 2)

        severe_quality = row.ohlc_invalid_rows > 0 or row.volume_invalid_rows > 0 or (row.stale_days is not None and row.stale_days > 45)
        if score >= 70 and not severe_quality:
            tier = "core"
        elif score >= 45 or (row.liquidity_value >= q75 and row.row_count > 0):
            tier = "extended"
        else:
            tier = "review"
        payload.append(_payload(row, score=score, tier=tier))

    return pd.DataFrame(payload, columns=REPORT_COLUMNS).sort_values(["asset_type", "tier", "score", "symbol"], ascending=[True, True, False, True]).reset_index(drop=True)


def _payload(row: SymbolMetrics, *, score: float, tier: str) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "asset_type": row.asset_type,
        "first_valid_date": row.first_valid_date,
        "last_valid_date": row.last_valid_date,
        "row_count": row.row_count,
        "coverage_ratio": row.coverage_ratio,
        "max_internal_gap": row.max_internal_gap,
        "median_turnover_60d": round(row.median_turnover_60d, 4),
        "median_turnover_252d": round(row.median_turnover_252d, 4),
        "score": score,
        "tier": tier,
        "reasons": ";".join(row.reasons),
    }


def _read_symbol(root: Path, symbol: str) -> pd.DataFrame:
    symbol_root = root / f"symbol={symbol}"
    if not symbol_root.exists():
        return pd.DataFrame()
    files: list[Path] = []
    for partition_dir in sorted(symbol_root.glob("year=*")):
        parquet = partition_dir / "part.parquet"
        csv = partition_dir / "part.csv.gz"
        if parquet.exists():
            files.append(parquet)
        elif csv.exists():
            files.append(csv)
    frames = []
    for path in files:
        try:
            frames.append(read_partition_file(path))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _normalize_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    required = ["time", "open", "high", "low", "close", "volume"]
    if any(col not in df.columns for col in required):
        return pd.DataFrame()
    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce").dt.normalize()
    work = work.dropna(subset=["time"])
    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["open", "high", "low", "close", "volume"])
    if work.empty:
        return work
    work["symbol"] = symbol
    return work.drop_duplicates(subset=["time", "symbol"], keep="last").sort_values("time").reset_index(drop=True)


def _ordered_unique(values: Iterable[object]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        symbol = str(value).strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    series = pd.Series(values, dtype="float64")
    return float(series.quantile(q))


def _recent_score(stale_days: int | None) -> float:
    if stale_days is None:
        return 0.0
    if stale_days <= 7:
        return 100.0
    if stale_days <= 30:
        return max(0.0, 100.0 - (stale_days - 7) * 3.0)
    return 0.0
