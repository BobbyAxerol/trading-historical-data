from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd

from collectors.common.calendar_vn import is_trading_day

MARKET_START = date(2017, 8, 10)
CANONICAL_RE = re.compile(r"^VN30F(?P<yy>\d{2})(?P<mm>\d{2})$")
KRX_RE = re.compile(r"^41I1(?P<year_code>[0-9A-HJ-NP-Z])(?P<month_code>[1-9ABC])000$")

YEAR_CODE_EPOCH = 2010
YEAR_CODES = "0123456789ABCDEFGHJKLMNPQRSTUV"
MONTH_CODES = {1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7", 8: "8", 9: "9", 10: "A", 11: "B", 12: "C"}


@dataclass(frozen=True)
class VN30FutureContract:
    canonical_symbol: str
    year: int
    month: int
    legacy_symbol: str
    krx_symbol: str
    expiry_date: date
    instrument_id: int


def canonical_symbol(year: int, month: int) -> str:
    return f"VN30F{year % 100:02d}{month:02d}"


def parse_canonical_symbol(symbol: str) -> tuple[int, int]:
    match = CANONICAL_RE.match(symbol.upper())
    if not match:
        raise ValueError(f"Invalid VN30 futures canonical symbol: {symbol!r}")
    year = 2000 + int(match.group("yy"))
    month = int(match.group("mm"))
    if month < 1 or month > 12:
        raise ValueError(f"Invalid VN30 futures month in symbol: {symbol!r}")
    return year, month


def legacy_to_krx(year: int, month: int, *, underlying_code: str = "I1") -> str:
    if month not in MONTH_CODES:
        raise ValueError(f"Invalid VN30 futures month: {month}")
    year_offset = (year - YEAR_CODE_EPOCH) % len(YEAR_CODES)
    return f"41{underlying_code}{YEAR_CODES[year_offset]}{MONTH_CODES[month]}000"


def instrument_id(symbol: str) -> int:
    digest = hashlib.blake2b(symbol.upper().encode("ascii"), digest_size=4).digest()
    return int.from_bytes(digest, "big", signed=False)


def third_thursday(year: int, month: int) -> date:
    day = date(year, month, 1)
    while day.weekday() != 3:
        day += timedelta(days=1)
    return day + timedelta(days=14)


def adjust_to_previous_trading_day(day: date) -> date:
    probe = day
    while not is_trading_day(datetime.combine(probe, datetime.min.time())):
        probe -= timedelta(days=1)
    return probe


def expiry_date(year: int, month: int) -> date:
    return adjust_to_previous_trading_day(third_thursday(year, month))


def is_vn30_future_symbol(symbol: str) -> bool:
    value = symbol.upper()
    if CANONICAL_RE.match(value):
        try:
            parse_canonical_symbol(value)
            return True
        except ValueError:
            return False
    return bool(KRX_RE.match(value) or value in {"VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"})


def contract_for_month(year: int, month: int) -> VN30FutureContract:
    symbol = canonical_symbol(year, month)
    return VN30FutureContract(
        canonical_symbol=symbol,
        year=year,
        month=month,
        legacy_symbol=symbol,
        krx_symbol=legacy_to_krx(year, month),
        expiry_date=expiry_date(year, month),
        instrument_id=instrument_id(symbol),
    )


def iter_months(start: date, end: date) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    current = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while current <= stop:
        months.append((current.year, current.month))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def generate_contracts(start: str | date = MARKET_START, end: str | date | None = None, *, horizon_months: int = 6) -> list[VN30FutureContract]:
    start_date = pd.Timestamp(start).date()
    if end is None:
        end_date = (pd.Timestamp.utcnow().normalize() + pd.DateOffset(months=horizon_months)).date()
    else:
        end_date = pd.Timestamp(end).date()
    return [contract_for_month(year, month) for year, month in iter_months(start_date, end_date)]


def provider_symbol_candidates(contract: VN30FutureContract) -> list[tuple[str, str]]:
    return [("legacy", contract.legacy_symbol), ("krx", contract.krx_symbol)]
