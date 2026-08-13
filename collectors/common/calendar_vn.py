from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

VN_HOLIDAYS = {
    "2024-01-01",
    "2024-02-08",
    "2024-02-09",
    "2024-02-12",
    "2024-02-13",
    "2024-02-14",
    "2024-04-18",
    # HNX adjusted the 30/4--1/5 exchange closure to include the Monday.
    "2024-04-29",
    "2024-04-30",
    "2024-05-01",
    "2024-09-02",
    "2024-09-03",
    "2025-01-01",
    "2025-01-27",
    "2025-01-28",
    "2025-01-29",
    "2025-01-30",
    "2025-01-31",
    "2025-04-07",
    "2025-04-30",
    "2025-05-01",
    "2025-05-02",
    "2025-09-01",
    "2025-09-02",
    "2026-01-01",
    "2026-01-02",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-04-27",
    "2026-04-30",
    "2026-05-01",
    "2026-09-02",
}


def vn_now() -> datetime:
    return datetime.now(VN_TZ)


def is_trading_day(dt: datetime) -> bool:
    local = dt.astimezone(VN_TZ) if dt.tzinfo else dt.replace(tzinfo=VN_TZ)
    return local.weekday() < 5 and local.strftime("%Y-%m-%d") not in VN_HOLIDAYS


def is_stock_session(dt: datetime) -> bool:
    local = dt.astimezone(VN_TZ) if dt.tzinfo else dt.replace(tzinfo=VN_TZ)
    if not is_trading_day(local):
        return False
    hhmm = local.hour * 100 + local.minute
    return 900 <= hhmm <= 1130 or 1300 <= hhmm <= 1445


def is_derivative_session(dt: datetime) -> bool:
    local = dt.astimezone(VN_TZ) if dt.tzinfo else dt.replace(tzinfo=VN_TZ)
    if not is_trading_day(local):
        return False
    hhmm = local.hour * 100 + local.minute
    return 845 <= hhmm <= 1130 or 1300 <= hhmm <= 1445


def seconds_until_next_session(*, derivative: bool = False) -> int:
    now = vn_now()
    probe = now.replace(second=0, microsecond=0)
    predicate = is_derivative_session if derivative else is_stock_session
    for _ in range(60 * 24 * 14):
        probe += timedelta(minutes=1)
        if predicate(probe):
            target = probe.replace(second=5)
            return max(int((target - now).total_seconds()), 5)
    return 3600


def filter_trading_hours(df: pd.DataFrame, *, derivative: bool) -> pd.DataFrame:
    if df.empty:
        return df

    times = pd.to_datetime(df["time"])
    if times.dt.tz is None:
        local_times = times.dt.tz_localize("Asia/Ho_Chi_Minh")
    else:
        local_times = times.dt.tz_convert("Asia/Ho_Chi_Minh")

    weekday = local_times.dt.weekday
    date_str = local_times.dt.strftime("%Y-%m-%d")
    hhmm = local_times.dt.hour * 100 + local_times.dt.minute

    is_trading_day_mask = (weekday < 5) & (~date_str.isin(VN_HOLIDAYS))

    if derivative:
        is_trading_hour = ((hhmm >= 845) & (hhmm <= 1130)) | ((hhmm >= 1300) & (hhmm <= 1445))
    else:
        is_trading_hour = ((hhmm >= 900) & (hhmm <= 1130)) | ((hhmm >= 1300) & (hhmm <= 1445))

    return df[is_trading_day_mask & is_trading_hour].copy()

