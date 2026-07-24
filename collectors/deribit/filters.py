from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from collectors.deribit.config import DeribitConfig

MS_PER_DAY = 86_400_000


@dataclass(frozen=True)
class BroadPolicy:
    max_dte_days: float
    min_strike_to_index: float
    max_strike_to_index: float


def broad_policy(config: DeribitConfig) -> BroadPolicy:
    raw = config.raw["broad_ingestion"]
    moneyness = raw.get("moneyness", {})
    return BroadPolicy(
        max_dte_days=float(raw.get("max_dte_days", 120)),
        min_strike_to_index=float(moneyness.get("min_strike_to_index", 0.5)),
        max_strike_to_index=float(moneyness.get("max_strike_to_index", 2.0)),
    )


def broad_candidate(*, timestamp_ms: int, expiry_timestamp_ms: int, strike_usd: float, index_price_usd: float, iv_pct: float, config: DeribitConfig) -> bool:
    policy = broad_policy(config)
    if timestamp_ms >= expiry_timestamp_ms or index_price_usd <= 0 or iv_pct <= 0:
        return False
    dte = (expiry_timestamp_ms - timestamp_ms) / MS_PER_DAY
    if dte < 0 or dte > policy.max_dte_days:
        return False
    strike_to_index = strike_usd / index_price_usd
    return policy.min_strike_to_index <= strike_to_index <= policy.max_strike_to_index


def active_until_expiry(*, timestamp_ms: int, expiry_timestamp_ms: int) -> bool:
    return timestamp_ms < expiry_timestamp_ms
