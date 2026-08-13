"""Deribit option column contracts required by the read-only loaders.

This lightweight module deliberately has no collector or PyArrow dependency so
the distributed reader wheel stays independent of ingestion runtime code.
"""

CANONICAL_TRADE_COLUMNS = [
    "timestamp_ms",
    "instrument_id",
    "trade_seq",
    "trade_id_hash",
    "price_btc",
    "mark_price_btc",
    "iv_pct",
    "index_price_usd",
    "amount_base",
    "contracts",
    "direction",
    "tick_direction",
    "flags",
    "dataset_version_id",
]

SNAPSHOT_5M_COLUMNS = [
    "timestamp_ms",
    "instrument_id",
    "mark_price_btc",
    "last_trade_price_btc",
    "index_price_usd",
    "iv_pct",
    "model_delta",
    "volume_5m",
    "trade_count_5m",
    "buy_volume_5m",
    "sell_volume_5m",
    "anchor_age_seconds",
    "quality_flags",
    "entry_eligible",
]

__all__ = ["CANONICAL_TRADE_COLUMNS", "SNAPSHOT_5M_COLUMNS"]
