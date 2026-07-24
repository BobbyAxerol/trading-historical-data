from __future__ import annotations

from enum import IntFlag

import pyarrow as pa

DATASET_VERSION = "deribit_btc_options_v1"
SCHEMA_VERSION = "trade_schema_v1"
UNIVERSE_VERSION = "compact_liquid_v1"
SNAPSHOT_VERSION = "compact_5m_v1"
PRICING_VERSION = "anchored_iv_v1"
EXECUTION_PROXY_VERSION = "trade_mark_v1"


class TradeFlags(IntFlag):
    IS_REGULAR = 1 << 0
    IS_BLOCK = 1 << 1
    IS_COMBO = 1 << 2
    IS_LIQUIDATION = 1 << 3
    MISSING_MARK_PRICE = 1 << 4
    MISSING_INDEX_PRICE = 1 << 5
    MISSING_IV = 1 << 6
    MISSING_CONTRACTS = 1 << 7
    INVALID_IV = 1 << 8
    INVALID_INDEX = 1 << 9
    SCHEMA_LEGACY = 1 << 10
    SCHEMA_UNKNOWN_FIELD = 1 << 11


class MarkSource:
    EXCHANGE_MARK_AT_TRADE = "exchange_mark_at_trade"
    ANCHORED_IV_RECONSTRUCTION = "anchored_iv_reconstruction"
    UNAVAILABLE = "unavailable"
    EXPIRED = "expired"


INSTRUMENT_COLUMNS = [
    "instrument_id",
    "instrument_name",
    "currency",
    "expiry_timestamp_ms",
    "strike_usd",
    "option_type",
    "creation_timestamp_ms",
    "contract_size",
    "tick_size",
    "min_trade_amount",
    "settlement_currency",
    "is_expired",
    "activated_at_ms",
    "activation_seq",
    "metadata_source",
    "parse_status",
    "quality_flags",
    "dataset_version_id",
]

STAGING_TRADE_COLUMNS = [
    "timestamp_ms",
    "instrument_id",
    "instrument_name",
    "trade_seq",
    "trade_id",
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
    "source_priority",
    "ingested_at",
    "dataset_version_id",
]

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


def instrument_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("instrument_name", pa.string(), nullable=False),
            pa.field("currency", pa.dictionary(pa.int8(), pa.string()), nullable=False),
            pa.field("expiry_timestamp_ms", pa.int64(), nullable=False),
            pa.field("strike_usd", pa.float64(), nullable=False),
            pa.field("option_type", pa.int8(), nullable=False),
            pa.field("creation_timestamp_ms", pa.int64(), nullable=True),
            pa.field("contract_size", pa.float32(), nullable=True),
            pa.field("tick_size", pa.float64(), nullable=True),
            pa.field("min_trade_amount", pa.float32(), nullable=True),
            pa.field("settlement_currency", pa.dictionary(pa.int8(), pa.string()), nullable=True),
            pa.field("is_expired", pa.bool_(), nullable=False),
            pa.field("activated_at_ms", pa.int64(), nullable=True),
            pa.field("activation_seq", pa.int64(), nullable=True),
            pa.field("metadata_source", pa.int8(), nullable=False),
            pa.field("parse_status", pa.int8(), nullable=False),
            pa.field("quality_flags", pa.uint16(), nullable=False),
            pa.field("dataset_version_id", pa.uint16(), nullable=False),
        ]
    )


def staging_trade_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("timestamp_ms", pa.int64(), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("instrument_name", pa.string(), nullable=False),
            pa.field("trade_seq", pa.int64(), nullable=False),
            pa.field("trade_id", pa.string(), nullable=True),
            pa.field("trade_id_hash", pa.uint64(), nullable=True),
            pa.field("price_btc", pa.float64(), nullable=False),
            pa.field("mark_price_btc", pa.float64(), nullable=True),
            pa.field("iv_pct", pa.float32(), nullable=True),
            pa.field("index_price_usd", pa.float64(), nullable=True),
            pa.field("amount_base", pa.float32(), nullable=False),
            pa.field("contracts", pa.float32(), nullable=True),
            pa.field("direction", pa.int8(), nullable=False),
            pa.field("tick_direction", pa.int8(), nullable=True),
            pa.field("flags", pa.uint16(), nullable=False),
            pa.field("source_priority", pa.int8(), nullable=False),
            pa.field("ingested_at", pa.timestamp("ms"), nullable=False),
            pa.field("dataset_version_id", pa.uint16(), nullable=False),
        ]
    )


def canonical_trade_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("timestamp_ms", pa.int64(), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("trade_seq", pa.int64(), nullable=False),
            pa.field("trade_id_hash", pa.uint64(), nullable=True),
            pa.field("price_btc", pa.float64(), nullable=False),
            pa.field("mark_price_btc", pa.float64(), nullable=True),
            pa.field("iv_pct", pa.float32(), nullable=True),
            pa.field("index_price_usd", pa.float64(), nullable=True),
            pa.field("amount_base", pa.float32(), nullable=False),
            pa.field("contracts", pa.float32(), nullable=True),
            pa.field("direction", pa.int8(), nullable=False),
            pa.field("tick_direction", pa.int8(), nullable=True),
            pa.field("flags", pa.uint16(), nullable=False),
            pa.field("dataset_version_id", pa.uint16(), nullable=False),
        ]
    )


def snapshot_5m_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("timestamp_ms", pa.int64(), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("mark_price_btc", pa.float64(), nullable=True),
            pa.field("last_trade_price_btc", pa.float64(), nullable=True),
            pa.field("index_price_usd", pa.float64(), nullable=True),
            pa.field("iv_pct", pa.float32(), nullable=True),
            pa.field("model_delta", pa.float32(), nullable=True),
            pa.field("volume_5m", pa.float32(), nullable=False),
            pa.field("trade_count_5m", pa.uint16(), nullable=False),
            pa.field("buy_volume_5m", pa.float32(), nullable=False),
            pa.field("sell_volume_5m", pa.float32(), nullable=False),
            pa.field("anchor_age_seconds", pa.uint32(), nullable=True),
            pa.field("quality_flags", pa.uint16(), nullable=False),
            pa.field("entry_eligible", pa.bool_(), nullable=False),
        ]
    )
