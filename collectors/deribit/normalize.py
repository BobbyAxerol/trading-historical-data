from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from collectors.deribit.config import DeribitConfig
from collectors.deribit.filters import active_until_expiry, broad_candidate
from collectors.deribit.schema import TradeFlags
from collectors.deribit.tasks import DownloadTask

@dataclass(frozen=True)
class ActivationState:
    activated_at_ms: int | None = None
    activation_seq: int | None = None

    @property
    def activated(self) -> bool:
        return self.activation_seq is not None


@dataclass(frozen=True)
class NormalizedChunk:
    rows: list[dict[str, Any]]
    response_count: int
    discarded_count: int
    response_min_seq: int | None
    response_max_seq: int | None
    activated_at_ms: int | None
    activation_seq: int | None


def normalize_trade_chunk(
    trades: list[dict[str, Any]],
    *,
    task: DownloadTask,
    instrument: dict[str, Any],
    config: DeribitConfig,
    activation_state: ActivationState | None = None,
) -> NormalizedChunk:
    sorted_trades = sorted((row for row in trades if isinstance(row, dict)), key=lambda row: _int_or_none(row.get("trade_seq")) or -1)
    seqs = [_int_or_none(row.get("trade_seq")) for row in sorted_trades]
    valid_seqs = [int(seq) for seq in seqs if seq is not None]
    response_min_seq = min(valid_seqs) if valid_seqs else None
    response_max_seq = max(valid_seqs) if valid_seqs else None

    state = activation_state or ActivationState(
        activated_at_ms=_int_or_none(instrument.get("activated_at_ms")),
        activation_seq=_int_or_none(instrument.get("activation_seq")),
    )
    activated_at_ms = state.activated_at_ms
    activation_seq = state.activation_seq
    rows: list[dict[str, Any]] = []
    discarded = 0
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).replace(tzinfo=None)

    for raw in sorted_trades:
        normalized = _normalize_trade(raw, task=task, instrument=instrument, config=config, ingested_at=ingested_at)
        if normalized is None:
            discarded += 1
            continue

        if activation_seq is None and _activates_contract(normalized, instrument, config):
            activated_at_ms = int(normalized["timestamp_ms"])
            activation_seq = int(normalized["trade_seq"])

        if activation_seq is not None and _retains_after_activation(normalized, instrument):
            rows.append(normalized)
        else:
            discarded += 1

    return NormalizedChunk(
        rows=rows,
        response_count=len(sorted_trades),
        discarded_count=discarded,
        response_min_seq=response_min_seq,
        response_max_seq=response_max_seq,
        activated_at_ms=activated_at_ms,
        activation_seq=activation_seq,
    )


def _normalize_trade(
    raw: dict[str, Any],
    *,
    task: DownloadTask,
    instrument: dict[str, Any],
    config: DeribitConfig,
    ingested_at: datetime,
) -> dict[str, Any] | None:
    del config
    timestamp_ms = _int_or_none(raw.get("timestamp"))
    trade_seq = _int_or_none(raw.get("trade_seq"))
    price = _float_or_none(raw.get("price"))
    amount = _float_or_none(raw.get("amount"))
    if timestamp_ms is None or trade_seq is None or price is None or amount is None:
        return None
    if price < 0 or amount <= 0:
        return None

    flags = TradeFlags.IS_REGULAR
    mark_price = _float_or_none(raw.get("mark_price"))
    index_price = _float_or_none(raw.get("index_price"))
    iv = _float_or_none(raw.get("iv"))
    contract_size = _float_or_none(instrument.get("contract_size"))
    contracts = _float_or_none(raw.get("contracts"))
    source_missing_contracts = contracts is None
    if contracts is None and contract_size is not None and contract_size > 0:
        contracts = amount / contract_size
    if mark_price is None:
        flags |= TradeFlags.MISSING_MARK_PRICE
    if index_price is None:
        flags |= TradeFlags.MISSING_INDEX_PRICE
    if iv is None:
        flags |= TradeFlags.MISSING_IV
    if source_missing_contracts:
        flags |= TradeFlags.MISSING_CONTRACTS
    if iv is not None and iv <= 0:
        flags |= TradeFlags.INVALID_IV
    if index_price is not None and index_price <= 0:
        flags |= TradeFlags.INVALID_INDEX
    if raw.get("block_trade_id") is not None or raw.get("block_rfq_id") is not None:
        flags |= TradeFlags.IS_BLOCK
    if raw.get("combo_id") is not None:
        flags |= TradeFlags.IS_COMBO
    if bool(raw.get("liquidation")):
        flags |= TradeFlags.IS_LIQUIDATION

    return {
        "timestamp_ms": int(timestamp_ms),
        "instrument_id": int(task.instrument_id),
        "instrument_name": str(task.instrument_name),
        "trade_seq": int(trade_seq),
        "trade_id": str(raw.get("trade_id")) if raw.get("trade_id") is not None else None,
        "trade_id_hash": _hash_trade_id(raw.get("trade_id")),
        "price_btc": float(price),
        "mark_price_btc": mark_price,
        "iv_pct": iv,
        "index_price_usd": index_price,
        "amount_base": float(amount),
        "contracts": contracts,
        "direction": _direction_code(raw.get("direction")),
        "tick_direction": _int_or_none(raw.get("tick_direction")),
        "flags": int(flags),
        "source_priority": 1,
        "ingested_at": ingested_at,
        "dataset_version_id": 1,
    }


def _activates_contract(row: dict[str, Any], instrument: dict[str, Any], config: DeribitConfig) -> bool:
    expiry = _int_or_none(instrument.get("expiry_timestamp_ms"))
    strike = _float_or_none(instrument.get("strike_usd"))
    timestamp_ms = _int_or_none(row.get("timestamp_ms"))
    index_price = _float_or_none(row.get("index_price_usd"))
    mark_price = _float_or_none(row.get("mark_price_btc"))
    iv = _float_or_none(row.get("iv_pct"))
    if expiry is None or strike is None or timestamp_ms is None or index_price is None:
        return False
    if timestamp_ms >= expiry:
        return False
    if iv is None:
        return False
    if mark_price is not None and mark_price < 0:
        return False
    return broad_candidate(
        timestamp_ms=timestamp_ms,
        expiry_timestamp_ms=expiry,
        strike_usd=strike,
        index_price_usd=index_price,
        iv_pct=iv,
        config=config,
    )


def _retains_after_activation(row: dict[str, Any], instrument: dict[str, Any]) -> bool:
    expiry = _int_or_none(instrument.get("expiry_timestamp_ms"))
    timestamp_ms = _int_or_none(row.get("timestamp_ms"))
    if expiry is None or timestamp_ms is None or not active_until_expiry(timestamp_ms=timestamp_ms, expiry_timestamp_ms=expiry):
        return False
    if row["price_btc"] < 0 or row["amount_base"] <= 0:
        return False
    index_price = _float_or_none(row.get("index_price_usd"))
    if index_price is not None and index_price <= 0:
        return False
    iv = _float_or_none(row.get("iv_pct"))
    if iv is not None and iv <= 0:
        return False
    mark_price = _float_or_none(row.get("mark_price_btc"))
    if mark_price is not None and mark_price < 0:
        return False
    return True


def _hash_trade_id(value: Any) -> int | None:
    if value is None:
        return None
    digest = hashlib.blake2b(str(value).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False)


def _direction_code(value: Any) -> int:
    text = str(value or "").lower()
    if text == "buy":
        return 1
    if text == "sell":
        return -1
    return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None
