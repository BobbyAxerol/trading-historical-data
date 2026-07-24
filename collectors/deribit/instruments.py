from __future__ import annotations

import os
import re
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum, IntFlag
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from collectors.common.env import data_root
from collectors.common.storage import release_unused_memory
from collectors.deribit.client import DeribitHistoryClient
from collectors.deribit.config import DeribitConfig
from collectors.deribit.schema import instrument_schema

MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

INSTRUMENT_RE = re.compile(r"^(?P<currency>[A-Z]+)-(?P<day>\d{1,2})(?P<month>[A-Z]{3})(?P<year>\d{2})-(?P<strike>\d+(?:\.\d+)?)-(?P<kind>[CP])$")


class OptionType(IntEnum):
    UNKNOWN = 0
    CALL = 1
    PUT = -1


class MetadataSource(IntEnum):
    UNKNOWN = 0
    API = 1
    NAME_FALLBACK = 2
    MIXED = 3


class ParseStatus(IntEnum):
    OK = 1
    PARTIAL = 2
    INVALID = 3


class InstrumentQualityFlag(IntFlag):
    NONE = 0
    USED_NAME_EXPIRY = 1 << 0
    USED_NAME_STRIKE = 1 << 1
    USED_NAME_OPTION_TYPE = 1 << 2
    INVALID_NAME = 1 << 3
    MISSING_EXPIRY = 1 << 4
    MISSING_STRIKE = 1 << 5
    MISSING_OPTION_TYPE = 1 << 6
    API_NAME_CONFLICT = 1 << 7


@dataclass(frozen=True)
class ParsedInstrumentName:
    currency: str | None
    expiry_timestamp_ms: int | None
    strike_usd: float | None
    option_type: OptionType | None
    ok: bool


@dataclass(frozen=True)
class DiscoveryResult:
    instrument_path: Path
    total_rows: int
    active_rows: int
    expired_rows: int
    invalid_rows: int
    config_hash: str

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "phase": "Phase 2",
            "instrument_path": str(self.instrument_path),
            "total_rows": self.total_rows,
            "active_rows": self.active_rows,
            "expired_rows": self.expired_rows,
            "invalid_rows": self.invalid_rows,
            "config_hash": self.config_hash,
        }


def instrument_dimension_path(config: DeribitConfig) -> Path:
    return data_root() / "options" / "deribit" / "instruments" / f"version={config.version}" / "instruments.parquet"


def stable_instrument_id(instrument_name: str) -> int:
    value = zlib.crc32(instrument_name.encode("utf-8")) & 0xFFFFFFFF
    return value or 1


def parse_instrument_name(instrument_name: str) -> ParsedInstrumentName:
    match = INSTRUMENT_RE.match(str(instrument_name).upper())
    if not match:
        return ParsedInstrumentName(None, None, None, None, False)
    month = MONTHS.get(match.group("month"))
    if month is None:
        return ParsedInstrumentName(match.group("currency"), None, None, None, False)
    year = 2000 + int(match.group("year"))
    expiry = datetime(year, month, int(match.group("day")), 8, 0, 0, tzinfo=timezone.utc)
    option_type = OptionType.CALL if match.group("kind") == "C" else OptionType.PUT
    return ParsedInstrumentName(
        currency=match.group("currency"),
        expiry_timestamp_ms=int(expiry.timestamp() * 1000),
        strike_usd=float(match.group("strike")),
        option_type=option_type,
        ok=True,
    )


def normalize_instrument(raw: dict[str, Any], *, is_expired: bool, config: DeribitConfig) -> dict[str, Any]:
    name = str(raw.get("instrument_name") or "").upper()
    parsed = parse_instrument_name(name)
    flags = InstrumentQualityFlag.NONE

    if not parsed.ok:
        flags |= InstrumentQualityFlag.INVALID_NAME

    currency = str(raw.get("base_currency") or raw.get("currency") or parsed.currency or config.currency).upper()
    expiry = _int_or_none(raw.get("expiration_timestamp"))
    strike = _float_or_none(raw.get("strike"))
    option_type = _option_type(raw.get("option_type"))
    source = MetadataSource.API

    if expiry is None and parsed.expiry_timestamp_ms is not None:
        expiry = parsed.expiry_timestamp_ms
        flags |= InstrumentQualityFlag.USED_NAME_EXPIRY
        source = MetadataSource.MIXED
    if strike is None and parsed.strike_usd is not None:
        strike = parsed.strike_usd
        flags |= InstrumentQualityFlag.USED_NAME_STRIKE
        source = MetadataSource.MIXED
    if option_type == OptionType.UNKNOWN and parsed.option_type is not None:
        option_type = parsed.option_type
        flags |= InstrumentQualityFlag.USED_NAME_OPTION_TYPE
        source = MetadataSource.MIXED

    if parsed.ok and parsed.currency and parsed.currency != currency:
        flags |= InstrumentQualityFlag.API_NAME_CONFLICT

    if expiry is None:
        flags |= InstrumentQualityFlag.MISSING_EXPIRY
    if strike is None:
        flags |= InstrumentQualityFlag.MISSING_STRIKE
    if option_type == OptionType.UNKNOWN:
        flags |= InstrumentQualityFlag.MISSING_OPTION_TYPE

    missing_required = flags & (
        InstrumentQualityFlag.INVALID_NAME
        | InstrumentQualityFlag.MISSING_EXPIRY
        | InstrumentQualityFlag.MISSING_STRIKE
        | InstrumentQualityFlag.MISSING_OPTION_TYPE
        | InstrumentQualityFlag.API_NAME_CONFLICT
    )
    parse_status = ParseStatus.INVALID if missing_required else ParseStatus.OK
    if source == MetadataSource.API and raw.get("expiration_timestamp") is None and raw.get("strike") is None and raw.get("option_type") is None:
        source = MetadataSource.NAME_FALLBACK
    elif source == MetadataSource.MIXED and flags & (InstrumentQualityFlag.USED_NAME_EXPIRY | InstrumentQualityFlag.USED_NAME_STRIKE | InstrumentQualityFlag.USED_NAME_OPTION_TYPE):
        source = MetadataSource.MIXED

    final_is_expired = bool(is_expired)
    if final_is_expired and expiry is not None and expiry > int(datetime.now(timezone.utc).timestamp() * 1000):
        final_is_expired = False

    return {
        "instrument_id": stable_instrument_id(name),
        "instrument_name": name,
        "currency": currency,
        "expiry_timestamp_ms": int(expiry) if expiry is not None else -1,
        "strike_usd": float(strike) if strike is not None else float("nan"),
        "option_type": int(option_type),
        "creation_timestamp_ms": _int_or_none(raw.get("creation_timestamp")),
        "contract_size": _float_or_none(raw.get("contract_size")),
        "tick_size": _float_or_none(raw.get("tick_size")),
        "min_trade_amount": _float_or_none(raw.get("min_trade_amount")),
        "settlement_currency": _str_or_none(raw.get("settlement_currency")),
        "is_expired": final_is_expired,
        "activated_at_ms": None,
        "activation_seq": None,
        "metadata_source": int(source),
        "parse_status": int(parse_status),
        "quality_flags": int(flags),
        "dataset_version_id": 1,
    }


class DeribitInstrumentDiscovery:
    def __init__(self, config: DeribitConfig, *, client: Any | None = None):
        self.config = config
        self.client = client or DeribitHistoryClient(config, requests_per_second=1.0)

    def run(self) -> DiscoveryResult:
        active_result = self.client.get_instruments(expired=False)
        expired_result = self.client.get_instruments(expired=True)
        if not active_result.ok:
            raise RuntimeError(f"active instrument discovery failed: {active_result.summary()}")
        if not expired_result.ok:
            raise RuntimeError(f"expired instrument discovery failed: {expired_result.summary()}")

        active_rows = _result_list(active_result)
        expired_rows = _result_list(expired_result)
        merged = _merge_instruments(active_rows, expired_rows, self.config)
        _assert_unique_ids(merged)
        path = instrument_dimension_path(self.config)
        write_instrument_dimension(merged, path)
        release_unused_memory()
        return DiscoveryResult(
            instrument_path=path,
            total_rows=len(merged),
            active_rows=sum(1 for row in merged if not row["is_expired"]),
            expired_rows=sum(1 for row in merged if row["is_expired"]),
            invalid_rows=sum(1 for row in merged if row["parse_status"] == int(ParseStatus.INVALID)),
            config_hash=self.config.config_hash,
        )


def write_instrument_dimension(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=instrument_schema())
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def _merge_instruments(active_rows: list[dict[str, Any]], expired_rows: list[dict[str, Any]], config: DeribitConfig) -> list[dict[str, Any]]:
    by_name: dict[str, tuple[dict[str, Any], bool]] = {}
    for row in expired_rows:
        name = str(row.get("instrument_name") or "").upper()
        if name:
            by_name[name] = (row, True)
    for row in active_rows:
        name = str(row.get("instrument_name") or "").upper()
        if name:
            by_name[name] = (row, False)
    return [normalize_instrument(row, is_expired=is_expired, config=config) for _, (row, is_expired) in sorted(by_name.items())]


def _assert_unique_ids(rows: list[dict[str, Any]]) -> None:
    seen: dict[int, str] = {}
    for row in rows:
        instrument_id = int(row["instrument_id"])
        previous = seen.get(instrument_id)
        if previous is not None and previous != row["instrument_name"]:
            raise ValueError(f"instrument_id collision: {instrument_id} for {previous} and {row['instrument_name']}")
        seen[instrument_id] = str(row["instrument_name"])


def _result_list(result: Any) -> list[dict[str, Any]]:
    if not result.ok or not isinstance(result.result, list):
        return []
    return [row for row in result.result if isinstance(row, dict)]


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _option_type(value: Any) -> OptionType:
    text = str(value or "").lower()
    if text in {"call", "c"}:
        return OptionType.CALL
    if text in {"put", "p"}:
        return OptionType.PUT
    return OptionType.UNKNOWN
