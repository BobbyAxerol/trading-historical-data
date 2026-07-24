from __future__ import annotations

import json
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collectors.common.env import state_root
from collectors.common.manifest import utc_now_iso
from collectors.deribit.client import DeribitApiResult, DeribitHistoryClient
from collectors.deribit.config import DeribitConfig

COUNT_CANDIDATES = (1000, 5000, 10000)
REQUIRED_TRADE_FIELDS = (
    "trade_id",
    "trade_seq",
    "instrument_name",
    "timestamp",
    "direction",
    "tick_direction",
    "index_price",
    "price",
    "amount",
    "mark_price",
)
OPTION_REQUIRED_FIELDS = ("iv",)


@dataclass(frozen=True)
class ProbeOptions:
    sample_instruments: int = 24
    rate_ramp: bool = False
    max_rps: float = 5.0
    requests_per_rps: int = 2


class DeribitApiProbeRunner:
    def __init__(self, config: DeribitConfig, *, client: Any | None = None, options: ProbeOptions | None = None):
        self.config = config
        self.client = client or DeribitHistoryClient(config, requests_per_second=1.0)
        self.options = options or ProbeOptions()

    @property
    def report_path(self) -> Path:
        return state_root() / "deribit_options" / f"version={self.config.version}" / "api_probe_report.json"

    def run(self) -> dict[str, Any]:
        report: dict[str, Any] = {
            "status": "ok",
            "phase": "Phase 1",
            "updated_at": utc_now_iso(),
            "config_path": str(self.config.config_path),
            "config_hash": self.config.config_hash,
            "dataset_version": self.config.version,
            "currency": self.config.currency,
            "base_url": self.config.raw["api"]["base_url"],
            "assumptions": [],
            "production_backfill_allowed": False,
        }

        instruments_report = self._probe_instruments()
        report["instrument_discovery"] = instruments_report
        probe_instrument = instruments_report.get("probe_instrument")
        if not probe_instrument:
            report["status"] = "blocked"
            report["assumptions"].append("No instrument with trades found; trade endpoint probes are incomplete.")
            report["selected_page_size"] = 1000
            report["safe_trade_rps"] = 1.0
            report["get_instruments_rps"] = 1.0
            self.write_report(report)
            return report

        count_report = self._probe_counts(str(probe_instrument))
        report["count_probe"] = count_report
        report["selected_page_size"] = count_report["selected_page_size"]

        sample_trades = count_report.get("sample_trades", [])
        report["sequence_probe"] = self._probe_sequence_boundaries(str(probe_instrument), sample_trades)
        report["has_more_probe"] = self._probe_has_more(str(probe_instrument), sample_trades)
        report["schema_probe"] = self._probe_schema(sample_trades)
        report["rate_probe"] = self._probe_rate(str(probe_instrument))
        report["safe_trade_rps"] = report["rate_probe"]["safe_trade_rps"]
        report["get_instruments_rps"] = report["rate_probe"]["get_instruments_rps"]
        report["retry_after_behavior"] = report["rate_probe"]["retry_after_behavior"]
        report["oldest_accessible_trade"] = self._oldest_trade(sample_trades)
        report["verified_sorting"] = bool(report["sequence_probe"].get("verified_sorting"))
        report["sequence_boundary_semantics"] = {
            "boundary_seq_appears_in_previous_window": report["sequence_probe"].get("boundary_seq_appears_in_a"),
            "boundary_seq_appears_in_next_window": report["sequence_probe"].get("boundary_seq_appears_in_b"),
            "out_of_range_rows": report["sequence_probe"].get("out_of_range_rows"),
            "interpretation": "start_seq/end_seq behavior is recorded from live probe; downloader must use overlap + dedupe.",
        }
        report["has_more_semantics"] = {
            "small_range": report["has_more_probe"].get("small_range"),
            "after_latest_range": report["has_more_probe"].get("after_latest_range"),
            "empty_vs_unknown_policy": report["has_more_probe"].get("empty_vs_unknown_policy"),
        }
        report["expired_instrument_coverage"] = report["instrument_discovery"].get("expired_instrument_coverage")
        report["field_presence_statistics"] = report["schema_probe"].get("field_presence_statistics")

        mandatory = [
            report.get("selected_page_size"),
            report.get("safe_trade_rps"),
            report.get("get_instruments_rps"),
            report.get("oldest_accessible_trade"),
            report.get("verified_sorting"),
        ]
        rate_verified = report["rate_probe"].get("status") == "ok"
        if all(value not in (None, False, "") for value in mandatory) and rate_verified:
            report["production_backfill_allowed"] = True
        else:
            report["status"] = "blocked"
            if not rate_verified:
                report["assumptions"].append(
                    "Rate ramp was not verified; conservative RPS values are diagnostic only and production backfill remains blocked."
                )
            if not all(value not in (None, False, "") for value in mandatory):
                report["assumptions"].append("Mandatory probe fields are incomplete; production backfill remains blocked.")

        self.write_report(report)
        return report

    def write_report(self, report: dict[str, Any]) -> None:
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.report_path.with_suffix(self.report_path.suffix + ".tmp")
        tmp.write_text(json.dumps(_json_safe(report), indent=2, sort_keys=True))
        os.replace(tmp, self.report_path)

    def _probe_instruments(self) -> dict[str, Any]:
        active = self.client.get_instruments(expired=False)
        expired = self.client.get_instruments(expired=True)
        active_items = _result_list(active)
        expired_items = _result_list(expired)
        all_items = active_items + expired_items
        probe_instrument = self._find_trade_instrument(all_items)
        return {
            "active": _instrument_summary(active, active_items),
            "expired": _instrument_summary(expired, expired_items),
            "total_seen": len({item.get("instrument_name") for item in all_items if isinstance(item, dict)}),
            "probe_instrument": probe_instrument,
            "expired_instrument_coverage": _expired_coverage(expired_items),
        }

    def _find_trade_instrument(self, instruments: list[dict[str, Any]]) -> str | None:
        names = _candidate_instrument_names(instruments, max(1, int(self.options.sample_instruments)))
        for name in names:
            result = self.client.get_last_trades_by_instrument(str(name), count=10, sorting="desc")
            if result.ok and result.trades:
                return str(name)
        return None

    def _probe_counts(self, instrument_name: str) -> dict[str, Any]:
        attempts: list[dict[str, Any]] = []
        selected = 1000
        sample_trades: list[dict[str, Any]] = []
        for count in COUNT_CANDIDATES:
            result = self.client.get_last_trades_by_instrument(instrument_name, count=count, sorting="desc")
            row = {
                "count": count,
                **result.summary(),
            }
            attempts.append(row)
            if result.ok:
                selected = count
                if result.trades:
                    sample_trades = result.trades
        return {
            "instrument_name": instrument_name,
            "attempts": attempts,
            "selected_page_size": selected,
            "sample_trade_count": len(sample_trades),
            "sample_trades": sample_trades[: min(len(sample_trades), 1000)],
        }

    def _probe_sequence_boundaries(self, instrument_name: str, sample_trades: list[dict[str, Any]]) -> dict[str, Any]:
        seqs = sorted({int(row["trade_seq"]) for row in sample_trades if _is_int_like(row.get("trade_seq"))})
        if len(seqs) < 3:
            return {"status": "blocked", "verified_sorting": False, "reason": "not_enough_sample_sequences"}
        start = seqs[0]
        pivot = seqs[min(10, len(seqs) - 2)]
        end = seqs[min(20, len(seqs) - 1)]
        req_a = self.client.get_last_trades_by_instrument(instrument_name, start_seq=start, end_seq=pivot, count=1000, sorting="asc")
        req_b = self.client.get_last_trades_by_instrument(instrument_name, start_seq=pivot, end_seq=end, count=1000, sorting="asc")
        req_c = self.client.get_last_trades_by_instrument(instrument_name, start_seq=start, end_seq=pivot, count=1000, sorting="desc")
        seq_a = _trade_seqs(req_a.trades)
        seq_b = _trade_seqs(req_b.trades)
        seq_c = _trade_seqs(req_c.trades)
        return {
            "status": "ok" if req_a.ok and req_b.ok and req_c.ok else "blocked",
            "requested": {"start_seq": start, "pivot_seq": pivot, "end_seq": end},
            "a_summary": req_a.summary(),
            "b_summary": req_b.summary(),
            "c_summary": req_c.summary(),
            "boundary_seq_appears_in_a": pivot in seq_a,
            "boundary_seq_appears_in_b": pivot in seq_b,
            "asc_monotonic": _is_monotonic(seq_a, ascending=True),
            "desc_monotonic": _is_monotonic(seq_c, ascending=False),
            "out_of_range_rows": int(sum(seq < start or seq > pivot for seq in seq_a) + sum(seq < pivot or seq > end for seq in seq_b)),
            "verified_sorting": bool(_is_monotonic(seq_a, ascending=True) and _is_monotonic(seq_c, ascending=False)),
        }

    def _probe_has_more(self, instrument_name: str, sample_trades: list[dict[str, Any]]) -> dict[str, Any]:
        seqs = sorted({int(row["trade_seq"]) for row in sample_trades if _is_int_like(row.get("trade_seq"))})
        if len(seqs) < 2:
            return {"status": "blocked", "reason": "not_enough_sample_sequences"}
        latest = seqs[-1]
        small = self.client.get_last_trades_by_instrument(instrument_name, start_seq=seqs[0], end_seq=seqs[0], count=1000, sorting="asc")
        after_latest = self.client.get_last_trades_by_instrument(instrument_name, start_seq=latest + 1, end_seq=latest + 10, count=1000, sorting="asc")
        exact_count = self.client.get_last_trades_by_instrument(instrument_name, count=max(1, min(10, len(seqs))), sorting="desc")
        return {
            "status": "ok" if small.ok and after_latest.ok and exact_count.ok else "blocked",
            "small_range": small.summary(),
            "after_latest_range": after_latest.summary(),
            "exact_count_like": exact_count.summary(),
            "empty_vs_unknown_policy": "Only HTTP/JSON-RPC success with result.trades == [] is EMPTY_CONFIRMED; errors are UNKNOWN.",
        }

    def _probe_schema(self, trades: list[dict[str, Any]]) -> dict[str, Any]:
        fields = sorted({key for row in trades if isinstance(row, dict) for key in row})
        stats: dict[str, dict[str, Any]] = {}
        for field in sorted(set(REQUIRED_TRADE_FIELDS + OPTION_REQUIRED_FIELDS)):
            values = [row.get(field) for row in trades if isinstance(row, dict)]
            present = [field in row for row in trades if isinstance(row, dict)]
            stats[field] = {
                "presence_rate": (sum(present) / len(present)) if present else None,
                "null_rate": (sum(value is None for value in values) / len(values)) if values else None,
                "types": sorted({type(value).__name__ for value in values if value is not None}),
            }
        unknown_fields = [field for field in fields if field not in set(REQUIRED_TRADE_FIELDS + OPTION_REQUIRED_FIELDS)]
        return {
            "sample_rows": len(trades),
            "field_presence_statistics": stats,
            "unknown_fields": unknown_fields,
            "required_missing_or_nullable": [
                field
                for field, item in stats.items()
                if item["presence_rate"] not in (None, 1.0) or (item["null_rate"] is not None and item["null_rate"] > 0)
            ],
        }

    def _probe_rate(self, instrument_name: str) -> dict[str, Any]:
        if not self.options.rate_ramp:
            return {
                "status": "conservative_default",
                "safe_trade_rps": min(5.0, float(self.config.raw["api"].get("target_requests_per_second", 5))),
                "get_instruments_rps": 1.0,
                "retry_after_behavior": {"observed": False, "preferred": True},
                "ramp_results": [],
            }
        candidates = [1.0, 2.0, 5.0, 10.0, 15.0, 20.0]
        candidates = [value for value in candidates if value <= self.options.max_rps]
        ramp_results: list[dict[str, Any]] = []
        safe = 1.0
        for rps in candidates:
            latencies: list[float] = []
            rate_limited = 0
            retry_after_seen = False
            limiter_sleep = 1.0 / max(rps, 0.001)
            for _ in range(max(1, int(self.options.requests_per_rps))):
                started = time.perf_counter()
                result = self.client.get_last_trades_by_instrument(instrument_name, count=1, sorting="desc", retry=False)
                if result.latency_ms is not None:
                    latencies.append(result.latency_ms)
                else:
                    latencies.append((time.perf_counter() - started) * 1000.0)
                if result.status_code == 429 or result.error_type == "jsonrpc_error":
                    rate_limited += 1
                retry_after_seen = retry_after_seen or result.retry_after_seconds is not None
                time.sleep(limiter_sleep)
            p95 = _p95(latencies)
            item = {
                "requested_rps": rps,
                "requests": max(1, int(self.options.requests_per_rps)),
                "rate_limited": rate_limited,
                "retry_after_seen": retry_after_seen,
                "latency_p95_ms": p95,
            }
            ramp_results.append(item)
            if rate_limited == 0:
                safe = rps
            else:
                break
        return {
            "status": "ok",
            "safe_trade_rps": safe,
            "get_instruments_rps": 1.0,
            "retry_after_behavior": {"observed": any(row["retry_after_seen"] for row in ramp_results), "preferred": True},
            "ramp_results": ramp_results,
        }

    def _oldest_trade(self, trades: list[dict[str, Any]]) -> str | None:
        timestamps = [int(row["timestamp"]) for row in trades if _is_int_like(row.get("timestamp"))]
        if not timestamps:
            return None
        return str(min(timestamps))


def _result_list(result: DeribitApiResult) -> list[dict[str, Any]]:
    if not result.ok or not isinstance(result.result, list):
        return []
    return [item for item in result.result if isinstance(item, dict)]


def _instrument_summary(result: DeribitApiResult, rows: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [item.get("expiration_timestamp") for item in rows if _is_int_like(item.get("expiration_timestamp"))]
    creations = [item.get("creation_timestamp") for item in rows if _is_int_like(item.get("creation_timestamp"))]
    return {
        **result.summary(),
        "instrument_count": len(rows),
        "oldest_expiration_timestamp": min(timestamps) if timestamps else None,
        "oldest_creation_timestamp": min(creations) if creations else None,
    }


def _candidate_instrument_names(instruments: list[dict[str, Any]], sample_limit: int) -> list[str]:
    raw_names = [item.get("instrument_name") for item in instruments if isinstance(item, dict) and item.get("instrument_name")]
    names = [str(name) for name in raw_names]
    if not names:
        return []

    candidates: list[str] = []
    tail_count = min(len(names), max(4, sample_limit))
    candidates.extend(reversed(names[-tail_count:]))

    if sample_limit > 1 and len(names) > 1:
        for idx in range(sample_limit):
            position = round(idx * (len(names) - 1) / (sample_limit - 1))
            candidates.append(names[position])
    else:
        candidates.append(names[0])

    deduped: list[str] = []
    seen: set[str] = set()
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        deduped.append(name)
        if len(deduped) >= sample_limit:
            break
    return deduped


def _expired_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    expirations = [int(item["expiration_timestamp"]) for item in rows if _is_int_like(item.get("expiration_timestamp"))]
    return {
        "expired_count": len(rows),
        "oldest_expiration_timestamp": min(expirations) if expirations else None,
        "complete_history_assumed": False,
        "note": "Official docs do not guarantee complete expired instrument master; Phase 1 records observed coverage only.",
    }


def _trade_seqs(trades: list[dict[str, Any]]) -> list[int]:
    return [int(row["trade_seq"]) for row in trades if _is_int_like(row.get("trade_seq"))]


def _is_monotonic(values: list[int], *, ascending: bool) -> bool:
    if len(values) < 2:
        return True
    pairs = zip(values, values[1:])
    return all(a <= b for a, b in pairs) if ascending else all(a >= b for a, b in pairs)


def _is_int_like(value: Any) -> bool:
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    return float(statistics.quantiles(values, n=20, method="inclusive")[18])


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value
