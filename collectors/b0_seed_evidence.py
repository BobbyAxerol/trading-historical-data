"""Bounded-seed evidence and capacity recording for Phase B0.

This module never contacts a market-data source.  It inspects only the
dedicated runtime tree after a fixed B0 seed has run, records bounded metadata,
and updates the B0 capacity report only when every seed result is demonstrably
successful.  Raw data and request/credential values are intentionally absent
from the evidence.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from collectors.production_preflight import POLICY_PATH, _atomic_write_json, _load_policy, _runtime_paths, utc_now_iso


EVIDENCE_RELATIVE_PATH = Path("state") / "bootstrap" / "b0_bounded_seed.json"
SEED_IDS = (
    "binance_futures_1m",
    "binance_quarterly_1m",
    "binance_spot_1m",
    "binance_metrics_5m",
    "binance_orderbook_1h",
    "vn_equity_daily",
    "vn30f1m_vndirect_daily",
)
SEED_GROUPS = {
    "binance_perpetual_spot_quarterly": ("binance_futures_1m", "binance_quarterly_1m", "binance_spot_1m"),
    "binance_metrics_orderbook": ("binance_metrics_5m", "binance_orderbook_1h"),
    "vn_daily_derivatives": ("vn_equity_daily", "vn30f1m_vndirect_daily"),
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for candidate in path.rglob("*"):
        try:
            if candidate.is_file() and not candidate.is_symlink():
                total += candidate.stat().st_size
        except OSError:
            continue
    return total


def _filesystem_snapshot(root: Path) -> dict[str, int]:
    stats = os.statvfs(root)
    return {
        "available_bytes": stats.f_frsize * stats.f_bavail,
        "available_inodes": stats.f_favail,
        "free_bytes": stats.f_frsize * stats.f_bfree,
        "free_inodes": stats.f_ffree,
    }


def _runtime_snapshot(runtime: dict[str, Path]) -> dict[str, Any]:
    return {
        "observed_at": utc_now_iso(),
        # In the collector container `/app` also contains the immutable image
        # layer.  `storage` is the bind-mounted runtime filesystem whose free
        # space actually limits canonical collection.
        "filesystem": _filesystem_snapshot(runtime["storage"]),
        "runtime_bytes": {
            "storage": _directory_bytes(runtime["storage"]),
            "state": _directory_bytes(runtime["state"]),
            "logs": _directory_bytes(runtime["logs"]),
        },
    }


def _delta_bytes(before: dict[str, Any], after: dict[str, Any]) -> dict[str, int]:
    previous = before.get("runtime_bytes") if isinstance(before, dict) else {}
    current = after.get("runtime_bytes") if isinstance(after, dict) else {}
    return {
        name: int(current.get(name, 0)) - int(previous.get(name, 0))
        for name in ("storage", "state", "logs")
    }


def _has_parquet(root: Path) -> bool:
    try:
        return next(root.rglob("*.parquet"), None) is not None
    except OSError:
        return False


def _heartbeat(state: Path, dataset: str) -> dict[str, Any]:
    payload = _read_json(state / "heartbeats" / f"{dataset}.json")
    return {
        "status": payload.get("status"),
        "updated_at": payload.get("updated_at"),
        "peak_rss_mb": payload.get("peak_rss_mb"),
    }


def _manifest_symbol_check(
    runtime: dict[str, Path],
    *,
    seed_id: str,
    dataset: str,
    symbol: str,
    component: tuple[str, ...],
) -> dict[str, Any]:
    manifest = _read_json(runtime["state"] / "manifests" / f"{dataset}.json")
    symbols = manifest.get("symbols") if isinstance(manifest.get("symbols"), dict) else {}
    record = symbols.get(symbol) if isinstance(symbols, dict) else None
    record = record if isinstance(record, dict) else {}
    heartbeat = _heartbeat(runtime["state"], dataset)
    component_root = runtime["storage"].joinpath(*component) / f"symbol={symbol}"
    success = bool(
        record.get("last_success_at")
        and not record.get("last_error")
        and heartbeat.get("status") in {"ok", "success"}
        and _has_parquet(component_root)
    )
    return {
        "seed_id": seed_id,
        "status": "pass" if success else "blocked",
        "dataset": dataset,
        "symbol": symbol,
        "last_success_at": record.get("last_success_at"),
        "last_error_present": bool(record.get("last_error")),
        "heartbeat": heartbeat,
        "canonical_component": str(component_root.relative_to(runtime["storage"])),
        "canonical_parquet_present": _has_parquet(component_root),
    }


def _quarterly_check(runtime: dict[str, Path]) -> dict[str, Any]:
    contracts = _read_json(runtime["state"] / "binance_usdm_quarterly_contracts.json")
    selected = contracts.get("selected_symbols")
    selected = [str(item).upper() for item in selected] if isinstance(selected, list) else []
    if len(selected) != 1:
        return {
            "seed_id": "binance_quarterly_1m",
            "status": "blocked",
            "dataset": "crypto_binance_usdm_quarterly_1m",
            "reason": "bounded quarterly seed must select exactly one current contract",
            "selected_symbol_count": len(selected),
        }
    return _manifest_symbol_check(
        runtime,
        seed_id="binance_quarterly_1m",
        dataset="crypto_binance_usdm_quarterly_1m",
        symbol=selected[0],
        component=("crypto", "binance_futures", "1m"),
    )


def _vndirect_check(runtime: dict[str, Path]) -> dict[str, Any]:
    payload = _read_json(runtime["state"] / "vn_derivatives" / "vndirect_dchart_1d.json")
    heartbeat = _heartbeat(runtime["state"], "vn30f1m_vndirect")
    component = runtime["storage"] / "vn" / "futures" / "continuous" / "1d" / "symbol=VN30F1M" / "source=vndirect_dchart" / "version=v1"
    success = bool(
        payload.get("status") == "ok"
        and int(payload.get("rows_written") or 0) > 0
        and heartbeat.get("status") in {"ok", "success"}
        and _has_parquet(component)
    )
    return {
        "seed_id": "vn30f1m_vndirect_daily",
        "status": "pass" if success else "blocked",
        "dataset": "vn30f1m_vndirect",
        "rows_written": payload.get("rows_written"),
        "heartbeat": heartbeat,
        "canonical_component": str(component.relative_to(runtime["storage"])),
        "canonical_parquet_present": _has_parquet(component),
    }


def evaluate_seed(runtime: dict[str, Path], seed_id: str, *, process_exit_code: int) -> dict[str, Any]:
    """Return secret-safe proof that one fixed seed published canonical data."""

    if seed_id not in SEED_IDS:
        raise ValueError(f"unknown bounded seed id: {seed_id!r}")
    if process_exit_code != 0:
        return {
            "seed_id": seed_id,
            "status": "blocked",
            "reason": "collector process exited non-zero",
            "process_exit_code": int(process_exit_code),
        }

    checks: dict[str, Any] = {
        "binance_futures_1m": lambda: _manifest_symbol_check(
            runtime,
            seed_id="binance_futures_1m",
            dataset="crypto_binance_futures_1m",
            symbol="BTCUSDT",
            component=("crypto", "binance_futures", "1m"),
        ),
        "binance_quarterly_1m": lambda: _quarterly_check(runtime),
        "binance_spot_1m": lambda: _manifest_symbol_check(
            runtime,
            seed_id="binance_spot_1m",
            dataset="crypto_binance_spot_1m",
            symbol="BTCUSDT",
            component=("crypto", "binance_spot", "1m"),
        ),
        "binance_metrics_5m": lambda: _manifest_symbol_check(
            runtime,
            seed_id="binance_metrics_5m",
            dataset="crypto_binance_futures_metrics_5m",
            symbol="BTCUSDT",
            component=("crypto", "binance_futures_metrics", "5m"),
        ),
        "binance_orderbook_1h": lambda: _manifest_symbol_check(
            runtime,
            seed_id="binance_orderbook_1h",
            dataset="crypto_binance_orderbook_snapshot_1h",
            symbol="BTCUSDT",
            component=("crypto", "binance_orderbook_snapshot", "1h"),
        ),
        "vn_equity_daily": lambda: _manifest_symbol_check(
            runtime,
            seed_id="vn_equity_daily",
            dataset="vn_equity_1d",
            symbol="FPT",
            component=("vn", "equity", "1d"),
        ),
        "vn30f1m_vndirect_daily": lambda: _vndirect_check(runtime),
    }
    result = checks[seed_id]()
    result["process_exit_code"] = 0
    return result


def _evidence_path(runtime: dict[str, Path]) -> Path:
    return runtime["root"] / EVIDENCE_RELATIVE_PATH


def start_bounded_seed(policy: dict[str, Any], *, plan: dict[str, Any]) -> dict[str, Any]:
    """Create a new evidence record before the fixed seed performs any write."""

    approval = policy.get("b0_seed_approval")
    if not isinstance(approval, dict) or approval.get("status") != "approved":
        raise RuntimeError("B0 bounded seed is not owner-approved in policy")
    runtime = _runtime_paths(policy)
    payload = {
        "schema_version": 1,
        "phase": "B0",
        "status": "running",
        "started_at": utc_now_iso(),
        "approval": {
            "approved_by": approval.get("approved_by"),
            "approved_at": approval.get("approved_at"),
            "scope": approval.get("scope"),
        },
        "fixed_plan": plan,
        "baseline": _runtime_snapshot(runtime),
        "steps": {},
        "deribit_capacity_basis": "No Deribit data seed is run here; B0 uses the official 6-9 GiB canonical and 20 GiB staging budget plus the independent new-VPS API probe.",
    }
    _atomic_write_json(_evidence_path(runtime), payload)
    return payload


def begin_seed_step(policy: dict[str, Any], seed_id: str) -> dict[str, Any]:
    if seed_id not in SEED_IDS:
        raise ValueError(f"unknown bounded seed id: {seed_id!r}")
    runtime = _runtime_paths(policy)
    path = _evidence_path(runtime)
    evidence = _read_json(path)
    if evidence.get("status") != "running":
        raise RuntimeError("bounded seed evidence is not running")
    steps = evidence.setdefault("steps", {})
    steps[seed_id] = {
        "seed_id": seed_id,
        "status": "running",
        "started_at": utc_now_iso(),
        "snapshot_before": _runtime_snapshot(runtime),
    }
    _atomic_write_json(path, evidence)
    return steps[seed_id]


def complete_seed_step(policy: dict[str, Any], seed_id: str, *, process_exit_code: int) -> dict[str, Any]:
    runtime = _runtime_paths(policy)
    path = _evidence_path(runtime)
    evidence = _read_json(path)
    steps = evidence.get("steps") if isinstance(evidence.get("steps"), dict) else {}
    started = steps.get(seed_id) if isinstance(steps.get(seed_id), dict) else {}
    before = started.get("snapshot_before") if isinstance(started.get("snapshot_before"), dict) else _runtime_snapshot(runtime)
    after = _runtime_snapshot(runtime)
    result = evaluate_seed(runtime, seed_id, process_exit_code=process_exit_code)
    result.update(
        {
            "completed_at": utc_now_iso(),
            "snapshot_before": before,
            "snapshot_after": after,
            "runtime_delta_bytes": _delta_bytes(before, after),
        }
    )
    evidence.setdefault("steps", {})[seed_id] = result
    _atomic_write_json(path, evidence)
    return result


def _seed_measurement(step: dict[str, Any]) -> dict[str, Any]:
    heartbeat = step.get("heartbeat") if isinstance(step.get("heartbeat"), dict) else {}
    return {
        "seed_id": step.get("seed_id"),
        "status": step.get("status"),
        "runtime_delta_bytes": step.get("runtime_delta_bytes"),
        "peak_rss_mb": heartbeat.get("peak_rss_mb"),
    }


def finalize_bounded_seed(policy: dict[str, Any]) -> dict[str, Any]:
    """Approve measured B0 capacity evidence for one-writer staged operation."""

    runtime = _runtime_paths(policy)
    evidence_path = _evidence_path(runtime)
    evidence = _read_json(evidence_path)
    steps = evidence.get("steps") if isinstance(evidence.get("steps"), dict) else {}
    missing_or_failed = [seed_id for seed_id in SEED_IDS if not isinstance(steps.get(seed_id), dict) or steps[seed_id].get("status") != "pass"]
    if missing_or_failed:
        evidence.update(
            {
                "status": "blocked",
                "completed_at": utc_now_iso(),
                "blockers": missing_or_failed,
            }
        )
        _atomic_write_json(evidence_path, evidence)
        raise RuntimeError(f"bounded seed cannot finalize; blocked steps: {missing_or_failed!r}")

    approval = policy["b0_seed_approval"]
    grouped_measurements = {
        group: [_seed_measurement(steps[seed_id]) for seed_id in seed_ids]
        for group, seed_ids in SEED_GROUPS.items()
    }
    capacity_path = runtime["bootstrap"] / "capacity_report.json"
    capacity = _read_json(capacity_path)
    if not capacity:
        raise RuntimeError("capacity report is missing")
    capacity.update(
        {
            "status": "pass",
            "status_reason": "Measured sequential bounded seeds passed. Approval is limited to one-writer staged collection; it is not approval for concurrent or full-history jobs.",
            "approval": {
                "status": "approved",
                "approved_by": approval.get("approved_by"),
                "approved_at": approval.get("approved_at"),
                "scope": "B0 bounded seed and staged single-writer operation only",
            },
            "bounded_seed_evidence": str(EVIDENCE_RELATIVE_PATH),
            "bounded_seed_measurements": {
                "completed_at": utc_now_iso(),
                "groups": grouped_measurements,
                "deribit_capacity_basis": evidence["deribit_capacity_basis"],
                "final_runtime_snapshot": _runtime_snapshot(runtime),
            },
        }
    )
    _atomic_write_json(capacity_path, capacity)
    evidence.update(
        {
            "status": "pass",
            "completed_at": utc_now_iso(),
            "final_runtime_snapshot": _runtime_snapshot(runtime),
            "capacity_report": "state/bootstrap/capacity_report.json",
        }
    )
    _atomic_write_json(evidence_path, evidence)
    return evidence


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect B0 bounded-seed evidence without contacting a source.")
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    policy = _load_policy(args.policy)
    payload = _read_json(_evidence_path(_runtime_paths(policy)))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"B0 bounded seed: {payload.get('status', 'absent')}")
    return 0 if payload.get("status") == "pass" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
