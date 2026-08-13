"""Bounded, non-publishing source probes used only by Phase B0.

This module intentionally makes a tiny, fixed number of public read requests.
It never calls collector sync/backfill commands, never mounts or writes a data
root, and records only redacted response metadata under ``STATE_ROOT``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from collectors.common.env import state_root


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
REQUEST_TIMEOUT_SECONDS = 20
BINANCE_S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
USER_AGENT = {"User-Agent": "primus-hmd-b0-source-probe/1"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _sha256(path: str) -> str:
    return hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest()


def _response_summary(
    *,
    name: str,
    response: requests.Response | None,
    started: float,
    error: Exception | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": name,
        "status": "pass" if response is not None and 200 <= response.status_code < 300 else "blocked",
        "http_status": None if response is None else response.status_code,
        "latency_ms": round((time.monotonic() - started) * 1000.0, 2),
        "response_bytes": None if response is None else len(response.content),
    }
    if error is not None:
        payload["error"] = f"{type(error).__name__}: {error}"
    if details:
        payload.update(details)
    return payload


def _json_probe(session: requests.Session, name: str, url: str, params: dict[str, Any]) -> tuple[dict[str, Any], Any | None]:
    started = time.monotonic()
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS, headers=USER_AGENT)
        summary = _response_summary(name=name, response=response, started=started)
        if summary["status"] != "pass":
            return summary, None
        try:
            return summary, response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            summary["status"] = "blocked"
            summary["error"] = f"{type(exc).__name__}: invalid JSON response"
            return summary, None
    except requests.RequestException as exc:
        return _response_summary(name=name, response=None, started=started, error=exc), None


def _s3_first_key(session: requests.Session, name: str, prefix: str) -> dict[str, Any]:
    started = time.monotonic()
    try:
        response = session.get(
            BINANCE_S3,
            params={"list-type": "2", "prefix": prefix, "max-keys": "1"},
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers=USER_AGENT,
        )
        summary = _response_summary(name=name, response=response, started=started)
        if summary["status"] != "pass":
            return summary
        try:
            root = ET.fromstring(response.content)
            key = next((node.text for node in root.findall(".//{*}Key") if node.text), None)
        except ET.ParseError as exc:
            summary["status"] = "blocked"
            summary["error"] = f"{type(exc).__name__}: invalid S3 listing XML"
            return summary
        if not key:
            summary["status"] = "blocked"
            summary["error"] = "empty S3 listing"
            return summary
        summary["first_available_key"] = key
        return summary
    except requests.RequestException as exc:
        return _response_summary(name=name, response=None, started=started, error=exc)


def probe_binance(session: requests.Session | None = None) -> dict[str, Any]:
    """Probe eight public endpoint families exactly once, sequentially."""

    client = session or requests.Session()
    probes: list[dict[str, Any]] = []
    spot, spot_payload = _json_probe(
        client,
        "spot_rest_kline_tail",
        "https://api.binance.com/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
    )
    spot["rows_returned"] = len(spot_payload) if isinstance(spot_payload, list) else 0
    if spot["status"] == "pass" and spot["rows_returned"] <= 0:
        spot.update({"status": "blocked", "error": "empty kline response"})
    probes.append(spot)

    perpetual, perpetual_payload = _json_probe(
        client,
        "usdm_perpetual_rest_kline_tail",
        "https://fapi.binance.com/fapi/v1/klines",
        {"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
    )
    perpetual["rows_returned"] = len(perpetual_payload) if isinstance(perpetual_payload, list) else 0
    if perpetual["status"] == "pass" and perpetual["rows_returned"] <= 0:
        perpetual.update({"status": "blocked", "error": "empty kline response"})
    probes.append(perpetual)

    quarterly, quarterly_payload = _json_probe(
        client,
        "usdm_quarterly_exchange_info",
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        {},
    )
    quarterly_symbols = []
    if isinstance(quarterly_payload, dict):
        quarterly_symbols = [
            item.get("symbol")
            for item in quarterly_payload.get("symbols", [])
            if isinstance(item, dict)
            and item.get("pair") in {"BTCUSDT", "ETHUSDT"}
            and item.get("contractType") in {"CURRENT_QUARTER", "NEXT_QUARTER"}
        ]
    quarterly["active_quarterly_symbols"] = sorted(str(item) for item in quarterly_symbols if item)
    if quarterly["status"] == "pass" and not quarterly["active_quarterly_symbols"]:
        quarterly.update({"status": "blocked", "error": "no configured active quarterly contract"})
    probes.append(quarterly)

    probes.extend(
        [
            _s3_first_key(client, "spot_monthly_vision_listing", "data/spot/monthly/klines/BTCUSDT/1m/"),
            _s3_first_key(client, "usdm_quarterly_monthly_vision_listing", "data/futures/um/monthly/klines/BTCUSDT/1m/"),
            _s3_first_key(client, "usdm_metrics_vision_listing", "data/futures/um/daily/metrics/BTCUSDT/"),
            _s3_first_key(client, "usdm_book_depth_vision_listing", "data/futures/um/daily/bookDepth/BTCUSDT/"),
        ]
    )

    depth, depth_payload = _json_probe(
        client,
        "usdm_orderbook_rest_snapshot",
        "https://fapi.binance.com/fapi/v1/depth",
        {"symbol": "BTCUSDT", "limit": 5},
    )
    bids = depth_payload.get("bids", []) if isinstance(depth_payload, dict) else []
    asks = depth_payload.get("asks", []) if isinstance(depth_payload, dict) else []
    depth["bid_levels"] = len(bids) if isinstance(bids, list) else 0
    depth["ask_levels"] = len(asks) if isinstance(asks, list) else 0
    if depth["status"] == "pass" and (depth["bid_levels"] <= 0 or depth["ask_levels"] <= 0):
        depth.update({"status": "blocked", "error": "empty orderbook response"})
    probes.append(depth)

    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "bounded_binance_source_probe",
        "status": "pass" if all(item["status"] == "pass" for item in probes) else "blocked",
        "observed_at": utc_now_iso(),
        "non_destructive": True,
        "request_policy": "eight sequential public GET requests; no retries, no downloads, no collector command",
        "request_budget": 8,
        "actual_request_count": len(probes),
        "resource_limit": {"container_cpus": 1, "container_memory": "512m"},
        "config_sha256": {
            path: _sha256(path)
            for path in (
                "configs/symbols.crypto.yml",
                "configs/symbols.binance_spot.yml",
                "configs/symbols.binance_usdm_quarterly.yml",
                "configs/symbols.binance_futures_metrics.yml",
                "configs/symbols.binance_orderbook_snapshot.yml",
            )
        },
        "probes": probes,
    }


def probe_vnstock_daily() -> dict[str, Any]:
    """Call one VNStock VCI daily-history request without collector retries."""

    from vnstock.explorer.vci import Quote as VCIQuote

    end = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=8)).isoformat()
    started = time.monotonic()
    try:
        frame = VCIQuote("FPT", show_log=False).history(start=start, end=end, interval="1D", show_log=False)
        rows = 0 if frame is None else len(frame)
        status = "pass" if rows > 0 else "blocked"
        error = None if rows > 0 else "empty VCI daily response"
    except Exception as exc:  # provider client wraps transport/schema errors differently across releases
        rows = 0
        status = "blocked"
        error = f"{type(exc).__name__}: {exc}"
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "bounded_vnstock_daily_source_probe",
        "status": status,
        "observed_at": utc_now_iso(),
        "non_destructive": True,
        "request_policy": "one VNStock VCI daily-history request; no collector retry or publish",
        "request_budget": 1,
        "actual_request_count": 1,
        "resource_limit": {"container_cpus": 1, "container_memory": "512m"},
        "symbol": "FPT",
        "requested_start": start,
        "requested_end": end,
        "rows_returned": rows,
        "latency_ms": round((time.monotonic() - started) * 1000.0, 2),
        "error": error,
        "config_sha256": _sha256("configs/symbols.vn_daily.yml"),
    }


def evidence_path(name: str) -> Path:
    return state_root() / "bootstrap" / "source_probes" / f"{name}.json"


def write_probe(name: str, payload: dict[str, Any]) -> Path:
    path = evidence_path(name)
    _atomic_write_json(path, payload)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Primus B0 bounded non-publishing source probes")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("binance", "vnstock-daily"):
        command = sub.add_parser(name)
        command.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "binance":
        payload = probe_binance()
    else:
        payload = probe_vnstock_daily()
    path = write_probe(args.command, payload)
    payload["evidence_path"] = str(path)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"{args.command} B0 source probe: {payload['status']} ({path})")
    return 0 if payload["status"] == "pass" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
