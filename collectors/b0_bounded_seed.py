"""The only writer permitted by the B0 bounded-seed exception.

The Docker entrypoint admits this module only when the owner-approved temporary
seed environment is present.  It accepts no CLI arguments and owns every
collector command and time window, so an operator cannot turn a B0 seed into a
broad historical backfill by changing Compose arguments.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from collectors.b0_seed_evidence import (
    SEED_IDS,
    begin_seed_step,
    complete_seed_step,
    finalize_bounded_seed,
    start_bounded_seed,
)
from collectors.common.operational_events import record_event
from collectors.production_preflight import POLICY_PATH, _load_policy


def _utc_window() -> tuple[str, str, str]:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    crypto_start = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    vn_start = (now - timedelta(days=7)).date().isoformat()
    vn_end = now.date().isoformat()
    return crypto_start, vn_start, vn_end


def _fixed_steps() -> tuple[dict[str, Any], list[tuple[str, list[str]]]]:
    crypto_start, vn_start, vn_end = _utc_window()
    plan = {
        "runner": "collectors.b0_bounded_seed",
        "maximum_windows": {
            "binance_1m": "24 hours, BTCUSDT only",
            "binance_metrics": "one day, BTCUSDT only",
            "binance_orderbook": "one current REST snapshot, BTCUSDT only",
            "vn": "seven calendar days, FPT and VN30F1M only",
        },
        "prohibited": ["old-VPS import", "archive-wide discovery", "concurrent jobs", "Deribit backfill"],
        "windows": {"crypto_start_utc": crypto_start, "vn_start": vn_start, "vn_end": vn_end},
    }
    steps = [
        (
            "binance_futures_1m",
            [
                sys.executable,
                "-m",
                "collectors.crypto_1m",
                "--mode",
                "backfill",
                "--symbols",
                "BTCUSDT",
                "--backfill-start",
                crypto_start,
            ],
        ),
        (
            "binance_quarterly_1m",
            [
                sys.executable,
                "-m",
                "collectors.binance_usdm_quarterly_1m",
                "--mode",
                "once",
                "--pairs",
                "BTCUSDT",
                "--max-symbols",
                "1",
                "--no-archive-discovery",
                "--no-monthly",
                "--no-daily",
                "--rest-start",
                crypto_start,
            ],
        ),
        (
            "binance_spot_1m",
            [
                sys.executable,
                "-m",
                "collectors.binance_spot_1m",
                "--mode",
                "once",
                "--symbols",
                "BTCUSDT",
                "--backfill-start",
                crypto_start,
                "--no-monthly",
                "--no-daily",
            ],
        ),
        (
            "binance_metrics_5m",
            [
                sys.executable,
                "-m",
                "collectors.binance_futures_metrics_5m",
                "--mode",
                "once",
                "--symbols",
                "BTCUSDT",
                "--no-legacy",
                "--no-vision",
                "--rest-tail-days",
                "1",
                "--rest-overlap-hours",
                "1",
            ],
        ),
        (
            "binance_orderbook_1h",
            [
                sys.executable,
                "-m",
                "collectors.binance_orderbook_snapshot_1h",
                "--mode",
                "once",
                "--symbols",
                "BTCUSDT",
                "--no-vision",
                "--lookback-days",
                "1",
            ],
        ),
        (
            "vn_equity_daily",
            [
                sys.executable,
                "-m",
                "collectors.vn_daily",
                "--mode",
                "once",
                "--symbols",
                "FPT",
                "--backfill-start",
                vn_start,
                "--skip-derived",
            ],
        ),
        (
            "vn30f1m_vndirect_daily",
            [
                sys.executable,
                "-m",
                "collectors.vn_derivatives",
                "sync-vndirect",
                "--mode",
                "once",
                "--start",
                vn_start,
                "--end",
                vn_end,
            ],
        ),
    ]
    return plan, steps


def _runtime_override() -> None:
    """Map the host policy to the collector service's mounted runtime tree."""

    data_root = os.getenv("DATA_ROOT")
    if data_root:
        os.environ["HISTORICAL_MARKET_DATA_RUNTIME_ROOT"] = str(Path(data_root).resolve().parent)


def run() -> int:
    _runtime_override()
    policy = _load_policy(POLICY_PATH)
    plan, steps = _fixed_steps()
    if tuple(seed_id for seed_id, _ in steps) != SEED_IDS:
        raise RuntimeError("bounded seed plan does not match the required evidence order")

    start_bounded_seed(policy, plan=plan)
    for seed_id, command in steps:
        begin_seed_step(policy, seed_id)
        completed = subprocess.run(command, check=False).returncode
        result = complete_seed_step(policy, seed_id, process_exit_code=completed)
        if result["status"] != "pass":
            record_event("collector_exit", status="alert", summary=f"B0 bounded seed failed: {seed_id}")
            try:
                finalize_bounded_seed(policy)
            except RuntimeError:
                pass
            return 2

    finalize_bounded_seed(policy)
    record_event("collector_exit", status="ok", summary="B0 bounded seed completed successfully")
    record_event("validation_repair", status="ok", summary="B0 bounded seed canonical publish checks completed successfully")
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv:
        raise SystemExit("collectors.b0_bounded_seed accepts no arguments")
    return run()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
