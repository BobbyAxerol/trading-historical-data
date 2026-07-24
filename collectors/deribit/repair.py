from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from collectors.deribit.config import DeribitConfig


@dataclass(frozen=True)
class RepairPlan:
    status: str
    phase: str
    only_unresolved: bool
    retryable_instruments: int
    missing_output_ranges: int
    tasks: list[dict[str, Any]]

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "only_unresolved": self.only_unresolved,
            "retryable_instruments": self.retryable_instruments,
            "missing_output_ranges": self.missing_output_ranges,
            "tasks": self.tasks,
        }


class DeribitRepairPlanner:
    def __init__(self, config: DeribitConfig):
        self.config = config

    def run(self, *, only_unresolved: bool = False, limit: int = 100) -> dict[str, Any]:
        if not self.config.checkpoint_path.exists():
            return RepairPlan("blocked", "Phase 4", only_unresolved, 0, 0, []).as_payload()
        with sqlite3.connect(self.config.checkpoint_path) as con:
            con.row_factory = sqlite3.Row
            retryable = con.execute(
                """
                SELECT instrument_name, last_processed_seq, status, failure_count, last_error_code
                FROM instrument_state
                WHERE status IN ('RETRYABLE_ERROR', 'DEAD_LETTER')
                ORDER BY failure_count DESC, instrument_name
                LIMIT ?
                """,
                [int(limit)],
            ).fetchall()
            missing_outputs = con.execute(
                """
                SELECT instrument_name, requested_start_seq, requested_end_seq, retained_trade_count, output_file
                FROM download_ranges
                WHERE retained_trade_count > 0
                  AND (output_file IS NULL OR output_file = '')
                ORDER BY id
                LIMIT ?
                """,
                [int(limit)],
            ).fetchall()
        tasks = [
            {
                "type": "retry_instrument",
                "instrument_name": row["instrument_name"],
                "start_seq": int(row["last_processed_seq"]) + 1,
                "status": row["status"],
                "failure_count": row["failure_count"],
                "last_error_code": row["last_error_code"],
            }
            for row in retryable
        ]
        tasks.extend(
            {
                "type": "missing_output_range",
                "instrument_name": row["instrument_name"],
                "start_seq": row["requested_start_seq"],
                "end_seq": row["requested_end_seq"],
                "retained_trade_count": row["retained_trade_count"],
            }
            for row in missing_outputs
        )
        if only_unresolved:
            tasks = [task for task in tasks if task["type"] in {"retry_instrument", "missing_output_range"}]
        status = "ok" if not tasks else "needs_repair"
        return RepairPlan(status, "Phase 4", only_unresolved, len(retryable), len(missing_outputs), tasks).as_payload()
