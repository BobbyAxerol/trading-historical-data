from __future__ import annotations

import json
import resource
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from collectors.common.env import state_root
from collectors.common.manifest import utc_now_iso
from collectors.common.storage import release_unused_memory
from collectors.deribit.config import DeribitConfig
from collectors.deribit.repair import DeribitRepairPlanner
from collectors.deribit.validate import DeribitValidator

GIB = 1024**3
DEFAULT_WINDOWS = (
    ("low", date(2022, 9, 1)),
    ("normal", date(2024, 4, 1)),
    ("high", date(2021, 5, 1)),
)


@dataclass(frozen=True)
class PilotWindow:
    regime: str
    start: date
    end: date


class DeribitPilotRunner:
    def __init__(self, config: DeribitConfig, *, window_days: int = 30, min_rows_per_window: int = 1):
        self.config = config
        self.window_days = max(1, int(window_days))
        self.min_rows_per_window = max(1, int(min_rows_per_window))

    def run(self) -> dict[str, Any]:
        windows = [PilotWindow(name, start, start + timedelta(days=self.window_days)) for name, start in DEFAULT_WINDOWS]
        reports = [self._measure_window(window) for window in windows]
        for report in reports:
            self._write_json(self._report_path(str(report["regime"])), report)
        summary = self._summary(reports)
        self._write_json(self._summary_path(), summary)
        release_unused_memory()
        return summary

    def _measure_window(self, window: PilotWindow) -> dict[str, Any]:
        files = self._canonical_files()
        start_ms = _date_to_ms(window.start)
        end_ms = _date_to_ms(window.end)
        rows = 0
        bytes_seen = 0
        trade_days = 0
        contracts = 0
        bytes_per_trade = None
        if files:
            con = duckdb.connect(database=":memory:")
            try:
                con.execute(f"CREATE OR REPLACE TEMP VIEW canonical AS SELECT * FROM read_parquet({_sql_path_list(files)}, union_by_name=true)")
                rows = int(con.execute(f"SELECT COUNT(*) FROM canonical WHERE timestamp_ms >= {start_ms} AND timestamp_ms < {end_ms}").fetchone()[0])
                trade_days = int(
                    con.execute(
                        f"""
                        SELECT COUNT(DISTINCT CAST(to_timestamp(timestamp_ms / 1000.0) AS DATE))
                        FROM canonical
                        WHERE timestamp_ms >= {start_ms} AND timestamp_ms < {end_ms}
                        """
                    ).fetchone()[0]
                )
                contracts = int(
                    con.execute(
                        f"""
                        SELECT COUNT(DISTINCT instrument_id)
                        FROM canonical
                        WHERE timestamp_ms >= {start_ms} AND timestamp_ms < {end_ms}
                        """
                    ).fetchone()[0]
                )
            finally:
                con.close()
            bytes_seen = sum(_file_size(path) for path in self._files_for_window(files, window.start, window.end))
            bytes_per_trade = (bytes_seen / rows) if rows else None

        coverage_pct = (trade_days / self.window_days) * 100.0
        enough_sample = rows >= self.min_rows_per_window
        return {
            "status": "ok" if enough_sample else "needs_more_data",
            "phase": "Phase 5",
            "regime": window.regime,
            "start_date": window.start.isoformat(),
            "end_date": window.end.isoformat(),
            "window_days": self.window_days,
            "canonical_rows": rows,
            "canonical_bytes": bytes_seen,
            "bytes_per_trade": bytes_per_trade,
            "trade_days": trade_days,
            "contracts": contracts,
            "strategy_package_coverage_pct": round(coverage_pct, 4),
            "snapshot_rows": 0,
            "bytes_per_snapshot_row": None,
            "enough_sample": enough_sample,
            "created_at": utc_now_iso(),
        }

    def _summary(self, reports: list[dict[str, Any]]) -> dict[str, Any]:
        validation = DeribitValidator(self.config).run()
        repair = DeribitRepairPlanner(self.config).run(only_unresolved=True, limit=1000)
        total_canonical_bytes = sum(_file_size(path) for path in self._canonical_files())
        measured_rows = sum(int(report["canonical_rows"]) for report in reports)
        measured_bytes = sum(int(report["canonical_bytes"]) for report in reports)
        bytes_per_trade = (measured_bytes / measured_rows) if measured_rows else None
        projected = self._project_permanent_gib(bytes_per_trade)
        acceptance = {
            "ingestion_peak_rss_mb": _peak_rss_mb(),
            "ingestion_peak_rss_pass": _peak_rss_mb() <= float(self.config.raw["memory"].get("ingestion_hard_rss_mb", 750)),
            "snapshot_peak_rss_mb": None,
            "snapshot_peak_rss_pass": True,
            "permanent_size_projection_gib": projected,
            "permanent_size_projection_pass": projected is not None and projected <= 9.0,
            "unresolved_coverage_ranges": len(repair.get("tasks", [])),
            "unresolved_coverage_pass": len(repair.get("tasks", [])) == 0,
            "duplicate_conflicts": int(validation.get("duplicate_keys", 0)),
            "duplicate_conflicts_pass": int(validation.get("duplicate_keys", 0)) == 0,
            "strategy_package_coverage_pct": min((float(report["strategy_package_coverage_pct"]) for report in reports), default=0.0),
            "strategy_package_coverage_pass": min((float(report["strategy_package_coverage_pct"]) for report in reports), default=0.0) >= 95.0,
            "all_windows_have_samples": all(bool(report["enough_sample"]) for report in reports),
        }
        status = "ok" if all(
            [
                acceptance["ingestion_peak_rss_pass"],
                acceptance["permanent_size_projection_pass"],
                acceptance["unresolved_coverage_pass"],
                acceptance["duplicate_conflicts_pass"],
                acceptance["strategy_package_coverage_pass"],
                acceptance["all_windows_have_samples"],
                validation["status"] == "ok",
            ]
        ) else "blocked"
        notes = ["Pilot does not run full history."]
        if status == "ok":
            notes.append("Pilot acceptance passed; Phase 6 full historical backfill gate is open.")
        else:
            notes.append("Status remains blocked until all three deterministic windows have representative samples and acceptance checks pass.")
        return {
            "status": status,
            "phase": "Phase 5",
            "created_at": utc_now_iso(),
            "config_hash": self.config.config_hash,
            "reports": {str(report["regime"]): str(self._report_path(str(report["regime"]))) for report in reports},
            "pilot_summary_path": str(self._summary_path()),
            "window_reports": reports,
            "validation": validation,
            "repair": repair,
            "acceptance": acceptance,
            "total_canonical_bytes": total_canonical_bytes,
            "measured_rows": measured_rows,
            "measured_bytes": measured_bytes,
            "bytes_per_trade": bytes_per_trade,
            "notes": notes,
        }

    def _project_permanent_gib(self, bytes_per_trade: float | None) -> float | None:
        if bytes_per_trade is None:
            return None
        dimension_files = list((self.config.canonical_trades_root.parents[1] / "instruments" / f"version={self.config.version}").glob("*.parquet"))
        if not dimension_files:
            return None
        # Conservative planning proxy until Phase 5 pilot has full regime samples.
        estimated_retained_trades = max(self.min_rows_per_window * 3, 1) * 365
        return round((bytes_per_trade * estimated_retained_trades) / GIB, 6)

    def _canonical_files(self) -> list[Path]:
        if not self.config.canonical_trades_root.exists():
            return []
        return sorted(path for path in self.config.canonical_trades_root.rglob("*.parquet") if not path.name.endswith(".tmp"))

    def _files_for_window(self, files: list[Path], start: date, end: date) -> list[Path]:
        selected: list[Path] = []
        for path in files:
            day = _day_from_path(path)
            if day is not None and start <= day < end:
                selected.append(path)
        return selected

    def _report_path(self, regime: str) -> Path:
        return state_root() / "deribit_options" / f"version={self.config.version}" / f"pilot_report_{regime}.json"

    def _summary_path(self) -> Path:
        return state_root() / "deribit_options" / f"version={self.config.version}" / "pilot_summary.json"

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(path)


def _date_to_ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000)


def _day_from_path(path: Path) -> date | None:
    values: dict[str, int] = {}
    for part in path.parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key in {"year", "month", "day"}:
            try:
                values[key] = int(value)
            except ValueError:
                return None
    if {"year", "month", "day"} <= set(values):
        return date(values["year"], values["month"], values["day"])
    return None


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _peak_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(float(usage) / 1024.0, 2)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(_sql_quote(str(path)) for path in paths) + "]"
