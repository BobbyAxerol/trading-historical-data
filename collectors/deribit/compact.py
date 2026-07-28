from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import duckdb

from collectors.common.env import data_root, state_root
from collectors.common.manifest import utc_now_iso
from collectors.common.storage import release_unused_memory
from collectors.deribit.config import DeribitConfig
from collectors.deribit.parquet_parts import publish_existing_file_atomic

CANONICAL_COLUMNS = [
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

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompactResult:
    status: str
    phase: str
    staging_files: int
    days_compacted: int
    output_files: int
    output_rows: int
    conflict_groups: int
    outputs: list[dict[str, Any]]
    conflict_reports: list[str]

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "staging_files": self.staging_files,
            "days_compacted": self.days_compacted,
            "output_files": self.output_files,
            "output_rows": self.output_rows,
            "conflict_groups": self.conflict_groups,
            "outputs": self.outputs,
            "conflict_reports": self.conflict_reports,
        }


class DeribitCompactor:
    def __init__(self, config: DeribitConfig):
        self.config = config

    def run(self, *, max_days: int | None = None, progress_every: int = 25) -> dict[str, Any]:
        staging_files = self._staging_files()
        if not staging_files:
            return CompactResult("ok", "Phase 4", 0, 0, 0, 0, 0, [], []).as_payload()

        con = duckdb.connect(database=":memory:")
        try:
            self._configure_duckdb(con)
            con.execute(f"CREATE OR REPLACE TEMP VIEW staging_input AS SELECT * FROM read_parquet({_sql_path_list(staging_files)}, union_by_name=true)")
            days = [row[0] for row in con.execute("SELECT DISTINCT CAST(to_timestamp(timestamp_ms / 1000.0) AS DATE) AS d FROM staging_input ORDER BY d").fetchall()]
            if max_days is not None:
                days = days[: max(0, int(max_days))]
            LOGGER.info(
                "deribit_compact_start staging_files=%s days_planned=%s max_days=%s",
                len(staging_files),
                len(days),
                max_days,
            )
            outputs: list[dict[str, Any]] = []
            conflict_reports: list[str] = []
            conflict_groups = 0
            output_rows = 0
            every = max(1, int(progress_every))
            for idx, day in enumerate(days, start=1):
                if idx == 1 or idx % every == 0 or idx == len(days):
                    LOGGER.info("deribit_compact_day_start day=%s progress=%s/%s", day, idx, len(days))
                item = self._compact_day(con, day)
                outputs.append(item["output"])
                output_rows += int(item["output"]["rows"])
                if item["conflict_report"]:
                    conflict_reports.append(str(item["conflict_report"]))
                conflict_groups += int(item["conflict_groups"])
                if idx == 1 or idx % every == 0 or idx == len(days):
                    LOGGER.info(
                        "deribit_compact_day_done day=%s progress=%s/%s rows=%s conflict_groups=%s",
                        day,
                        idx,
                        len(days),
                        item["output"]["rows"],
                        item["conflict_groups"],
                    )
                release_unused_memory()
            status = "ok" if conflict_groups == 0 else "warning"
            LOGGER.info(
                "deribit_compact_done status=%s staging_files=%s days_compacted=%s output_files=%s output_rows=%s conflict_groups=%s",
                status,
                len(staging_files),
                len(days),
                len(outputs),
                output_rows,
                conflict_groups,
            )
            return CompactResult(status, "Phase 4", len(staging_files), len(days), len(outputs), output_rows, conflict_groups, outputs, conflict_reports).as_payload()
        finally:
            con.close()
            release_unused_memory()

    def _compact_day(self, con: duckdb.DuckDBPyConnection, day: date) -> dict[str, Any]:
        start_ms, end_ms = _day_bounds_ms(day)
        existing = self._canonical_path(day)
        canonical_cols = ", ".join(CANONICAL_COLUMNS)
        staging_sql = f"""
            SELECT {canonical_cols}, source_priority, ingested_at
            FROM staging_input
            WHERE timestamp_ms >= {start_ms} AND timestamp_ms < {end_ms}
        """
        if existing.exists():
            existing_sql = f"""
                SELECT
                    {canonical_cols},
                    CAST(0 AS BIGINT) AS source_priority,
                    CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS ingested_at
                FROM read_parquet({_sql_quote(str(existing))}, union_by_name=true)
            """
            con.execute(f"CREATE OR REPLACE TEMP VIEW day_input AS {staging_sql} UNION ALL {existing_sql}")
        else:
            con.execute(f"CREATE OR REPLACE TEMP VIEW day_input AS {staging_sql}")
        conflict_rows = con.execute(
            """
            SELECT instrument_id, trade_seq, COUNT(*) AS row_count,
                   COUNT(DISTINCT hash(timestamp_ms, trade_id_hash, price_btc, mark_price_btc, iv_pct, index_price_usd, amount_base, direction, tick_direction, flags)) AS variant_count
            FROM day_input
            GROUP BY instrument_id, trade_seq
            HAVING row_count > 1 AND variant_count > 1
            ORDER BY instrument_id, trade_seq
            LIMIT 1000
            """
        ).fetchall()
        conflict_report = self._write_conflicts(day, conflict_rows) if conflict_rows else None

        output = self._canonical_path(day)
        output.parent.mkdir(parents=True, exist_ok=True)
        tmp = output.with_name(output.name + ".tmp")
        if tmp.exists():
            tmp.unlink()
        columns = ", ".join(CANONICAL_COLUMNS)
        con.execute(
            f"""
            COPY (
                SELECT {columns}
                FROM (
                    SELECT
                        {columns},
                        row_number() OVER (
                            PARTITION BY instrument_id, trade_seq
                            ORDER BY source_priority DESC, ingested_at DESC
                        ) AS rn
                    FROM day_input
                )
                WHERE rn = 1
                ORDER BY timestamp_ms, instrument_id, trade_seq
            )
            TO {_sql_quote(str(tmp))}
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)
            """
        )
        result = publish_existing_file_atomic(tmp, output)
        rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_sql_quote(str(output))})").fetchone()[0])
        result.update({"rows": rows, "day": day.isoformat()})
        return {"output": {key: str(value) if isinstance(value, Path) else value for key, value in result.items()}, "conflict_groups": len(conflict_rows), "conflict_report": conflict_report}

    def _configure_duckdb(self, con: duckdb.DuckDBPyConnection) -> None:
        memory_mb = int(self.config.raw["compaction"].get("memory_limit_mb", 1024))
        temp_dir = self.config.raw["compaction"].get("temp_directory", "storage/_tmp/duckdb")
        temp_path = data_root() / Path(str(temp_dir)).relative_to("storage") if str(temp_dir).startswith("storage/") else Path(str(temp_dir))
        temp_path.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET memory_limit='{memory_mb}MB'")
        con.execute(f"SET temp_directory={_sql_quote(str(temp_path))}")

    def _canonical_path(self, day: date) -> Path:
        return self.config.canonical_trades_root / f"currency={self.config.currency}" / f"year={day.year:04d}" / f"month={day.month:02d}" / f"day={day.day:02d}" / "part-00000.parquet"

    def _write_conflicts(self, day: date, rows: list[tuple[Any, ...]]) -> Path:
        path = state_root() / "deribit_options" / f"version={self.config.version}" / "conflicts" / f"{day.isoformat()}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": "warning",
            "day": day.isoformat(),
            "created_at": utc_now_iso(),
            "conflict_groups": len(rows),
            "sample": [
                {"instrument_id": int(item[0]), "trade_seq": int(item[1]), "row_count": int(item[2]), "variant_count": int(item[3])}
                for item in rows[:100]
            ],
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(path)
        return path

    def _staging_files(self) -> list[Path]:
        if not self.config.staging_root.exists():
            return []
        return sorted(path for path in self.config.staging_root.rglob("*.parquet") if not path.name.endswith(".tmp"))


def _day_bounds_ms(day: date) -> tuple[int, int]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = datetime.combine(day, time.max, tzinfo=timezone.utc).replace(microsecond=0)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000) + 1000


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(_sql_quote(str(path)) for path in paths) + "]"
