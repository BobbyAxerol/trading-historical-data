from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from collectors.common.storage import release_unused_memory
from collectors.deribit.config import DeribitConfig
from collectors.deribit.instruments import instrument_dimension_path
from collectors.deribit.parquet_parts import publish_existing_file_atomic


@dataclass(frozen=True)
class ContractsRepairResult:
    status: str
    phase: str
    dry_run: bool
    files_seen: int
    files_repaired: int
    rows_seen: int
    rows_repaired: int

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "dry_run": self.dry_run,
            "files_seen": self.files_seen,
            "files_repaired": self.files_repaired,
            "rows_seen": self.rows_seen,
            "rows_repaired": self.rows_repaired,
        }


class DeribitContractsRepair:
    """Backfill nullable trade contracts from amount_base / instrument contract_size."""

    def __init__(self, config: DeribitConfig):
        self.config = config

    def run(self, *, confirm: bool = False) -> dict[str, Any]:
        files = self._canonical_files()
        dimension_path = instrument_dimension_path(self.config)
        if not dimension_path.exists():
            return ContractsRepairResult("blocked", "Phase 6", not confirm, len(files), 0, 0, 0).as_payload()

        con = duckdb.connect(database=":memory:")
        try:
            files_seen = 0
            files_repaired = 0
            rows_seen = 0
            rows_repaired = 0
            for path in files:
                files_seen += 1
                stats = con.execute(
                    """
                    SELECT
                        COUNT(*) AS rows_seen,
                        SUM(CASE WHEN contracts IS NULL THEN 1 ELSE 0 END) AS rows_repaired
                    FROM read_parquet(?)
                    """,
                    [str(path)],
                ).fetchone()
                file_rows = int(stats[0] or 0)
                file_missing = int(stats[1] or 0)
                rows_seen += file_rows
                rows_repaired += file_missing
                if file_missing <= 0:
                    continue
                files_repaired += 1
                if not confirm:
                    continue
                tmp = path.with_name(path.name + ".contracts_repair.tmp")
                if tmp.exists():
                    tmp.unlink()
                con.execute(
                    f"""
                    COPY (
                        SELECT
                            t.timestamp_ms,
                            t.instrument_id,
                            t.trade_seq,
                            t.trade_id_hash,
                            t.price_btc,
                            t.mark_price_btc,
                            t.iv_pct,
                            t.index_price_usd,
                            t.amount_base,
                            COALESCE(t.contracts, t.amount_base / NULLIF(d.contract_size, 0))::FLOAT AS contracts,
                            t.direction,
                            t.tick_direction,
                            t.flags,
                            t.dataset_version_id
                        FROM read_parquet(?) t
                        LEFT JOIN read_parquet(?) d
                          ON t.instrument_id = d.instrument_id
                        ORDER BY t.timestamp_ms, t.instrument_id, t.trade_seq
                    )
                    TO {_sql_quote(str(tmp))} (FORMAT PARQUET, COMPRESSION ZSTD)
                    """,
                    [str(path), str(dimension_path)],
                )
                publish_existing_file_atomic(tmp, path)
                release_unused_memory()
        finally:
            con.close()
        status = "ok" if rows_repaired == 0 or confirm else "needs_repair"
        return ContractsRepairResult(status, "Phase 6", not confirm, files_seen, files_repaired, rows_seen, rows_repaired).as_payload()

    def _canonical_files(self) -> list[Path]:
        root = self.config.canonical_trades_root
        if not root.exists():
            return []
        return sorted(path for path in root.rglob("*.parquet") if not path.name.endswith(".tmp"))


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
