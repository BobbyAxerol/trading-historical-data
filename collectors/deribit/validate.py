from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from collectors.deribit.config import DeribitConfig, storage_reference
from collectors.deribit.instruments import instrument_dimension_path
from collectors.deribit.parquet_parts import file_checksum
from collectors.deribit.schema import CANONICAL_TRADE_COLUMNS


@dataclass
class ValidationReport:
    status: str = "ok"
    phase: str = "Phase 4"
    acquisition_errors: list[str] = field(default_factory=list)
    canonical_errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    canonical_files: int = 0
    canonical_rows: int = 0
    duplicate_keys: int = 0

    def as_payload(self) -> dict[str, Any]:
        errors = self.acquisition_errors + self.canonical_errors
        return {
            "status": "ok" if not errors else "blocked",
            "phase": self.phase,
            "acquisition_errors": self.acquisition_errors,
            "canonical_errors": self.canonical_errors,
            "warnings": self.warnings,
            "canonical_files": self.canonical_files,
            "canonical_rows": self.canonical_rows,
            "duplicate_keys": self.duplicate_keys,
        }


class DeribitValidator:
    def __init__(self, config: DeribitConfig):
        self.config = config

    def run(self) -> dict[str, Any]:
        report = ValidationReport()
        self._validate_acquisition(report)
        self._validate_canonical(report)
        return report.as_payload()

    def _validate_acquisition(self, report: ValidationReport) -> None:
        if not self.config.checkpoint_path.exists():
            report.acquisition_errors.append(f"checkpoint missing: {self.config.checkpoint_path}")
            return
        cleaned_files = self._cleanup_manifest_files()
        with sqlite3.connect(self.config.checkpoint_path) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                """
                SELECT
                    instrument_name,
                    requested_start_seq,
                    requested_end_seq,
                    response_trade_count,
                    retained_trade_count,
                    discarded_trade_count,
                    output_file,
                    output_checksum,
                    status
                FROM download_ranges
                ORDER BY id
                """
            ).fetchall()
        for row in rows:
            label = f"{row['instrument_name']}:{row['requested_start_seq']}-{row['requested_end_seq']}"
            if int(row["response_trade_count"]) != int(row["retained_trade_count"]) + int(row["discarded_trade_count"]):
                report.acquisition_errors.append(f"{label} response_count != retained + discarded")
            output_file = row["output_file"]
            if int(row["retained_trade_count"]) > 0:
                if not output_file:
                    report.acquisition_errors.append(f"{label} retained rows without output_file")
                    continue
                path = self.config.resolve_storage_reference(str(output_file))
                if not path.exists():
                    if self._is_cleaned_output(str(output_file), path, row["output_checksum"], cleaned_files):
                        continue
                    report.acquisition_errors.append(f"{label} output missing and not in cleanup manifest: {output_file}")
                    continue
                expected = row["output_checksum"]
                actual = file_checksum(path)
                if expected and actual != expected:
                    report.acquisition_errors.append(f"{label} checksum mismatch: {actual} != {expected}")
            elif output_file:
                report.warnings.append(f"{label} has output_file despite zero retained rows")

    def _validate_canonical(self, report: ValidationReport) -> None:
        files = self._canonical_files()
        report.canonical_files = len(files)
        if not files:
            report.warnings.append("canonical storage is empty")
            return
        for path in files:
            schema_names = pq.ParquetFile(path).schema_arrow.names
            if schema_names != CANONICAL_TRADE_COLUMNS:
                report.canonical_errors.append(f"{path} schema mismatch: {schema_names}")

        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"CREATE OR REPLACE TEMP VIEW canonical AS SELECT * FROM read_parquet({_sql_path_list(files)}, union_by_name=true)")
            report.canonical_rows = int(con.execute("SELECT COUNT(*) FROM canonical").fetchone()[0])
            report.duplicate_keys = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT instrument_id, trade_seq, COUNT(*) AS n
                        FROM canonical
                        GROUP BY instrument_id, trade_seq
                        HAVING n > 1
                    )
                    """
                ).fetchone()[0]
            )
            if report.duplicate_keys:
                report.canonical_errors.append(f"duplicate canonical keys: {report.duplicate_keys}")
            invalid_rows = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM canonical
                    WHERE timestamp_ms IS NULL
                       OR instrument_id IS NULL
                       OR trade_seq IS NULL
                       OR price_btc < 0
                       OR amount_base <= 0
                       OR (index_price_usd IS NOT NULL AND index_price_usd <= 0)
                       OR (iv_pct IS NOT NULL AND iv_pct <= 0)
                       OR dataset_version_id != 1
                    """
                ).fetchone()[0]
            )
            if invalid_rows:
                report.canonical_errors.append(f"invalid canonical rows: {invalid_rows}")
            dimension_path = instrument_dimension_path(self.config)
            if dimension_path.exists():
                missing_dimension_rows = int(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM canonical c
                        LEFT JOIN read_parquet(%s) d
                          ON c.instrument_id = d.instrument_id
                        WHERE d.instrument_id IS NULL
                        """
                        % _sql_quote(str(dimension_path))
                    ).fetchone()[0]
                )
                if missing_dimension_rows:
                    report.canonical_errors.append(f"canonical rows missing instrument dimension: {missing_dimension_rows}")
            else:
                report.canonical_errors.append(f"instrument dimension missing: {dimension_path}")
        finally:
            con.close()

    def _canonical_files(self) -> list[Path]:
        if not self.config.canonical_trades_root.exists():
            return []
        return sorted(path for path in self.config.canonical_trades_root.rglob("*.parquet") if not path.name.endswith(".tmp"))

    def _cleanup_manifest_files(self) -> dict[str, Any]:
        path = self.config.checkpoint_path.parent / "staging_cleanup_manifest.json"
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}
        if payload.get("status") != "ok":
            return {}
        files = payload.get("files")
        return dict(files) if isinstance(files, dict) else {}

    def _is_cleaned_output(self, output_file: str, resolved_path: Path, expected_checksum: Any, cleaned_files: dict[str, Any]) -> bool:
        keys = {
            str(output_file),
            str(resolved_path),
        }
        portable = storage_reference(output_file)
        if portable:
            keys.add(portable)
        resolved_portable = storage_reference(resolved_path)
        if resolved_portable:
            keys.add(resolved_portable)
        entry = next((cleaned_files.get(key) for key in keys if key in cleaned_files), None)
        if not isinstance(entry, dict):
            return False
        expected = str(expected_checksum or "")
        recorded = str(entry.get("checksum") or "")
        return bool(expected and recorded and expected == recorded)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(_sql_quote(str(path)) for path in paths) + "]"
